"""Étape 1 — Collecte multi-sources vers la zone RAW (aucune transformation métier).

| Source               | Type                  | Table RAW          | Incrémental (filigrane)                 |
|----------------------|-----------------------|--------------------|-----------------------------------------|
| Transactions         | SQL core_banking.db   | raw_transactions   | updated_at (insertions + mises à jour)  |
| Cas de fraude        | SQL core_banking.db   | raw_fraud_labels   | confirmed_at (étiquettes tardives)      |
| Sessions applicatives| Parquet (NOUVELLE)    | raw_sessions       | session_id (voir ci-dessous)            |
| Référentiel marchands| API REST OAuth2       | raw_merchants      | updated_since                           |

Sessions : le filigrane porte sur `session_id` (croissant) et non sur `login_at`, car une session peut être
exportée APRÈS une session plus récente qu'elle (elle précède de quelques minutes une transaction tardive) :
un filigrane sur `login_at` la perdrait définitivement.

Chaque source est isolée (l'échec de l'une n'empêche pas les autres), auditée, et le filigrane n'avance
qu'APRÈS un chargement réussi. Code de sortie 1 si une source a échoué (exploité par Airflow).

    python jour3/projet_integre/src/collect.py --mode full
    python jour3/projet_integre/src/collect.py --mode incremental --sources db,labels
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import Engine, create_engine, text

from api_client import NexaApiClient
from common import (audit, get_engine, get_watermark, load_params, new_run_id, set_watermark, setup_logging,
                    write_landing)
from fil_rouge import config

logger = logging.getLogger("projet.collecte")


def read_sql_incremental(source: Engine, table: str, column: str, watermark: str | None, chunksize: int) -> pd.DataFrame:
    """Requête PARAMÉTRÉE (jamais de f-string sur une valeur), lecture par morceaux."""
    where = f"WHERE {column} > :wm " if watermark else ""          # noms de table/colonne : constantes du code
    query = text(f"SELECT * FROM {table} {where}ORDER BY {column}")
    with source.connect() as conn:
        chunks = list(pd.read_sql_query(query, conn, params={"wm": watermark} if watermark else {}, chunksize=chunksize))
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def extract_transactions(source: Engine, wm: str | None, chunksize: int) -> pd.DataFrame:
    return read_sql_incremental(source, "transactions", "updated_at", wm, chunksize)


def extract_labels(source: Engine, wm: str | None, chunksize: int) -> pd.DataFrame:
    # Les étiquettes arrivent APRÈS la transaction : le filigrane porte sur la date de confirmation,
    # pas sur la date de la transaction, sinon une fraude confirmée tardivement serait perdue.
    return read_sql_incremental(source, "fraud_cases", "confirmed_at", wm, chunksize)


def extract_sessions(wm: str | None, columns: list[str]) -> pd.DataFrame:
    path = config.SOURCES_DIR / "app_sessions.parquet"
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_parquet(path, columns=columns)                     # lecture sélective (minimisation)
    if wm:
        df = df[df["session_id"] > wm]
    # Horodatage normalisé en texte UTC, même format que la base core banking
    df["login_at"] = pd.to_datetime(df["login_at"], utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
    return df.sort_values("session_id").reset_index(drop=True)


def extract_merchants(updated_since: str | None) -> tuple[pd.DataFrame, dict]:
    client = NexaApiClient(config.API_BASE_URL, config.API_CLIENT_ID, config.API_CLIENT_SECRET)
    params = {"updated_since": updated_since} if updated_since else {}
    records = [rec for page in client.iter_pages("/v1/merchants", params) for rec in page]
    return pd.DataFrame.from_records(records), client.stats


def load_raw(df: pd.DataFrame, engine: Engine, table: str, source: str, run_id: str, replace: bool) -> int:
    if df.empty:
        logger.info("%s : rien de nouveau", table)
        return 0
    out = df.copy()
    for col in out.columns:   # objets imbriqués (API) -> JSON texte
        if out[col].map(lambda v: isinstance(v, (dict, list))).any():
            out[col] = out[col].map(lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v)
    out["_source"] = source
    out["_batch_id"] = run_id
    out["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    out.to_sql(table, engine, if_exists="replace" if replace else "append", index=False, chunksize=5_000)
    logger.info("%s : %d lignes chargées (%s)", table, len(out), "remplacement" if replace else "ajout")
    return len(out)


def run(mode: str, sources: list[str]) -> int:
    run_id = new_run_id()
    setup_logging("collecte", run_id)
    config.ensure_dirs()
    params = load_params("collecte")
    engine = get_engine()
    full = mode == "full"
    core = create_engine(config.SOURCE_DB_URL)
    failures = []

    def wm(name: str) -> str | None:
        return None if full else get_watermark(engine, name)

    def do_db():
        w = wm("transactions")
        df = extract_transactions(core, w, params["chunksize"])
        write_landing(df, "transactions", run_id)
        n = load_raw(df, engine, "raw_transactions", "db:core_banking.transactions", run_id, replace=full)
        if not df.empty:
            set_watermark(engine, "transactions", str(df["updated_at"].max()))   # APRÈS le chargement
        return n, f"filigrane_entree={w}"

    def do_labels():
        w = wm("labels")
        df = extract_labels(core, w, params["chunksize"])
        write_landing(df, "fraud_labels", run_id)
        n = load_raw(df, engine, "raw_fraud_labels", "db:core_banking.fraud_cases", run_id, replace=full)
        if not df.empty:
            set_watermark(engine, "labels", str(df["confirmed_at"].max()))
        return n, f"filigrane_entree={w}"

    def do_sessions():
        w = wm("sessions")
        df = extract_sessions(w, params["colonnes_sessions"])
        write_landing(df, "sessions", run_id)
        n = load_raw(df, engine, "raw_sessions", "file:app_sessions.parquet", run_id, replace=full)
        if not df.empty:
            set_watermark(engine, "sessions", str(df["session_id"].max()))
        return n, f"filigrane_entree={w}"

    def do_api():
        w = wm("merchants")
        df, stats = extract_merchants(w)
        write_landing(df, "merchants", run_id)
        n = load_raw(df, engine, "raw_merchants", "api:/v1/merchants", run_id, replace=full)
        if not df.empty:
            set_watermark(engine, "merchants", str(df["updated_at"].max()))
        return n, f"stats={stats}"

    registry = {"db": ("transactions", do_db), "labels": ("fraud_labels", do_labels),
                "sessions": ("sessions", do_sessions), "api": ("merchants", do_api)}
    for key in sources:
        name, fn = registry[key]
        started = datetime.now(timezone.utc)
        try:
            rows, msg = fn()
            audit(engine, run_id, f"collecte_{name}", mode, "SUCCESS", rows, started, msg)
        except Exception as exc:  # noqa: BLE001 — chaque source est isolée
            logger.exception("Échec de la source %s", name)
            audit(engine, run_id, f"collecte_{name}", mode, "FAILED", 0, started, str(exc))
            failures.append(name)
    logger.info("Bilan collecte : %s", "OK" if not failures else f"ÉCHEC {failures}")
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Collecte multi-sources — projet fraude comportementale")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--sources", default="db,labels,sessions,api")
    args = parser.parse_args()
    return run(args.mode, [s.strip() for s in args.sources.split(",") if s.strip()])


if __name__ == "__main__":
    sys.exit(main())
