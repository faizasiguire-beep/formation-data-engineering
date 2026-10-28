"""TP1 — CORRIGÉ — Pipeline de collecte multi-sources robuste.

Sources collectées (zone RAW de l'entrepôt, sans transformation métier) :
    1. API REST  : référentiel marchands (OAuth2, pagination, 429, 503)  -> raw_merchants
    2. Base SQL  : transactions du core banking (full / incrémental)     -> raw_transactions
    3. Fichier CSV  : export CRM clients                                 -> raw_customers
    4. Fichier JSON : liste noire marchands (imbriqué)                   -> raw_merchant_blacklist

Principes appliqués :
    * Aucun secret en dur : tout vient de l'environnement (.env)
    * Chaque source est isolée : l'échec de l'une n'empêche pas les autres
    * Traçabilité : run_id, colonnes techniques (_source, _batch_id, _ingested_at),
      copie brute en zone "landing" (Parquet), journal d'audit en base
    * Idempotence : full load = remplacement ; incrémental = filigrane (watermark)
      mis à jour uniquement APRÈS un chargement réussi
    * Code de sortie non nul en cas d'échec -> exploitable par Airflow (Jour 2)

Usage :
    python jour1/TP1_collecte/corrige/collect_pipeline.py --mode full
    python jour1/TP1_collecte/corrige/collect_pipeline.py --mode incremental --sources db,api
"""
from __future__ import annotations

import argparse
import json
import logging
import random
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import pandas as pd
import requests
from sqlalchemy import Engine, create_engine, text

from fil_rouge import config

logger = logging.getLogger("collecte")


# ======================================================================================
# 0. Journalisation
# ======================================================================================
def setup_logging(run_id: str, level: int = logging.INFO) -> Path:
    """Logs console + fichier, avec le run_id sur chaque ligne (corrélation des événements)."""
    config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = config.LOGS_DIR / f"collecte_{datetime.now():%Y%m%d}.log"
    fmt = logging.Formatter(f"%(asctime)s | %(levelname)-7s | run={run_id} | %(name)s | %(message)s")
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_file, encoding="utf-8")):
        handler.setFormatter(fmt)
        root.addHandler(handler)
    # Le bavardage des bibliothèques HTTP est inutile au niveau INFO
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    return log_file


# ======================================================================================
# 1. Source API REST — client robuste
# ======================================================================================
class ApiError(RuntimeError):
    """Erreur définitive de l'API (après épuisement des tentatives ou erreur non récupérable)."""


class NexaApiClient:
    """Client OAuth2 (client_credentials) avec retry, backoff exponentiel + jitter et gestion du 429."""

    RETRYABLE = {500, 502, 503, 504}

    def __init__(self, base_url: str, client_id: str, client_secret: str,
                 timeout: tuple[float, float] = (3.05, 15.0), max_retries: int = 6, backoff_base: float = 0.5):
        if not client_id or not client_secret:
            raise ValueError("Identifiants API absents : renseignez NEXA_API_CLIENT_ID / NEXA_API_CLIENT_SECRET dans .env")
        self.base_url = base_url.rstrip("/")
        self._client_id, self._client_secret = client_id, client_secret
        self.timeout = timeout                     # (connexion, lecture) : ne JAMAIS appeler sans timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.session = requests.Session()          # réutilise les connexions TCP (keep-alive)
        self.session.headers["User-Agent"] = "nexa-de-pipeline/1.0"
        self._token: str | None = None
        self._token_exp: float = 0.0
        self.stats = {"requests": 0, "retries": 0, "rate_limited": 0, "token_refresh": 0}

    # --- Authentification ---------------------------------------------------------------
    def _fetch_token(self) -> None:
        resp = self.session.post(
            f"{self.base_url}/oauth/token",
            data={"grant_type": "client_credentials", "client_id": self._client_id, "client_secret": self._client_secret},
            timeout=self.timeout,
        )
        if resp.status_code != 200:
            # On ne logge JAMAIS le secret, seulement le statut
            raise ApiError(f"Échec d'authentification OAuth2 (HTTP {resp.status_code})")
        payload = resp.json()
        self._token = payload["access_token"]
        # Marge de sécurité de 10 s pour éviter d'utiliser un jeton qui expire en vol
        self._token_exp = time.time() + payload.get("expires_in", 60) - 10
        self.stats["token_refresh"] += 1
        logger.info("Jeton OAuth2 obtenu (expire dans %ss)", payload.get("expires_in"))

    def _auth_header(self) -> dict[str, str]:
        if self._token is None or time.time() >= self._token_exp:
            self._fetch_token()
        return {"Authorization": f"Bearer {self._token}"}

    # --- Requête avec retry -----------------------------------------------------------
    def _sleep_backoff(self, attempt: int) -> float:
        # Backoff exponentiel plafonné + jitter (évite que tous les clients réessaient en même temps)
        delay = min(30.0, self.backoff_base * 2 ** attempt) + random.uniform(0, 0.5)
        time.sleep(delay)
        return delay

    def get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self.base_url}{path}"
        refreshed = False
        for attempt in range(self.max_retries + 1):
            self.stats["requests"] += 1
            try:
                resp = self.session.get(url, params=params, headers=self._auth_header(), timeout=self.timeout)
            except (requests.ConnectionError, requests.Timeout) as exc:
                if attempt == self.max_retries:
                    raise ApiError(f"API injoignable après {attempt + 1} tentatives : {exc}") from exc
                self.stats["retries"] += 1
                d = self._sleep_backoff(attempt)
                logger.warning("Erreur réseau (%s) — nouvelle tentative dans %.1fs", type(exc).__name__, d)
                continue

            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 401 and not refreshed:
                logger.warning("401 reçu : jeton expiré ou révoqué, renouvellement")
                self._token, refreshed = None, True
                continue
            if resp.status_code == 429:
                self.stats["rate_limited"] += 1
                wait = float(resp.headers.get("Retry-After", 1))
                logger.info("429 Too Many Requests — pause de %.1fs (Retry-After)", wait)
                time.sleep(wait)
                continue  # un 429 n'est pas une panne : on ne consomme pas le budget de tentatives
            if resp.status_code in self.RETRYABLE and attempt < self.max_retries:
                self.stats["retries"] += 1
                d = self._sleep_backoff(attempt)
                logger.warning("HTTP %s sur %s — tentative %d/%d dans %.1fs",
                               resp.status_code, path, attempt + 1, self.max_retries, d)
                continue
            # 4xx (hors 401/429) = erreur du client : réessayer ne servirait à rien
            raise ApiError(f"HTTP {resp.status_code} sur {path} : {resp.text[:200]}")
        raise ApiError(f"Nombre maximal de tentatives atteint sur {path}")

    def iter_pages(self, path: str, params: dict | None = None, page_size: int = 100) -> Iterator[list[dict]]:
        """Générateur : une page à la fois (mémoire constante, quel que soit le volume)."""
        page = 1
        while page is not None:
            payload = self.get(path, {**(params or {}), "page": page, "page_size": page_size})
            meta = payload["pagination"]
            logger.debug("Page %s/%s reçue (%s éléments)", meta["page"], meta["total_pages"], len(payload["data"]))
            yield payload["data"]
            page = meta["next_page"]


def extract_merchants(client: NexaApiClient, updated_since: str | None = None) -> pd.DataFrame:
    params = {"updated_since": updated_since} if updated_since else {}
    records = [rec for page in client.iter_pages("/v1/merchants", params) for rec in page]
    logger.info("API marchands : %d enregistrements | stats client : %s", len(records), client.stats)
    return pd.DataFrame.from_records(records)


# ======================================================================================
# 2. Source base de données — full load vs incrémental
# ======================================================================================
def extract_transactions(source: Engine, watermark: str | None, chunksize: int = 20_000) -> pd.DataFrame:
    """Extraction par curseur `updated_at` (capte les insertions ET les mises à jour).

    La requête est PARAMÉTRÉE (:wm) : jamais de f-string dans du SQL (injection SQL).
    La lecture par morceaux (chunksize) borne la mémoire consommée.
    """
    if watermark is None:
        query, params = text("SELECT * FROM transactions ORDER BY updated_at"), {}
        logger.info("Transactions : FULL LOAD")
    else:
        query = text("SELECT * FROM transactions WHERE updated_at > :wm ORDER BY updated_at")
        params = {"wm": watermark}
        logger.info("Transactions : INCRÉMENTAL depuis %s", watermark)
    with source.connect() as conn:
        chunks = list(pd.read_sql_query(query, conn, params=params, chunksize=chunksize))
    df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    logger.info("Transactions extraites : %d lignes en %d morceau(x)", len(df), len(chunks))
    return df


# ======================================================================================
# 3. Sources fichiers
# ======================================================================================
def extract_customers_csv(path: Path) -> pd.DataFrame:
    """En zone RAW on lit tout en texte : aucune interprétation (dates, nombres) à ce stade."""
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_csv(path, sep=";", dtype=str, encoding="utf-8", keep_default_na=False,
                     na_values=[""], on_bad_lines="warn")
    logger.info("CSV clients : %d lignes, %d colonnes", len(df), df.shape[1])
    return df


def extract_blacklist_json(path: Path) -> pd.DataFrame:
    doc = json.loads(path.read_text(encoding="utf-8"))
    # Aplatissement de la structure imbriquée : merchant.id -> merchant_id
    df = pd.json_normalize(doc["entries"], sep="_")
    df["list_generated_at"] = doc.get("generated_at")
    df["list_schema_version"] = doc.get("schema_version")
    logger.info("JSON liste noire : %d entrées (schéma v%s)", len(df), doc.get("schema_version"))
    return df


# ======================================================================================
# 4. Chargement, filigranes, audit
# ======================================================================================
DDL = [
    """CREATE TABLE IF NOT EXISTS etl_watermarks (
           source TEXT PRIMARY KEY, last_value TEXT NOT NULL, updated_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS audit_log (
           run_id TEXT, source TEXT, mode TEXT, status TEXT, rows_loaded INTEGER,
           started_at TEXT, ended_at TEXT, duration_s REAL, message TEXT)""",
]


def init_warehouse(engine: Engine) -> None:
    with engine.begin() as conn:
        for ddl in DDL:
            conn.execute(text(ddl))


def get_watermark(engine: Engine, source: str) -> str | None:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT last_value FROM etl_watermarks WHERE source = :s"), {"s": source}).fetchone()
    return row[0] if row else None


def set_watermark(engine: Engine, source: str, value: str) -> None:
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM etl_watermarks WHERE source = :s"), {"s": source})
        conn.execute(text("INSERT INTO etl_watermarks VALUES (:s, :v, :u)"),
                     {"s": source, "v": value, "u": datetime.now(timezone.utc).isoformat()})


def write_landing(df: pd.DataFrame, source: str, batch_id: str) -> Path:
    """Copie brute horodatée : permet de rejouer un chargement sans re-solliciter la source."""
    target = config.LANDING_DIR / source / f"dt={datetime.now():%Y-%m-%d}"
    target.mkdir(parents=True, exist_ok=True)
    path = target / f"{batch_id}.parquet"
    df.to_parquet(path, index=False)
    return path


def load_raw(df: pd.DataFrame, engine: Engine, table: str, source: str, batch_id: str, replace: bool) -> int:
    if df.empty:
        logger.info("%s : rien à charger", table)
        return 0
    out = df.copy()
    out["_source"] = source
    out["_batch_id"] = batch_id
    out["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    # Colonnes non scalaires (dict/list) -> JSON texte, pour rester compatible SQL
    for col in out.columns:
        if out[col].map(lambda v: isinstance(v, (dict, list))).any():
            out[col] = out[col].map(lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v)
    out.to_sql(table, engine, if_exists="replace" if replace else "append", index=False, chunksize=5_000)
    logger.info("%s : %d lignes chargées (%s)", table, len(out), "remplacement" if replace else "ajout")
    return len(out)


def audit(engine: Engine, run_id: str, source: str, mode: str, status: str, rows: int,
          started: datetime, message: str = "") -> None:
    ended = datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO audit_log VALUES (:r,:s,:m,:st,:n,:a,:b,:d,:msg)"), {
            "r": run_id, "s": source, "m": mode, "st": status, "n": rows, "a": started.isoformat(),
            "b": ended.isoformat(), "d": round((ended - started).total_seconds(), 2), "msg": message[:500]})


# ======================================================================================
# 5. Orchestration locale du pipeline
# ======================================================================================
@dataclass
class SourceResult:
    source: str
    status: str
    rows: int
    message: str = ""


def run(mode: str, sources: list[str]) -> list[SourceResult]:
    run_id = datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    log_file = setup_logging(run_id)
    config.ensure_dirs()
    logger.info("Démarrage collecte | mode=%s | sources=%s | logs=%s", mode, sources, log_file)

    warehouse = create_engine(config.WAREHOUSE_DB_URL)
    init_warehouse(warehouse)
    incremental = mode == "incremental"
    results: list[SourceResult] = []

    def step(name: str, fn) -> None:
        started = datetime.now(timezone.utc)
        try:
            rows, msg = fn()
            results.append(SourceResult(name, "SUCCESS", rows, msg))
            audit(warehouse, run_id, name, mode, "SUCCESS", rows, started, msg)
        except Exception as exc:  # noqa: BLE001 — on isole volontairement chaque source
            logger.exception("Échec de la source %s", name)
            results.append(SourceResult(name, "FAILED", 0, str(exc)))
            audit(warehouse, run_id, name, mode, "FAILED", 0, started, str(exc))

    # --- API -----------------------------------------------------------------
    def do_api():
        client = NexaApiClient(config.API_BASE_URL, config.API_CLIENT_ID, config.API_CLIENT_SECRET)
        since = get_watermark(warehouse, "api_merchants") if incremental else None
        df = extract_merchants(client, since)
        write_landing(df, "api_merchants", run_id)
        n = load_raw(df, warehouse, "raw_merchants", "api:/v1/merchants", run_id, replace=not incremental)
        if not df.empty:
            set_watermark(warehouse, "api_merchants", df["updated_at"].max())
        return n, f"stats={client.stats}"

    # --- Base de données -------------------------------------------------------
    def do_db():
        source = create_engine(config.SOURCE_DB_URL)
        wm = get_watermark(warehouse, "db_transactions") if incremental else None
        df = extract_transactions(source, wm)
        write_landing(df, "db_transactions", run_id)
        n = load_raw(df, warehouse, "raw_transactions", "db:core_banking.transactions", run_id, replace=not incremental)
        if not df.empty:
            set_watermark(warehouse, "db_transactions", df["updated_at"].max())  # APRÈS le chargement
        return n, f"watermark_in={wm}"

    # --- Fichiers -----------------------------------------------------------------
    def do_csv():
        df = extract_customers_csv(config.SOURCES_DIR / "customers_crm.csv")
        write_landing(df, "csv_customers", run_id)
        # Un export fichier est un instantané complet : on remplace toujours
        return load_raw(df, warehouse, "raw_customers", "file:customers_crm.csv", run_id, replace=True), ""

    def do_json():
        df = extract_blacklist_json(config.SOURCES_DIR / "merchant_blacklist.json")
        write_landing(df, "json_blacklist", run_id)
        return load_raw(df, warehouse, "raw_merchant_blacklist", "file:merchant_blacklist.json", run_id, replace=True), ""

    registry = {"api": ("api_merchants", do_api), "db": ("db_transactions", do_db),
                "csv": ("csv_customers", do_csv), "json": ("json_blacklist", do_json)}
    for key in sources:
        name, fn = registry[key]
        step(name, fn)

    logger.info("Bilan : %s", " | ".join(f"{r.source}={r.status}({r.rows})" for r in results))
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Collecte multi-sources NexaPay (TP1)")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--sources", default="api,db,csv,json", help="liste séparée par des virgules parmi api,db,csv,json")
    args = parser.parse_args()
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    results = run(args.mode, sources)
    return 1 if any(r.status == "FAILED" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
