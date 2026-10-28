"""TP1 — SQUELETTE — Pipeline de collecte multi-sources robuste.

Complétez les blocs marqués `# TODO`. L'infrastructure (logs, audit, orchestration,
chargement) est fournie : concentrez-vous sur la ROBUSTESSE de la collecte.

Testez au fil de l'eau :
    python jour1/TP1_collecte/squelette/collect_pipeline.py --mode full --sources json
    python jour1/TP1_collecte/squelette/collect_pipeline.py --mode full --sources csv,json
    python jour1/TP1_collecte/squelette/collect_pipeline.py --mode full            # tout
    python jour1/TP1_collecte/squelette/collect_pipeline.py --mode incremental     # après --append
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
# 0. Journalisation (FOURNI)
# ======================================================================================
def setup_logging(run_id: str, level: int = logging.INFO) -> Path:
    config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = config.LOGS_DIR / f"collecte_{datetime.now():%Y%m%d}.log"
    fmt = logging.Formatter(f"%(asctime)s | %(levelname)-7s | run={run_id} | %(name)s | %(message)s")
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_file, encoding="utf-8")):
        handler.setFormatter(fmt)
        root.addHandler(handler)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    return log_file


# ======================================================================================
# 1. Source API REST — client robuste (À COMPLÉTER)
# ======================================================================================
class ApiError(RuntimeError):
    pass


class NexaApiClient:
    RETRYABLE = {500, 502, 503, 504}

    def __init__(self, base_url: str, client_id: str, client_secret: str,
                 timeout: tuple[float, float] = (3.05, 15.0), max_retries: int = 6, backoff_base: float = 0.5,
                 max_wait_s: float = 300.0):
        if not client_id or not client_secret:
            raise ValueError("Identifiants API absents : renseignez .env")
        self.base_url = base_url.rstrip("/")
        self._client_id, self._client_secret = client_id, client_secret
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.max_wait_s = max_wait_s      # garde-fou global : durée maximale d'un appel get(), attentes comprises
        self.session = requests.Session()
        self._token: str | None = None
        self._token_exp: float = 0.0
        self.stats = {"requests": 0, "retries": 0, "rate_limited": 0, "token_refresh": 0}

    def _fetch_token(self) -> None:
        # TODO 1 — Obtenir un jeton OAuth2 (flux client_credentials)
        #   * POST {base_url}/oauth/token, corps encodé en formulaire (data=...) :
        #       grant_type=client_credentials, client_id, client_secret
        #   * Lever ApiError si le statut n'est pas 200 (sans jamais logger le secret !)
        #   * Stocker self._token et self._token_exp (prévoir une marge de sécurité)
        #   * Incrémenter self.stats["token_refresh"]
        resp = self.session.post(
            f"{self.base_url}/oauth/token",
            data={"grant_type": "client_credentials",
                  "client_id": self._client_id, "client_secret": self._client_secret},
            timeout=self.timeout,
        )
        if resp.status_code != 200:
            # On ne logge que le statut : ni le secret, ni le corps de la requête
            raise ApiError(f"Échec d'authentification OAuth2 (HTTP {resp.status_code})")
        payload = resp.json()
        self._token = payload["access_token"]
        # Marge de 10 % (au moins 5 s) pour ne jamais envoyer un jeton qui expire en vol
        ttl = float(payload.get("expires_in", 60))
        self._token_exp = time.time() + ttl - max(5.0, ttl * 0.1)
        self.stats["token_refresh"] += 1
        logger.info("Jeton OAuth2 obtenu (valide %.0f s)", ttl)

    def _backoff(self, attempt: int) -> float:
        return min(30, self.backoff_base * 2 ** attempt) + random.uniform(0, 0.5)

    def _auth_header(self) -> dict[str, str]:
        if self._token is None or time.time() >= self._token_exp:
            self._fetch_token()
        return {"Authorization": f"Bearer {self._token}"}

    def get(self, path: str, params: dict | None = None) -> dict:
        # TODO 2 — GET robuste. Pour chaque tentative (au plus max_retries + 1) :
        #   * Toujours passer timeout=self.timeout
        #   * ConnectionError / Timeout          -> backoff exponentiel + jitter, puis réessayer
        #   * 200                                 -> retourner resp.json()
        #   * 401 (une seule fois)                -> invalider le jeton et réessayer
        #   * 429                                 -> attendre Retry-After secondes, réessayer
        #   * 500/502/503/504                     -> backoff exponentiel + jitter, réessayer
        #   * autre 4xx                           -> ApiError immédiate (inutile de réessayer)
        # Indice backoff : délai = min(30, backoff_base * 2**tentative) + random.uniform(0, 0.5)
        url = f"{self.base_url}{path}"
        attempt = 0              # tentatives "consommées" (erreurs réseau / 5xx)
        reauth_done = False
        deadline = time.monotonic() + self.max_wait_s
        while attempt <= self.max_retries:
            # Les 429 ne consomment pas de tentative : sans ce garde-fou, un 429 permanent bouclerait à l'infini
            if time.monotonic() >= deadline:
                raise ApiError(f"GET {path} : durée maximale de {self.max_wait_s:.0f} s dépassée")
            self.stats["requests"] += 1
            try:
                resp = self.session.get(url, params=params, headers=self._auth_header(), timeout=self.timeout)
            except (requests.ConnectionError, requests.Timeout) as exc:
                if attempt == self.max_retries:
                    raise ApiError(f"GET {path} : réseau indisponible après {attempt + 1} tentatives") from exc
                delay = self._backoff(attempt)
                logger.warning("GET %s : %s, nouvel essai dans %.1f s", path, type(exc).__name__, delay)
                self.stats["retries"] += 1
                attempt += 1
                time.sleep(delay)
                continue

            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 401 and not reauth_done:
                logger.warning("GET %s : 401, renouvellement du jeton", path)
                self._token = None
                reauth_done = True
                continue
            if resp.status_code == 429:
                # Le serveur nous dit quand revenir : on attend, sans consommer de tentative
                wait = float(resp.headers.get("Retry-After", 1))
                self.stats["rate_limited"] += 1
                if time.monotonic() + wait > deadline:
                    raise ApiError(f"GET {path} : quota (429) toujours atteint après {self.max_wait_s:.0f} s")
                logger.warning("GET %s : 429 (quota), attente de %.0f s", path, wait)
                time.sleep(wait)
                continue
            if resp.status_code in self.RETRYABLE:
                if attempt == self.max_retries:
                    break
                delay = self._backoff(attempt)
                logger.warning("GET %s : %d, nouvel essai dans %.1f s", path, resp.status_code, delay)
                self.stats["retries"] += 1
                attempt += 1
                time.sleep(delay)
                continue
            raise ApiError(f"GET {path} : HTTP {resp.status_code} non récupérable")
        raise ApiError(f"GET {path} : échec après {self.max_retries + 1} tentatives")

    def iter_pages(self, path: str, params: dict | None = None, page_size: int = 100) -> Iterator[list[dict]]:
        # TODO 3 — Générateur de pages : appeler self.get(...) avec page/page_size,
        #          `yield` la liste "data", puis suivre pagination.next_page jusqu'à None.
        page: int | None = 1
        while page is not None:
            body = self.get(path, {**(params or {}), "page": page, "page_size": page_size})
            yield body.get("data", [])
            page = body.get("pagination", {}).get("next_page")


def extract_merchants(client: NexaApiClient, updated_since: str | None = None) -> pd.DataFrame:
    params = {"updated_since": updated_since} if updated_since else {}
    records = [rec for page in client.iter_pages("/v1/merchants", params) for rec in page]
    logger.info("API marchands : %d enregistrements | stats : %s", len(records), client.stats)
    return pd.DataFrame.from_records(records)


# ======================================================================================
# 2. Source base de données (À COMPLÉTER)
# ======================================================================================
def extract_transactions(source: Engine, watermark: str | None, chunksize: int = 20_000) -> pd.DataFrame:
    # TODO 4 — Extraction full / incrémentale
    #   * watermark None  -> toute la table `transactions`
    #   * sinon           -> uniquement updated_at > watermark
    #   * Requête PARAMÉTRÉE avec sqlalchemy.text(...) et un paramètre nommé (:wm)
    #     -> interdiction de construire le SQL par f-string
    #   * Lecture par morceaux : pd.read_sql_query(..., chunksize=chunksize), puis concat
    #   * Trier par updated_at
    if watermark is None:
        query = text("SELECT * FROM transactions ORDER BY updated_at")
        params: dict = {}
    else:
        query = text("SELECT * FROM transactions WHERE updated_at > :wm ORDER BY updated_at")
        params = {"wm": watermark}
    with source.connect() as conn:
        chunks = list(pd.read_sql_query(query, conn, params=params, chunksize=chunksize))
    df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    if not df.empty:
        df = df.sort_values("updated_at", kind="stable").reset_index(drop=True)
    logger.info("DB transactions : %d lignes (watermark=%s)", len(df), watermark)
    return df


# ======================================================================================
# 3. Sources fichiers (À COMPLÉTER)
# ======================================================================================
def extract_customers_csv(path: Path) -> pd.DataFrame:
    # Vérifier que le fichier existe
    if not path.exists():
        raise FileNotFoundError(f"Fichier CRM introuvable : {path}")

    # Lire le CSV en conservant toutes les valeurs sous forme de texte
    # keep_default_na=False : "NA", "null", "None"... restent du texte ; seule une case vide est manquante
    df = pd.read_csv(
        path,
        sep=";",
        encoding="utf-8",
        dtype=str,
        keep_default_na=False,
        na_values=[""],
        on_bad_lines="warn",
    )

    return df


def extract_blacklist_json(path: Path) -> pd.DataFrame:
    # TODO 6 — Lire le JSON imbriqué et l'aplatir (pd.json_normalize, sep="_")
    #   * Conserver les métadonnées du document (generated_at, schema_version) en colonnes
    if not path.exists():
        raise FileNotFoundError(f"Liste noire introuvable : {path}")
    doc = json.loads(path.read_text(encoding="utf-8"))
    # Chaque entrée de "entries" devient une ligne ; merchant.id -> merchant_id, merchant.name -> merchant_name
    df = pd.json_normalize(doc["entries"], sep="_")
    # Métadonnées du document répétées sur chaque ligne (traçabilité de la version de la liste)
    df["list_generated_at"] = doc.get("generated_at")
    df["list_schema_version"] = doc.get("schema_version")
    return df


# ======================================================================================
# 4. Chargement, filigranes, audit (FOURNI)
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
    for col in out.columns:
        if out[col].map(lambda v: isinstance(v, (dict, list))).any():
            out[col] = out[col].map(lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v)
    out.to_sql(table, engine, if_exists="replace" if replace else "append", index=False, chunksize=5_000)
    logger.info("%s : %d lignes chargées", table, len(out))
    return len(out)


def audit(engine: Engine, run_id: str, source: str, mode: str, status: str, rows: int,
          started: datetime, message: str = "") -> None:
    ended = datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO audit_log VALUES (:r,:s,:m,:st,:n,:a,:b,:d,:msg)"), {
            "r": run_id, "s": source, "m": mode, "st": status, "n": rows, "a": started.isoformat(),
            "b": ended.isoformat(), "d": round((ended - started).total_seconds(), 2), "msg": message[:500]})


# ======================================================================================
# 5. Orchestration locale
# ======================================================================================
@dataclass
class SourceResult:
    source: str
    status: str
    rows: int
    message: str = ""


def run(mode: str, sources: list[str]) -> list[SourceResult]:
    run_id = datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    setup_logging(run_id)
    config.ensure_dirs()
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
        except Exception as exc:  # noqa: BLE001
            logger.exception("Échec de la source %s", name)
            results.append(SourceResult(name, "FAILED", 0, str(exc)))
            audit(warehouse, run_id, name, mode, "FAILED", 0, started, str(exc))

    def do_api():
        client = NexaApiClient(config.API_BASE_URL, config.API_CLIENT_ID, config.API_CLIENT_SECRET)
        since = get_watermark(warehouse, "api_merchants") if incremental else None
        df = extract_merchants(client, since)
        write_landing(df, "api_merchants", run_id)
        n = load_raw(df, warehouse, "raw_merchants", "api:/v1/merchants", run_id, replace=not incremental)
        if not df.empty:
            set_watermark(warehouse, "api_merchants", df["updated_at"].max())
        return n, f"stats={client.stats}"

    def do_db():
        source = create_engine(config.SOURCE_DB_URL)
        wm = get_watermark(warehouse, "db_transactions") if incremental else None
        df = extract_transactions(source, wm)
        write_landing(df, "db_transactions", run_id)
        n = load_raw(df, warehouse, "raw_transactions", "db:core_banking.transactions", run_id, replace=not incremental)
        # TODO 7 — Mettre à jour le filigrane "db_transactions" avec le max(updated_at) extrait.
        #          Question : pourquoi APRÈS load_raw et pas avant ? Que se passe-t-il sinon en cas de panne ?
        if not df.empty:
            set_watermark(warehouse, "db_transactions", str(df["updated_at"].max()))
        return n, f"watermark_in={wm}"

    def do_csv():
        df = extract_customers_csv(config.SOURCES_DIR / "customers_crm.csv")
        write_landing(df, "csv_customers", run_id)
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
    parser.add_argument("--sources", default="api,db,csv,json")
    args = parser.parse_args()
    results = run(args.mode, [s.strip() for s in args.sources.split(",") if s.strip()])
    # TODO 8 — Retourner un code de sortie NON NUL si au moins une source a échoué.
    #          Pourquoi est-ce indispensable pour l'orchestration Airflow de demain ?
    return 1 if any(r.status != "SUCCESS" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
