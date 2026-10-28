"""Socle commun du projet : chemins, paramètres, base cible, journalisation, filigranes, audit.

Repris et adapté du TP1 (filigranes, audit_log, zone landing) : la réutilisation du code des TP est
encouragée par le brief.

Base cible : `FRAUDE_DB_URL` (SQLite par défaut, PostgreSQL possible sans changer le code), distincte de
l'entrepôt des TP pour que le projet soit rejouable indépendamment.
"""
from __future__ import annotations

import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml
from sqlalchemy import Engine, create_engine, text

from fil_rouge import config

PROJECT_DIR = Path(__file__).resolve().parent.parent          # jour3/projet_integre
REPORTS_DIR = config.REPORTS_DIR / "projet_fraude"
LANDING_DIR = config.LANDING_DIR / "projet_fraude"
DB_URL = os.getenv("FRAUDE_DB_URL", f"sqlite:///{(config.DATA_DIR / 'projet_fraude.db').as_posix()}")

logger = logging.getLogger("projet")


def load_params(section: str) -> dict:
    return yaml.safe_load((PROJECT_DIR / "params.yaml").read_text(encoding="utf-8"))[section]


def new_run_id() -> str:
    return datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]


def setup_logging(name: str, run_id: str) -> Path:
    config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = config.LOGS_DIR / f"projet_fraude_{datetime.now():%Y%m%d}.log"
    fmt = logging.Formatter(f"%(asctime)s | %(levelname)-7s | run={run_id} | {name} | %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_file, encoding="utf-8")):
        handler.setFormatter(fmt)
        root.addHandler(handler)
    for noisy in ("urllib3", "great_expectations"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return log_file


def get_engine() -> Engine:
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE IF NOT EXISTS etl_watermarks (
                                 source VARCHAR(40) PRIMARY KEY, last_value VARCHAR(40) NOT NULL,
                                 updated_at VARCHAR(40) NOT NULL)"""))
        conn.execute(text("""CREATE TABLE IF NOT EXISTS audit_log (
                                 run_id VARCHAR(40), etape VARCHAR(40), mode VARCHAR(20), status VARCHAR(10),
                                 rows_loaded INTEGER, started_at VARCHAR(40), ended_at VARCHAR(40),
                                 duration_s REAL, message VARCHAR(500))"""))
    return engine


def get_watermark(engine: Engine, source: str) -> str | None:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT last_value FROM etl_watermarks WHERE source = :s"), {"s": source}).fetchone()
    return row[0] if row else None


def set_watermark(engine: Engine, source: str, value: str) -> None:
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM etl_watermarks WHERE source = :s"), {"s": source})
        conn.execute(text("INSERT INTO etl_watermarks VALUES (:s, :v, :u)"),
                     {"s": source, "v": value, "u": datetime.now(timezone.utc).isoformat()})


def audit(engine: Engine, run_id: str, etape: str, mode: str, status: str, rows: int,
          started: datetime, message: str = "") -> None:
    ended = datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO audit_log VALUES (:r,:e,:m,:st,:n,:a,:b,:d,:msg)"), {
            "r": run_id, "e": etape, "m": mode, "st": status, "n": rows, "a": started.isoformat(),
            "b": ended.isoformat(), "d": round((ended - started).total_seconds(), 2), "msg": message[:500]})


def write_landing(df: pd.DataFrame, source: str, run_id: str) -> Path:
    """Copie brute horodatée de chaque extraction (rejouabilité, preuve de ce qui a été reçu)."""
    target = LANDING_DIR / source / f"dt={datetime.now():%Y-%m-%d}"
    target.mkdir(parents=True, exist_ok=True)
    path = target / f"{run_id}.parquet"
    df.to_parquet(path, index=False)
    return path
