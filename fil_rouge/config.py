"""Configuration centralisée.

Règle d'or du module : AUCUN secret dans le code. Tout passe par des variables
d'environnement, chargées depuis un fichier `.env` (jamais commité, cf. .gitignore).
"""
from __future__ import annotations

import os
from pathlib import Path

try:  # python-dotenv est optionnel : sans lui, on lit seulement l'environnement
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if load_dotenv is not None:
    load_dotenv(PROJECT_ROOT / ".env")


def _path(env_name: str, default: Path) -> Path:
    value = os.getenv(env_name)
    return Path(value).expanduser().resolve() if value else default


DATA_DIR = _path("DATA_DIR", PROJECT_ROOT / "data")
SOURCES_DIR = DATA_DIR / "sources"          # données "de production" (générées)
LANDING_DIR = DATA_DIR / "landing"          # zone d'atterrissage brute (fichiers)
CURATED_DIR = DATA_DIR / "curated"          # données nettoyées / pseudonymisées
REPORTS_DIR = _path("REPORTS_DIR", PROJECT_ROOT / "reports")
LOGS_DIR = _path("LOGS_DIR", PROJECT_ROOT / "logs")

# Base "source" : simule le core banking (lecture seule pour les stagiaires)
SOURCE_DB_URL = os.getenv("SOURCE_DB_URL", f"sqlite:///{(SOURCES_DIR / 'core_banking.db').as_posix()}")
# Base "cible" : l'entrepôt alimenté par nos pipelines (SQLite par défaut, PostgreSQL possible)
WAREHOUSE_DB_URL = os.getenv("WAREHOUSE_DB_URL", f"sqlite:///{(DATA_DIR / 'warehouse.db').as_posix()}")

API_BASE_URL = os.getenv("NEXA_API_BASE_URL", "http://127.0.0.1:8000")
API_CLIENT_ID = os.getenv("NEXA_API_CLIENT_ID", "")
API_CLIENT_SECRET = os.getenv("NEXA_API_CLIENT_SECRET", "")

# Clé secrète de pseudonymisation (HMAC). En production : coffre-fort (Vault, KMS...)
PSEUDO_SECRET_KEY = os.getenv("PSEUDO_SECRET_KEY", "")


def ensure_dirs() -> None:
    for d in (SOURCES_DIR, LANDING_DIR, CURATED_DIR, REPORTS_DIR, LOGS_DIR):
        d.mkdir(parents=True, exist_ok=True)
