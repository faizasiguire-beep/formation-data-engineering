"""Étape 5 — Instantané immuable du jeu de données final, versionné par DVC.

Une base est un objet MUTABLE : on versionne un instantané Parquet DÉTERMINISTE de la table curated
(tri stable, colonne technique `_built_at` retirée) — même contenu métier ⇒ même hash DVC.

    python jour3/projet_integre/src/export_snapshot.py
    dvc add jour3/projet_integre/data/ml_fraud_features          # cf. scripts/versionner.sh
"""
from __future__ import annotations

import hashlib
import json
import logging
import sys
from datetime import datetime, timezone

import pandas as pd

from common import PROJECT_DIR, audit, get_engine, new_run_id, setup_logging

logger = logging.getLogger("projet.snapshot")
OUT = PROJECT_DIR / "data" / "ml_fraud_features"


def main() -> int:
    run_id = new_run_id()
    setup_logging("snapshot", run_id)
    started = datetime.now(timezone.utc)
    engine = get_engine()
    df = (pd.read_sql_table("ml_fraud_features", engine)
          .drop(columns=["_built_at"])
          .sort_values("transaction_id").reset_index(drop=True))
    OUT.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT / "ml_fraud_features.parquet", index=False)
    manifest = {
        "table": "ml_fraud_features",
        "lignes": len(df),
        "fraudes": int(df["is_fraud"].sum()),
        "taux_fraude": round(float(df["is_fraud"].mean()), 5),
        "periode": [str(df["created_at"].min()), str(df["created_at"].max())],
        "colonnes": list(df.columns),
        # empreinte du CONTENU (indépendante du format Parquet) : preuve d'identité entre deux exports
        "empreinte_contenu": hashlib.sha256(pd.util.hash_pandas_object(df, index=False).values.tobytes()).hexdigest()[:16],
    }
    (OUT / "MANIFEST.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Instantané : %d lignes, %d fraudes -> %s", len(df), manifest["fraudes"], OUT)
    audit(engine, run_id, "export_snapshot", "full", "SUCCESS", len(df), started, manifest["empreinte_contenu"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
