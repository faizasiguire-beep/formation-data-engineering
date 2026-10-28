"""Outils Great Expectations 1.x, repris du TP2 (quality_checks.py).

Chaque expectation porte dans `meta` la dimension qualité qu'elle mesure : une seule source de vérité pour
la validation (GX) et pour le rapport (scores par dimension).
"""
from __future__ import annotations

from datetime import datetime, timezone

import great_expectations as gx
import pandas as pd

from common import PROJECT_DIR


def m(dimension: str, regle: str) -> dict:
    return {"dimension": dimension, "regle": regle}


def get_context():
    return gx.get_context(mode="file", project_root_dir=str(PROJECT_DIR))


def run_suite(context, name: str, df: pd.DataFrame, expectations: list):
    """Crée (ou met à jour) source + suite + validation et exécute via un Checkpoint (Data Docs à jour)."""
    source = context.data_sources.add_or_update_pandas("projet_fraude")
    try:
        asset = source.get_asset(name)
    except LookupError:
        asset = source.add_dataframe_asset(name)
    try:
        batch_def = asset.get_batch_definition("complet")
    except KeyError:
        batch_def = asset.add_batch_definition_whole_dataframe("complet")
    suite = context.suites.add_or_update(gx.ExpectationSuite(name=f"{name}_suite", expectations=expectations))
    vd = context.validation_definitions.add_or_update(gx.ValidationDefinition(name=f"vd_{name}", data=batch_def, suite=suite))
    checkpoint = context.checkpoints.add_or_update(gx.Checkpoint(
        name=f"cp_{name}", validation_definitions=[vd],
        actions=[gx.checkpoint.UpdateDataDocsAction(name="maj_data_docs")],
        result_format={"result_format": "SUMMARY"}))
    result = checkpoint.run(batch_parameters={"dataframe": df})
    return list(result.run_results.values())[0]


def results_to_frame(table: str, validation_result) -> pd.DataFrame:
    rows = []
    for r in validation_result.results:
        cfg = r.expectation_config
        unexpected = r.result.get("unexpected_percent")
        rows.append({
            "table": table,
            "dimension": (cfg.meta or {}).get("dimension", "?"),
            "regle": (cfg.meta or {}).get("regle", cfg.type),
            "colonne": cfg.kwargs.get("column") or (f"{cfg.kwargs.get('column_A')}/{cfg.kwargs.get('column_B')}"
                                                   if cfg.kwargs.get("column_A") else "-"),
            "succes": bool(r.success),
            "taux_conformite": round(100 - (unexpected or 0.0), 2),
            "exemples_non_conformes": ", ".join(map(str, (r.result.get("partial_unexpected_list") or [])[:3])),
        })
    return pd.DataFrame(rows)


def freshness(series: pd.Series, sla_hours: float) -> dict:
    latest = pd.to_datetime(series, errors="coerce", utc=True).max()
    age_h = (datetime.now(timezone.utc) - latest.to_pydatetime()).total_seconds() / 3600
    return {"derniere_valeur": str(latest), "age_heures": round(age_h, 2), "sla_heures": sla_hours,
            "succes": bool(age_h <= sla_hours)}


def md_table(df: pd.DataFrame) -> str:
    return df.to_markdown(index=False) if not df.empty else "_aucune ligne_"
