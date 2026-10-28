"""Suites Great Expectations (GX 1.x) des tables RAW, organisées par dimension de qualité.

Chaque expectation porte dans `meta` la dimension qu'elle mesure. Le score d'une dimension
est la moyenne des taux de conformité de ses expectations : une seule source de vérité
pour la validation (GX) et pour le tableau de bord qualité (rapport).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

logger = logging.getLogger("qualite")

CITIES = ["Abidjan", "Bouaké", "Yamoussoukro", "Dakar", "Thiès", "Bamako", "Ouagadougou", "Lomé", "Cotonou", "Niamey"]
COUNTRIES = ["CI", "SN", "ML", "BF", "TG", "BJ", "NE"]
EMAIL_RE = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
PHONE_RE = r"^\+22[1-9]\d{8,10}$"
ISO_DATE_RE = r"^\d{4}-\d{2}-\d{2}$"


def _m(dimension: str, rule: str) -> dict:
    return {"dimension": dimension, "regle": rule}


def customers_expectations() -> list:
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="customer_id", meta=_m("complétude", "identifiant client obligatoire")),
        gxe.ExpectColumnValuesToNotBeNull(column="email", mostly=0.99, meta=_m("complétude", "email renseigné (≥ 99 %)")),
        gxe.ExpectColumnValuesToNotBeNull(column="country", mostly=0.99, meta=_m("complétude", "pays renseigné")),
        gxe.ExpectColumnValuesToBeUnique(column="customer_id", meta=_m("unicité", "un client = une ligne")),
        gxe.ExpectColumnValuesToMatchRegex(column="email", regex=EMAIL_RE, mostly=0.999,
                                           meta=_m("validité", "format email minuscule valide")),
        gxe.ExpectColumnValuesToMatchRegex(column="phone", regex=PHONE_RE, meta=_m("validité", "téléphone E.164 UEMOA")),
        gxe.ExpectColumnValuesToMatchRegex(column="birth_date", regex=ISO_DATE_RE,
                                           meta=_m("validité", "date de naissance ISO 8601")),
        gxe.ExpectColumnValuesToBeInSet(column="city", value_set=CITIES, meta=_m("cohérence", "ville du référentiel (casse)")),
        gxe.ExpectColumnValuesToBeInSet(column="consent_marketing", value_set=["oui", "non"],
                                        meta=_m("cohérence", "consentement codifié oui/non")),
        gxe.ExpectColumnValuesToBeBetween(column="monthly_income_num", min_value=0, max_value=50_000_000,
                                          meta=_m("exactitude", "revenu plausible (≥ 0)")),
        gxe.ExpectColumnValuesToBeBetween(column="age", min_value=18, max_value=100,
                                          meta=_m("exactitude", "âge client plausible (18-100 ans)")),
    ]


def transactions_expectations(customer_ids: list[str], merchant_ids: list[str]) -> list:
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="amount", mostly=0.999, meta=_m("complétude", "montant renseigné")),
        # Zone RAW append-only : une transaction mise à jour apparaît une fois par version.
        # L'unicité porte donc sur (identifiant, version) ; le dédoublonnage se fait en staging.
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["transaction_id", "updated_at"],
                                            meta=_m("unicité", "une ligne par (transaction, version)")),
        gxe.ExpectColumnValuesToBeInSet(column="currency", value_set=["XOF"], meta=_m("validité", "devise ISO 4217 XOF")),
        gxe.ExpectColumnValuesToBeInSet(column="status", value_set=["SUCCESS", "FAILED", "REVERSED", "PENDING"],
                                        meta=_m("validité", "statut du référentiel")),
        gxe.ExpectColumnValuesToBeInSet(column="channel", value_set=["app", "ussd", "card", "web"],
                                        meta=_m("validité", "canal connu")),
        gxe.ExpectColumnValuesToBeBetween(column="amount", min_value=1, max_value=50_000_000,
                                          meta=_m("exactitude", "montant strictement positif et plausible")),
        gxe.ExpectColumnValuesToBeInSet(column="customer_id", value_set=customer_ids,
                                        meta=_m("cohérence", "intégrité référentielle client")),
        gxe.ExpectColumnValuesToBeInSet(column="merchant_id", value_set=merchant_ids,
                                        meta=_m("cohérence", "intégrité référentielle marchand")),
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="updated_at", column_B="created_at", or_equal=True,
                                                    meta=_m("cohérence", "updated_at ≥ created_at")),
    ]


def merchants_expectations() -> list:
    return [
        gxe.ExpectColumnValuesToBeUnique(column="merchant_id", meta=_m("unicité", "identifiant marchand unique")),
        gxe.ExpectColumnValuesToNotBeNull(column="mcc", meta=_m("complétude", "code MCC renseigné")),
        gxe.ExpectColumnValuesToMatchRegex(column="mcc", regex=r"^\d{4}$", meta=_m("validité", "MCC à 4 chiffres")),
        gxe.ExpectColumnValuesToBeInSet(column="risk_level", value_set=["low", "medium", "high"],
                                        meta=_m("validité", "niveau de risque codifié")),
    ]


# --------------------------------------------------------------------------------------
def prepare_customers_for_checks(df: pd.DataFrame) -> pd.DataFrame:
    """Colonnes DÉRIVÉES nécessaires aux contrôles d'exactitude (la donnée brute reste intacte)."""
    out = df.copy()
    out["monthly_income_num"] = pd.to_numeric(out["monthly_income_xof"], errors="coerce")
    birth = pd.to_datetime(out["birth_date"], format="mixed", dayfirst=True, errors="coerce")
    out["age"] = (pd.Timestamp.now() - birth).dt.days / 365.25
    # une date impossible (2031-02-30) devient NaT -> âge manquant : on la compte comme invalide
    out["age"] = out["age"].fillna(-1)
    return out


def run_suite(context, name: str, df: pd.DataFrame, expectations: list):
    """Crée (ou met à jour) source + suite + validation et exécute via un Checkpoint."""
    source = context.data_sources.add_or_update_pandas("nexapay_warehouse")
    try:
        asset = source.get_asset(name)
    except LookupError:
        asset = source.add_dataframe_asset(name)
    try:
        batch_def = asset.get_batch_definition("complet")
    except KeyError:
        batch_def = asset.add_batch_definition_whole_dataframe("complet")
    suite = gx.ExpectationSuite(name=f"{name}_suite", expectations=expectations)
    suite = context.suites.add_or_update(suite)
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
            "expectation": cfg.type,
            "colonne": cfg.kwargs.get("column") or f"{cfg.kwargs.get('column_A')}/{cfg.kwargs.get('column_B')}",
            "succes": bool(r.success),
            "taux_conformite": round(100 - (unexpected or 0.0), 2),
            "exemples_non_conformes": ", ".join(map(str, (r.result.get("partial_unexpected_list") or [])[:3])),
        })
    return pd.DataFrame(rows)


def freshness(df: pd.DataFrame, column: str, sla_hours: float) -> dict:
    latest = pd.to_datetime(df[column], errors="coerce").max()
    if latest.tzinfo is None:
        latest = latest.tz_localize("UTC")
    age_h = (datetime.now(timezone.utc) - latest.to_pydatetime()).total_seconds() / 3600
    return {"colonne": column, "derniere_valeur": str(latest), "age_heures": round(age_h, 2),
            "sla_heures": sla_hours, "succes": age_h <= sla_hours}


def get_context(project_dir: Path):
    project_dir.mkdir(parents=True, exist_ok=True)
    return gx.get_context(mode="file", project_root_dir=str(project_dir))
