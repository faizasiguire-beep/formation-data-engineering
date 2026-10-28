"""Étape 2 — Contrôles qualité d'ENTRÉE (diagnostic) sur la zone RAW.

La zone RAW reflète les sources : ses défauts sont attendus et documentés, pas bloquants (le seuil
bloquant est en SORTIE, cf. quality_out.py). Ce diagnostic mesure 5 dimensions + fraîcheur, et vérifie
une contrainte propre au brief : aucune étiquette de fraude confirmée AVANT la transaction (fuite).

Sorties : reports/projet_fraude/qualite_entree.md (+ .csv, .json), Data Docs GX, ligne dans audit_log.

    python jour3/projet_integre/src/quality_in.py                  # toujours code 0 (diagnostic)
    python jour3/projet_integre/src/quality_in.py --fail-on-error  # code 1 si un contrôle échoue
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

import great_expectations.expectations as gxe
import pandas as pd

from common import REPORTS_DIR, audit, get_engine, load_params, new_run_id, setup_logging
from gx_utils import freshness, get_context, m, md_table, results_to_frame, run_suite

logger = logging.getLogger("projet.qualite_entree")
STATUTS = ["SUCCESS", "FAILED", "REVERSED", "PENDING"]
TYPES_FRAUDE = ["account_takeover", "sim_swap", "merchant_collusion", "stolen_card"]


def sessions_expectations(customer_ids: list[str]) -> list:
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="session_id", meta=m("complétude", "identifiant de session obligatoire")),
        gxe.ExpectColumnValuesToNotBeNull(column="device_id", meta=m("complétude", "appareil renseigné")),
        gxe.ExpectColumnValuesToBeUnique(column="session_id", meta=m("unicité", "une session = une ligne")),
        gxe.ExpectColumnValuesToMatchRegex(column="customer_id", regex=r"^C\d{6}$", meta=m("validité", "format client C000000")),
        gxe.ExpectColumnValuesToMatchRegex(column="device_id", regex=r"^D\d{5}$", meta=m("validité", "format appareil D00000")),
        gxe.ExpectColumnValuesToMatchRegex(column="ip_address", regex=r"^(\d{1,3}\.){3}\d{1,3}$", meta=m("validité", "IPv4")),
        gxe.ExpectColumnValuesToBeInSet(column="os", value_set=["Android", "iOS", "KaiOS"], meta=m("validité", "OS connu")),
        gxe.ExpectColumnValuesToBeBetween(column="latitude", min_value=4.0, max_value=16.0,
                                          meta=m("exactitude", "latitude dans la zone UEMOA")),
        gxe.ExpectColumnValuesToBeBetween(column="longitude", min_value=-18.0, max_value=3.0,
                                          meta=m("exactitude", "longitude dans la zone UEMOA")),
        gxe.ExpectColumnValuesToBeBetween(column="duration_s", min_value=1, max_value=86_400,
                                          meta=m("exactitude", "durée de session plausible")),
        gxe.ExpectColumnValuesToBeInSet(column="customer_id", value_set=customer_ids,
                                        meta=m("cohérence", "client connu du core banking")),
    ]


def transactions_expectations(merchant_ids: list[str]) -> list:
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="amount", mostly=0.999, meta=m("complétude", "montant renseigné")),
        gxe.ExpectColumnValuesToNotBeNull(column="device_id", meta=m("complétude", "appareil renseigné")),
        # RAW append-only : une transaction mise à jour apparaît une fois par version
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["transaction_id", "updated_at"],
                                            meta=m("unicité", "une ligne par (transaction, version)")),
        gxe.ExpectColumnValuesToBeInSet(column="currency", value_set=["XOF"], meta=m("validité", "devise XOF")),
        gxe.ExpectColumnValuesToBeInSet(column="status", value_set=STATUTS, meta=m("validité", "statut du référentiel")),
        gxe.ExpectColumnValuesToBeBetween(column="amount", min_value=1, max_value=50_000_000,
                                          meta=m("exactitude", "montant strictement positif et plausible")),
        gxe.ExpectColumnValuesToBeInSet(column="merchant_id", value_set=merchant_ids,
                                        meta=m("cohérence", "marchand connu du référentiel API")),
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="updated_at", column_B="created_at", or_equal=True,
                                                    meta=m("cohérence", "updated_at ≥ created_at")),
    ]


def labels_expectations(tx_ids: list[str]) -> list:
    return [
        gxe.ExpectColumnValuesToBeUnique(column="transaction_id", meta=m("unicité", "une étiquette par transaction")),
        gxe.ExpectColumnValuesToBeInSet(column="fraud_type", value_set=TYPES_FRAUDE, meta=m("validité", "type de fraude connu")),
        gxe.ExpectColumnValuesToBeInSet(column="transaction_id", value_set=tx_ids,
                                        meta=m("cohérence", "étiquette rattachée à une transaction")),
        # Contrainte du brief : une étiquette confirmée avant la transaction serait une fuite (ou une erreur source)
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="confirmed_at_ts", column_B="tx_created_at_ts", or_equal=True,
                                                    meta=m("exactitude", "pas d'étiquette confirmée avant la transaction")),
    ]


def merchants_expectations() -> list:
    return [
        gxe.ExpectColumnValuesToBeUnique(column="merchant_id", meta=m("unicité", "marchand unique")),
        gxe.ExpectColumnValuesToMatchRegex(column="mcc", regex=r"^\d{4}$", meta=m("validité", "MCC à 4 chiffres")),
        gxe.ExpectColumnValuesToBeInSet(column="risk_level", value_set=["low", "medium", "high"],
                                        meta=m("validité", "niveau de risque codifié")),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Qualité d'entrée (diagnostic) — projet fraude")
    parser.add_argument("--fail-on-error", action="store_true")
    args = parser.parse_args()
    run_id = new_run_id()
    setup_logging("qualite_entree", run_id)
    started = datetime.now(timezone.utc)
    params = load_params("qualite")
    engine = get_engine()

    tx = pd.read_sql_table("raw_transactions", engine)
    labels = pd.read_sql_table("raw_fraud_labels", engine)
    sessions = pd.read_sql_table("raw_sessions", engine)
    merchants = pd.read_sql_table("raw_merchants", engine)

    # Date de création de chaque transaction (dernière version) pour le contrôle de fuite des étiquettes
    created = tx.drop_duplicates("transaction_id", keep="last").set_index("transaction_id")["created_at"]
    labels_chk = labels.drop_duplicates("transaction_id", keep="last").copy()
    labels_chk["confirmed_at_ts"] = pd.to_datetime(labels_chk["confirmed_at"], errors="coerce")
    labels_chk["tx_created_at_ts"] = pd.to_datetime(labels_chk["transaction_id"].map(created), errors="coerce")

    context = get_context()
    results = pd.concat([
        results_to_frame("raw_sessions", run_suite(context, "raw_sessions", sessions,
                                                   sessions_expectations(sorted(tx["customer_id"].unique())))),
        results_to_frame("raw_transactions", run_suite(context, "raw_transactions", tx,
                                                       transactions_expectations(sorted(merchants["merchant_id"].unique())))),
        results_to_frame("raw_fraud_labels", run_suite(context, "raw_fraud_labels", labels_chk,
                                                       labels_expectations(sorted(created.index)))),
        results_to_frame("raw_merchants", run_suite(context, "raw_merchants", merchants, merchants_expectations())),
    ], ignore_index=True)
    fresh = pd.DataFrame([
        {"table": "raw_transactions", "colonne": "updated_at", **freshness(tx["updated_at"], params["fraicheur_max_h"])},
        {"table": "raw_sessions", "colonne": "login_at", **freshness(sessions["login_at"], params["fraicheur_max_h"])},
    ])
    scores = results.groupby(["table", "dimension"])["taux_conformite"].mean().round(2).unstack()
    failed = results[~results["succes"]].sort_values("taux_conformite")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    volumes = pd.DataFrame([{"table": t, "lignes": len(d)} for t, d in
                            [("raw_transactions", tx), ("raw_fraud_labels", labels), ("raw_sessions", sessions),
                             ("raw_merchants", merchants)]])
    report = REPORTS_DIR / "qualite_entree.md"
    report.write_text("\n".join([
        "# Qualité d'entrée (zone RAW) — projet fraude comportementale",
        f"_Généré le {datetime.now():%d/%m/%Y à %H:%M} — diagnostic, non bloquant_", "",
        f"- Contrôles : {len(results)} — en échec : **{len(failed)}**",
        f"- Fraîcheur : {', '.join(f'{r.table} {r.age_heures} h' for r in fresh.itertuples())} (SLA {params['fraicheur_max_h']} h)", "",
        "## Volumétrie", md_table(volumes), "",
        "## Scores par dimension (% de conformité moyen)", md_table(scores.reset_index().astype(object).fillna("—")), "",
        "## Contrôles en échec", md_table(failed.drop(columns=["succes"])), "",
        "## Fraîcheur", md_table(fresh), "",
        "Les défauts de la zone RAW sont traités en staging (dédoublonnage, quarantaine) ; le contrôle "
        "bloquant porte sur la table finale (`quality_out.py`).",
    ]), encoding="utf-8")
    results.to_csv(REPORTS_DIR / "qualite_entree.csv", index=False, encoding="utf-8")
    (REPORTS_DIR / "qualite_entree.json").write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(), "controles": len(results), "echecs": len(failed),
        "scores": json.loads(scores.reset_index().to_json(orient="records", force_ascii=False)),
        "fraicheur": json.loads(fresh.to_json(orient="records", force_ascii=False)),
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n" + scores.to_string())
    logger.info("Rapport : %s | %d contrôles, %d en échec", report, len(results), len(failed))
    status = "ALERTE" if len(failed) or not fresh["succes"].all() else "SUCCESS"
    audit(engine, run_id, "qualite_entree", "diagnostic", status, 0, started,
          "; ".join(f"{r.table}.{r.regle}" for r in failed.itertuples())[:500])
    return 1 if (args.fail_on_error and status != "SUCCESS") else 0


if __name__ == "__main__":
    sys.exit(main())
