"""Étape 4 — Contrôle qualité de SORTIE, BLOQUANT, sur la table curated `ml_fraud_features`.

Ce qui doit être garanti, c'est ce que consomment les data scientists. Contraintes du brief :
  * unicité de `transaction_id` ;
  * complétude ≥ 99 % des features ;
  * aucune étiquette confirmée AVANT la transaction (fuite) ;
  * fraîcheur des transactions < 24 h ;
  * conformité : ni IP, ni coordonnées, ni identifiant direct ; pseudonymes cohérents entre sources.

Code de sortie 1 si un contrôle échoue : la tâche Airflow échoue (sans retry : une non-conformité ne se
corrige pas en réessayant), les tâches aval ne tournent pas, l'alerte part.

    python jour3/projet_integre/src/quality_out.py
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

import great_expectations.expectations as gxe
import pandas as pd

from build_features import FEATURES, ml_fraud_features
from common import REPORTS_DIR, audit, get_engine, load_params, new_run_id, setup_logging
from gx_utils import freshness, get_context, m, md_table, results_to_frame, run_suite
from privacy import PSEUDO_REGEX

logger = logging.getLogger("projet.qualite_sortie")
COLONNES_ATTENDUES = [c.name for c in ml_fraud_features.columns]
IPV4 = r"^(\d{1,3}\.){3}\d{1,3}$"


def expectations(p: dict, plafond_minutes: float) -> list:
    exps = [
        gxe.ExpectTableColumnsToMatchSet(column_set=COLONNES_ATTENDUES, exact_match=True,
                                         meta=m("conformité", "schéma exact : aucune colonne IP, GPS ou identifiant")),
        gxe.ExpectColumnValuesToNotBeNull(column="transaction_id", meta=m("complétude", "transaction_id renseigné")),
        gxe.ExpectColumnValuesToBeUnique(column="transaction_id", meta=m("unicité", "une ligne par transaction")),
        gxe.ExpectColumnValuesToMatchRegex(column="customer_pseudo", regex=PSEUDO_REGEX,
                                           meta=m("conformité", "client pseudonymisé (HMAC)")),
        gxe.ExpectColumnValuesToBeInSet(column="is_fraud", value_set=[0, 1], meta=m("validité", "étiquette binaire")),
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="fraud_confirmed_at", column_B="created_at", or_equal=True, ignore_row_if="either_value_is_missing",
            meta=m("exactitude", "aucune étiquette confirmée avant la transaction")),
        gxe.ExpectColumnValuesToBeBetween(column="minutes_depuis_derniere_session", min_value=0, max_value=plafond_minutes,
                                          meta=m("validité", "récence de session dans [0, plafond]")),
    ]
    for f in FEATURES:
        exps.append(gxe.ExpectColumnValuesToNotBeNull(column=f, mostly=p["completude_min"],
                                                      meta=m("complétude", f"feature {f} ≥ {p['completude_min']:.0%}")))
    for f in ("nb_sessions_24h_avant", "nb_appareils_distincts_7j", "nb_zones_distinctes_24h", "velocite_1h", "velocite_24h"):
        exps.append(gxe.ExpectColumnValuesToBeBetween(column=f, min_value=0, meta=m("validité", f"{f} ≥ 0")))
    return exps


def checks_hors_gx(features: pd.DataFrame, stg_tx: pd.DataFrame, stg_sessions: pd.DataFrame, p: dict) -> pd.DataFrame:
    """Contrôles transverses (plusieurs tables, fraîcheur, contenu) que GX n'exprime pas simplement."""
    fresh = freshness(features["created_at"], p["fraicheur_max_h"])
    clients = stg_tx["customer_pseudo"].isin(set(stg_sessions["customer_pseudo"])).mean()
    appareils = stg_tx["device_pseudo"].isin(set(stg_sessions["device_pseudo"])).mean()
    texte = features.select_dtypes(include="object")
    ip_en_clair = int(sum(texte[c].astype(str).str.match(IPV4).sum() for c in texte.columns))
    return pd.DataFrame([
        {"controle": f"fraîcheur des transactions < {p['fraicheur_max_h']} h", "valeur": f"{fresh['age_heures']} h",
         "succes": fresh["succes"]},
        {"controle": f"pseudonymes clients cohérents transactions/sessions ≥ {p['recouvrement_pseudo_min']:.0%}",
         "valeur": f"{clients:.2%}", "succes": bool(clients >= p["recouvrement_pseudo_min"])},
        {"controle": "pseudonymes appareils transactions retrouvés en session (information)",
         "valeur": f"{appareils:.2%}", "succes": True},
        {"controle": "aucune adresse IP en clair dans la table finale", "valeur": str(ip_en_clair),
         "succes": ip_en_clair == 0},
    ])


def main() -> int:
    run_id = new_run_id()
    setup_logging("qualite_sortie", run_id)
    started = datetime.now(timezone.utc)
    p = load_params("qualite")
    engine = get_engine()
    features = pd.read_sql_table("ml_fraud_features", engine)
    stg_tx = pd.read_sql_table("stg_transactions", engine)
    stg_sessions = pd.read_sql_table("stg_sessions", engine)

    res = run_suite(get_context(), "ml_fraud_features", features,
                    expectations(p, float(load_params("features")["plafond_minutes"])))
    gx_results = results_to_frame("ml_fraud_features", res)
    autres = checks_hors_gx(features, stg_tx, stg_sessions, p)
    ok = bool(res.success) and bool(autres["succes"].all())

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    failed = gx_results[~gx_results["succes"]]
    (REPORTS_DIR / "qualite_sortie.md").write_text("\n".join([
        "# Qualité de sortie (BLOQUANT) — table ml_fraud_features",
        f"_Généré le {datetime.now():%d/%m/%Y à %H:%M}_", "",
        f"- Statut : **{'CONFORME' if ok else 'NON CONFORME — pipeline arrêté'}**",
        f"- Lignes : {len(features)} — fraudes : {int(features['is_fraud'].sum())}",
        f"- Contrôles GX : {len(gx_results)} — en échec : {len(failed)}", "",
        "## Contrôles transverses", md_table(autres), "",
        "## Contrôles Great Expectations", md_table(gx_results.drop(columns=["table"])),
    ]), encoding="utf-8")
    (REPORTS_DIR / "qualite_sortie.json").write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(), "conforme": ok, "lignes": len(features),
        "echecs_gx": failed["regle"].tolist(), "controles_transverses": json.loads(autres.to_json(orient="records", force_ascii=False)),
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    for r in autres.itertuples():
        logger.info("%s %s : %s", "OK   " if r.succes else "ÉCHEC", r.controle, r.valeur)
    for r in failed.itertuples():
        logger.error("ÉCHEC GX %s (%s %%)", r.regle, r.taux_conformite)
    msg = "; ".join(failed["regle"].tolist() + autres.loc[~autres["succes"], "controle"].tolist())
    audit(engine, run_id, "qualite_sortie", "bloquant", "SUCCESS" if ok else "FAILED", len(features), started, msg)
    if not ok:
        logger.error("Table ml_fraud_features NON CONFORME : arrêt du pipeline")
        return 1
    logger.info("Table ml_fraud_features conforme ✔ (%d contrôles GX + %d transverses)", len(gx_results), len(autres))
    return 0


if __name__ == "__main__":
    sys.exit(main())
