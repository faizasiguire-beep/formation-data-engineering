"""TP2 — CORRIGÉ — Audit qualité & conformité du dataset collecté au TP1.

Étapes :
    1. Lecture des tables RAW de l'entrepôt
    2. Validation Great Expectations par dimension (complétude, unicité, validité,
       cohérence, exactitude) + contrôle de fraîcheur
    3. Scan RGPD : détection des données personnelles (nom de colonne + contenu)
    4. Contrôle des droits d'accès (politique YAML validée DPO/RSSI)
    5. Mise à jour du data catalog (table `data_catalog`)
    6. Rapport Markdown + JSON + Data Docs HTML

Usage :
    python jour1/TP2_qualite_rgpd/corrige/audit_quality.py
    python jour1/TP2_qualite_rgpd/corrige/audit_quality.py --fail-on-error   # code retour 1 si échec bloquant
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml
from sqlalchemy import create_engine, inspect, text

import quality_checks as qc
from fil_rouge import config
from privacy import SEVERITY, detect_pii

logger = logging.getLogger("audit")
HERE = Path(__file__).resolve().parent
TABLES = ["raw_customers", "raw_transactions", "raw_merchants", "raw_merchant_blacklist"]
# Seuils bloquants : en dessous, le pipeline doit s'arrêter (utilisé par Airflow au Jour 2)
BLOCKING_THRESHOLDS = {"unicité": 95.0, "complétude": 95.0, "validité": 90.0, "cohérence": 85.0, "exactitude": 95.0}


def load_raw(engine) -> dict[str, pd.DataFrame]:
    existing = set(inspect(engine).get_table_names())
    missing = [t for t in TABLES if t not in existing]
    if missing:
        raise SystemExit(f"Tables absentes : {missing}. Lancez d'abord la collecte (TP1) en --mode full.")
    return {t: pd.read_sql_table(t, engine) for t in TABLES}


# --------------------------------------------------------------------------------------
def quality_audit(data: dict[str, pd.DataFrame], gx_dir: Path) -> tuple[pd.DataFrame, list[dict], str]:
    context = qc.get_context(gx_dir)
    customers = qc.prepare_customers_for_checks(data["raw_customers"])
    tx = data["raw_transactions"]
    merchants = data["raw_merchants"]

    frames = [
        qc.results_to_frame("raw_customers", qc.run_suite(context, "raw_customers", customers, qc.customers_expectations())),
        qc.results_to_frame("raw_transactions", qc.run_suite(
            context, "raw_transactions", tx,
            qc.transactions_expectations(sorted(customers["customer_id"].dropna().unique()),
                                         sorted(merchants["merchant_id"].dropna().unique())))),
        qc.results_to_frame("raw_merchants", qc.run_suite(context, "raw_merchants", merchants, qc.merchants_expectations())),
    ]
    results = pd.concat(frames, ignore_index=True)
    fresh = [
        {"table": "raw_transactions", **qc.freshness(tx, "updated_at", sla_hours=24)},
        {"table": "raw_merchants", **qc.freshness(merchants, "updated_at", sla_hours=24 * 30)},
    ]
    docs = context.get_docs_sites_urls()[0]["site_url"]
    return results, fresh, docs


def dimension_scores(results: pd.DataFrame, fresh: list[dict]) -> pd.DataFrame:
    scores = results.groupby(["table", "dimension"])["taux_conformite"].mean().round(2).unstack()
    for f in fresh:
        scores.loc[f["table"], "fraîcheur"] = 100.0 if f["succes"] else 0.0
    return scores


# --------------------------------------------------------------------------------------
def privacy_audit(data: dict[str, pd.DataFrame], policy: dict) -> pd.DataFrame:
    rows = []
    for table, df in data.items():
        zone = next((z for z, spec in policy["zones"].items() if table in spec["tables"]), "inconnue")
        roles_with_access = [r for r, spec in policy["roles"].items() if zone in spec["zones"]]
        for f in detect_pii(df):
            if f.category == "NONE":
                continue
            exposed_to = [r for r in roles_with_access if not policy["roles"][r]["pii_direct"]]
            risk = []
            if f.category == "DIRECT":
                risk.append("en clair (non pseudonymisé)")
                if exposed_to:
                    risk.append(f"lisible par {', '.join(exposed_to)} (pii_direct=false)")
            if f.category == "SENSIBLE":
                risk.append("donnée financière : minimisation à justifier")
            rows.append({"table": table, "colonne": f.column, "categorie": f.category, "detecte_par": f.detected_by,
                         "zone": zone, "roles_acces": ", ".join(roles_with_access),
                         "base_legale": policy["base_legale"].get(table, "⚠ NON DOCUMENTÉE"),
                         "risques": "; ".join(risk) or "-"})
    return pd.DataFrame(rows).sort_values(["table", "categorie"], key=lambda s: s.map(SEVERITY) if s.name == "categorie" else s,
                                          ascending=[True, False])


def file_permission_check(db_url: str) -> str:
    if not db_url.startswith("sqlite:///"):
        return "Base serveur : vérifier les GRANT (SELECT par rôle) côté SGBD."
    path = Path(db_url.removeprefix("sqlite:///"))
    mode = path.stat().st_mode & 0o777
    world = bool(mode & 0o004)
    return (f"Fichier `{path.name}` : permissions {oct(mode)} — "
            + ("⚠ lisible par tous les utilisateurs du poste (chmod 600 recommandé)" if world else "accès restreint ✔"))


# --------------------------------------------------------------------------------------
def build_catalog(data: dict[str, pd.DataFrame], pii: pd.DataFrame, policy: dict) -> pd.DataFrame:
    rows = []
    for table, df in data.items():
        zone = next((z for z, s in policy["zones"].items() if table in s["tables"]), "inconnue")
        src = df["_source"].iloc[0] if "_source" in df and len(df) else "?"
        batch = df["_batch_id"].iloc[-1] if "_batch_id" in df and len(df) else "?"
        for col in df.columns:
            hit = pii[(pii["table"] == table) & (pii["colonne"] == col)]
            rows.append({
                "table_name": table, "column_name": col, "dtype": str(df[col].dtype),
                "null_pct": round(float(df[col].isna().mean() * 100), 2),
                "distinct_count": int(df[col].nunique(dropna=True)),
                "pii_category": hit["categorie"].iloc[0] if len(hit) else "NONE",
                "zone": zone, "source": src, "last_batch_id": batch,
                "owner": policy["responsables"].get(table, "?"),
                "legal_basis": policy["base_legale"].get(table, "?"),
                "retention_days": policy["zones"].get(zone, {}).get("retention_jours"),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------------------
def md_table(df: pd.DataFrame) -> str:
    return df.to_markdown(index=False) if not df.empty else "_aucune ligne_"


def write_report(path: Path, scores: pd.DataFrame, results: pd.DataFrame, fresh: list[dict],
                 pii: pd.DataFrame, perm: str, data: dict[str, pd.DataFrame], docs_url: str, blocking: list[str]) -> None:
    volumes = pd.DataFrame([{"table": t, "lignes": len(d), "colonnes": d.shape[1]} for t, d in data.items()])
    failed = results[~results["succes"]].sort_values("taux_conformite")
    dup_customers = data["raw_customers"]["customer_id"].duplicated().sum()
    lines = [
        "# Rapport d'audit qualité & conformité — NexaPay",
        f"_Généré le {datetime.now():%d/%m/%Y à %H:%M} — périmètre : zone RAW de l'entrepôt_",
        "",
        "## 1. Synthèse",
        f"- Statut global : **{'BLOQUANT' if blocking else 'ACCEPTABLE AVEC RÉSERVES' if len(failed) else 'CONFORME'}**",
        f"- Contrôles exécutés : {len(results)} — en échec : **{len(failed)}**",
        f"- Doublons d'identifiant client : **{dup_customers}** lignes",
        f"- Colonnes contenant des données personnelles : **{len(pii)}** dont **{(pii['categorie'] == 'DIRECT').sum()}** identifiants directs en clair",
        *([f"- ⛔ Seuils bloquants franchis : {', '.join(blocking)}"] if blocking else []),
        "",
        "## 2. Volumétrie",
        md_table(volumes),
        "",
        "## 3. Scores par dimension (% de conformité moyen)",
        md_table(scores.reset_index().fillna("—")),
        "",
        f"Seuils bloquants : {', '.join(f'{k} ≥ {v} %' for k, v in BLOCKING_THRESHOLDS.items())}.",
        "",
        "## 4. Contrôles en échec",
        md_table(failed[["table", "dimension", "regle", "colonne", "taux_conformite", "exemples_non_conformes"]]),
        "",
        "## 5. Fraîcheur",
        md_table(pd.DataFrame(fresh)),
        "",
        "## 6. Conformité RGPD / AI Act",
        "### 6.1 Données personnelles détectées",
        md_table(pii),
        "",
        "### 6.2 Droits d'accès",
        f"- {perm}",
        "- Politique d'accès : `gouvernance/politique_acces.yaml` (moindre privilège, rôles DPO / RSSI).",
        "",
        "### 6.3 AI Act",
        "- Un système de scoring de fraude n'est pas, en principe, classé à haut risque (annexe III exclut la "
        "détection de fraude financière du point 5.b) — mais s'il conditionne l'accès à un service essentiel, "
        "requalification possible : documenter la qualité et la représentativité des données d'entraînement (art. 10).",
        "- Variables `gender`, `birth_date`, `city` : risque de biais discriminatoire — à exclure des features ou à auditer.",
        "",
        "## 7. Recommandations",
        "1. **Unicité** : dédoublonner les clients sur `customer_id` + email normalisé (lower/strip) en zone staging.",
        "2. **Validité** : normaliser les dates de naissance (formats mixtes) ; rejeter les dates impossibles en quarantaine.",
        "3. **Cohérence** : normaliser la casse des villes ; recoder le consentement en booléen.",
        "4. **Exactitude** : revenus négatifs et montants ≤ 0 → valeurs manquantes + signalement à la source.",
        "5. **RGPD** : pseudonymiser (HMAC) email, téléphone, n° d'identité ; supprimer nom/prénom de la zone curated ; "
        "généraliser âge et revenus en tranches ; tronquer les IP en /24.",
        "6. **Accès** : restreindre le fichier de base (chmod 600) ou passer sur un SGBD avec GRANT par rôle.",
        "",
        f"Rapport détaillé Great Expectations (Data Docs) : `{docs_url}`",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


# --------------------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Audit qualité & conformité (TP2)")
    parser.add_argument("--fail-on-error", action="store_true", help="code retour 1 si un seuil bloquant est franchi")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    logging.getLogger("great_expectations").setLevel(logging.WARNING)

    config.ensure_dirs()
    engine = create_engine(config.WAREHOUSE_DB_URL)
    policy = yaml.safe_load((HERE / "gouvernance" / "politique_acces.yaml").read_text(encoding="utf-8"))

    data = load_raw(engine)
    logger.info("Tables chargées : %s", {t: len(d) for t, d in data.items()})

    results, fresh, docs_url = quality_audit(data, config.PROJECT_ROOT)
    scores = dimension_scores(results, fresh)
    blocking = [f"{t}.{dim} = {val} %" for t, row in scores.iterrows() for dim, val in row.items()
                if dim in BLOCKING_THRESHOLDS and pd.notna(val) and val < BLOCKING_THRESHOLDS[dim]]

    pii = privacy_audit(data, policy)
    perm = file_permission_check(config.WAREHOUSE_DB_URL)
    catalog = build_catalog(data, pii, policy)
    catalog.to_sql("data_catalog", engine, if_exists="replace", index=False)

    report = config.REPORTS_DIR / "rapport_qualite_conformite.md"
    write_report(report, scores, results, fresh, pii, perm, data, docs_url, blocking)
    results.to_csv(config.REPORTS_DIR / "resultats_controles_qualite.csv", index=False, encoding="utf-8")
    (config.REPORTS_DIR / "scores_qualite.json").write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scores": json.loads(scores.reset_index().to_json(orient="records", force_ascii=False)),
        "blocking": blocking, "pii_direct_columns": int((pii["categorie"] == "DIRECT").sum()),
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS audit_log (run_id TEXT, source TEXT, mode TEXT, status TEXT, "
                          "rows_loaded INTEGER, started_at TEXT, ended_at TEXT, duration_s REAL, message TEXT)"))
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(text("INSERT INTO audit_log VALUES (:r,'quality_audit','audit',:s,0,:t,:t,0,:m)"),
                     {"r": now, "s": "ALERTE" if blocking else "SUCCESS", "t": now, "m": "; ".join(blocking)[:500]})

    print("\n" + scores.to_string())
    logger.info("Rapport : %s", report)
    logger.info("Data Docs : %s", docs_url)
    logger.info("Catalogue : %d colonnes documentées dans data_catalog", len(catalog))
    if blocking:
        logger.warning("Seuils bloquants franchis : %s", blocking)
    return 1 if (blocking and args.fail_on_error) else 0


if __name__ == "__main__":
    sys.exit(main())
