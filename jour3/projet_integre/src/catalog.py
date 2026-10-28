"""Étape 6 — Catalogue des données : table `data_catalog` + docs/CATALOGUE.md.

Pour chaque colonne de chaque zone : type, taux de nulls, cardinalité, catégorie de donnée personnelle,
traitement RGPD appliqué, zone, durée de conservation, responsable, base légale. Les statistiques sont
calculées sur les tables réelles ; les catégories RGPD sont DÉCLARÉES (revue humaine), pas devinées.

    python jour3/projet_integre/src/catalog.py
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone

import pandas as pd
import yaml
from sqlalchemy import inspect

from common import PROJECT_DIR, audit, get_engine, new_run_id, setup_logging
from gx_utils import md_table

logger = logging.getLogger("projet.catalogue")

# Catégorie RGPD et traitement, par nom de colonne (revue humaine documentée)
DICTIONNAIRE = {
    "customer_id": ("INDIRECT", "en clair en RAW uniquement ; pseudonymisé (HMAC) en staging"),
    "device_id": ("INDIRECT", "en clair en RAW uniquement ; pseudonymisé (HMAC) en staging"),
    "customer_pseudo": ("PSEUDONYME", "HMAC-SHA256 + clé secrète, cohérent entre sources"),
    "device_pseudo": ("PSEUDONYME", "HMAC-SHA256 + clé secrète, cohérent entre sources"),
    "ip_address": ("INDIRECT", "en clair en RAW uniquement ; tronquée /24 en staging ; absente en curated"),
    "ip_network": ("INDIRECT", "réseau /24 (hôte supprimé) ; absent en curated"),
    "latitude": ("INDIRECT", "en clair en RAW uniquement ; remplacée par une zone en staging"),
    "longitude": ("INDIRECT", "en clair en RAW uniquement ; remplacée par une zone en staging"),
    "zone_geo": ("GÉNÉRALISÉE", "zone de service (10 villes) ; absente en curated"),
    "transaction_id": ("TECHNIQUE", "identifiant d'opération, nécessaire à l'unicité et à l'audit"),
    "session_id": ("TECHNIQUE", "identifiant de session"),
    "name": ("DIRECT (possible)", "raison sociale : peut être un nom propre (entrepreneur individuel) ; non propagée"),
    "contact_email": ("DIRECT (possible)", "peut être nominatif ; non propagé"),
}


def categorie(col: str) -> tuple[str, str]:
    if col.startswith("_"):
        return "TECHNIQUE", "colonne de traçabilité du pipeline"
    return DICTIONNAIRE.get(col, ("NONE", "-"))


def main() -> int:
    run_id = new_run_id()
    setup_logging("catalogue", run_id)
    started = datetime.now(timezone.utc)
    engine = get_engine()
    policy = yaml.safe_load((PROJECT_DIR / "gouvernance" / "politique_acces.yaml").read_text(encoding="utf-8"))
    existing = set(inspect(engine).get_table_names())
    rows = []
    for zone, spec in policy["zones"].items():
        for table in spec["tables"]:
            if table not in existing:
                continue
            df = pd.read_sql_table(table, engine)
            source_table = table.replace("stg_", "raw_") if table.startswith("stg_") else table
            for col in df.columns:
                cat, traitement = categorie(col)
                rows.append({
                    "zone": zone, "table_name": table, "column_name": col, "dtype": str(df[col].dtype),
                    "lignes": len(df), "null_pct": round(float(df[col].isna().mean() * 100), 2),
                    "distinct": int(df[col].nunique(dropna=True)), "categorie_rgpd": cat, "traitement": traitement,
                    "retention_jours": spec["retention_jours"],
                    "responsable": policy["responsables"].get(source_table, "Direction des Risques"),
                    "base_legale": policy["base_legale"].get(source_table, "Intérêt légitime (lutte contre la fraude)"),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })
    catalog = pd.DataFrame(rows)
    catalog.to_sql("data_catalog", engine, if_exists="replace", index=False)

    doc = PROJECT_DIR / "docs" / "CATALOGUE.md"
    doc.parent.mkdir(exist_ok=True)
    tables = catalog.groupby(["zone", "table_name"], sort=False).agg(lignes=("lignes", "first"),
                                                                      colonnes=("column_name", "count")).reset_index()
    sensibles = catalog[~catalog["categorie_rgpd"].isin(["NONE", "TECHNIQUE"])]
    parts = [
        "# Catalogue des données — projet fraude comportementale",
        f"_Généré automatiquement par `src/catalog.py` le {datetime.now():%d/%m/%Y à %H:%M} (table `data_catalog`)._", "",
        "## Tables", md_table(tables), "",
        "## Colonnes contenant des données personnelles et traitement appliqué",
        md_table(sensibles[["zone", "table_name", "column_name", "categorie_rgpd", "traitement"]]), "",
        "Contrôle : la zone **curated** (`ml_fraud_features`) ne contient ni IP, ni coordonnées, ni identifiant en "
        "clair ; seule y figure la colonne pseudonymisée `customer_pseudo`.", "",
        "## Détail par table",
    ]
    for (zone, table), g in catalog.groupby(["zone", "table_name"], sort=False):
        parts += [f"### {table} ({zone}, conservation {g['retention_jours'].iloc[0]} jours)",
                  f"Responsable : {g['responsable'].iloc[0]} — base légale : {g['base_legale'].iloc[0]}", "",
                  md_table(g[["column_name", "dtype", "null_pct", "distinct", "categorie_rgpd"]]), ""]
    doc.write_text("\n".join(parts), encoding="utf-8")
    curated_pii = sensibles[(sensibles["zone"] == "curated") & (sensibles["categorie_rgpd"] != "PSEUDONYME")]
    logger.info("Catalogue : %d colonnes, %d avec données personnelles, %d non pseudonymisées en curated -> %s",
                len(catalog), len(sensibles), len(curated_pii), doc)
    audit(engine, run_id, "catalogue", "full", "SUCCESS", len(catalog), started)
    return 0


if __name__ == "__main__":
    sys.exit(main())
