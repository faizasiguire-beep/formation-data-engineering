"""TP2 (partie B) — CORRIGÉ — Nettoyage, protection RGPD et alimentation de la zone STAGING.

RAW (brut, append-only, identifiants en clair)  ──►  STAGING (typé, dédoublonné, pseudonymisé)

Compétences : C1.8 (corriger anomalies et valeurs manquantes) · C1.9 (alimenter des tables optimisées).

Choix structurants :
  * Les lignes irrécupérables ne sont PAS supprimées silencieusement : elles partent en QUARANTAINE
    avec le motif de rejet (traçabilité, retour vers la source).
  * La protection est intégrée au traitement ("privacy by design", RGPD art. 25) : la zone staging
    ne contient plus aucun identifiant direct en clair.
  * Tables créées avec un schéma explicite (types, clés primaires, index) : pas de `to_sql` qui
    devine les types.
  * Contrôle qualité de SORTIE (Great Expectations) : si la staging n'est pas conforme, code retour 1.

Usage :
    python jour1/TP2_qualite_rgpd/corrige/clean_and_load.py
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone

import great_expectations.expectations as gxe
import numpy as np
import pandas as pd
from sqlalchemy import (Boolean, Column, DateTime, Float, Integer, MetaData, String, Table,
                        create_engine, inspect, text)

import quality_checks as qc
from fil_rouge import config
from privacy import age_band, income_band, mask_email, pseudonymize, require_key, truncate_ip

logger = logging.getLogger("staging")
CITY_CANON = {c.upper(): c for c in qc.CITIES}
COUNTRY_OF = {"Abidjan": "CI", "Bouaké": "CI", "Yamoussoukro": "CI", "Dakar": "SN", "Thiès": "SN",
              "Bamako": "ML", "Ouagadougou": "BF", "Lomé": "TG", "Cotonou": "BJ", "Niamey": "NE"}

# --------------------------------------------------------------------------------------
# Schéma explicite de la zone staging
# --------------------------------------------------------------------------------------
metadata = MetaData()

stg_customers = Table(
    "stg_customers", metadata,
    Column("customer_id", String(10), primary_key=True),
    Column("email_pseudo", String(32), index=True),      # HMAC : jointure / dédoublonnage possibles
    Column("email_masked", String(120)),                 # pour l'investigation (a***@gmail.com)
    Column("phone_pseudo", String(32)),
    Column("national_id_pseudo", String(32)),
    Column("birth_year", Integer),
    Column("age_band", String(8)),
    Column("gender", String(1)),
    Column("city", String(40), index=True),
    Column("country", String(2)),
    Column("income_band", String(10)),
    Column("kyc_level", Integer),
    Column("segment", String(20)),
    Column("consent_marketing", Boolean),
    Column("created_at", DateTime),
    Column("_batch_id", String(40)),
    Column("_loaded_at", DateTime),
)

stg_transactions = Table(
    "stg_transactions", metadata,
    Column("transaction_id", String(12), primary_key=True),
    Column("customer_id", String(10), nullable=False, index=True),
    Column("merchant_id", String(8), nullable=False, index=True),
    Column("amount", Float, nullable=False),
    Column("currency", String(3), nullable=False),
    Column("channel", String(8)),
    Column("status", String(10)),
    Column("device_id", String(8)),
    Column("ip_network", String(15)),                    # IP tronquée /24
    Column("created_at", DateTime, nullable=False, index=True),
    Column("updated_at", DateTime, nullable=False),
    Column("_batch_id", String(40)),
    Column("_loaded_at", DateTime),
)

stg_merchants = Table(
    "stg_merchants", metadata,
    Column("merchant_id", String(8), primary_key=True),
    Column("name", String(120)),
    Column("mcc", String(4)),
    Column("category", String(40)),
    Column("city", String(40)),
    Column("country", String(2)),
    Column("risk_level", String(8)),
    Column("is_active", Boolean),
    Column("is_blacklisted", Boolean, nullable=False),
    Column("blacklist_severity", String(10)),
    Column("onboarded_at", DateTime),
    Column("updated_at", DateTime),
    Column("_loaded_at", DateTime),
)

quarantine = Table(
    "quarantine", metadata,
    Column("table_name", String(40)),
    Column("record_id", String(20)),
    Column("reason", String(200)),
    Column("payload", String),
    Column("quarantined_at", DateTime),
)


# --------------------------------------------------------------------------------------
# Nettoyage
# --------------------------------------------------------------------------------------
def _quarantine(df: pd.DataFrame, table: str, id_col: str, reason: str) -> pd.DataFrame:
    return pd.DataFrame({"table_name": table, "record_id": df[id_col].astype(str), "reason": reason,
                         "payload": df.astype(str).to_json(orient="records", lines=True, force_ascii=False).splitlines(),
                         "quarantined_at": datetime.now(timezone.utc)})


def parse_birth_date(s: pd.Series) -> pd.Series:
    """Formats mixtes : ISO (AAAA-MM-JJ) puis français (JJ/MM/AAAA, JJ-MM-AAAA). Invalide -> NaT."""
    iso = pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
    fr_slash = pd.to_datetime(s, format="%d/%m/%Y", errors="coerce")
    fr_dash = pd.to_datetime(s, format="%d-%m-%Y", errors="coerce")
    return iso.fillna(fr_slash).fillna(fr_dash)


def clean_customers(raw: pd.DataFrame, key: bytes) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = raw.copy()
    n0 = len(df)
    # 1) Normalisations élémentaires (cohérence)
    df["email"] = df["email"].str.strip().str.lower()
    df["city"] = df["city"].str.strip().str.upper().map(CITY_CANON)
    df["country"] = df["country"].fillna(df["city"].map(COUNTRY_OF))          # complétion par référentiel
    df["consent_marketing"] = df["consent_marketing"].str.strip().str.lower().map({"oui": True, "non": False})
    # 2) Validité
    df.loc[~df["phone"].fillna("").str.match(qc.PHONE_RE), "phone"] = None
    df.loc[~df["email"].fillna("").str.match(qc.EMAIL_RE), "email"] = None
    birth = parse_birth_date(df["birth_date"])
    age = (pd.Timestamp.now() - birth).dt.days / 365.25
    # 3) Exactitude : valeurs impossibles -> manquantes (on ne les "invente" pas)
    income = pd.to_numeric(df["monthly_income_xof"], errors="coerce")
    df["income"] = income.where(income >= 0)
    implausible_age = age.notna() & ((age < 18) | (age > 100))
    birth[implausible_age] = pd.NaT
    df["birth"] = birth
    df["age"] = age.where(~implausible_age)
    # 4) Unicité : doublons exacts puis quasi-doublons (même id) -> on garde la ligne la plus complète
    df["_completeness"] = df.notna().sum(axis=1)
    df = df.sort_values(["customer_id", "_completeness"], ascending=[True, False])
    dups = df[df.duplicated("customer_id", keep="first")]
    df = df.drop_duplicates("customer_id", keep="first")
    q = _quarantine(dups.drop(columns=["_completeness"]), "raw_customers", "customer_id", "doublon customer_id")
    logger.info("Clients : %d lignes brutes -> %d uniques (%d doublons en quarantaine)", n0, len(df), len(dups))

    # 5) Protection RGPD intégrée : pseudonymisation / généralisation / suppression
    out = pd.DataFrame({
        "customer_id": df["customer_id"],
        "email_pseudo": df["email"].map(lambda v: pseudonymize(v, key, "e_")),
        "email_masked": df["email"].map(mask_email),
        "phone_pseudo": df["phone"].map(lambda v: pseudonymize(v, key, "p_")),
        "national_id_pseudo": df["national_id"].map(lambda v: pseudonymize(v, key, "n_")),
        "birth_year": df["birth"].dt.year.astype("Int64"),
        "age_band": df["age"].map(age_band),
        "gender": df["gender"],
        "city": df["city"],
        "country": df["country"],
        "income_band": df["income"].map(income_band),
        "kyc_level": pd.to_numeric(df["kyc_level"], errors="coerce").astype("Int64"),
        "segment": df["segment"],
        "consent_marketing": df["consent_marketing"],
        "created_at": pd.to_datetime(df["created_at"], errors="coerce"),
        "_batch_id": df["_batch_id"],
    })
    # first_name, last_name, email, phone, national_id, monthly_income_xof : NON propagés (minimisation)
    return out, q


def clean_transactions(raw: pd.DataFrame, valid_customers: set[str], valid_merchants: set[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = raw.copy()
    n0 = len(df)
    # Dernière version de chaque transaction (la zone RAW est append-only)
    df = df.sort_values("updated_at").drop_duplicates("transaction_id", keep="last")
    df["currency"] = df["currency"].str.strip().str.upper()
    df["status"] = df["status"].str.strip().str.upper().replace({"SUCCES": "SUCCESS"})
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    rules = {
        "montant manquant": df["amount"].isna(),
        "montant ≤ 0": df["amount"] <= 0,
        "client inconnu (intégrité référentielle)": ~df["customer_id"].isin(valid_customers),
        "marchand inconnu (intégrité référentielle)": ~df["merchant_id"].isin(valid_merchants),
        "horodatage invalide": df["created_at"].isna(),
    }
    rejected = pd.Series(False, index=df.index)
    quarantined = []
    for reason, mask in rules.items():
        new = mask & ~rejected
        if new.any():
            quarantined.append(_quarantine(df[new], "raw_transactions", "transaction_id", reason))
        rejected |= mask
    clean = df[~rejected].copy()
    clean["ip_network"] = clean["ip_address"].map(truncate_ip)
    clean = clean[[c.name for c in stg_transactions.columns if c.name != "_loaded_at"]]
    logger.info("Transactions : %d lignes brutes -> %d versions courantes -> %d valides (%d en quarantaine)",
                n0, len(df), len(clean), int(rejected.sum()))
    return clean, (pd.concat(quarantined, ignore_index=True) if quarantined else pd.DataFrame())


def clean_merchants(raw: pd.DataFrame, blacklist: pd.DataFrame) -> pd.DataFrame:
    df = raw.sort_values("updated_at").drop_duplicates("merchant_id", keep="last").copy()
    bl = blacklist.rename(columns={"merchant_id": "bl_id"})[["bl_id", "severity"]]
    df = df.merge(bl, left_on="merchant_id", right_on="bl_id", how="left")
    df["is_blacklisted"] = df["bl_id"].notna()
    df["blacklist_severity"] = df["severity"]
    df["is_active"] = df["is_active"].astype(str).str.lower().isin(["1", "true"])
    for c in ("onboarded_at", "updated_at"):
        df[c] = pd.to_datetime(df[c], errors="coerce", utc=True).dt.tz_localize(None)
    return df[[c.name for c in stg_merchants.columns if c.name != "_loaded_at"]]


# --------------------------------------------------------------------------------------
# Chargement
# --------------------------------------------------------------------------------------
def load_table(engine, table: Table, df: pd.DataFrame) -> int:
    """Remplacement complet (idempotent) d'une table au schéma explicite, en une transaction."""
    df = df.copy()
    if "_loaded_at" in table.columns:
        df["_loaded_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    df = df.replace({np.nan: None, pd.NaT: None})
    with engine.begin() as conn:
        table.drop(conn, checkfirst=True)
        table.create(conn)
        if len(df):
            conn.execute(table.insert(), df.to_dict(orient="records"))
    logger.info("%s : %d lignes chargées", table.name, len(df))
    return len(df)


def staging_gate(engine) -> bool:
    """Contrôle qualité de SORTIE : la staging doit être exploitable sans réserve."""
    context = qc.get_context(config.PROJECT_ROOT)
    cust = pd.read_sql_table("stg_customers", engine)
    tx = pd.read_sql_table("stg_transactions", engine)
    ok = True
    checks = {
        "stg_customers": (cust, [
            gxe.ExpectColumnValuesToBeUnique(column="customer_id"),
            gxe.ExpectColumnValuesToBeInSet(column="city", value_set=qc.CITIES),
            gxe.ExpectColumnValuesToNotBeNull(column="country"),
            gxe.ExpectColumnValuesToMatchRegex(column="email_pseudo", regex=r"^e_[0-9a-f]{24}$", mostly=0.95),
        ]),
        "stg_transactions": (tx, [
            gxe.ExpectColumnValuesToBeUnique(column="transaction_id"),
            gxe.ExpectColumnValuesToBeBetween(column="amount", min_value=1),
            gxe.ExpectColumnValuesToBeInSet(column="currency", value_set=["XOF"]),
            gxe.ExpectColumnValuesToBeInSet(column="status", value_set=["SUCCESS", "FAILED", "REVERSED", "PENDING"]),
            gxe.ExpectColumnValuesToBeInSet(column="customer_id", value_set=cust["customer_id"].tolist()),
        ]),
    }
    for name, (df, exps) in checks.items():
        res = qc.run_suite(context, name, df, exps)
        logger.info("Contrôle de sortie %s : %s", name, "OK" if res.success else "ÉCHEC")
        ok &= bool(res.success)
    return ok


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    logging.getLogger("great_expectations").setLevel(logging.WARNING)
    key = require_key(config.PSEUDO_SECRET_KEY)
    engine = create_engine(config.WAREHOUSE_DB_URL)
    missing = {"raw_customers", "raw_transactions", "raw_merchants", "raw_merchant_blacklist"} - set(inspect(engine).get_table_names())
    if missing:
        logger.error("Tables RAW absentes : %s (lancer le TP1)", missing)
        return 1

    raw_c = pd.read_sql_table("raw_customers", engine)
    raw_t = pd.read_sql_table("raw_transactions", engine)
    raw_m = pd.read_sql_table("raw_merchants", engine)
    raw_bl = pd.read_sql_table("raw_merchant_blacklist", engine)

    customers, q_c = clean_customers(raw_c, key)
    merchants = clean_merchants(raw_m, raw_bl)
    tx, q_t = clean_transactions(raw_t, set(customers["customer_id"]), set(merchants["merchant_id"]))

    load_table(engine, stg_customers, customers)
    load_table(engine, stg_merchants, merchants)
    load_table(engine, stg_transactions, tx)
    load_table(engine, quarantine, pd.concat([q_c, q_t], ignore_index=True))

    with engine.begin() as conn:  # statistiques pour l'optimiseur (C1.9)
        conn.execute(text("ANALYZE") if engine.dialect.name == "sqlite" else text("ANALYZE stg_transactions"))

    if not staging_gate(engine):
        logger.error("La zone staging ne respecte pas les contrôles de sortie : arrêt du pipeline")
        return 1
    logger.info("Zone staging alimentée et validée ✔")
    return 0


if __name__ == "__main__":
    sys.exit(main())
