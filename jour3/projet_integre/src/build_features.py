"""Étape 3 — Staging pseudonymisée puis table curated `ml_fraud_features` (une ligne par transaction).

RAW (identifiants, IP, GPS en clair) ──► STAGING (dédoublonnée, pseudonymisée, généralisée) ──► CURATED

Choix structurants :
  * Pseudonymisation COHÉRENTE entre sources : même HMAC, même clé, même préfixe pour `customer_id` et
    `device_id` dans les transactions ET les sessions -> les jointures restent possibles sans identifiant.
  * Généralisation dès la staging : IP tronquée en /24, GPS ramené à une zone de service. La table finale
    ne contient ni IP, ni coordonnées, ni identifiant direct (critère éliminatoire de la grille).
  * Lignes irrécupérables envoyées en QUARANTAINE avec leur motif (jamais supprimées en silence).
  * Features calculées SANS FUITE DU FUTUR : pour une transaction à l'instant T, on ne regarde que les
    sessions et transactions STRICTEMENT antérieures à T.
  * Tables au schéma explicite (types, clé primaire), remplacées en une transaction : relancer l'étape
    produit exactement le même résultat (idempotence).

    python jour3/projet_integre/src/build_features.py
"""
from __future__ import annotations

import logging
import sys
from collections import Counter
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sqlalchemy import Column, DateTime, Float, Integer, MetaData, String, Table, text

from common import audit, get_engine, load_params, new_run_id, setup_logging
from fil_rouge import config
from privacy import (PREFIX_APPAREIL, PREFIX_CLIENT, RAYON_ZONE_KM, ZONES, pseudonymize_series, require_key,
                     truncate_ip)

logger = logging.getLogger("projet.features")

# --------------------------------------------------------------------------------------
# Schéma explicite
# --------------------------------------------------------------------------------------
metadata = MetaData()

stg_transactions = Table(
    "stg_transactions", metadata,
    Column("transaction_id", String(12), primary_key=True),
    Column("customer_pseudo", String(26), nullable=False, index=True),
    Column("device_pseudo", String(26)),
    Column("merchant_id", String(8), nullable=False),
    Column("amount", Float, nullable=False),
    Column("currency", String(3)),
    Column("channel", String(8)),
    Column("status", String(10)),
    Column("ip_network", String(15)),                 # /24
    Column("created_at", DateTime, nullable=False, index=True),
    Column("updated_at", DateTime, nullable=False),
)

stg_sessions = Table(
    "stg_sessions", metadata,
    Column("session_id", String(10), primary_key=True),
    Column("customer_pseudo", String(26), nullable=False, index=True),
    Column("device_pseudo", String(26), nullable=False),
    Column("os", String(10)),
    Column("app_version", String(10)),
    Column("ip_network", String(15)),                 # /24
    Column("zone_geo", String(20)),                   # zone de service, plus de coordonnées
    Column("login_at", DateTime, nullable=False, index=True),
    Column("duration_s", Integer),
)

stg_fraud_labels = Table(
    "stg_fraud_labels", metadata,
    Column("transaction_id", String(12), primary_key=True),
    Column("fraud_type", String(30)),
    Column("confirmed_at", DateTime, nullable=False),
)

stg_merchants = Table(
    "stg_merchants", metadata,
    Column("merchant_id", String(8), primary_key=True),
    Column("mcc", String(4)),
    Column("category", String(40)),
    Column("country", String(2)),
    Column("risk_level", String(8)),
    Column("updated_at", DateTime),
)

quarantine = Table(
    "quarantine", metadata,
    Column("table_name", String(40)),
    Column("record_id", String(20)),
    Column("reason", String(200)),
    Column("quarantined_at", DateTime),
)

FEATURES_SESSIONS = ["nb_sessions_24h_avant", "minutes_depuis_derniere_session", "duree_derniere_session_s",
                     "a_historique_session", "appareil_deja_vu_session", "appareil_connu_plus_24h",
                     "nb_appareils_distincts_7j", "nb_zones_distinctes_24h", "zone_derniere_session_inhabituelle"]
FEATURES_TRANSACTION = ["montant_log", "heure", "est_nuit", "canal_app", "canal_ussd", "canal_card", "canal_web",
                        "velocite_1h", "velocite_24h", "risque_marchand"]
FEATURES = FEATURES_TRANSACTION + FEATURES_SESSIONS

ml_fraud_features = Table(
    "ml_fraud_features", metadata,
    Column("transaction_id", String(12), primary_key=True),
    Column("customer_pseudo", String(26), nullable=False, index=True),
    Column("created_at", DateTime, nullable=False, index=True),
    *[Column(f, Float if f in ("montant_log", "minutes_depuis_derniere_session") else Integer) for f in FEATURES],
    Column("is_fraud", Integer, nullable=False),
    Column("fraud_type", String(30)),
    Column("fraud_confirmed_at", DateTime),
    Column("_built_at", DateTime),
)


# --------------------------------------------------------------------------------------
# Staging
# --------------------------------------------------------------------------------------
def _quarantine(df: pd.DataFrame, table: str, id_col: str, reason: str) -> pd.DataFrame:
    return pd.DataFrame({"table_name": table, "record_id": df[id_col].astype(str), "reason": reason,
                         "quarantined_at": datetime.now(timezone.utc).replace(tzinfo=None)})


def _apply_rules(df: pd.DataFrame, table: str, id_col: str, rules: dict) -> tuple[pd.DataFrame, list]:
    """Applique des règles de rejet dans l'ordre ; une ligne n'est mise en quarantaine qu'une fois."""
    rejected = pd.Series(False, index=df.index)
    q = []
    for reason, mask in rules.items():
        new = mask & ~rejected
        if new.any():
            q.append(_quarantine(df[new], table, id_col, reason))
        rejected |= mask
    return df[~rejected].copy(), q


def zone_geo_vectorise(lat: pd.Series, lon: pd.Series) -> pd.Series:
    """Zone de service la plus proche (haversine vectorisé) ; au-delà du rayon : 'hors_zone'."""
    names = list(ZONES)
    zlat = np.radians([ZONES[z][0] for z in names])
    zlon = np.radians([ZONES[z][1] for z in names])
    la, lo = np.radians(lat.to_numpy())[:, None], np.radians(lon.to_numpy())[:, None]
    a = np.sin((zlat - la) / 2) ** 2 + np.cos(la) * np.cos(zlat) * np.sin((zlon - lo) / 2) ** 2
    dist = 2 * 6371 * np.arcsin(np.sqrt(a))
    best = dist.argmin(axis=1)
    out = np.where(dist[np.arange(len(best)), best] <= RAYON_ZONE_KM, np.array(names)[best], "hors_zone")
    return pd.Series(out, index=lat.index)


def build_merchants(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.sort_values("updated_at").drop_duplicates("merchant_id", keep="last").copy()
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True).dt.tz_localize(None)
    # Raison sociale et contacts NON propagés : inutiles au modèle (minimisation, cf. entrepreneurs individuels)
    return df[[c.name for c in stg_merchants.columns]]


def build_transactions(raw: pd.DataFrame, merchants: set[str], key: bytes) -> tuple[pd.DataFrame, list]:
    df = raw.sort_values("updated_at").drop_duplicates("transaction_id", keep="last").copy()   # version courante
    df["currency"] = df["currency"].str.strip().str.upper()
    df["status"] = df["status"].str.strip().str.upper().replace({"SUCCES": "SUCCESS"})
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
    clean, q = _apply_rules(df, "raw_transactions", "transaction_id", {
        "montant manquant": df["amount"].isna(),
        "montant ≤ 0": df["amount"] <= 0,
        "marchand inconnu (intégrité référentielle)": ~df["merchant_id"].isin(merchants),
        "horodatage invalide": df["created_at"].isna(),
    })
    clean["customer_pseudo"] = pseudonymize_series(clean["customer_id"], key, PREFIX_CLIENT)
    clean["device_pseudo"] = pseudonymize_series(clean["device_id"], key, PREFIX_APPAREIL)
    clean["ip_network"] = clean["ip_address"].map(truncate_ip)
    logger.info("Transactions : %d lignes brutes -> %d versions courantes -> %d valides", len(raw), len(df), len(clean))
    return clean[[c.name for c in stg_transactions.columns]], q


def build_sessions(raw: pd.DataFrame, key: bytes) -> tuple[pd.DataFrame, list]:
    df = raw.drop_duplicates("session_id", keep="last").copy()
    df["login_at"] = pd.to_datetime(df["login_at"], errors="coerce")
    lat, lon = pd.to_numeric(df["latitude"], errors="coerce"), pd.to_numeric(df["longitude"], errors="coerce")
    clean, q = _apply_rules(df, "raw_sessions", "session_id", {
        "horodatage invalide": df["login_at"].isna(),
        "client ou appareil manquant": df["customer_id"].isna() | df["device_id"].isna(),
        "coordonnées invalides": lat.isna() | lon.isna() | ~lat.between(-90, 90) | ~lon.between(-180, 180),
    })
    clean["zone_geo"] = zone_geo_vectorise(lat[clean.index], lon[clean.index])
    clean["customer_pseudo"] = pseudonymize_series(clean["customer_id"], key, PREFIX_CLIENT)
    clean["device_pseudo"] = pseudonymize_series(clean["device_id"], key, PREFIX_APPAREIL)
    clean["ip_network"] = clean["ip_address"].map(truncate_ip)
    clean["duration_s"] = pd.to_numeric(clean["duration_s"], errors="coerce").astype("Int64")
    logger.info("Sessions : %d lignes brutes -> %d valides (zones : %s)", len(raw), len(clean),
                clean["zone_geo"].value_counts().to_dict())
    return clean[[c.name for c in stg_sessions.columns]], q


def build_labels(raw: pd.DataFrame, tx: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    df = raw.sort_values("confirmed_at").drop_duplicates("transaction_id", keep="last").copy()
    df["confirmed_at"] = pd.to_datetime(df["confirmed_at"], errors="coerce")
    created = df["transaction_id"].map(tx.set_index("transaction_id")["created_at"])
    clean, q = _apply_rules(df, "raw_fraud_labels", "transaction_id", {
        "étiquette sans transaction valide": created.isna(),
        "confirmation antérieure à la transaction (fuite)": df["confirmed_at"] < created,
        "date de confirmation invalide": df["confirmed_at"].isna(),
    })
    return clean[[c.name for c in stg_fraud_labels.columns]], q


# --------------------------------------------------------------------------------------
# Features (point-in-time)
# --------------------------------------------------------------------------------------
def velocity(tx: pd.DataFrame, window: str) -> np.ndarray:
    """Nb de transactions du client dans la fenêtre PASSÉE (transaction courante exclue : closed='left')."""
    s = tx.set_index("created_at").groupby("customer_pseudo")["amount"].rolling(window, closed="left").count()
    return s.reset_index(level=0, drop=True).fillna(0).to_numpy()


def session_features(tx: pd.DataFrame, sessions: pd.DataFrame, p: dict) -> pd.DataFrame:
    """Pour chaque transaction (client c, instant T), ne regarde que les sessions de c STRICTEMENT avant T."""
    win_s = np.timedelta64(int(p["fenetre_sessions_h"]), "h")
    win_d = np.timedelta64(int(p["fenetre_appareils_j"]), "D")
    cap = float(p["plafond_minutes"])
    by_cust = {c: g.sort_values("login_at") for c, g in sessions.groupby("customer_pseudo")}
    out = np.zeros((len(tx), len(FEATURES_SESSIONS)))
    for pos, (cust, t_time, t_device) in enumerate(zip(tx["customer_pseudo"], tx["created_at"].to_numpy(),
                                                       tx["device_pseudo"])):
        g = by_cust.get(cust)
        if g is None:
            out[pos] = [0, cap, 0, 0, 0, 0, 0, 0, 0]
            continue
        times = g["login_at"].to_numpy()
        idx = np.searchsorted(times, t_time, side="left")            # sessions [0, idx) : strictement avant T
        i24 = np.searchsorted(times, t_time - win_s, side="left")
        i7 = np.searchsorted(times, t_time - win_d, side="left")
        devices, zones, durations = g["device_pseudo"].to_numpy(), g["zone_geo"].to_numpy(), g["duration_s"].to_numpy()
        if idx == 0:
            out[pos] = [0, cap, 0, 0, 0, 0, 0, 0, 0]
            continue
        last = idx - 1
        minutes = (t_time - times[last]) / np.timedelta64(1, "m")
        habituelle = Counter(zones[:last]).most_common(1)[0][0] if last > 0 else zones[last]
        out[pos] = [
            idx - i24,                                               # sessions dans les 24 h précédentes
            min(minutes, cap),                                       # récence de la dernière session
            durations[last],
            1,
            int(t_device in set(devices[:idx])),                     # appareil déjà vu en session
            int(t_device in set(devices[:i24])),                     # ... et connu depuis plus de 24 h
            len(set(devices[i7:idx])),                               # appareils distincts sur 7 jours
            len(set(zones[i24:idx])),                                # zones distinctes sur 24 h
            int(zones[last] != habituelle),                          # dernière connexion hors zone habituelle
        ]
    return pd.DataFrame(out, columns=FEATURES_SESSIONS, index=tx.index)


def build_features(tx: pd.DataFrame, sessions: pd.DataFrame, labels: pd.DataFrame, merchants: pd.DataFrame,
                   p: dict) -> pd.DataFrame:
    tx = tx.sort_values(["customer_pseudo", "created_at"]).reset_index(drop=True)
    f = pd.DataFrame({"transaction_id": tx["transaction_id"], "customer_pseudo": tx["customer_pseudo"],
                      "created_at": tx["created_at"]})
    f["montant_log"] = np.log1p(tx["amount"])
    f["heure"] = tx["created_at"].dt.hour
    f["est_nuit"] = f["heure"].isin(p["heures_nuit"]).astype(int)
    for ch in ("app", "ussd", "card", "web"):
        f[f"canal_{ch}"] = (tx["channel"] == ch).astype(int)
    for w in p["fenetres_velocite"]:
        f[f"velocite_{w}"] = velocity(tx, w).astype(int)
    f["risque_marchand"] = (tx["merchant_id"].map(merchants.set_index("merchant_id")["risk_level"])
                            .map({"low": 0, "medium": 1, "high": 2}).fillna(1).astype(int))
    f = pd.concat([f, session_features(tx, sessions, p)], axis=1)
    lab = labels.set_index("transaction_id")
    f["is_fraud"] = f["transaction_id"].isin(lab.index).astype(int)
    f["fraud_type"] = f["transaction_id"].map(lab["fraud_type"])
    f["fraud_confirmed_at"] = f["transaction_id"].map(lab["confirmed_at"])
    return f.sort_values("transaction_id").reset_index(drop=True)


# --------------------------------------------------------------------------------------
# Chargement
# --------------------------------------------------------------------------------------
def load_table(engine, table: Table, df: pd.DataFrame) -> int:
    """Remplacement complet (idempotent) d'une table au schéma explicite, en une seule transaction."""
    df = df.copy()
    if "_built_at" in table.columns:
        df["_built_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    df = df.astype(object).where(df.notna(), None)
    with engine.begin() as conn:
        table.drop(conn, checkfirst=True)
        table.create(conn)
        if len(df):
            conn.execute(table.insert(), df.to_dict(orient="records"))
    logger.info("%s : %d lignes", table.name, len(df))
    return len(df)


def main() -> int:
    run_id = new_run_id()
    setup_logging("features", run_id)
    started = datetime.now(timezone.utc)
    engine = get_engine()
    try:
        key = require_key(config.PSEUDO_SECRET_KEY)
        p = load_params("features")
        merchants = build_merchants(pd.read_sql_table("raw_merchants", engine))
        tx, q_tx = build_transactions(pd.read_sql_table("raw_transactions", engine), set(merchants["merchant_id"]), key)
        sessions, q_s = build_sessions(pd.read_sql_table("raw_sessions", engine), key)
        labels, q_l = build_labels(pd.read_sql_table("raw_fraud_labels", engine), tx)
        features = build_features(tx, sessions, labels, merchants, p)

        load_table(engine, stg_merchants, merchants)
        load_table(engine, stg_transactions, tx)
        load_table(engine, stg_sessions, sessions)
        load_table(engine, stg_fraud_labels, labels)
        q = pd.concat(q_tx + q_s + q_l, ignore_index=True) if (q_tx + q_s + q_l) else pd.DataFrame(
            columns=[c.name for c in quarantine.columns])
        load_table(engine, quarantine, q)
        n = load_table(engine, ml_fraud_features, features)
        if engine.dialect.name == "sqlite":
            with engine.begin() as conn:
                conn.execute(text("ANALYZE"))
        logger.info("ml_fraud_features : %d transactions, %d fraudes (%.2f %%), %d lignes en quarantaine",
                    n, int(features["is_fraud"].sum()), features["is_fraud"].mean() * 100, len(q))
        audit(engine, run_id, "build_features", "full", "SUCCESS", n, started, f"quarantaine={len(q)}")
        return 0
    except Exception as exc:  # noqa: BLE001
        logger.exception("Échec de la construction des features")
        audit(engine, run_id, "build_features", "full", "FAILED", 0, started, str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
