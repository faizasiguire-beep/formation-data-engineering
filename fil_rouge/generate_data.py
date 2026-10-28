"""Générateur des données du fil rouge NexaPay.

Produit, de façon reproductible (graine fixe), toutes les sources utilisées dans le module :

    data/sources/
        core_banking.db          SQLite  - transactions + cas de fraude confirmés (source "base de données")
        customers_crm.csv        CSV ';' - export CRM des clients, volontairement "sale"
        merchant_blacklist.json  JSON    - liste noire de marchands (structure imbriquée)
        app_sessions.parquet     Parquet - sessions de l'application mobile (device, IP...)
        api/merchants.json       données servies par l'API simulée (source "API REST")
        extra/credit_bureau.parquet, extra/sanctions_list.json  - sources des briefs projet

Les défauts de qualité sont INTENTIONNELS (doublons, valeurs manquantes, formats hétérogènes,
valeurs aberrantes) : ils servent de matière au TP2.

Usage :
    python -m fil_rouge.generate_data                 # génération complète
    python -m fil_rouge.generate_data --append 500    # ajoute 500 nouvelles transactions "maintenant"
                                                      # (pour tester l'extraction incrémentale)
"""
from __future__ import annotations

import argparse
import json
import random
import sqlite3
import string
import unicodedata
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

from fil_rouge.config import SOURCES_DIR, ensure_dirs

SEED = 42
N_CUSTOMERS = 5_000
N_MERCHANTS = 320
N_TRANSACTIONS = 60_000
HISTORY_DAYS = 90

CITIES = ["Abidjan", "Bouaké", "Yamoussoukro", "Dakar", "Thiès", "Bamako", "Ouagadougou", "Lomé", "Cotonou", "Niamey"]
COUNTRY_OF = {"Abidjan": "CI", "Bouaké": "CI", "Yamoussoukro": "CI", "Dakar": "SN", "Thiès": "SN",
              "Bamako": "ML", "Ouagadougou": "BF", "Lomé": "TG", "Cotonou": "BJ", "Niamey": "NE"}
PHONE_PREFIX = {"CI": "+225", "SN": "+221", "ML": "+223", "BF": "+226", "TG": "+228", "BJ": "+229", "NE": "+227"}
MCC = {"5411": "Alimentation", "5812": "Restauration", "4814": "Télécom", "5732": "Électronique",
       "6011": "Retrait espèces", "4900": "Énergie & eau", "5999": "Commerce divers", "7995": "Jeux & paris",
       "4121": "Transport", "5912": "Pharmacie"}
CHANNELS = ["app", "ussd", "card", "web"]


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# --------------------------------------------------------------------------------------
# Marchands (source API)
# --------------------------------------------------------------------------------------
def make_merchants(rng: random.Random, fake: Faker) -> list[dict]:
    now = _now()
    merchants = []
    for i in range(1, N_MERCHANTS + 1):
        mcc = rng.choice(list(MCC))
        city = rng.choice(CITIES)
        created = now - timedelta(days=rng.randint(100, 1500))
        updated = created + timedelta(days=rng.randint(0, (now - created).days))
        merchants.append({
            "merchant_id": f"M{i:05d}",
            "name": fake.company(),
            "mcc": mcc,
            "category": MCC[mcc],
            "city": city,
            "country": COUNTRY_OF[city],
            "risk_level": rng.choices(["low", "medium", "high"], weights=[70, 22, 8])[0],
            "contact_email": fake.company_email(),
            "onboarded_at": created.isoformat(),
            "updated_at": updated.isoformat(),
            "is_active": rng.random() > 0.05,
        })
    return merchants


def make_blacklist(rng: random.Random, merchants: list[dict]) -> dict:
    risky = [m for m in merchants if m["risk_level"] == "high"]
    listed = rng.sample(risky, k=min(15, len(risky)))
    now = _now()
    return {
        "source": "Cellule conformité NexaPay",
        "generated_at": now.isoformat(),
        "schema_version": "2.1",
        "entries": [
            {
                "merchant": {"id": m["merchant_id"], "name": m["name"]},
                "reason": rng.choice(["chargebacks répétés", "signalement BCEAO", "fraude avérée", "KYB incomplet"]),
                "listed_at": (now - timedelta(days=rng.randint(1, 400))).date().isoformat(),
                "severity": rng.choice(["block", "review"]),
            }
            for m in listed
        ],
    }


# --------------------------------------------------------------------------------------
# Clients (source CSV, volontairement sale)
# --------------------------------------------------------------------------------------
def _random_birth_date(rng: random.Random) -> datetime:
    return datetime(1950, 1, 1) + timedelta(days=rng.randint(0, 365 * 55))


def _format_birth_date(rng: random.Random, d: datetime) -> str:
    # Formats hétérogènes : défaut de validité classique d'un export CRM
    fmt = rng.choices(["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"], weights=[80, 15, 5])[0]
    return d.strftime(fmt)


def make_customers(rng: random.Random, fake: Faker) -> pd.DataFrame:
    now = _now()
    rows = []
    for i in range(1, N_CUSTOMERS + 1):
        city = rng.choice(CITIES)
        country = COUNTRY_OF[city]
        first, last = fake.first_name(), fake.last_name()
        email = f"{first}.{last}{rng.randint(1, 999)}@{rng.choice(['gmail.com', 'yahoo.fr', 'orange.ci', 'outlook.com'])}"
        email = email.lower().replace(" ", "").replace("'", "")
        if rng.random() > 0.02:  # 2 % d'emails accentués conservés comme défaut de validité
            email = unicodedata.normalize("NFKD", email).encode("ascii", "ignore").decode()
        phone = PHONE_PREFIX[country] + "".join(rng.choices(string.digits, k=10 if country == "CI" else 8))
        rows.append({
            "customer_id": f"C{i:06d}",
            "first_name": first,
            "last_name": last,
            "email": email,
            "phone": phone,
            "birth_date": _format_birth_date(rng, _random_birth_date(rng)),
            "gender": rng.choice(["F", "M"]),
            "city": city,
            "country": country,
            "national_id": "".join(rng.choices(string.ascii_uppercase, k=2)) + "".join(rng.choices(string.digits, k=9)),
            "monthly_income_xof": int(rng.lognormvariate(12.3, 0.6)),
            "kyc_level": rng.choices([1, 2, 3], weights=[30, 50, 20])[0],
            "segment": rng.choice(["particulier", "particulier", "particulier", "commerçant", "étudiant"]),
            "consent_marketing": rng.choice(["oui", "non", "Oui", "NON", ""]),
            "created_at": _iso(now - timedelta(days=rng.randint(91, 1200))),
        })
    df = pd.DataFrame(rows)

    # ---- Injection des défauts de qualité (matière du TP2) ----
    n = len(df)
    idx = lambda frac: df.sample(frac=frac, random_state=rng.randint(0, 10_000)).index  # noqa: E731
    df.loc[idx(0.03), "email"] = None                                     # complétude
    df.loc[idx(0.02), "phone"] = "000"                                    # validité
    df.loc[idx(0.01), "birth_date"] = "2031-02-30"                       # date impossible
    df.loc[idx(0.005), "birth_date"] = "1899-01-01"                      # âge aberrant
    upper_idx = idx(0.04)
    df.loc[upper_idx, "city"] = df.loc[upper_idx, "city"].str.upper()   # cohérence (casse)
    df.loc[idx(0.01), "monthly_income_xof"] = -1                          # exactitude
    df.loc[idx(0.02), "country"] = None
    # Doublons exacts + quasi-doublons (même client, email en majuscules)
    dup_exact = df.sample(n=int(n * 0.01), random_state=SEED)
    dup_near = df.sample(n=int(n * 0.01), random_state=SEED + 1).copy()
    dup_near["email"] = dup_near["email"].str.upper()
    df = pd.concat([df, dup_exact, dup_near], ignore_index=True)
    return df.sample(frac=1, random_state=SEED).reset_index(drop=True)


# --------------------------------------------------------------------------------------
# Transactions (source base de données "core banking")
# --------------------------------------------------------------------------------------
def make_transactions(rng: random.Random, customers: pd.DataFrame, merchants: list[dict],
                      blacklist_ids: set[str], n: int, start: datetime, end: datetime,
                      id_offset: int = 0) -> tuple[pd.DataFrame, pd.DataFrame]:
    np_rng = np.random.default_rng(rng.randint(0, 10**9))
    cust_ids = customers["customer_id"].drop_duplicates().to_numpy()
    fraudsters = set(np_rng.choice(cust_ids, size=max(1, len(cust_ids) // 60), replace=False))
    merchant_ids = [m["merchant_id"] for m in merchants]
    bl = sorted(blacklist_ids)
    span = (end - start).total_seconds()

    rows, frauds = [], []
    for k in range(n):
        cid = str(np_rng.choice(cust_ids))
        is_fraudster = cid in fraudsters
        fraud = is_fraudster and np_rng.random() < 0.35 or (not is_fraudster and np_rng.random() < 0.002)
        created = start + timedelta(seconds=float(np_rng.random() * span))
        if fraud and np_rng.random() < 0.4:  # la fraude se concentre (un peu) la nuit
            created = max(start, created.replace(hour=int(np_rng.integers(0, 5))))
        amount = float(np.round(np_rng.lognormal(9.2, 1.0), 0))
        if fraud and np_rng.random() < 0.5:  # une fraude sur deux seulement porte sur un montant inhabituel
            amount = float(np.round(amount * np_rng.uniform(2, 6), 0))
        merchant = rng.choice(bl) if (fraud and np_rng.random() < 0.2 and bl) else rng.choice(merchant_ids)
        # Appareil : le client légitime change parfois de téléphone ; le fraudeur pas toujours détectable
        usual_device = f"D{zlib.crc32(cid.encode()) % 9000 + 1:05d}"
        if (fraud and np_rng.random() < 0.55) or (not fraud and np_rng.random() < 0.03):
            device = f"D{int(np_rng.integers(1, 9000)):05d}"
        else:
            device = usual_device
        status = rng.choices(["SUCCESS", "FAILED", "REVERSED", "PENDING"], weights=[88, 7, 2, 3])[0]
        updated = created + timedelta(minutes=int(np_rng.integers(0, 4000))) if status in ("REVERSED", "PENDING") else created
        updated = min(updated, end)
        tx_id = f"T{id_offset + k + 1:09d}"
        rows.append({
            "transaction_id": tx_id,
            "customer_id": cid,
            "merchant_id": merchant,
            "amount": amount,
            "currency": "XOF",
            "channel": rng.choices(CHANNELS, weights=[55, 25, 12, 8])[0],
            "status": status,
            "device_id": device,
            "ip_address": f"{int(np_rng.integers(41, 197))}.{int(np_rng.integers(0, 255))}.{int(np_rng.integers(0, 255))}.{int(np_rng.integers(1, 254))}",
            "created_at": _iso(created),
            "updated_at": _iso(updated),
        })
        if fraud:
            frauds.append({"transaction_id": tx_id, "confirmed_at": _iso(min(created + timedelta(days=int(np_rng.integers(1, 20))), end)),
                           "fraud_type": rng.choice(["account_takeover", "sim_swap", "merchant_collusion", "stolen_card"])})
    tx = pd.DataFrame(rows)
    # Défauts de qualité côté base (plus rares qu'au CRM)
    sample = tx.sample(frac=0.003, random_state=SEED).index
    tx.loc[sample, "amount"] = None
    tx.loc[tx.sample(frac=0.002, random_state=SEED + 2).index, "currency"] = "xof"
    tx.loc[tx.sample(frac=0.001, random_state=SEED + 3).index, "amount"] = -500.0
    tx.loc[tx.sample(frac=0.001, random_state=SEED + 4).index, "status"] = "SUCCES"
    return tx, pd.DataFrame(frauds)


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id TEXT PRIMARY KEY,
    customer_id    TEXT NOT NULL,
    merchant_id    TEXT NOT NULL,
    amount         REAL,
    currency       TEXT,
    channel        TEXT,
    status         TEXT,
    device_id      TEXT,
    ip_address     TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tx_updated_at ON transactions(updated_at);
CREATE TABLE IF NOT EXISTS fraud_cases (
    transaction_id TEXT PRIMARY KEY REFERENCES transactions(transaction_id),
    confirmed_at   TEXT NOT NULL,
    fraud_type     TEXT
);
"""


def write_core_banking(db_path: Path, tx: pd.DataFrame, frauds: pd.DataFrame, replace: bool) -> None:
    if replace and db_path.exists():
        db_path.unlink()
    with sqlite3.connect(db_path) as con:
        con.executescript(SCHEMA_SQL)
        tx.to_sql("transactions", con, if_exists="append", index=False)
        if not frauds.empty:
            frauds.to_sql("fraud_cases", con, if_exists="append", index=False)


# --------------------------------------------------------------------------------------
# Sessions applicatives (Parquet) + sources des briefs projet
# --------------------------------------------------------------------------------------
CITY_COORDS = {"Abidjan": (5.36, -4.01), "Bouaké": (7.69, -5.03), "Yamoussoukro": (6.82, -5.28), "Dakar": (14.72, -17.47),
               "Thiès": (14.79, -16.93), "Bamako": (12.64, -8.00), "Ouagadougou": (12.37, -1.52), "Lomé": (6.13, 1.22),
               "Cotonou": (6.37, 2.39), "Niamey": (13.51, 2.11)}


def make_sessions(customers: pd.DataFrame, tx: pd.DataFrame, frauds: pd.DataFrame, seed: int = SEED,
                  id_offset: int = 0, background_per_customer: float = 3.0) -> pd.DataFrame:
    """Sessions de l'application, COHÉRENTES avec les transactions :
      * sessions de fond du client (appareil habituel, près de sa ville) ;
      * une session précède souvent une transaction légitime (même appareil) ;
      * une partie des fraudes est précédée d'une session « attaquant » : appareil de la fraude,
        quelques minutes avant, depuis une autre ville.
    """
    np_rng = np.random.default_rng(seed)
    home = (customers.drop_duplicates("customer_id").set_index("customer_id")["city"]
            .fillna("Abidjan").str.title().map(lambda c: c if c in CITY_COORDS else "Abidjan"))
    usual = lambda cid: f"D{zlib.crc32(cid.encode()) % 9000 + 1:05d}"  # noqa: E731
    rows = []

    def add(cid, device, when, city, jitter=0.05):
        lat, lon = CITY_COORDS[city]
        rows.append((cid, device, when, round(lat + np_rng.normal(0, jitter), 5), round(lon + np_rng.normal(0, jitter), 5)))

    if background_per_customer:
        now = _now()
        for cid, city in home.items():
            for _ in range(np_rng.poisson(background_per_customer)):
                add(cid, usual(cid), now - timedelta(seconds=int(np_rng.integers(0, HISTORY_DAYS * 86400))), city)
    fraud_ids = set(frauds["transaction_id"]) if len(frauds) else set()
    for t in tx.itertuples(index=False):
        created = datetime.strptime(t.created_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        city = home.get(t.customer_id, "Abidjan")
        if t.transaction_id in fraud_ids:
            if np_rng.random() < 0.6:
                other = str(np_rng.choice([c for c in CITY_COORDS if c != city]))
                add(t.customer_id, t.device_id, created - timedelta(minutes=float(np_rng.uniform(1, 15))), other)
        elif np_rng.random() < 0.5:
            add(t.customer_id, t.device_id, created - timedelta(minutes=float(np_rng.uniform(1, 30))), city)

    n = len(rows)
    df = pd.DataFrame(rows, columns=["customer_id", "device_id", "login_at", "latitude", "longitude"])
    df.insert(0, "session_id", [f"S{i:08d}" for i in range(id_offset + 1, id_offset + n + 1)])
    df["os"] = np_rng.choice(["Android", "iOS", "KaiOS"], n, p=[0.78, 0.17, 0.05])
    df["app_version"] = np_rng.choice(["5.2.0", "5.3.1", "5.4.0", "4.9.8"], n)
    df["ip_address"] = [f"{a}.{b}.{c}.{d}" for a, b, c, d in np_rng.integers(1, 254, (n, 4))]
    df["login_at"] = pd.to_datetime(df["login_at"], utc=True)
    df["duration_s"] = np_rng.integers(5, 1800, n)
    cols = ["session_id", "customer_id", "device_id", "os", "app_version", "ip_address", "latitude", "longitude",
            "login_at", "duration_s"]
    return df[cols].sort_values("login_at").reset_index(drop=True)


def make_credit_bureau(customers: pd.DataFrame) -> pd.DataFrame:
    np_rng = np.random.default_rng(SEED + 10)
    cust = customers["customer_id"].drop_duplicates()
    sample = cust.sample(frac=0.7, random_state=SEED).to_numpy()
    n = len(sample)
    return pd.DataFrame({
        "customer_id": sample,
        "bureau_score": np_rng.integers(300, 850, n),
        "open_loans": np_rng.poisson(1.2, n),
        "outstanding_xof": np.round(np_rng.lognormal(12, 1.2, n), 0),
        "days_past_due_max": np_rng.choice([0, 0, 0, 15, 30, 60, 90], n),
        "report_date": pd.Timestamp.now(tz="UTC").normalize(),
    })


def make_sanctions(rng: random.Random, fake: Faker, customers: pd.DataFrame) -> dict:
    # Quelques vrais clients apparaissent (avec variations d'orthographe) pour le brief AML
    hits = customers.drop_duplicates("customer_id").sample(n=12, random_state=SEED)
    entries = [{"list_id": f"SAN-{i:04d}", "full_name": fake.name(), "birth_year": rng.randint(1950, 1995),
                "programme": rng.choice(["UE", "ONU", "OFAC"])} for i in range(1, 189)]
    for j, (_, c) in enumerate(hits.iterrows(), start=189):
        entries.append({"list_id": f"SAN-{j:04d}", "full_name": f"{c['last_name'].upper()} {c['first_name']}",
                        "birth_year": None, "programme": "ONU"})
    return {"published_at": _now().date().isoformat(), "entries": entries}


# --------------------------------------------------------------------------------------
def generate_all() -> None:
    ensure_dirs()
    rng = random.Random(SEED)
    Faker.seed(SEED)
    fake = Faker("fr_FR")
    (SOURCES_DIR / "api").mkdir(exist_ok=True)
    (SOURCES_DIR / "extra").mkdir(exist_ok=True)

    merchants = make_merchants(rng, fake)
    (SOURCES_DIR / "api" / "merchants.json").write_text(json.dumps(merchants, ensure_ascii=False, indent=1), encoding="utf-8")

    blacklist = make_blacklist(rng, merchants)
    (SOURCES_DIR / "merchant_blacklist.json").write_text(json.dumps(blacklist, ensure_ascii=False, indent=2), encoding="utf-8")

    customers = make_customers(rng, fake)
    customers.to_csv(SOURCES_DIR / "customers_crm.csv", sep=";", index=False, encoding="utf-8")

    end = _now()
    start = end - timedelta(days=HISTORY_DAYS)
    bl_ids = {e["merchant"]["id"] for e in blacklist["entries"]}
    tx, frauds = make_transactions(rng, customers, merchants, bl_ids, N_TRANSACTIONS, start, end)
    write_core_banking(SOURCES_DIR / "core_banking.db", tx, frauds, replace=True)

    sessions = make_sessions(customers, tx, frauds)
    sessions.to_parquet(SOURCES_DIR / "app_sessions.parquet", index=False)
    make_credit_bureau(customers).to_parquet(SOURCES_DIR / "extra" / "credit_bureau.parquet", index=False)
    (SOURCES_DIR / "extra" / "sanctions_list.json").write_text(
        json.dumps(make_sanctions(rng, fake, customers), ensure_ascii=False, indent=1), encoding="utf-8")

    print(f"✔ Sources générées dans {SOURCES_DIR}")
    print(f"  clients CRM : {len(customers):>6} lignes (doublons inclus)")
    print(f"  marchands   : {len(merchants):>6} (servis par l'API)")
    print(f"  transactions: {len(tx):>6}  | fraudes confirmées : {len(frauds)}")
    print(f"  sessions app: {len(sessions):>6}")


def append_transactions(n: int) -> None:
    """Simule l'activité du jour : nouvelles transactions + mises à jour de statut."""
    db = SOURCES_DIR / "core_banking.db"
    if not db.exists():
        raise SystemExit("Base source absente : lancez d'abord `python -m fil_rouge.generate_data`.")
    rng = random.Random()
    customers = pd.read_csv(SOURCES_DIR / "customers_crm.csv", sep=";", usecols=["customer_id", "city"])
    merchants = json.loads((SOURCES_DIR / "api" / "merchants.json").read_text(encoding="utf-8"))
    blacklist = json.loads((SOURCES_DIR / "merchant_blacklist.json").read_text(encoding="utf-8"))
    with sqlite3.connect(db) as con:
        last = con.execute("SELECT MAX(CAST(SUBSTR(transaction_id, 2) AS INTEGER)) FROM transactions").fetchone()[0] or 0
        end = _now()
        max_updated = con.execute("SELECT MAX(updated_at) FROM transactions").fetchone()[0]
        start = datetime.strptime(max_updated, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc) + timedelta(seconds=1)
        if start >= end:
            end = start + timedelta(minutes=1)
        tx, frauds = make_transactions(rng, customers, merchants, {e["merchant"]["id"] for e in blacklist["entries"]},
                                       n, start, end, id_offset=last)
        tx.to_sql("transactions", con, if_exists="append", index=False)
        if not frauds.empty:
            frauds.to_sql("fraud_cases", con, if_exists="append", index=False)
        # Mises à jour de statut sur des transactions PENDING existantes
        con.execute("UPDATE transactions SET status='SUCCESS', updated_at=? WHERE transaction_id IN "
                    "(SELECT transaction_id FROM transactions WHERE status='PENDING' LIMIT 50)", (_iso(end),))
    # Sessions associées aux nouvelles transactions (le fichier Parquet est réécrit : export quotidien)
    sess_path = SOURCES_DIR / "app_sessions.parquet"
    existing = pd.read_parquet(sess_path) if sess_path.exists() else pd.DataFrame()
    new_sessions = make_sessions(customers, tx, frauds, seed=rng.randint(0, 10**6),
                                 id_offset=len(existing), background_per_customer=0)
    pd.concat([existing, new_sessions], ignore_index=True).to_parquet(sess_path, index=False)
    print(f"✔ {n} transactions ajoutées (+ mises à jour de statut, + {len(new_sessions)} sessions) dans {SOURCES_DIR}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--append", type=int, default=0, help="ajouter N transactions récentes au lieu de tout régénérer")
    args = parser.parse_args()
    append_transactions(args.append) if args.append else generate_all()


if __name__ == "__main__":
    main()
