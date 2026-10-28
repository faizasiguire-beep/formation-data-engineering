"""Boîte à outils RGPD : détection, pseudonymisation, anonymisation, masquage.

Rappels juridiques (à garder en tête en lisant le code) :
  * Pseudonymisation (RGPD art. 4.5) : la ré-identification reste possible avec une information
    additionnelle (la clé). Les données RESTENT personnelles -> RGPD applicable.
  * Anonymisation (avis G29 05/2014) : ré-identification impossible par des moyens raisonnables
    (individualisation, corrélation, inférence). Les données SORTENT du champ du RGPD.
  * Un simple hash SHA-256 d'un email ou d'un téléphone N'EST PAS une anonymisation :
    l'espace des valeurs est petit -> attaque par dictionnaire. D'où le HMAC avec clé secrète.
"""
from __future__ import annotations

import hashlib
import hmac
import re
from dataclasses import dataclass

import pandas as pd
from faker import Faker

# --------------------------------------------------------------------------------------
# 1. Détection des données personnelles
# --------------------------------------------------------------------------------------
# Catégories utilisées dans le catalogue :
#   DIRECT    : identifie directement une personne (nom, email, téléphone, n° d'identité)
#   INDIRECT  : identifie par recoupement (date de naissance, IP, device, géolocalisation, ville)
#   SENSIBLE  : à protéger renforcée (revenus, santé...) — ici les revenus (donnée financière)
#   NONE      : pas de donnée personnelle détectée
NAME_RULES: list[tuple[str, str]] = [
    (r"(^|_)(first|last|full)?_?name$|prenom|nom", "DIRECT"),
    (r"e?mail", "DIRECT"),
    (r"phone|tel|mobile|msisdn", "DIRECT"),
    (r"national_id|passport|cni|nir|ssn", "DIRECT"),
    (r"iban|card_number|pan", "DIRECT"),
    (r"birth|naissance|dob", "INDIRECT"),
    (r"(^|_)ip(_|$)|ip_address", "INDIRECT"),
    (r"device", "INDIRECT"),
    (r"lat|lon|geo|address|adresse", "INDIRECT"),
    (r"income|salaire|revenu|salary", "SENSIBLE"),
    (r"gender|sexe", "INDIRECT"),
]

CONTENT_PATTERNS: dict[str, tuple[str, str]] = {
    "email": (r"^[\w.+-]+@[\w-]+\.[\w.-]+$", "DIRECT"),
    "phone_intl": (r"^\+\d{8,14}$", "DIRECT"),
    "ipv4": (r"^(\d{1,3}\.){3}\d{1,3}$", "INDIRECT"),
    "iban": (r"^[A-Z]{2}\d{2}[A-Z0-9]{10,30}$", "DIRECT"),
}

SEVERITY = {"NONE": 0, "INDIRECT": 1, "SENSIBLE": 2, "DIRECT": 3}


@dataclass
class PiiFinding:
    column: str
    category: str
    detected_by: str      # "nom de colonne" / "contenu:<motif>"
    match_rate: float     # part des valeurs non nulles correspondant au motif (détection par contenu)


def detect_pii(df: pd.DataFrame, sample_size: int = 500) -> list[PiiFinding]:
    """Double détection : par le NOM de la colonne et par le CONTENU (échantillon)."""
    findings = []
    for col in df.columns:
        if col.startswith("_"):            # colonnes techniques du pipeline
            continue
        best = PiiFinding(col, "NONE", "-", 0.0)
        for pattern, cat in NAME_RULES:
            if re.search(pattern, col, flags=re.IGNORECASE) and SEVERITY[cat] > SEVERITY[best.category]:
                best = PiiFinding(col, cat, "nom de colonne", 1.0)
        values = df[col].dropna().astype(str)
        if len(values):
            sample = values.sample(min(sample_size, len(values)), random_state=0)
            for name, (pattern, cat) in CONTENT_PATTERNS.items():
                rate = sample.str.match(pattern).mean()
                if rate >= 0.8 and SEVERITY[cat] >= SEVERITY[best.category]:
                    best = PiiFinding(col, cat, f"contenu:{name}", round(float(rate), 3))
        findings.append(best)
    return findings


# --------------------------------------------------------------------------------------
# 2. Pseudonymisation
# --------------------------------------------------------------------------------------
def pseudonymize(value, key: bytes, prefix: str = "") -> str | None:
    """HMAC-SHA256 tronqué : déterministe (jointures possibles) mais non réversible sans la clé."""
    if value is None or (isinstance(value, float) and pd.isna(value)) or value == "":
        return None
    normalized = str(value).strip().lower()           # même client -> même pseudonyme
    digest = hmac.new(key, normalized.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{prefix}{digest[:24]}"


def naive_sha256(value: str) -> str:
    """CONTRE-EXEMPLE pédagogique : vulnérable à une attaque par dictionnaire."""
    return hashlib.sha256(value.encode()).hexdigest()


def require_key(raw_key: str) -> bytes:
    if not raw_key or raw_key.startswith("remplacer"):
        raise ValueError("PSEUDO_SECRET_KEY absente ou non personnalisée dans .env "
                         "(python -c \"import secrets; print(secrets.token_hex(32))\")")
    return raw_key.encode("utf-8")


# --------------------------------------------------------------------------------------
# 3. Masquage et généralisation (réduction de la précision)
# --------------------------------------------------------------------------------------
def mask_email(email) -> str | None:
    if not isinstance(email, str) or "@" not in email:
        return None
    local, domain = email.split("@", 1)
    return f"{local[0]}***@{domain.lower()}"


def mask_phone(phone) -> str | None:
    if not isinstance(phone, str) or len(phone) < 6:
        return None
    return phone[:4] + "*" * (len(phone) - 6) + phone[-2:]


def truncate_ip(ip) -> str | None:
    """Généralisation /24 : on conserve le réseau (utile pour la fraude), pas l'hôte."""
    if not isinstance(ip, str) or ip.count(".") != 3:
        return None
    return ".".join(ip.split(".")[:3]) + ".0"


def age_band(age: float | None) -> str | None:
    if age is None or pd.isna(age):
        return None
    bands = [(18, "<18"), (25, "18-24"), (35, "25-34"), (45, "35-44"), (55, "45-54"), (65, "55-64")]
    for upper, label in bands:
        if age < upper:
            return label
    return "65+"


def income_band(income: float | None) -> str | None:
    if income is None or pd.isna(income) or income < 0:
        return None
    for upper, label in [(100_000, "<100k"), (250_000, "100-250k"), (500_000, "250-500k"), (1_000_000, "500k-1M")]:
        if income < upper:
            return label
    return "1M+"


# --------------------------------------------------------------------------------------
# 4. Données de substitution (jeu de développement partageable)
# --------------------------------------------------------------------------------------
def fake_substitute(df: pd.DataFrame, seed: int = 0) -> pd.DataFrame:
    """Remplace les identifiants directs par des valeurs fictives réalistes (Faker).

    Usage : fournir un jeu de données à un prestataire / un environnement de recette.
    Attention : les colonnes INDIRECTES restent ; à combiner avec la généralisation.
    """
    fake = Faker("fr_FR")
    Faker.seed(seed)
    out = df.copy()
    generators = {"first_name": fake.first_name, "last_name": fake.last_name, "email": fake.free_email,
                  "phone": lambda: "+225" + fake.numerify("07########"), "national_id": lambda: fake.bothify("??#########")}
    for col, gen in generators.items():
        if col in out.columns:
            out[col] = [gen() for _ in range(len(out))]
    return out


# --------------------------------------------------------------------------------------
# 5. Contrôle k-anonymat (vérification de l'anonymisation)
# --------------------------------------------------------------------------------------
def k_anonymity(df: pd.DataFrame, quasi_identifiers: list[str]) -> int:
    """Taille du plus petit groupe partageant les mêmes quasi-identifiants (k)."""
    return int(df.groupby(quasi_identifiers, dropna=False).size().min())
