"""Protection RGPD du projet : pseudonymisation cohérente entre sources, généralisation IP et GPS.

Reprend la boîte à outils du TP2 (HMAC-SHA256 avec clé secrète, troncature /24) et l'étend à la
géolocalisation des sessions.

Contraintes du brief :
  * `customer_id` et `device_id` pseudonymisés de façon COHÉRENTE entre sources -> même fonction, même clé,
    même normalisation, même préfixe : un client a le même pseudonyme dans les transactions et les sessions.
  * Pas de coordonnées précises ni d'IP complète en zone curated -> GPS ramené à une zone (ville du
    référentiel) en staging, IP tronquée en /24 ; la table finale ne contient ni l'une ni l'autre.
"""
from __future__ import annotations

import hashlib
import hmac
import math

import pandas as pd

PREFIX_CLIENT = "c_"
PREFIX_APPAREIL = "d_"
PSEUDO_REGEX = r"^[cd]_[0-9a-f]{24}$"

# Référentiel des zones de service NexaPay (centre-ville, WGS84). Une session est rattachée à la zone la
# plus proche : on garde l'information utile à la fraude (« connexion depuis une autre ville ») sans la
# position précise de la personne.
ZONES = {"Abidjan": (5.36, -4.01), "Bouaké": (7.69, -5.03), "Yamoussoukro": (6.82, -5.28), "Dakar": (14.72, -17.47),
         "Thiès": (14.79, -16.93), "Bamako": (12.64, -8.00), "Ouagadougou": (12.37, -1.52), "Lomé": (6.13, 1.22),
         "Cotonou": (6.37, 2.39), "Niamey": (13.51, 2.11)}
RAYON_ZONE_KM = 100      # au-delà, la position ne correspond à aucune zone connue -> "hors_zone"


def require_key(raw_key: str | None) -> bytes:
    if not raw_key or raw_key.startswith("remplacer"):
        raise ValueError("PSEUDO_SECRET_KEY absente ou non personnalisée (.env ou Variable Airflow)")
    return raw_key.encode("utf-8")


def pseudonymize(value, key: bytes, prefix: str) -> str | None:
    """HMAC-SHA256 tronqué : déterministe (jointures entre sources) mais non réversible sans la clé."""
    if value is None or (isinstance(value, float) and pd.isna(value)) or value == "":
        return None
    normalized = str(value).strip().upper()          # « c001 » et « C001 » -> même pseudonyme
    digest = hmac.new(key, normalized.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{prefix}{digest[:24]}"


def pseudonymize_series(s: pd.Series, key: bytes, prefix: str) -> pd.Series:
    """Version vectorisée : calcule une seule fois le pseudonyme de chaque valeur distincte."""
    mapping = {v: pseudonymize(v, key, prefix) for v in s.dropna().unique()}
    return s.map(mapping)


def truncate_ip(ip) -> str | None:
    """Généralisation /24 : on garde le réseau (utile pour la fraude), pas l'hôte."""
    if not isinstance(ip, str) or ip.count(".") != 3:
        return None
    return ".".join(ip.split(".")[:3]) + ".0"


def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371 * math.asin(math.sqrt(a))


def zone_geo(lat, lon) -> str | None:
    """Rattache une position précise à la zone de service la plus proche (généralisation)."""
    if lat is None or lon is None or pd.isna(lat) or pd.isna(lon):
        return None
    nom, dist = min(((z, _haversine_km(lat, lon, *c)) for z, c in ZONES.items()), key=lambda t: t[1])
    return nom if dist <= RAYON_ZONE_KM else "hors_zone"


def k_anonymity(df: pd.DataFrame, quasi_identifiers: list[str]) -> int:
    """Taille du plus petit groupe partageant les mêmes quasi-identifiants (k)."""
    return int(df.groupby(quasi_identifiers, dropna=False).size().min())
