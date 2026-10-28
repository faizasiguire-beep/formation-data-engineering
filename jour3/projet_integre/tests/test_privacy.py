"""Conformité : pseudonymisation cohérente entre sources, généralisation IP et GPS."""
import re

import pandas as pd

from privacy import (PREFIX_APPAREIL, PREFIX_CLIENT, PSEUDO_REGEX, pseudonymize, pseudonymize_series, truncate_ip,
                     zone_geo)

KEY = b"cle-de-test-uniquement"


def test_meme_client_meme_pseudonyme_dans_toutes_les_sources():
    tx = pseudonymize_series(pd.Series(["C000042", "C000007"]), KEY, PREFIX_CLIENT)
    sessions = pseudonymize_series(pd.Series(["C000007", " c000042 "]), KEY, PREFIX_CLIENT)   # casse / espaces
    assert set(tx) == set(sessions)


def test_pseudonyme_depend_de_la_cle_et_ne_contient_pas_l_identifiant():
    a = pseudonymize("C000042", KEY, PREFIX_CLIENT)
    assert a != pseudonymize("C000042", b"autre-cle", PREFIX_CLIENT)
    assert "000042" not in a
    assert re.match(PSEUDO_REGEX, a)


def test_client_et_appareil_ne_se_confondent_pas():
    assert pseudonymize("X1", KEY, PREFIX_CLIENT) != pseudonymize("X1", KEY, PREFIX_APPAREIL)


def test_valeurs_manquantes_restent_manquantes():
    assert pseudonymize(None, KEY, PREFIX_CLIENT) is None
    assert pseudonymize("", KEY, PREFIX_CLIENT) is None


def test_ip_tronquee_en_24():
    assert truncate_ip("41.202.17.88") == "41.202.17.0"
    assert truncate_ip("pas une ip") is None


def test_gps_ramene_a_une_zone_sans_coordonnees():
    assert zone_geo(5.3512, -4.0233) == "Abidjan"
    assert zone_geo(14.7001, -17.4502) == "Dakar"
    assert zone_geo(48.85, 2.35) == "hors_zone"       # Paris : aucune zone de service à moins de 100 km
