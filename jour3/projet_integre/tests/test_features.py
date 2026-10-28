"""Absence de fuite du futur : une feature ne doit utiliser que l'information antérieure à la transaction."""
import pandas as pd
import pytest

from build_features import build_labels, session_features, velocity

P = {"fenetre_sessions_h": 24, "fenetre_appareils_j": 7, "plafond_minutes": 10080}
T = pd.Timestamp("2026-10-20 12:00:00")


def tx(device="d_tel", when=T):
    return pd.DataFrame({"customer_pseudo": ["c_1"], "created_at": [when], "device_pseudo": [device]})


def sessions(rows):
    return pd.DataFrame(rows, columns=["customer_pseudo", "device_pseudo", "zone_geo", "login_at", "duration_s"])


def test_session_posterieure_ignoree():
    s = sessions([("c_1", "d_tel", "Abidjan", T + pd.Timedelta(minutes=1), 60)])     # 1 min APRÈS la transaction
    f = session_features(tx(), s, P).iloc[0]
    assert f["nb_sessions_24h_avant"] == 0
    assert f["a_historique_session"] == 0
    assert f["minutes_depuis_derniere_session"] == P["plafond_minutes"]


def test_session_simultanee_ignoree():
    s = sessions([("c_1", "d_tel", "Abidjan", T, 60)])                                # même instant : pas « avant »
    assert session_features(tx(), s, P).iloc[0]["nb_sessions_24h_avant"] == 0


def test_fenetre_24h_et_recence():
    s = sessions([
        ("c_1", "d_tel", "Abidjan", T - pd.Timedelta(days=3), 60),                    # hors fenêtre 24 h
        ("c_1", "d_tel", "Abidjan", T - pd.Timedelta(hours=2), 300),
        ("c_1", "d_tel", "Abidjan", T - pd.Timedelta(minutes=10), 120),
    ])
    f = session_features(tx(), s, P).iloc[0]
    assert f["nb_sessions_24h_avant"] == 2
    assert f["minutes_depuis_derniere_session"] == pytest.approx(10)
    assert f["duree_derniere_session_s"] == 120


def test_appareil_inconnu_et_zone_inhabituelle():
    s = sessions([
        ("c_1", "d_tel", "Abidjan", T - pd.Timedelta(days=5), 60),
        ("c_1", "d_tel", "Abidjan", T - pd.Timedelta(days=2), 60),
        ("c_1", "d_pirate", "Dakar", T - pd.Timedelta(minutes=5), 60),                # session « attaquant »
    ])
    f = session_features(tx(device="d_pirate"), s, P).iloc[0]
    assert f["appareil_deja_vu_session"] == 1          # vu 5 min avant...
    assert f["appareil_connu_plus_24h"] == 0           # ... mais jamais avant les dernières 24 h
    assert f["zone_derniere_session_inhabituelle"] == 1
    assert f["nb_appareils_distincts_7j"] == 2


def test_velocite_exclut_la_transaction_courante():
    t = pd.DataFrame({"customer_pseudo": ["c_1"] * 3, "amount": [10.0, 20.0, 30.0],
                      "created_at": [T, T + pd.Timedelta(minutes=10), T + pd.Timedelta(hours=3)]})
    assert list(velocity(t, "1h")) == [0, 1, 0]


def test_etiquette_anterieure_a_la_transaction_mise_en_quarantaine():
    transactions = pd.DataFrame({"transaction_id": ["T1", "T2"], "created_at": [T, T]})
    labels = pd.DataFrame({"transaction_id": ["T1", "T2"], "fraud_type": ["sim_swap", "sim_swap"],
                           "confirmed_at": [str(T + pd.Timedelta(days=3)), str(T - pd.Timedelta(days=1))]})
    clean, quarantaine = build_labels(labels, transactions)
    assert list(clean["transaction_id"]) == ["T1"]
    assert "fuite" in quarantaine[0]["reason"].iloc[0]
