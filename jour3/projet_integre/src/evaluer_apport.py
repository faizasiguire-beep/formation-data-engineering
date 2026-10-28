"""Mesure de l'apport métier des features de session (cas d'usage : améliorer le modèle de fraude du TP4).

Même modèle que la baseline du TP4 (régression logistique standardisée, classes équilibrées), même
découpage TEMPOREL (le test est postérieur à l'apprentissage), avec et sans les features de session.
PR-AUC : métrique pertinente quand la classe positive est rare (< 1 %).

    python jour3/projet_integre/src/evaluer_apport.py
"""
from __future__ import annotations

import json

import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from build_features import FEATURES_SESSIONS, FEATURES_TRANSACTION
from common import PROJECT_DIR, REPORTS_DIR


def main() -> None:
    df = pd.read_parquet(PROJECT_DIR / "data" / "ml_fraud_features" / "ml_fraud_features.parquet")
    df = df.sort_values("created_at")
    cut = df["created_at"].quantile(0.8)
    train, test = df[df["created_at"] < cut], df[df["created_at"] >= cut]
    resultats = {"coupure": str(cut), "train": len(train), "test": len(test),
                 "fraudes_test": int(test["is_fraud"].sum()), "pr_auc_aleatoire": round(float(test["is_fraud"].mean()), 4)}
    for nom, cols in {"transactions_seules": FEATURES_TRANSACTION,
                      "transactions_et_sessions": FEATURES_TRANSACTION + FEATURES_SESSIONS}.items():
        model = make_pipeline(StandardScaler(), LogisticRegression(class_weight="balanced", max_iter=1000))
        model.fit(train[cols], train["is_fraud"])
        proba = model.predict_proba(test[cols])[:, 1]
        resultats[nom] = {"pr_auc": round(average_precision_score(test["is_fraud"], proba), 4),
                          "roc_auc": round(roc_auc_score(test["is_fraud"], proba), 4)}
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    (REPORTS_DIR / "apport_features_sessions.json").write_text(json.dumps(resultats, indent=2), encoding="utf-8")
    print(json.dumps(resultats, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
