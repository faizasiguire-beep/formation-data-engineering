# Guide de soutenance (10 min + 5 min de questions)

## Déroulé (plan imposé par le professeur)

> **Le texte complet à dire, mot pour mot, réparti entre les 4 intervenants, est dans
> [`ORAL_SOUTENANCE.md`](ORAL_SOUTENANCE.md)** (≈ 1 300 mots, soit environ 10 minutes). Ce fichier-ci ne garde que la
> préparation de la démo et les réponses aux questions du jury.

| Temps | Partie | Qui |
|---|---|---|
| 0:00 → 1:00 | Besoin et sources | [Prénom 1] |
| 1:00 → 4:00 | Architecture et démonstration | [Prénom 2] |
| 4:00 → 6:00 | Une panne provoquée et sa reprise dans Airflow | [Prénom 3] |
| 6:00 → 8:00 | Conformité : ce qui sort, ce qui ne sort pas | [Prénom 4] |
| 8:00 → 9:00 | Versions DVC et reproduction de la v1 | [Prénom 4] |
| 9:00 → 10:00 | Limites et suites | [Prénom 1] |

## Démonstration (à préparer avant de passer)

```bash
# Terminal 1 : API          # Terminal 2 : Airflow
uvicorn mock_api.app:app --port 8000
source .venv-airflow/bin/activate && export DE_PYTHON="$(pwd)/.venv/bin/python" \
  && export DAGS_FOLDER="$(pwd)/jour3/projet_integre/dags" && source jour2/TP3_airflow/airflow_env.sh \
  && airflow standalone
# Juste avant la soutenance : activité récente (contrôle de fraîcheur < 24 h)
.venv/bin/python -m fil_rouge.generate_data --append 300
```

Puis dans l'UI : déclencher `projet_fraude_comportementale`, montrer la vue *Graph*, le log de `qualite_sortie`
(« conforme ✔ »), l'onglet *XCom* de `bilan_run`.

## Questions types du jury

**« Je relance votre DAG deux fois : que se passe-t-il dans vos tables ? »**
Rien ne change. La collecte est incrémentale (filigrane mis à jour *après* le chargement) : au 2e run, « rien de
nouveau ». Staging et curated sont remplacées en une transaction. Vérifié : deux runs successifs → mêmes volumes et
même empreinte de contenu (`eac5d42532f7843b`).

**« La clé de pseudonymisation a fuité ce matin : votre procédure ? »**
Révocation et nouvelle clé (coffre-fort), re-pseudonymisation par un run de `build_features` (idempotent), nouvelle
version DVC, purge des versions exposées (`dvc gc`), évaluation avec le DPO et notification sous 72 h si risque
(art. 33). Détail dans `docs/REGISTRE_TRAITEMENTS.md`. Démontrable : sans clé, `build_features` échoue sans retry, les
tâches aval passent en *upstream_failed*, l'alerte est écrite sans jamais afficher la clé.

**« Montrez-moi les données exactes de la version 1. »**
`git checkout data-v1 && dvc checkout` puis `cat jour3/projet_integre/data/ml_fraud_features/MANIFEST.json` :
60 358 lignes, empreinte `eac5d42532f7843b`. Retour : `git checkout main && dvc checkout`.

**« Quelle anomalie n'attendiez-vous pas ? »** (trois réponses vécues)
1. **Sessions antidatées** : lors d'un ajout de données, une session précède de 1 à 30 min sa transaction, donc peut
   être plus ancienne que la dernière session déjà collectée. Un filigrane sur `login_at` l'aurait perdue → filigrane
   sur `session_id` (croissant).
2. **Great Expectations écrit les listes d'identifiants dans ses fichiers de configuration** (contrôles
   d'intégrité référentielle) : ils s'étaient retrouvés dans Git → retirés de l'historique et ignorés.
3. **`retry_exponential_backoff=True` ne fait plus de backoff en Airflow 3.3** : c'est un multiplicateur, `True`
   vaut 1.0 donc délai constant (vérifié dans le code d'Airflow) → `2.0`.

**« Qu'est-ce qui casse en premier avec un volume × 1 000 ? »**
1. **SQLite** : un seul écrivain (le pool l'impose déjà) et 60 M de lignes → PostgreSQL (`FRAUDE_DB_URL`).
2. **Le calcul des features en pandas** (boucle par client, tout en mémoire) → calcul partitionné par client (Spark,
   ou SQL avec fonctions de fenêtre), incrémental sur les seules nouvelles transactions.
3. **Les contrôles GX `ExpectColumnValuesToBeInSet`** avec des listes de millions d'identifiants → contrôle
   d'intégrité par jointure SQL.
4. **Le remplacement complet de la table finale** à chaque run → mise à jour incrémentale (upsert) des transactions
   nouvelles ou dont l'étiquette a changé.

## Autres questions probables

- **Pourquoi ne pas mettre `device_id` dans la table finale ?** Inutile au modèle une fois les features calculées
  (minimisation) ; `customer_pseudo` reste pour un découpage par client.
- **L'étiquette est-elle une fuite ?** Non : c'est la cible d'apprentissage. La contrainte est qu'aucune étiquette ne
  soit confirmée *avant* la transaction : vérifiée en entrée (diagnostic), en staging (quarantaine) et en sortie
  (bloquant). Les étiquettes tardives sont captées par un filigrane sur `confirmed_at`.
- **Pourquoi `qualite_entree` ne bloque-t-elle pas ?** La RAW reflète la source : ses défauts (montants négatifs,
  devise `xof`, statut `SUCCES`) sont attendus et traités en staging. Bloquer chaque jour ne protégerait rien ; on
  bloque sur ce que consomment les data scientists.
- **Combien de lignes en quarantaine ?** 250 transactions (188 montants manquants, 62 montants ≤ 0), avec leur motif.
