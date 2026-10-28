# TP3 — Orchestrer le pipeline du Jour 1 avec Apache Airflow

**Jour 2 · matin · durée : 2 h** · Compétences : **C1.7** (agréger les sources de façon industrialisée), **C1.9** (alimenter les tables de façon fiable et répétable)

## Contexte

Les scripts d'hier fonctionnent… quand quelqu'un les lance, dans le bon ordre, et regarde les logs.
L'équipe Risque veut des données fraîches **chaque matin à 02:00**, sans intervention, avec une alerte
en cas de problème. Vous allez écrire le DAG qui orchestre : collecte → audit → staging → contrôles → KPI.

## Installation (sans Docker) — 15 min

Airflow ne s'exécute pas nativement sous Windows : utilisez **WSL2** (Ubuntu). Sous Linux/macOS, directement.
Airflow a **son propre environnement virtuel** (ses dépendances entrent en conflit avec Great Expectations).

```bash
# Depuis la racine du projet, dans WSL / Linux / macOS
python3 -m venv .venv-airflow
source .venv-airflow/bin/activate
AIRFLOW_VERSION=3.3.2
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
pip install "apache-airflow[http,sqlite]==${AIRFLOW_VERSION}" apache-airflow-providers-common-sql \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Configuration (Variables, Connections, dossier des DAGs) — lisez le script !
export DE_PYTHON="$(pwd)/.venv/bin/python"                         # Python de l'environnement "data"
export DAGS_FOLDER="$(pwd)/jour2/TP3_airflow/squelette/dags"       # VOTRE DAG
source jour2/TP3_airflow/airflow_env.sh
airflow db migrate
airflow pools set entrepot_sqlite 1 "SQLite : un seul écrivain"

airflow standalone      # UI : http://localhost:8080 (identifiants affichés dans le terminal)
```

L'API simulée doit tourner dans un autre terminal : `uvicorn mock_api.app:app --port 8000`.

## Étapes

### 1. Premier DAG minimal (20 min) · TODO 4, 5, 6, 7
Écrivez `default_args`, le planning, le sensor et le contrôle de santé. Vérifiez :
```bash
airflow dags list-import-errors        # aucune erreur de parsing
airflow tasks test nexapay_pipeline_quotidien sante_api
```
Dans l'UI : vue *Graph*, onglet *Code*, *Details*. Arrêtez l'API : que fait le sensor ?

### 2. Collecte et secrets (30 min) · TODO 1, 2, 8
- Les identifiants de l'API sont dans la **Connection** `nexa_api` (définie par variable d'environnement
  `AIRFLOW_CONN_NEXA_API`, cf. `airflow_env.sh`). Injectez-les dans la tâche par templating.
- Ouvrez *Admin → Variables* : la valeur de `pseudo_secret_key` apparaît-elle ? Pourquoi ?
- Testez `airflow tasks test nexapay_pipeline_quotidien collecte_db`.

### 3. Qualité, staging, contrôles SQL (30 min) · TODO 9, 10, 11
- `SQLExecuteQueryOperator` / `SQLCheckOperator` remplacent `PostgresOperator` (retiré) :
  le même code fonctionne sur SQLite, PostgreSQL, Snowflake… seule la Connection change.
- Le SQL des KPI doit être **idempotent** : relancer le run du même jour ne doit pas doubler les lignes.

### 4. XCom et branchement (20 min) · TODO 12, 13, 14
Un XCom transporte une **petite donnée de contrôle** (quelques Ko), jamais un DataFrame.

### 5. Exécution complète et pannes provoquées (15 min)
```bash
airflow dags test nexapay_pipeline_quotidien          # exécution complète en local
```
Puis provoquez des pannes et observez (UI : *Grid*, logs par tâche, *Clear* pour relancer) :
1. Mauvais mot de passe API dans `AIRFLOW_CONN_NEXA_API` → retries, callback, `logs/alertes_airflow.jsonl`.
2. Supprimez `PSEUDO_SECRET_KEY` → `nettoyage_staging` échoue sans retry ; les tâches aval sont *upstream_failed*.
3. Corrigez puis relancez **uniquement** les tâches en échec (*Clear* → *Downstream*).
4. Abaissez `seuil_alerte_blacklist` à `0.01` → la branche `alerte_fraude` s'exécute.

## Critères de réussite
- [ ] Aucune erreur d'import ; le graphe correspond au schéma
- [ ] Aucun identifiant, mot de passe ou chemin machine écrit en dur dans le DAG
- [ ] Un run complet passe au vert ; relancer le même run ne duplique pas les KPI
- [ ] Une panne déclenche retries puis alerte, et la reprise ciblée fonctionne

## Questions de débrief
1. Pourquoi les scripts métier tournent-ils dans un autre interpréteur Python que celui d'Airflow ?
2. `mode="reschedule"` vs `mode="poke"` pour un sensor : quel impact sur les workers ?
3. Pourquoi `retries=0` sur `nettoyage_staging` ?
4. `catchup=True` avec `start_date` au 1er octobre : que se passerait-il au premier démarrage ?
5. À quoi sert le pool `entrepot_sqlite` ? Serait-il nécessaire avec PostgreSQL ?
6. NiFi ou Airflow pour ingérer en continu les événements de 3 000 terminaux de paiement ? Et pour ce pipeline ?
