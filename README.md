# Module Data Engineering — Mastère 2 Data & IA (RNCP 37137)

**3 jours · 21 heures** · Bloc 1 — compétences **C1.7** (agréger des sources hétérogènes dans le respect du
RGPD), **C1.8** (analyser anomalies et valeurs manquantes), **C1.9** (alimenter des tables optimisées).

Fil rouge : **NexaPay**, fintech fictive de paiement mobile (zone UEMOA, XOF). Cas d'usage IA : **détection de fraude**.
Tout fonctionne **en local, sans Docker** : SQLite par défaut, API simulée en Python, Kafka simulé (vrai Kafka en option).

## Contenu

| Jour | Matin (3 h 30) | Après-midi (3 h 30) |
|---|---|---|
| **1** | Collecter des sources variées  · **TP1** collecte multi-sources | Qualité et conformité RGPD / AI Act · **TP2** audit, pseudonymisation, staging |
| **2** | Apache Airflow · **TP3** DAG du pipeline | DVC & MLflow · **TP4** versioning et reproductibilité |
| **3** | Streaming (Kafka, Spark) · **TP5** consommateur temps réel | **Projet intégré** en binôme (4 briefs) + soutenances |

```
formation-data-engineering/
├── supports/                  présentations PowerPoint des 3 jours
├── fil_rouge/                 socle commun : configuration, générateur de données, mini-Kafka
├── mock_api/                  API REST simulée (OAuth2, pagination, 429, 503)
├── jour1/  TP1_collecte/  TP2_qualite_rgpd/  demo_nifi/
├── jour2/  TP3_airflow/   TP4_dvc_mlflow/
├── jour3/  TP5_streaming/ projet_integre/ (briefs, grille)
│           └─ chaque TP : ENONCE.md, squelette/ 
├── outils/make_student_pack.py   version stagiaires 
```

## Installation (≈ 15 min)

**Prérequis** : Python 3.11 à 3.13, Git. Pour Airflow et Spark sous Windows : **WSL2** (Ubuntu) ; Java 17+ pour Spark.

```bash
python -m venv .venv
# Windows : .venv\Scripts\activate      Linux / macOS / WSL : source .venv/bin/activate
pip install -r requirements.txt
pip install -e .                        # rend le package `fil_rouge` importable partout
cp .env.example .env                    # Windows : copy .env.example .env
python -c "import secrets; print(secrets.token_hex(32))"   # coller la valeur dans PSEUDO_SECRET_KEY du .env
python -m fil_rouge.generate_data       # génère toutes les sources dans data/sources (~5 s)
uvicorn mock_api.app:app --port 8000    # API simulée — laisser ce terminal ouvert
```
. Les adresses à ouvrir sont :

http://127.0.0.1:8000/docs : la documentation interactive, celle utilisée au TP1.
http://127.0.0.1:8000/health : doit renvoyer {"status":"ok","merchants":320,...}.

Pour tester l’authentification dans /docs :

Ouvrez POST /oauth/token et cliquez sur Try it out.
Saisissez grant_type = client_credentials, client_id = etudiant-de et client_secret = change-me-s3cret. Ce sont les valeurs de votre .env.
Cliquez sur Execute : vous obtenez un access_token.

Vérification rapide (autre terminal) :
```bash
python jour1/TP1_collecte/corrige/collect_pipeline.py --mode full
python jour1/TP2_qualite_rgpd/corrige/audit_quality.py
python jour1/TP2_qualite_rgpd/corrige/clean_and_load.py
```

Tous les scripts se lancent **depuis la racine du projet**.

## Dépannage

| Symptôme | Cause / solution |
|---|---|
| `Form data requires "python-multipart"` | `pip install python-multipart` |
| `ModuleNotFoundError: fil_rouge` | `pip install -e .` depuis la racine, environnement activé |
| `PSEUDO_SECRET_KEY absente` | générer une clé et la copier dans `.env` |
| `database is locked` | deux écritures SQLite simultanées : fermer DB Browser, utiliser le pool Airflow |
| Airflow ne démarre pas sous Windows | normal : utiliser WSL2 |
| Spark : `JAVA_HOME is not set` / erreurs `winutils` | installer Java 17+ ; sous Windows, utiliser WSL2 |
| `FutureWarning: PySpark does not yet fully support pandas >= 3` | avertissement sans effet sur le module |
| Tutoriels Great Expectations introuvables | le module utilise **GX 1.x** (`gx.get_context`, `context.suites`, `gx.expectations.*`) ; la plupart des tutoriels en ligne sont en 0.x |
| PostgreSQL au lieu de SQLite | installer PostgreSQL localement, `pip install "psycopg[binary]"`, définir `WAREHOUSE_DB_URL` dans `.env` |
