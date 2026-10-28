# TP1 - Construire un pipeline de collecte multi-sources robuste

**Jour 1 · matin · durée : 2 h** · Compétence visée : **C1.7** (agréger des sources hétérogènes dans une base relationnelle)

## Contexte

Vous rejoignez l'équipe data de **NexaPay**, fintech de paiement mobile (zone UEMOA, devise XOF).
L'équipe Risque veut entraîner un **modèle de détection de fraude**. Avant toute modélisation, il faut
rassembler dans l'entrepôt les données éparpillées dans le SI :

| Source | Type | Contraintes |
|---|---|---|
| Référentiel marchands | **API REST** `http://127.0.0.1:8000` | OAuth2, pagination, quotas (429), pannes (503) |
| Core banking | **Base SQL** `data/sources/core_banking.db` | 60 000 transactions, mises à jour de statut |
| Export CRM clients | **CSV** `data/sources/customers_crm.csv` | format "Excel français", qualité douteuse |
| Liste noire marchands | **JSON** `data/sources/merchant_blacklist.json` | structure imbriquée |

Cible : l'entrepôt `data/warehouse.db` (SQLite), tables `raw_*` — **zone brute, sans transformation métier**.

## Prérequis (5 min)

```bash
# Terminal 1 — l'API doit tourner pendant tout le TP
uvicorn mock_api.app:app --port 8000
# Vérifier : http://127.0.0.1:8000/docs
```

Vérifiez que `.env` contient bien `NEXA_API_CLIENT_ID` et `NEXA_API_CLIENT_SECRET`.

## Travail demandé

Ouvrez `squelette/collect_pipeline.py`. L'infrastructure est fournie ; complétez les 8 `TODO`.

### Étape 1 — Les fichiers (25 min) · TODO 5 et 6
1. Ouvrez le CSV dans un éditeur de texte (pas Excel !). Identifiez le séparateur et l'encodage.
2. Implémentez `extract_customers_csv`. Justifiez `dtype=str` en zone brute.
3. Implémentez `extract_blacklist_json` avec `pd.json_normalize`.
4. Testez : `--mode full --sources csv,json`, puis inspectez les tables (DB Browser for SQLite, ou `pd.read_sql`).

### Étape 2 — La base de données (25 min) · TODO 4 et 7
1. Implémentez `extract_transactions` : requête paramétrée, lecture par morceaux.
2. Mettez à jour le filigrane (TODO 7).
3. Testez le full load, puis simulez l'activité et testez l'incrémental :
   ```bash
   python -m fil_rouge.generate_data --append 300
   python jour1/TP1_collecte/squelette/collect_pipeline.py --mode incremental --sources db
   ```
   Vous devez récupérer **les 300 nouvelles transactions + les ~50 transactions dont le statut a changé**.

### Étape 3 — L'API (50 min) · TODO 1, 2, 3
1. Explorez l'API dans `/docs`. Obtenez un jeton à la main (bouton *Try it out*).
2. Implémentez `_fetch_token`, puis `get` (retry), puis `iter_pages`.
3. Testez `--sources api` plusieurs fois et observez les logs : vous devez voir des 503 rattrapés et,
   si vous baissez `NEXA_API_RATE_LIMIT=3` dans `.env` (puis relancez l'API), des 429 gérés.

### Étape 4 — Industrialisation (10 min) · TODO 8
Code de sortie non nul en cas d'échec. Testez en arrêtant l'API : le pipeline doit charger les fichiers
et la base, journaliser l'échec API dans `audit_log`, et renvoyer `1` (`echo $?` / `echo %ERRORLEVEL%`).

## Critères de réussite

- [ ] `--mode full` charge les 4 tables `raw_*` (320 marchands, ≥ 60 000 transactions, 5 100 clients, 15 entrées)
- [ ] L'incrémental ne recharge que les lignes nouvelles ou modifiées
- [ ] Les 503 et 429 sont absorbés sans faire échouer le run
- [ ] Aucun secret dans le code ni dans les logs
- [ ] `audit_log` contient une ligne par source et par run, `logs/` un fichier lisible
- [ ] Une copie brute de chaque extraction existe dans `data/landing/`

## Questions de réflexion 

1. Pourquoi filtrer sur `updated_at` plutôt que `created_at` ? Quelle est la limite de cette approche (suppressions) ?
2. Que se passe-t-il si deux transactions ont exactement le même `updated_at` que le filigrane ? Proposez une parade.
3. Pourquoi un 429 ne doit-il pas consommer le budget de tentatives, alors qu'un 503 oui ?
4. Pourquoi garder une copie en zone *landing* alors que les données sont déjà en base ?

## Pour aller plus loin

- Ajouter la source `data/sources/app_sessions.parquet` (lecture sélective de colonnes : `columns=[...]`).
- Remplacer la boucle de retry par `tenacity` (`@retry(wait=wait_exponential_jitter(...))`) et comparer.
- Basculer l'entrepôt sur PostgreSQL installé en local : seule `WAREHOUSE_DB_URL` change.
