# Projet intégré — Fraude : signaux comportementaux de l'application mobile

**Commanditaire** : Direction des Risques NexaPay · **Cas d'usage IA** : améliorer le modèle de fraude du TP4 avec le
comportement dans l'application (sessions, appareils) · Compétences **C1.7, C1.8, C1.9**.

**Livrable métier** : table `ml_fraud_features`, une ligne par transaction, 9 features issues des sessions calculées
**sans fuite du futur**, pseudonymisée, sans IP ni coordonnées.

**Résultat** : avec le même modèle que le TP4 (régression logistique, split temporel), les features de session font
passer la **PR-AUC de 0,054 à 0,593** (×11) et la ROC-AUC de 0,736 à 0,963 (`src/evaluer_apport.py`).

---

## Architecture

```
 SOURCES                         RAW (brut)            STAGING (pseudonymisé)       CURATED
 core_banking.db  transactions ─► raw_transactions ─┐
                  fraud_cases  ─► raw_fraud_labels ─┤   stg_transactions
 app_sessions.parquet (NOUVEAU)─► raw_sessions     ─┼─► stg_sessions         ─► ml_fraud_features ─► DVC
 API REST OAuth2  marchands    ─► raw_merchants    ─┘   stg_fraud_labels          (contrôle BLOQUANT)    data-v1
                                       │                stg_merchants                                    data-v2
                              contrôle d'ENTRÉE         quarantine
                                (diagnostic)
```

| Étape | Script | Rôle |
|---|---|---|
| 1 | `src/collect.py` | collecte incrémentale à filigrane, 4 sources, zone landing, `audit_log` |
| 2 | `src/quality_in.py` | Great Expectations sur la zone RAW : 26 contrôles, 5 dimensions + fraîcheur (non bloquant) |
| 3 | `src/build_features.py` | dédoublonnage, quarantaine, pseudonymisation HMAC, généralisation IP/GPS, features point-in-time |
| 4 | `src/quality_out.py` | 31 contrôles GX + 4 transverses sur la table finale : **code 1 si non conforme** |
| 5 | `src/export_snapshot.py` | instantané Parquet déterministe pour DVC |
| 6 | `src/catalog.py` | catalogue des données (`data_catalog` + `docs/CATALOGUE.md`) |
| — | `dags/projet_fraude_dag.py` | orchestration Airflow 3 (13 tâches) |
| — | `scripts/versionner.sh` | nouvelle version DVC taguée |

## Features de session (sans fuite du futur)

Pour une transaction du client *c* à l'instant *T*, seules les sessions de *c* **strictement antérieures à T** sont lues
(`searchsorted(..., side="left")`). Vérifié par `tests/test_features.py`.

| Feature | Définition | Légitimes | Fraudes |
|---|---|---|---|
| `nb_sessions_24h_avant` | sessions dans les 24 h précédant T | 0,60 | 0,68 |
| `minutes_depuis_derniere_session` | récence (plafonnée à 7 jours si aucune session) | 3 630 | 3 194 |
| `duree_derniere_session_s` | durée de la dernière session | 855 | 906 |
| `a_historique_session` | au moins une session avant T | 0,95 | 0,95 |
| `appareil_deja_vu_session` | appareil de la transaction déjà vu en session | 0,93 | 0,73 |
| `appareil_connu_plus_24h` | … et vu plus de 24 h avant T | **0,85** | **0,38** |
| `nb_appareils_distincts_7j` | appareils distincts en session sur 7 jours | 0,77 | 0,99 |
| `nb_zones_distinctes_24h` | zones géographiques distinctes sur 24 h | 0,55 | 0,68 |
| `zone_derniere_session_inhabituelle` | dernière connexion hors de la zone habituelle du client | **0,003** | **0,596** |

S'y ajoutent les features transactionnelles du TP4 (montant, heure, nuit, canal, vélocité 1 h / 24 h, risque marchand).

## Conformité RGPD (résumé — détail dans `docs/REGISTRE_TRAITEMENTS.md`)

- `customer_id` et `device_id` pseudonymisés par **HMAC-SHA256 avec clé secrète**, même normalisation et même clé dans
  toutes les sources : **100 %** des clients des transactions retrouvés dans les sessions (contrôle bloquant ≥ 99 %).
- **IP** tronquée en /24 en staging, **GPS** remplacé par une zone de service (10 villes) : ni l'un ni l'autre en curated
  (schéma exact vérifié par GX + recherche d'IP dans le contenu).
- Aucune variable protégée (genre, âge, ville de résidence) : seul le comportement est utilisé (AI Act art. 10).
- Clé et identifiants API hors du code : `.env` (ignoré par Git), Variable et Connection Airflow masquées.

---

## Exécution

Prérequis : socle installé (README racine), environnement « data » `.venv` (Linux/WSL), `PSEUDO_SECRET_KEY` dans `.env`.
**Toutes les commandes se lancent depuis la racine du projet.**

```bash
source .venv/bin/activate
uvicorn mock_api.app:app --port 8000 &                         # API simulée (autre terminal)
python -m fil_rouge.generate_data --append 300                 # activité récente (fraîcheur < 24 h)

python jour3/projet_integre/src/collect.py --mode full         # 1re fois ; ensuite --mode incremental
python jour3/projet_integre/src/quality_in.py                  # rapport : reports/projet_fraude/qualite_entree.md
python jour3/projet_integre/src/build_features.py
python jour3/projet_integre/src/quality_out.py                 # code 1 si non conforme
python jour3/projet_integre/src/export_snapshot.py
python jour3/projet_integre/src/catalog.py
python -m pytest jour3/projet_integre/tests -q                 # 12 tests
```

### Avec Airflow (environnement séparé `.venv-airflow`)

```bash
source .venv-airflow/bin/activate
export DE_PYTHON="$(pwd)/.venv/bin/python"
export DAGS_FOLDER="$(pwd)/jour3/projet_integre/dags"
source jour2/TP3_airflow/airflow_env.sh                        # Variables + Connections depuis .env
airflow pools set entrepot_sqlite 1 "SQLite : un seul écrivain" # une seule fois
airflow dags test projet_fraude_comportementale                # exécution complète
airflow standalone                                             # UI : http://localhost:8080
```

### Versioning des données

```bash
bash jour3/projet_integre/scripts/versionner.sh data-v3 "description"   # après un run réussi, code commité
git checkout data-v1 && dvc checkout                                     # retour exact à la v1
git checkout main && dvc checkout
```

### Bascule PostgreSQL (bonus)

Le code n'emploie que SQLAlchemy (schémas explicites, requêtes paramétrées) : définir
`FRAUDE_DB_URL=postgresql+psycopg://user@localhost/nexapay` dans `.env` suffit (non testé faute de PostgreSQL local).

## Structure

```
jour3/projet_integre/
├── README.md                 ce fichier
├── params.yaml               fenêtres, plafonds, seuils qualité (versionnés)
├── src/                      scripts du pipeline (exécutables et testables sans Airflow)
├── dags/                     DAG Airflow 3
├── tests/                    pytest : pseudonymisation, absence de fuite du futur
├── scripts/versionner.sh     version DVC + tag Git
├── gouvernance/              politique d'accès par rôle et par zone
├── data/ml_fraud_features.dvc  pointeur DVC (les données sont dans le cache / stockage DVC)
└── docs/                     registre art. 30, catalogue, historique des versions, guide de soutenance
```

Base cible : `data/projet_fraude.db` (SQLite). Rapports : `reports/projet_fraude/`. Journaux : `logs/projet_fraude_*.log`,
alertes Airflow : `logs/alertes_projet_fraude.jsonl`.
