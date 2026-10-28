# TP3 — Orchestrer le pipeline du Jour 1 avec Apache Airflow

**Jour 2 · matin · durée : 2 h** · Compétences : **C1.7** (agréger les sources de façon industrialisée), **C1.9** (alimenter les tables de façon fiable et répétable)

## Contexte

Les scripts d'hier fonctionnent… quand quelqu'un les lance, dans le bon ordre, et regarde les logs.
L'équipe Risque veut des données fraîches **chaque matin à 02:00**, sans intervention, avec une alerte
en cas de problème. Vous allez écrire le DAG qui orchestre : collecte → audit → staging → contrôles → KPI.

## Installation (sans Docker) — 15 min

Airflow ne s'exécute pas nativement sous Windows : utilisez **WSL2** (Ubuntu). Sous Linux/macOS, directement.
Airflow a **son propre environnement virtuel** (ses dépendances entrent en conflit avec Great Expectations).



WSL (Windows Subsystem for Linux) est une fonctionnalité de Windows qui permet de faire tourner un vrai Linux (Ubuntu par défaut) directement sur votre PC, sans machine virtuelle à gérer ni double démarrage. Vous ouvrez un terminal Ubuntu et vous avez accès aux commandes Linux, à côté de vos applications Windows.

### Installation (une seule fois, 10 minutes)
1. Ouvrez PowerShell en administrateur : touche Windows, tapez powershell, puis clic droit et Exécuter en tant qu’administrateur.
2. Lancez :
powershell
   wsl --install
commande pour regarder les distributions Ubuntun installées : wsl --list --verbose

Cette commande installe WSL et Ubuntu.
3. Redémarrez le PC.
4. Au redémarrage, une fenêtre Ubuntu s’ouvre : choisissez un nom d’utilisateur et un mot de passe Linux, distincts de ceux de Windows.

Ensuite, vous ouvrez Ubuntu depuis le menu Démarrer, ou en tapant wsl dans un terminal.
Si l’écran reste bloqué sur « Provisioning » au-delà de 10 à 15 minutes, faites Ctrl+C, 
fermez la fenêtre, puis relancez wsl dans une nouvelle fenêtre PowerShell.

Utiliser le projet depuis WSL

Vos disques Windows sont visibles sous /mnt/ :

bash
cd /mnt/c/Users/yamdj/formation-data-engineering/formation-data-engineering
dans votre cas faire cd /mnt/CHEMIN LOCAL WINDOW

Python et les environnements sont séparés entre Windows et Linux. Le .venv créé sous Windows ne fonctionne pas dans WSL, il faut en créer un sous Linux :

# Attention avant d'exécuter cette commande, liser le paragraphe sur VS qui juste en bas
bash
sudo apt update && sudo apt install -y python3-venv python3-pip
python3 -m venv .venv-wsl
source .venv-wsl/bin/activate
pip install -r requirements.txt && pip install -e .


Bonnes pratiques
Performances : travailler sur /mnt/c/... est plus lent. Pour Airflow, copiez plutôt le projet dans votre dossier Linux 'formation', par exemple cp -r /mnt/c/Users/yamdj/formation-data-engineering/formation-data-engineering ~/formation.  
Accès aux fichiers Linux depuis Windows : tapez \\wsl$\Ubuntu\home\<votre_nom> dans l’Explorateur.
Réseau : un service lancé dans WSL, comme l’interface Airflow sur le port 8080, s’ouvre normalement dans votre navigateur Windows à l’adresse http://localhost:8080.
PyCharm : PyCharm Professional peut utiliser un interpréteur Python situé dans WSL, via Settings, puis Python Interpreter, puis Add, puis WSL.


### Il vaut mieux utiliser VS Code dans ce tp qui accepte WSL : la procédure d'installation est à la fin du document
Dans ce cas finissez la procédure pour VS code et crééer l'environnement data

cd ~/formation-data-engineering
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
python -m fil_rouge.generate_data






```bash
# Depuis la racine du projet, dans WSL ou VScode / Linux / macOS
python3 -m venv .venv-airflow
si la commande précédente ne marche pas mettez à jour apt et taper les commandes après *
*sudo apt update
*sudo apt install -y python3.14-venv python3-pip
*rm -rf .venv-airflow          # supprime l'environnement à moitié créé
*python3 -m venv .venv-airflow



source .venv-airflow/bin/activate
AIRFLOW_VERSION=3.3.2
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" #Cette commande récupère la version de Python sous la forme « majeure.mineure », par exemple 3.13 ou 3.14, et la range dans une variable PYTHON_VERSION.
# dans mon cas 3.14 est compatible avec la version 3.3.2 de Airflow
pip install "apache-airflow[http,sqlite]==${AIRFLOW_VERSION}" apache-airflow-providers-common-sql \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Configuration (Variables, Connections, dossier des DAGs) — lisez le script !
export DE_PYTHON="$(pwd)/.venv/bin/python"                         # Python de l'environnement "data" Indique à Airflow le Python « data » (.venv), avec lequel il lancera les scripts du jour 1
export DAGS_FOLDER="$(pwd)/jour2/TP3_airflow/squelette/dags"       # VOTRE DAG Choisit le dossier du DAG à charger : ici le squelette à compléter
source jour2/TP3_airflow/airflow_env.sh                            # Charge le .env et crée les Variables et Connections Airflow (API, entrepôt, clé secrète), sans aucun secret dans le DAG
airflow db migrate                                                 # Crée la base interne d’Airflow (dans airflow_home/)
airflow pools set entrepot_sqlite 1 "SQLite : un seul écrivain"    #  Crée un pool à 1 place : les écritures dans SQLite passent une par une, ce qui évite l’erreur database is locked

airflow standalone      # UI : #Démarre tout Airflow. Interface sur http://localhost:8080, identifiant et mot de passe affichés dans le terminal, arrêt avec Ctrl+C
```
utilisateur et mdp de l'interface (mdp liés au profil du formateur, trouvez le votre) 
http://localhost:8080
{"admin": "WcwgteExqT2cGDwq"}.

L'API simulée doit tourner dans un autre terminal : `uvicorn mock_api.app:app --port 8000`.







## Étapes
Ouvrer un troisième terminal
cd ~/formation-data-engineering
source .venv-airflow/bin/activate
export DE_PYTHON="$(pwd)/.venv/bin/python"
export DAGS_FOLDER="$(pwd)/jour2/TP3_airflow/squelette/dags"
source jour2/TP3_airflow/airflow_env.sh
echo $AIRFLOW_HOME            # doit afficher /home/yamdjeu/formation-data-engineering/airflow_home


### 1. Premier DAG minimal (20 min) · TODO 4, 5, 6, 7
Écrivez `default_args`, le planning, le sensor et le contrôle de santé. Vérifiez :
```bash
airflow dags list-import-errors        # aucune erreur de parsing
airflow tasks test nexapay_pipeline_quotien api_disponible # à lancer quand vous faites le dag avec la première task
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




### Installation VS Code 
1. Installer VS Code sous Windows

Dans PowerShell :

winget install Microsoft.VisualStudioCode

Ou téléchargez-le sur https://code.visualstudio.com. Pendant l’installation, gardez cochée l’option « Ajouter à PATH ».

2. Installer l’extension WSL

Ouvrez VS Code, puis le panneau Extensions (Ctrl+Shift+X). Recherchez WSL, éditée par Microsoft, et cliquez sur Install.

3. Ouvrir le projet depuis Ubuntu


Dans le terminal Ubuntu :

cd ~/formation-data-engineering
code .

La première fois, VS Code installe automatiquement son composant côté Linux, ce qui prend une minute.
La fenêtre s’ouvre avec, en bas à gauche, un badge vert WSL: Ubuntu : vous travaillez dans Ubuntu.

Autre méthode, depuis VS Code : cliquez sur le badge >< en bas à gauche, puis Connect to WSL, puis File, puis Open Folder, et choisissez /home/yamdjeu/formation-data-engineering.

4. Installer les extensions Python côté Ubuntu

Dans le panneau Extensions, les extensions locales sont séparées de celles installées dans WSL. Installez dans WSL, via le bouton Install in WSL: Ubuntu :

Python (Microsoft), qui inclut Pylance pour l’autocomplétion ;
Jupyter, facultatif, pour les notebooks d’exploration ;
SQLite Viewer, facultatif, pour inspecter data/warehouse.db d’un clic.
5. Choisir l’interpréteur

Ctrl+Shift+P, puis tapez Python: Select Interpreter, et choisissez :

./.venv-airflow/bin/python pour écrire le DAG du TP3, avec l’autocomplétion d’Airflow ;
./.venv/bin/python pour les TP data, le projet et les tests.

L’interpréteur actif s’affiche en bas à droite : cliquez dessus pour en chang