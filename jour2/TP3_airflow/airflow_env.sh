#!/usr/bin/env bash
# =====================================================================================
# Configuration d'Airflow 3 en local, SANS Docker (Linux, macOS, ou Windows via WSL2).
#
#   source jour2/TP3_airflow/airflow_env.sh          # à lancer depuis la racine du projet
#   airflow standalone                               # UI : http://localhost:8080
#
# Airflow ne fonctionne pas nativement sous Windows : utiliser WSL2 (Ubuntu), cf. README.
# Toutes les valeurs sensibles sont lues dans .env : rien n'est écrit en dur ici.
# =====================================================================================

PROJECT_ROOT="$(pwd)"
if [ ! -f "$PROJECT_ROOT/pyproject.toml" ]; then
  echo "⚠ Lancez ce script depuis la racine du projet (là où se trouve pyproject.toml)"; return 1 2>/dev/null || exit 1
fi
set -a; . "$PROJECT_ROOT/.env"; set +a          # charge NEXA_API_*, PSEUDO_SECRET_KEY...

# Dossier de travail d'Airflow (base de métadonnées SQLite, logs, config)
export AIRFLOW_HOME="${AIRFLOW_HOME:-$PROJECT_ROOT/airflow_home}"
# DAGs : par défaut le CORRIGÉ ; pour travailler sur votre version :
#   export DAGS_FOLDER=$PROJECT_ROOT/jour2/TP3_airflow/squelette/dags
export AIRFLOW__CORE__DAGS_FOLDER="${DAGS_FOLDER:-$PROJECT_ROOT/jour2/TP3_airflow/corrige/dags}"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DEFAULT_TIMEZONE=Africa/Abidjan
export AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=30

# --- Variables Airflow (préfixe AIRFLOW_VAR_) -------------------------------------
# Python de l'environnement "data" (celui qui contient pandas, great_expectations...)
export AIRFLOW_VAR_DE_PYTHON="${DE_PYTHON:-$PROJECT_ROOT/.venv/bin/python}"
export AIRFLOW_VAR_PROJECT_ROOT="$PROJECT_ROOT"
export AIRFLOW_VAR_PSEUDO_SECRET_KEY="$PSEUDO_SECRET_KEY"     # nom contenant "secret" -> masqué
export AIRFLOW_VAR_SEUIL_ALERTE_BLACKLIST=0.08   # part "normale" observée : ~5 %

# --- Connections Airflow (préfixe AIRFLOW_CONN_, format JSON) ---------------------
API_HOST=$(echo "${NEXA_API_BASE_URL:-http://127.0.0.1:8000}" | sed -E 's#https?://##; s#:.*##')
API_PORT=$(echo "${NEXA_API_BASE_URL:-http://127.0.0.1:8000}" | sed -E 's#.*:([0-9]+).*#\1#')
export AIRFLOW_CONN_NEXA_API="{\"conn_type\": \"http\", \"schema\": \"http\", \"host\": \"$API_HOST\", \"port\": $API_PORT, \"login\": \"$NEXA_API_CLIENT_ID\", \"password\": \"$NEXA_API_CLIENT_SECRET\"}"
export AIRFLOW_CONN_NEXA_DWH="{\"conn_type\": \"sqlite\", \"host\": \"$PROJECT_ROOT/data/warehouse.db\"}"

echo "✔ AIRFLOW_HOME=$AIRFLOW_HOME"
echo "✔ DAGs        =$AIRFLOW__CORE__DAGS_FOLDER"
echo "✔ Python data =$AIRFLOW_VAR_DE_PYTHON"
echo "Première fois : airflow db migrate && airflow pools set entrepot_sqlite 1 'SQLite : un seul écrivain'"
