"""DAG Airflow 3 — Projet intégré « Fraude : signaux comportementaux de l'application mobile ».

    api_disponible ─► sante_api ─► collecte_api ──────┐
                                   collecte_db ───────┤
                                   collecte_labels ───┼─► qualite_entree ─► build_features ─► qualite_sortie
                                   collecte_sessions ─┘     (diagnostic)     (pseudonymisation)  (BLOQUANT)
                                                                                                    │
                                                              fin ◄─ bilan_run ◄─ export_snapshot ◄─┘

Principes (repris du TP3) :
  * AUCUN secret ni chemin machine : identifiants API dans la Connection `nexa_api`, clé de pseudonymisation
    dans la Variable `pseudo_secret_key` (masquée), chemins dans les Variables `project_root` / `de_python`.
  * Les scripts tournent dans l'environnement « data » (`de_python`) : Airflow orchestre, il n'exécute pas
    le code métier dans son propre interpréteur.
  * Retries + backoff pour les pannes transitoires ; retries=0 pour les étapes dont l'échec est une
    non-conformité des données (réessayer ne changerait rien).
  * Idempotence : collecte incrémentale à filigrane, staging et curated remplacées en une transaction.
"""
from __future__ import annotations

import json
import logging
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

log = logging.getLogger(__name__)

PY = "{{ var.value.de_python }}"
ROOT = "{{ var.value.project_root }}"
SRC = f"{ROOT}/jour3/projet_integre/src"

API_ENV = {
    "NEXA_API_BASE_URL": "{{ conn.nexa_api.schema }}://{{ conn.nexa_api.host }}:{{ conn.nexa_api.port }}",
    "NEXA_API_CLIENT_ID": "{{ conn.nexa_api.login }}",
    "NEXA_API_CLIENT_SECRET": "{{ conn.nexa_api.password }}",
}
PSEUDO_ENV = {"PSEUDO_SECRET_KEY": "{{ var.value.pseudo_secret_key }}"}


def alerte_echec(context) -> None:
    """Callback d'échec : journal d'alertes JSONL + webhook optionnel. Ne doit jamais planter lui-même."""
    ti = context["task_instance"]
    exc = context.get("exception")
    alerte = {"dag": ti.dag_id, "tache": ti.task_id, "run_id": context["run_id"], "tentative": ti.try_number,
              "erreur": f"{type(exc).__name__}: {exc}"[:500] if exc else None,
              "horodatage": pendulum.now("UTC").isoformat()}
    log.error("ALERTE projet fraude : %s", alerte)
    try:
        fichier = Path(Variable.get("project_root")) / "logs" / "alertes_projet_fraude.jsonl"
        fichier.parent.mkdir(parents=True, exist_ok=True)
        with fichier.open("a", encoding="utf-8") as f:
            f.write(json.dumps(alerte, ensure_ascii=False) + "\n")
    except Exception:  # noqa: BLE001
        log.exception("Impossible d'écrire le fichier d'alertes")
    try:
        webhook = Variable.get("alert_webhook_url", default=None)
        if webhook:
            import requests
            requests.post(webhook, json=alerte, timeout=10)
    except Exception:  # noqa: BLE001
        log.exception("Échec de l'envoi au webhook d'alerte")


def bilan(**context) -> dict:
    """Petit dict de contrôle (XCom) lu dans les rapports : jamais un DataFrame."""
    root = Path(Variable.get("project_root"))
    sortie = json.loads((root / "reports" / "projet_fraude" / "qualite_sortie.json").read_text(encoding="utf-8"))
    manifest = json.loads((root / "jour3" / "projet_integre" / "data" / "ml_fraud_features" / "MANIFEST.json")
                          .read_text(encoding="utf-8"))
    resume = {"lignes": manifest["lignes"], "fraudes": manifest["fraudes"], "taux_fraude": manifest["taux_fraude"],
              "empreinte": manifest["empreinte_contenu"], "qualite_sortie_conforme": sortie["conforme"]}
    log.info("Bilan du run : %s", resume)
    return resume


default_args = {
    "owner": "equipe-data",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": 2.0,          # Airflow 3 : multiplicateur (True vaudrait 1.0 = délai constant)
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": alerte_echec,
}

with DAG(
    dag_id="projet_fraude_comportementale",
    description="Brief A — features de fraude issues des sessions applicatives (collecte, qualité, RGPD, DVC)",
    schedule="0 3 * * *",                       # chaque nuit à 03:00, après la collecte quotidienne
    start_date=pendulum.datetime(2026, 10, 1, tz="Africa/Abidjan"),
    catchup=False,                              # pipeline à filigrane : rejouer le passé n'a pas de sens
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    tags=["nexapay", "projet-integre", "fraude"],
    doc_md=__doc__,
) as dag:

    api_disponible = HttpSensor(task_id="api_disponible", http_conn_id="nexa_api", endpoint="health",
                                poke_interval=30, timeout=10 * 60, mode="reschedule")
    sante_api = HttpOperator(task_id="sante_api", http_conn_id="nexa_api", endpoint="health", method="GET",
                             response_check=lambda r: r.json().get("merchants", 0) > 0, log_response=True)

    collecte = {
        src: BashOperator(
            task_id=f"collecte_{src}",
            bash_command=f"{PY} {SRC}/collect.py --mode incremental --sources {src}",
            env=API_ENV if src == "api" else None,
            append_env=True,
            pool="entrepot_sqlite",             # une seule écriture SQLite à la fois
            cwd=ROOT,
        )
        for src in ("api", "db", "labels", "sessions")
    }

    qualite_entree = BashOperator(task_id="qualite_entree", bash_command=f"{PY} {SRC}/quality_in.py",
                                  pool="entrepot_sqlite", cwd=ROOT)
    build_features = BashOperator(task_id="build_features", bash_command=f"{PY} {SRC}/build_features.py",
                                  env=PSEUDO_ENV, append_env=True, pool="entrepot_sqlite", cwd=ROOT,
                                  retries=0)    # clé absente ou données invalides : réessayer ne change rien
    qualite_sortie = BashOperator(task_id="qualite_sortie", bash_command=f"{PY} {SRC}/quality_out.py",
                                  pool="entrepot_sqlite", cwd=ROOT, retries=0)   # contrôle BLOQUANT
    export_snapshot = BashOperator(task_id="export_snapshot", bash_command=f"{PY} {SRC}/export_snapshot.py", cwd=ROOT)
    bilan_run = PythonOperator(task_id="bilan_run", python_callable=bilan)
    fin = EmptyOperator(task_id="fin")

    api_disponible >> sante_api >> collecte["api"]
    list(collecte.values()) >> qualite_entree >> build_features >> qualite_sortie
    qualite_sortie >> export_snapshot >> bilan_run >> fin
