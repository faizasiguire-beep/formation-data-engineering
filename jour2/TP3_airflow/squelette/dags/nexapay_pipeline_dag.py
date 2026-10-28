"""TP3 — SQUELETTE — DAG Airflow 3 orchestrant le pipeline NexaPay du Jour 1.

Cible (à construire progressivement, cf. ENONCE.md) :

    api_disponible ─► sante_api ─► collecte_api ─┐
                                   collecte_db  ─┤
                                   collecte_csv ─┼─► audit_qualite ─► nettoyage_staging ─► controle_volumetrie
                                   collecte_json─┘                                                 │
                                                        kpi_journaliers ◄──────────────────────────┘
                                                              │
                                                          bilan_run ─► choix_alerte ─┬─► alerte_fraude
                                                                                     └─► fin_normale

Imports Airflow 3 : les opérateurs « standard » sont dans airflow.providers.standard.*
et les objets de définition (DAG, Variable...) dans airflow.sdk.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import DAG, Variable

log = logging.getLogger(__name__)

PY = "{{ var.value.de_python }}"
ROOT = "{{ var.value.project_root }}"
COLLECT = f"{PY} {ROOT}/jour1/TP1_collecte/corrige/collect_pipeline.py"
TP2 = f"{ROOT}/jour1/TP2_qualite_rgpd/corrige"

# TODO 1 — Variables d'environnement à injecter dans la tâche de collecte API, construites à partir
#          de la Connection `nexa_api` grâce au templating Jinja ({{ conn.nexa_api.login }}, etc.) :
#          NEXA_API_BASE_URL, NEXA_API_CLIENT_ID, NEXA_API_CLIENT_SECRET
# Ces chaînes ne sont résolues qu'au moment de l'exécution de la tâche : aucun secret dans le fichier.
API_ENV: dict[str, str] = {
    "NEXA_API_BASE_URL": "{{ conn.nexa_api.schema }}://{{ conn.nexa_api.host }}:{{ conn.nexa_api.port }}",
    "NEXA_API_CLIENT_ID": "{{ conn.nexa_api.login }}",
    "NEXA_API_CLIENT_SECRET": "{{ conn.nexa_api.password }}",
}
# TODO 2 — Idem pour la clé de pseudonymisation (Variable `pseudo_secret_key`)
PSEUDO_ENV: dict[str, str] = {"PSEUDO_SECRET_KEY": "{{ var.value.pseudo_secret_key }}"}


def calculer_bilan() -> dict:
    """Bilan du run (TODO 12) : petit dict de contrôle, renvoyé dans XCom (jamais un DataFrame)."""
    import sqlite3

    dwh = Path(Variable.get("project_root")) / "data" / "warehouse.db"
    with sqlite3.connect(dwh) as conn:
        # Dernier statut connu de chaque source dans audit_log
        statuts = dict(conn.execute(
            """SELECT source, status FROM audit_log a
               WHERE started_at = (SELECT MAX(started_at) FROM audit_log b WHERE b.source = a.source)"""
        ).fetchall())
        # DISTINCT : en incrémental, une transaction mise à jour est chargée une seconde fois en zone brute
        total, blacklistees = conn.execute(
            """SELECT COUNT(DISTINCT t.transaction_id),
                      COUNT(DISTINCT CASE WHEN b.merchant_id IS NOT NULL THEN t.transaction_id END)
               FROM raw_transactions t
               LEFT JOIN raw_merchant_blacklist b ON b.merchant_id = t.merchant_id
               WHERE t.created_at >= datetime('now', '-1 day')"""
        ).fetchone()
    bilan = {
        "statuts_sources": statuts,
        "sources_en_echec": sorted(s for s, st in statuts.items() if st != "SUCCESS"),
        "transactions_24h": total,
        "transactions_blacklist_24h": blacklistees,
        "part_blacklist": round(blacklistees / total, 4) if total else 0.0,
    }
    log.info("Bilan du run : %s", bilan)
    return bilan


def choisir_branche(ti) -> str:
    """TODO 13 : renvoie le task_id de la branche à exécuter."""
    bilan = ti.xcom_pull(task_ids="bilan_run")
    seuil = float(Variable.get("seuil_alerte_blacklist"))
    part = bilan["part_blacklist"]
    log.info("Part blacklist %.2f %% (seuil %.2f %%)", part * 100, seuil * 100)
    return "alerte_fraude" if part > seuil else "fin_normale"


def alerte_echec(context) -> None:
    """Callback d'échec.

    TODO 3 — Construire un dictionnaire (dag, tâche, run_id, tentative, erreur, horodatage) à partir
    de `context` (context["task_instance"], context["run_id"], context.get("exception")), le logger
    en ERROR et l'ajouter (une ligne JSON) dans <project_root>/logs/alertes_airflow.jsonl.
    Bonus : POST vers un webhook si la Variable `alert_webhook_url` est définie.
    """
    ti = context["task_instance"]
    exc = context.get("exception")
    alerte = {
        "dag": ti.dag_id,
        "tache": ti.task_id,
        "run_id": context["run_id"],
        "tentative": ti.try_number,
        "erreur": f"{type(exc).__name__}: {exc}"[:500] if exc else None,
        "horodatage": pendulum.now("UTC").isoformat(),
    }
    log.error("ALERTE pipeline : %s", alerte)
    # Le callback ne doit jamais planter lui-même : on protège chaque canal d'alerte
    try:
        fichier = Path(Variable.get("project_root")) / "logs" / "alertes_airflow.jsonl"
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


default_args = {
    "owner": "equipe-data",
    # TODO 4 — 2 tentatives, délai 30 s, backoff exponentiel plafonné à 10 min,
    #          timeout d'exécution 20 min, callback d'échec
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    # Airflow 3 : c'est un MULTIPLICATEUR (2.0 = le délai double), plus un booléen comme en Airflow 2
    "retry_exponential_backoff": 2.0,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": alerte_echec,
}

with DAG(
    dag_id="nexapay_pipeline_quotidien",
    description="Collecte multi-sources, audit qualité/RGPD, staging et KPI — fil rouge NexaPay",
    # TODO 5 — tous les jours à 02:00, fuseau Africa/Abidjan, pas de rattrapage,
    #          un seul run actif à la fois, timeout global de 2 h
    schedule="0 2 * * *",                # cron : minute 0, heure 2, tous les jours
    start_date=pendulum.datetime(2026, 10, 1, tz="Africa/Abidjan"),   # le fuseau du cron vient d'ici
    catchup=False,                       # ne pas rejouer les jours passés depuis start_date
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    tags=["nexapay", "formation", "jour2"],
    doc_md=__doc__,
) as dag:

    # TODO 6 — HttpSensor `api_disponible` sur l'endpoint "health" de la connexion `nexa_api`
    #          (poke toutes les 30 s, abandon après 10 min, mode="reschedule" : pourquoi ?)
    api_disponible = HttpSensor(
        task_id="api_disponible",
        http_conn_id="nexa_api",
        endpoint="health",
        poke_interval=30,
        timeout=10 * 60,
        mode="reschedule",               # libère le worker entre deux vérifications
    )

    # TODO 7 — HttpOperator `sante_api` : GET /health, réussite seulement si le champ "merchants" > 0
    #          (paramètre response_check)
    sante_api = HttpOperator(
        task_id="sante_api",
        http_conn_id="nexa_api",
        endpoint="health",
        method="GET",
        # L'API peut répondre 200 tout en étant vide : on exige au moins un marchand
        response_check=lambda response: response.json().get("merchants", 0) > 0,
        log_response=True,
    )

    # TODO 8 — Quatre BashOperator `collecte_<source>` (api, db, csv, json) appelant :
    #          f"{COLLECT} --mode incremental --sources <source>"
    #          * env=API_ENV pour l'API uniquement, append_env=True
    #          * pool="entrepot_sqlite" (créé par : airflow pools set entrepot_sqlite 1 "...")
    #          * cwd=ROOT
    collecte: dict[str, BashOperator] = {}
    for source in ("api", "db", "csv", "json"):
        collecte[source] = BashOperator(
            task_id=f"collecte_{source}",
            bash_command=f"{COLLECT} --mode incremental --sources {source}",
            env=API_ENV if source == "api" else None,
            append_env=True,             # garder PATH, HOME... en plus des variables injectées
            pool="entrepot_sqlite",      # une seule écriture SQLite à la fois
            cwd=ROOT,
        )

    # TODO 9 — BashOperator `audit_qualite` (audit_quality.py) puis `nettoyage_staging`
    #          (clean_and_load.py, env=PSEUDO_ENV, retries=0 : pourquoi ?)

    # TODO 10 — SQLCheckOperator `controle_volumetrie` (conn_id="nexa_dwh") :
    #           ≥ 1000 transactions et ≥ 100 clients en staging, aucune transaction orpheline

    # TODO 11 — SQLExecuteQueryOperator `kpi_journaliers` : créer si besoin la table kpi_journaliers,
    #           supprimer la ligne du jour {{ ds }} (idempotence !) puis l'insérer

    # TODO 12 — PythonOperator `bilan_run` : lire audit_log (sqlite3), calculer la part des
    #           transactions des dernières 24 h vers des marchands blacklistés, RETOURNER un dict (-> XCom)
    bilan_run = PythonOperator(task_id="bilan_run", python_callable=calculer_bilan)

    # TODO 13 — BranchPythonOperator `choix_alerte` : xcom_pull du bilan, comparer à la Variable
    #           `seuil_alerte_blacklist` -> "alerte_fraude" (BashOperator) ou "fin_normale" (EmptyOperator)
    choix_alerte = BranchPythonOperator(task_id="choix_alerte", python_callable=choisir_branche)
    alerte_fraude = BashOperator(
        task_id="alerte_fraude",
        bash_command=(
            "echo \"ALERTE FRAUDE {{ ds }} : part blacklist = "
            "{{ ti.xcom_pull(task_ids='bilan_run')['part_blacklist'] }} "
            "(seuil {{ var.value.seuil_alerte_blacklist }})\""
        ),
    )
    fin_normale = EmptyOperator(task_id="fin_normale")

    # TODO 14 — Dépendances (cf. schéma en tête de fichier)
    api_disponible >> sante_api >> collecte["api"]
    # PROVISOIRE, en attendant les fichiers du TP2 (TODO 9, 10, 11). Version finale :
    #   list(collecte.values()) >> audit_qualite >> nettoyage_staging >> controle_volumetrie
    #   controle_volumetrie >> kpi_journaliers >> bilan_run
    list(collecte.values()) >> bilan_run
    bilan_run >> choix_alerte >> [alerte_fraude, fin_normale]
