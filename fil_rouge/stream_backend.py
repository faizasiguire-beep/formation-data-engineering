"""Choix du backend de streaming : vrai Kafka (confluent-kafka) ou simulateur sur fichiers.

    STREAM_BACKEND=simulator   (défaut, aucune installation)
    STREAM_BACKEND=kafka       (broker Kafka joignable sur KAFKA_BOOTSTRAP_SERVERS)

Le code des TP est IDENTIQUE dans les deux cas : seule cette fabrique change.
"""
from __future__ import annotations

import os

from fil_rouge import config  # noqa: F401 — charge le fichier .env

BACKEND = os.getenv("STREAM_BACKEND", "simulator").lower()
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def make_producer(extra: dict | None = None):
    conf = {"bootstrap.servers": BOOTSTRAP, "client.id": "nexapay-producer",
            "acks": "all",                 # attendre la réplication : pas de perte silencieuse
            "enable.idempotence": True,    # pas de doublon en cas de retry réseau
            "linger.ms": 20, **(extra or {})}
    if BACKEND == "kafka":
        from confluent_kafka import Producer
        return Producer(conf)
    from fil_rouge.mini_kafka import Producer
    return Producer(conf)


def make_consumer(group_id: str, extra: dict | None = None):
    conf = {"bootstrap.servers": BOOTSTRAP, "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # commit manuel APRÈS traitement -> « au moins une fois »
            **(extra or {})}
    if BACKEND == "kafka":
        from confluent_kafka import Consumer
        conf.pop("simulator.member", None)
        return Consumer(conf)
    from fil_rouge.mini_kafka import Consumer
    return Consumer(conf)


def is_partition_eof(msg) -> bool:
    err = msg.error()
    if err is None:
        return False
    if BACKEND == "kafka":
        from confluent_kafka import KafkaError
        return err.code() == KafkaError._PARTITION_EOF
    return err.code() == -191
