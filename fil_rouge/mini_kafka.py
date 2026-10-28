"""Mini-broker « façon Kafka » sur fichiers, pour pratiquer SANS installer Kafka.

Reproduit l'API de `confluent_kafka` utilisée en TP (Producer.produce/poll/flush,
Consumer.subscribe/poll/commit/close, Message.value/key/partition/offset/error/timestamp)
et les concepts essentiels :

  * un TOPIC est découpé en PARTITIONS ; chaque partition est un JOURNAL append-only
    (un fichier JSONL : data/stream/<topic>/partition-<n>.log) ;
  * la partition d'un message est déterminée par sa CLÉ (hash) -> ordre garanti par clé ;
  * un message est identifié par son OFFSET (position dans la partition) ;
  * chaque GROUPE de consommateurs mémorise ses offsets commités
    (data/stream/<topic>/_offsets/<group>.json) : deux groupes lisent indépendamment,
    un consommateur redémarré reprend là où son groupe s'était arrêté.

Limites assumées (à expliquer en cours) : pas de réplication, pas de rééquilibrage automatique
(l'affectation des partitions aux membres d'un groupe est statique : `simulator.member = "i/n"`),
pas de rétention. Pour la vraie chose : STREAM_BACKEND=kafka.
"""
from __future__ import annotations

import json
import os
import time
import zlib
from pathlib import Path
from typing import Callable

from fil_rouge import config

DEFAULT_PARTITIONS = 3


def _topic_dir(topic: str) -> Path:
    d = config.DATA_DIR / "stream" / topic
    (d / "_offsets").mkdir(parents=True, exist_ok=True)
    return d


def _n_partitions(topic: str) -> int:
    meta = _topic_dir(topic) / "_meta.json"
    if not meta.exists():
        meta.write_text(json.dumps({"partitions": DEFAULT_PARTITIONS}))
    return json.loads(meta.read_text())["partitions"]


def create_topic(topic: str, partitions: int = DEFAULT_PARTITIONS) -> None:
    (_topic_dir(topic) / "_meta.json").write_text(json.dumps({"partitions": partitions}))


class KafkaError:
    _PARTITION_EOF = -191

    def __init__(self, code: int, text: str = ""):
        self._code, self._text = code, text

    def code(self) -> int:
        return self._code

    def __str__(self) -> str:
        return self._text


class Message:
    def __init__(self, topic, partition, offset, key, value, ts_ms, error=None):
        self._t, self._p, self._o, self._k, self._v, self._ts, self._e = topic, partition, offset, key, value, ts_ms, error

    def topic(self): return self._t
    def partition(self): return self._p
    def offset(self): return self._o
    def key(self): return self._k
    def value(self): return self._v
    def timestamp(self): return (1, self._ts)       # (TIMESTAMP_CREATE_TIME, ms)
    def error(self): return self._e


def _to_bytes(x) -> bytes | None:
    if x is None:
        return None
    return x if isinstance(x, bytes) else str(x).encode("utf-8")


class Producer:
    def __init__(self, conf: dict):
        self.conf = conf
        self._pending: list[tuple[Callable, Message]] = []
        self._next_offset: dict[tuple[str, int], int] = {}

    def _offset_for(self, topic: str, partition: int, path: Path) -> int:
        key = (topic, partition)
        if key not in self._next_offset:
            self._next_offset[key] = sum(1 for _ in path.open("rb")) if path.exists() else 0
        off = self._next_offset[key]
        self._next_offset[key] += 1
        return off

    def produce(self, topic: str, value=None, key=None, on_delivery=None, callback=None, partition: int | None = None):
        n = _n_partitions(topic)
        k, v = _to_bytes(key), _to_bytes(value)
        p = partition if partition is not None else (zlib.crc32(k) % n if k is not None else int(time.time_ns()) % n)
        path = _topic_dir(topic) / f"partition-{p}.log"
        offset = self._offset_for(topic, p, path)
        ts = int(time.time() * 1000)
        record = {"offset": offset, "ts": ts, "key": k.decode() if k else None, "value": v.decode() if v else None}
        with path.open("a", encoding="utf-8") as fh:            # une ligne complète par écriture
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        cb = on_delivery or callback
        if cb:
            self._pending.append((cb, Message(topic, p, offset, k, v, ts)))

    def poll(self, timeout: float = 0) -> int:
        served = len(self._pending)
        for cb, msg in self._pending:
            cb(None, msg)
        self._pending.clear()
        return served

    def flush(self, timeout: float | None = None) -> int:
        self.poll(0)
        return 0


class Consumer:
    def __init__(self, conf: dict):
        self.group = conf["group.id"]
        self.reset = conf.get("auto.offset.reset", "latest")
        self.auto_commit = str(conf.get("enable.auto.commit", True)).lower() == "true"
        self.eof = str(conf.get("enable.partition.eof", False)).lower() == "true"
        member = conf.get("simulator.member", "0/1")
        self.member_idx, self.member_count = (int(x) for x in member.split("/"))
        self._topics: list[str] = []
        self._pos: dict[tuple[str, int], int] = {}             # prochain offset à lire
        self._handles: dict[tuple[str, int], object] = {}
        self._eof_sent: set[tuple[str, int]] = set()
        self._rr = 0

    # --- abonnement et offsets --------------------------------------------------
    def _offsets_file(self, topic: str) -> Path:
        return _topic_dir(topic) / "_offsets" / f"{self.group}.json"

    def subscribe(self, topics: list[str], on_assign=None) -> None:
        self._topics = topics
        for t in topics:
            committed = json.loads(self._offsets_file(t).read_text()) if self._offsets_file(t).exists() else {}
            for p in range(_n_partitions(t)):
                if p % self.member_count != self.member_idx:
                    continue                                   # partition affectée à un autre membre
                if str(p) in committed:
                    start = committed[str(p)]
                elif self.reset == "earliest":
                    start = 0
                else:
                    path = _topic_dir(t) / f"partition-{p}.log"
                    start = sum(1 for _ in path.open("rb")) if path.exists() else 0
                self._pos[(t, p)] = start
        if on_assign:
            on_assign(self, sorted(self._pos))

    def _handle(self, tp):
        if tp not in self._handles:
            path = _topic_dir(tp[0]) / f"partition-{tp[1]}.log"
            if not path.exists():
                return None
            fh = path.open("r", encoding="utf-8")
            for _ in range(self._pos[tp]):                     # se positionner sur l'offset de départ
                fh.readline()
            self._handles[tp] = fh
        return self._handles[tp]

    def _read_one(self, tp) -> Message | None:
        fh = self._handle(tp)
        if fh is None:
            return None
        where = fh.tell()
        line = fh.readline()
        if not line or not line.endswith("\n"):               # rien de neuf, ou ligne en cours d'écriture
            fh.seek(where)
            return None
        rec = json.loads(line)
        self._pos[tp] = rec["offset"] + 1
        self._eof_sent.discard(tp)
        msg = Message(tp[0], tp[1], rec["offset"], _to_bytes(rec["key"]), _to_bytes(rec["value"]), rec["ts"])
        if self.auto_commit:
            self._commit_positions()
        return msg

    def poll(self, timeout: float = 1.0) -> Message | None:
        deadline = time.time() + (timeout or 0)
        parts = sorted(self._pos)
        while True:
            for i in range(len(parts)):                        # tourniquet entre partitions
                tp = parts[(self._rr + i) % len(parts)]
                msg = self._read_one(tp)
                if msg is not None:
                    self._rr = (self._rr + i + 1) % len(parts)
                    return msg
                if self.eof and tp not in self._eof_sent:
                    self._eof_sent.add(tp)
                    return Message(tp[0], tp[1], self._pos[tp], None, None, 0,
                                   KafkaError(KafkaError._PARTITION_EOF, "fin de partition"))
            if time.time() >= deadline:
                return None
            time.sleep(0.05)

    def _commit_positions(self, only: dict | None = None) -> None:
        by_topic: dict[str, dict] = {}
        for (t, p), pos in (only or self._pos).items():
            by_topic.setdefault(t, {})[str(p)] = pos
        for t, offsets in by_topic.items():
            f = self._offsets_file(t)
            current = json.loads(f.read_text()) if f.exists() else {}
            current.update(offsets)
            tmp = f.with_suffix(".tmp")
            tmp.write_text(json.dumps(current))
            os.replace(tmp, f)                                 # écriture atomique

    def commit(self, message: Message | None = None, asynchronous: bool = True) -> None:
        if message is not None:
            self._commit_positions({(message.topic(), message.partition()): message.offset() + 1})
        else:
            self._commit_positions()

    def close(self) -> None:
        if self.auto_commit:
            self._commit_positions()
        for fh in self._handles.values():
            fh.close()
