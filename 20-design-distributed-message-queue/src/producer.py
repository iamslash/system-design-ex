"""Producer with batching and partition routing.

The producer accumulates records in a per-partition batch and flushes
them to the broker when the batch is full or an explicit flush is called.
Partition selection is by key hash (deterministic) or round-robin.
"""

from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

from .broker import Broker
from .partition import Record


@dataclass
class ProducerConfig:
    batch_size: int = 16  # Max records per batch before auto-flush
    linger_ms: int = 0  # Not used in sync mode, kept for API parity


class ProducerRecord:
    """A record to be sent to a topic."""

    __slots__ = ("topic", "key", "value")

    def __init__(self, topic: str, value: bytes, key: Optional[str] = None) -> None:
        self.topic = topic
        self.key = key
        self.value = value


class Producer:
    """High-level producer that batches and routes records to a broker."""

    def __init__(
        self,
        broker: Broker,
        config: Optional[ProducerConfig] = None,
    ) -> None:
        self.broker = broker
        self.config = config or ProducerConfig()
        self._lock = threading.Lock()
        # (topic, partition_id) -> list[ProducerRecord]
        self._batches: dict[tuple[str, int], list[ProducerRecord]] = {}
        self._rr_counters: dict[str, int] = {}
        self._sent_count = 0

    # -- public API ----------------------------------------------------------

    def send(self, record: ProducerRecord) -> list[Record]:
        """Send a record. May trigger an auto-flush if the batch is full.

        Returns any records that were flushed (empty list if batched).
        """
        pid = self._select_partition(record.topic, record.key)
        batch_key = (record.topic, pid)

        with self._lock:
            batch = self._batches.setdefault(batch_key, [])
            batch.append(record)
            if len(batch) >= self.config.batch_size:
                return self._flush_batch(batch_key)
        return []

    def flush(self) -> list[Record]:
        """Flush all pending batches and return all produced records."""
        with self._lock:
            results: list[Record] = []
            for batch_key in list(self._batches.keys()):
                results.extend(self._flush_batch(batch_key))
            return results

    @property
    def pending_count(self) -> int:
        with self._lock:
            return sum(len(b) for b in self._batches.values())

    @property
    def sent_count(self) -> int:
        return self._sent_count

    # -- partition selection --------------------------------------------------

    def _select_partition(self, topic: str, key: Optional[str]) -> int:
        """Select partition by key hash or round-robin."""
        num_parts = self.broker.num_partitions(topic)
        if key is not None:
            h = int(hashlib.md5(key.encode()).hexdigest(), 16)
            return h % num_parts
        # Round-robin.
        with self._lock:
            counter = self._rr_counters.get(topic, 0)
            pid = counter % num_parts
            self._rr_counters[topic] = counter + 1
        return pid

    # -- internal flush -------------------------------------------------------

    def _flush_batch(self, batch_key: tuple[str, int]) -> list[Record]:
        """Flush a single batch to the broker. Caller must hold _lock."""
        topic, pid = batch_key
        batch = self._batches.pop(batch_key, [])
        results: list[Record] = []
        for rec in batch:
            record = self.broker.produce(topic, pid, rec.key, rec.value)
            results.append(record)
            self._sent_count += 1
        return results
