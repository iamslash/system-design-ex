"""Consumer with offset tracking and consumer groups.

Consumers pull records from the broker by offset. Consumer groups
coordinate partition assignment so each partition is consumed by
exactly one consumer in the group (exclusive assignment).
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Optional

from .broker import Broker
from .partition import Record


class OffsetStore:
    """In-memory offset store (simulates __consumer_offsets topic).

    Keys are (group_id, topic, partition_id) -> committed offset.
    """

    def __init__(self) -> None:
        self._offsets: dict[tuple[str, str, int], int] = {}
        self._lock = threading.Lock()

    def commit(self, group_id: str, topic: str, partition_id: int, offset: int) -> None:
        with self._lock:
            self._offsets[(group_id, topic, partition_id)] = offset

    def fetch(self, group_id: str, topic: str, partition_id: int) -> int:
        """Return the committed offset, or 0 if none."""
        with self._lock:
            return self._offsets.get((group_id, topic, partition_id), 0)

    def all_offsets(self) -> dict[tuple[str, str, int], int]:
        with self._lock:
            return dict(self._offsets)


# Global shared offset store (simulates a cluster-wide store).
_global_offset_store = OffsetStore()


def get_offset_store() -> OffsetStore:
    return _global_offset_store


def reset_offset_store() -> None:
    """Reset the global offset store (useful for tests)."""
    global _global_offset_store
    _global_offset_store = OffsetStore()


class Consumer:
    """A single consumer that reads from assigned partitions.

    Tracks its own current read position per partition and supports
    manual or auto offset commit.
    """

    def __init__(
        self,
        consumer_id: str,
        broker: Broker,
        group_id: Optional[str] = None,
        auto_commit: bool = False,
        offset_store: Optional[OffsetStore] = None,
    ) -> None:
        self.consumer_id = consumer_id
        self.broker = broker
        self.group_id = group_id or "__default__"
        self.auto_commit = auto_commit
        self._offset_store = offset_store or get_offset_store()

        # partition assignments: set of (topic, partition_id)
        self._assignments: set[tuple[str, int]] = set()
        # Local read positions (may be ahead of committed offsets).
        self._positions: dict[tuple[str, int], int] = {}

    # -- assignment ----------------------------------------------------------

    def assign(self, topic: str, partition_ids: list[int]) -> None:
        """Manually assign partitions. Loads committed offsets."""
        for pid in partition_ids:
            key = (topic, pid)
            self._assignments.add(key)
            committed = self._offset_store.fetch(self.group_id, topic, pid)
            self._positions[key] = committed

    def revoke_all(self) -> None:
        """Revoke all partition assignments."""
        self._assignments.clear()
        self._positions.clear()

    @property
    def assignments(self) -> set[tuple[str, int]]:
        return set(self._assignments)

    # -- poll / read ---------------------------------------------------------

    def poll(self, max_records: int = 100) -> dict[tuple[str, int], list[Record]]:
        """Poll all assigned partitions and return new records.

        Returns a dict mapping (topic, partition_id) to a list of records.
        """
        result: dict[tuple[str, int], list[Record]] = {}
        for topic, pid in self._assignments:
            key = (topic, pid)
            offset = self._positions.get(key, 0)
            records = self.broker.consume(topic, pid, offset, max_records)
            if records:
                result[key] = records
                # Advance local position past the last record.
                self._positions[key] = records[-1].offset + 1
                if self.auto_commit:
                    self._offset_store.commit(
                        self.group_id, topic, pid, records[-1].offset + 1
                    )
        return result

    def commit(self) -> None:
        """Commit current positions for all assigned partitions."""
        for (topic, pid), offset in self._positions.items():
            self._offset_store.commit(self.group_id, topic, pid, offset)

    def committed(self, topic: str, partition_id: int) -> int:
        """Return the last committed offset for a partition."""
        return self._offset_store.fetch(self.group_id, topic, partition_id)

    def position(self, topic: str, partition_id: int) -> int:
        """Return the current local read position."""
        return self._positions.get((topic, partition_id), 0)

    def seek(self, topic: str, partition_id: int, offset: int) -> None:
        """Seek to a specific offset for a partition."""
        key = (topic, partition_id)
        if key not in self._assignments:
            raise ValueError(
                f"Partition ({topic}, {partition_id}) is not assigned to this consumer"
            )
        self._positions[key] = offset


class ConsumerGroup:
    """Manages partition assignment across consumers in a group.

    Uses a simple range-based assignment strategy:
    partitions are distributed as evenly as possible across consumers.
    """

    def __init__(
        self,
        group_id: str,
        broker: Broker,
        offset_store: Optional[OffsetStore] = None,
    ) -> None:
        self.group_id = group_id
        self.broker = broker
        self._offset_store = offset_store or get_offset_store()
        self._consumers: list[Consumer] = []
        self._subscriptions: set[str] = set()

    def subscribe(self, topic: str) -> None:
        self._subscriptions.add(topic)

    def add_consumer(self, consumer_id: Optional[str] = None) -> Consumer:
        """Create and register a new consumer in this group."""
        cid = consumer_id or f"{self.group_id}-consumer-{len(self._consumers)}"
        consumer = Consumer(
            consumer_id=cid,
            broker=self.broker,
            group_id=self.group_id,
            offset_store=self._offset_store,
        )
        self._consumers.append(consumer)
        return consumer

    def remove_consumer(self, consumer: Consumer) -> None:
        consumer.revoke_all()
        self._consumers.remove(consumer)

    @property
    def consumers(self) -> list[Consumer]:
        return list(self._consumers)

    def rebalance(self) -> dict[str, list[tuple[str, int]]]:
        """Rebalance partitions across consumers using range assignment.

        Returns a mapping of consumer_id -> list of (topic, partition_id).
        """
        # Revoke existing assignments.
        for c in self._consumers:
            c.revoke_all()

        if not self._consumers:
            return {}

        assignment: dict[str, list[tuple[str, int]]] = {
            c.consumer_id: [] for c in self._consumers
        }

        for topic in sorted(self._subscriptions):
            num_parts = self.broker.num_partitions(topic)
            num_consumers = len(self._consumers)
            parts_per_consumer = num_parts // num_consumers
            remainder = num_parts % num_consumers

            idx = 0
            for i, consumer in enumerate(self._consumers):
                count = parts_per_consumer + (1 if i < remainder else 0)
                pids = list(range(idx, idx + count))
                consumer.assign(topic, pids)
                assignment[consumer.consumer_id].extend((topic, p) for p in pids)
                idx += count

        return assignment
