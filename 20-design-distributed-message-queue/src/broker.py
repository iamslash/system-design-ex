"""Broker that manages topics, partitions, and routes produce/consume requests.

A broker owns a set of partitions. In this simplified model a single broker
process can host multiple topics, each with configurable partition counts.
Replication-aware: can create ReplicatedPartitions when follower brokers exist.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Optional, Union

from .partition import Partition, PartitionConfig, Record
from .replication import AckMode, ReplicatedPartition


@dataclass
class TopicConfig:
    num_partitions: int = 3
    replication_factor: int = 1
    segment_max_size: int = 1000


class Broker:
    """Central message broker managing topics and partitions.

    Supports both plain partitions (replication_factor=1) and replicated
    partitions (replication_factor > 1 with peer broker ids).
    """

    def __init__(
        self,
        broker_id: int = 0,
        peer_broker_ids: Optional[list[int]] = None,
    ) -> None:
        self.broker_id = broker_id
        self.peer_broker_ids = peer_broker_ids or []
        self._lock = threading.Lock()
        # topic_name -> list of Partition | ReplicatedPartition
        self._topics: dict[str, list[Union[Partition, ReplicatedPartition]]] = {}
        self._topic_configs: dict[str, TopicConfig] = {}

    # -- topic management ----------------------------------------------------

    def create_topic(
        self,
        name: str,
        config: Optional[TopicConfig] = None,
    ) -> None:
        """Create a new topic with the given configuration."""
        cfg = config or TopicConfig()
        with self._lock:
            if name in self._topics:
                raise ValueError(f"Topic '{name}' already exists")
            pcfg = PartitionConfig(segment_max_size=cfg.segment_max_size)
            partitions: list[Union[Partition, ReplicatedPartition]] = []
            for pid in range(cfg.num_partitions):
                if cfg.replication_factor > 1 and self.peer_broker_ids:
                    # Assign followers round-robin from peers.
                    followers = self.peer_broker_ids[: cfg.replication_factor - 1]
                    rp = ReplicatedPartition(
                        topic=name,
                        partition_id=pid,
                        leader_broker_id=self.broker_id,
                        follower_broker_ids=followers,
                        partition_config=pcfg,
                    )
                    partitions.append(rp)
                else:
                    partitions.append(Partition(name, pid, pcfg))
            self._topics[name] = partitions
            self._topic_configs[name] = cfg

    def delete_topic(self, name: str) -> None:
        with self._lock:
            if name not in self._topics:
                raise ValueError(f"Topic '{name}' does not exist")
            del self._topics[name]
            del self._topic_configs[name]

    def list_topics(self) -> list[str]:
        with self._lock:
            return list(self._topics.keys())

    def get_topic_config(self, name: str) -> TopicConfig:
        with self._lock:
            if name not in self._topic_configs:
                raise ValueError(f"Topic '{name}' does not exist")
            return self._topic_configs[name]

    # -- partition access ----------------------------------------------------

    def get_partition(
        self, topic: str, partition_id: int
    ) -> Union[Partition, ReplicatedPartition]:
        with self._lock:
            parts = self._topics.get(topic)
            if parts is None:
                raise ValueError(f"Topic '{topic}' does not exist")
            if partition_id < 0 or partition_id >= len(parts):
                raise ValueError(
                    f"Partition {partition_id} out of range for topic '{topic}'"
                )
            return parts[partition_id]

    def num_partitions(self, topic: str) -> int:
        with self._lock:
            parts = self._topics.get(topic)
            if parts is None:
                raise ValueError(f"Topic '{topic}' does not exist")
            return len(parts)

    # -- produce / consume shortcuts -----------------------------------------

    def produce(
        self,
        topic: str,
        partition_id: int,
        key: Optional[str],
        value: bytes,
    ) -> Record:
        """Append a record to a specific partition."""
        part = self.get_partition(topic, partition_id)
        if isinstance(part, ReplicatedPartition):
            return part.produce(key, value)
        return part.append(key, value)

    def consume(
        self,
        topic: str,
        partition_id: int,
        offset: int,
        max_records: int = 100,
    ) -> list[Record]:
        """Read records from a specific partition starting at *offset*."""
        part = self.get_partition(topic, partition_id)
        if isinstance(part, ReplicatedPartition):
            return part.consume(offset, max_records)
        return part.read(offset, max_records)
