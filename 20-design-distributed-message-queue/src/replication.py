"""Leader-follower replication with ISR (In-Sync Replicas).

Each ReplicatedPartition wraps a Partition and adds:
- A leader that accepts writes.
- Followers that replicate from the leader.
- An ISR set tracking which followers are caught up.
- Configurable ACK modes: 0 (fire-and-forget), 1 (leader only), "all" (all ISR).
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Literal, Optional, Union

from .partition import Partition, PartitionConfig, Record


class AckMode(Enum):
    """Producer acknowledgement modes."""

    NONE = 0  # Fire and forget
    LEADER = 1  # Wait for leader write
    ALL = "all"  # Wait for all ISR replicas


@dataclass
class ReplicaInfo:
    """Metadata about a single replica."""

    broker_id: int
    partition: Partition
    leo: int = 0  # Log End Offset the follower has reached
    last_fetch_time: float = 0.0


class ReplicatedPartition:
    """A partition with leader-follower replication and ISR tracking.

    The leader is the single writer. Followers pull records from the leader.
    A follower is in the ISR if its LEO is within *replica_lag_max* of the
    leader's LEO and it has fetched within *replica_time_max* seconds.
    """

    def __init__(
        self,
        topic: str,
        partition_id: int,
        leader_broker_id: int,
        follower_broker_ids: Optional[list[int]] = None,
        ack_mode: Union[AckMode, int, str] = AckMode.LEADER,
        replica_lag_max: int = 10,
        replica_time_max: float = 30.0,
        partition_config: Optional[PartitionConfig] = None,
    ) -> None:
        self.topic = topic
        self.partition_id = partition_id
        self.ack_mode = self._normalize_ack(ack_mode)
        self.replica_lag_max = replica_lag_max
        self.replica_time_max = replica_time_max

        cfg = partition_config or PartitionConfig()

        # Leader replica.
        leader_partition = Partition(topic, partition_id, cfg)
        self.leader = ReplicaInfo(
            broker_id=leader_broker_id,
            partition=leader_partition,
        )

        # Follower replicas.
        self.followers: dict[int, ReplicaInfo] = {}
        for bid in (follower_broker_ids or []):
            self.followers[bid] = ReplicaInfo(
                broker_id=bid,
                partition=Partition(topic, partition_id, cfg),
                last_fetch_time=time.time(),
            )

        # ISR starts with leader + all followers.
        self._isr_broker_ids: set[int] = {leader_broker_id}
        self._isr_broker_ids.update(self.followers.keys())

        self._lock = threading.Lock()
        # High water mark: the offset up to which all ISR replicas have data.
        self._hw: int = 0

    # -- public properties ---------------------------------------------------

    @property
    def isr(self) -> set[int]:
        with self._lock:
            return set(self._isr_broker_ids)

    @property
    def high_watermark(self) -> int:
        with self._lock:
            return self._hw

    # -- write path ----------------------------------------------------------

    def produce(
        self,
        key: Optional[str],
        value: bytes,
        timestamp: Optional[float] = None,
    ) -> Record:
        """Produce a record to the leader and replicate based on ack_mode."""
        record = self.leader.partition.append(key, value, timestamp)
        self.leader.leo = self.leader.partition.log_end_offset

        if self.ack_mode == AckMode.NONE:
            # Fire and forget -- don't even replicate synchronously.
            return record

        if self.ack_mode == AckMode.ALL:
            # Synchronously replicate to all ISR followers.
            self._replicate_to_isr()

        self._update_high_watermark()
        return record

    # -- follower fetch (pull-based replication) -----------------------------

    def follower_fetch(self, broker_id: int, max_records: int = 100) -> list[Record]:
        """Simulate a follower fetching records from the leader."""
        with self._lock:
            follower = self.followers.get(broker_id)
            if follower is None:
                raise ValueError(f"Unknown follower broker_id={broker_id}")

            records = self.leader.partition.read(follower.leo, max_records)
            for r in records:
                follower.partition.append(r.key, r.value, r.timestamp)
            if records:
                follower.leo = records[-1].offset + 1
            follower.last_fetch_time = time.time()

            # Re-evaluate ISR membership.
            self._check_isr(broker_id, follower)
            self._update_high_watermark_unlocked()
            return records

    # -- read path -----------------------------------------------------------

    def consume(self, offset: int, max_records: int = 100) -> list[Record]:
        """Read committed records (up to the high watermark)."""
        with self._lock:
            hw = self._hw
        records = self.leader.partition.read(offset, max_records)
        # Only return records below the high watermark.
        return [r for r in records if r.offset < hw]

    # -- internal ------------------------------------------------------------

    def _replicate_to_isr(self) -> None:
        """Replicate new records to all in-sync followers (synchronous)."""
        with self._lock:
            for bid in list(self._isr_broker_ids):
                if bid == self.leader.broker_id:
                    continue
                follower = self.followers.get(bid)
                if follower is None:
                    continue
                records = self.leader.partition.read(follower.leo)
                for r in records:
                    follower.partition.append(r.key, r.value, r.timestamp)
                if records:
                    follower.leo = records[-1].offset + 1
                follower.last_fetch_time = time.time()
                self._check_isr(bid, follower)

    def _check_isr(self, broker_id: int, follower: ReplicaInfo) -> None:
        """Add or remove a follower from the ISR based on lag."""
        leader_leo = self.leader.partition.log_end_offset
        lag = leader_leo - follower.leo
        time_since_fetch = time.time() - follower.last_fetch_time
        in_sync = lag <= self.replica_lag_max and time_since_fetch <= self.replica_time_max
        if in_sync:
            self._isr_broker_ids.add(broker_id)
        else:
            self._isr_broker_ids.discard(broker_id)
            # Leader is always in ISR.
            self._isr_broker_ids.add(self.leader.broker_id)

    def _update_high_watermark(self) -> None:
        with self._lock:
            self._update_high_watermark_unlocked()

    def _update_high_watermark_unlocked(self) -> None:
        """HW = min LEO among all ISR replicas."""
        leos = [self.leader.leo]
        for bid in self._isr_broker_ids:
            if bid == self.leader.broker_id:
                continue
            f = self.followers.get(bid)
            if f is not None:
                leos.append(f.leo)
        self._hw = min(leos) if leos else 0

    @staticmethod
    def _normalize_ack(mode: Union[AckMode, int, str]) -> AckMode:
        if isinstance(mode, AckMode):
            return mode
        if mode == 0:
            return AckMode.NONE
        if mode == 1:
            return AckMode.LEADER
        if mode in ("all", -1):
            return AckMode.ALL
        raise ValueError(f"Invalid ack mode: {mode}")
