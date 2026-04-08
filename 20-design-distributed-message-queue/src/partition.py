"""Partition with WAL-based append-only log segments.

Each partition is an ordered, immutable sequence of records.
Records are appended to the active segment. When a segment reaches
its max size, it is sealed and a new segment is created.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class Record:
    """Single record stored in a partition."""

    offset: int
    key: Optional[str]
    value: bytes
    timestamp: float


class Segment:
    """A single WAL segment file (in-memory simulation).

    Segments are append-only. Once sealed, no more writes are accepted.
    """

    def __init__(self, base_offset: int, max_size: int = 1000) -> None:
        self.base_offset = base_offset
        self.max_size = max_size
        self.records: list[Record] = []
        self.sealed = False

    @property
    def next_offset(self) -> int:
        if not self.records:
            return self.base_offset
        return self.records[-1].offset + 1

    @property
    def is_full(self) -> bool:
        return len(self.records) >= self.max_size

    def append(self, key: Optional[str], value: bytes, timestamp: float) -> Record:
        """Append a record to this segment. Raises if sealed or full."""
        if self.sealed:
            raise RuntimeError("Cannot append to a sealed segment")
        if self.is_full:
            raise RuntimeError("Segment is full")
        record = Record(
            offset=self.next_offset,
            key=key,
            value=value,
            timestamp=timestamp,
        )
        self.records.append(record)
        return record

    def read(self, offset: int, max_records: int = 100) -> list[Record]:
        """Read records starting from *offset* (inclusive)."""
        if not self.records:
            return []
        start_idx = offset - self.base_offset
        if start_idx < 0:
            start_idx = 0
        return self.records[start_idx : start_idx + max_records]

    def seal(self) -> None:
        self.sealed = True


@dataclass
class PartitionConfig:
    segment_max_size: int = 1000


class Partition:
    """Append-only log partitioned into segments.

    Provides offset-based sequential reads and FIFO ordering guarantees.
    """

    def __init__(
        self,
        topic: str,
        partition_id: int,
        config: Optional[PartitionConfig] = None,
    ) -> None:
        self.topic = topic
        self.partition_id = partition_id
        self.config = config or PartitionConfig()
        self._lock = threading.Lock()
        # Start with one active segment.
        self._segments: list[Segment] = [
            Segment(base_offset=0, max_size=self.config.segment_max_size)
        ]

    # -- public helpers ------------------------------------------------------

    @property
    def log_end_offset(self) -> int:
        """The next offset that will be assigned (aka LEO)."""
        return self._active_segment.next_offset

    @property
    def log_start_offset(self) -> int:
        """Earliest available offset."""
        if not self._segments or not self._segments[0].records:
            return 0
        return self._segments[0].base_offset

    @property
    def segments(self) -> list[Segment]:
        return list(self._segments)

    # -- write path ----------------------------------------------------------

    def append(
        self,
        key: Optional[str],
        value: bytes,
        timestamp: Optional[float] = None,
    ) -> Record:
        """Append a record and return it (with assigned offset)."""
        ts = timestamp if timestamp is not None else time.time()
        with self._lock:
            seg = self._active_segment
            if seg.is_full:
                seg.seal()
                seg = Segment(
                    base_offset=seg.next_offset,
                    max_size=self.config.segment_max_size,
                )
                self._segments.append(seg)
            return seg.append(key, value, ts)

    # -- read path -----------------------------------------------------------

    def read(self, offset: int, max_records: int = 100) -> list[Record]:
        """Read up to *max_records* starting from *offset*."""
        with self._lock:
            result: list[Record] = []
            remaining = max_records
            for seg in self._segments:
                # Skip segments that are entirely before the requested offset.
                seg_end = seg.next_offset
                if seg_end <= offset:
                    continue
                records = seg.read(offset, remaining)
                result.extend(records)
                remaining -= len(records)
                if remaining <= 0:
                    break
                # Next read should continue from where we left off.
                if records:
                    offset = records[-1].offset + 1
            return result

    # -- internal ------------------------------------------------------------

    @property
    def _active_segment(self) -> Segment:
        return self._segments[-1]
