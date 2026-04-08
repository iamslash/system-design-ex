"""In-memory storage engine with write-ahead log (WAL) and SSTable flush.

Data flow:
  1. PUT  -> append to WAL -> write to memtable (in-memory dict)
  2. When memtable size exceeds threshold -> flush to SSTable, clear memtable
  3. GET  -> check memtable first, then SSTables
  4. On restart -> replay WAL to rebuild memtable
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field

from node.store.sstable import SSTable


@dataclass
class StoredValue:
    value: str
    timestamp: float
    deleted: bool = False


class StorageEngine:
    """Single-node storage engine backed by memtable + WAL + SSTables."""

    def __init__(self, data_dir: str, memtable_threshold: int = 100) -> None:
        self._data_dir = data_dir
        self._memtable_threshold = memtable_threshold
        self._memtable: dict[str, StoredValue] = {}
        self._lock = threading.Lock()

        os.makedirs(data_dir, exist_ok=True)
        self._wal_path = os.path.join(data_dir, "wal.log")
        self._sstable = SSTable(os.path.join(data_dir, "sstables"))

        self._replay_wal()

    # -- public API ------------------------------------------------------

    def put(self, key: str, value: str) -> float:
        """Store *key*/*value* and return the write timestamp."""
        ts = time.time()
        with self._lock:
            self._append_wal("PUT", key, value, ts)
            self._memtable[key] = StoredValue(value=value, timestamp=ts)
            self._maybe_flush()
        return ts

    def get(self, key: str) -> StoredValue | None:
        """Return the stored value or ``None``."""
        with self._lock:
            entry = self._memtable.get(key)
        if entry is not None:
            return None if entry.deleted else entry

        # Fall through to SSTables
        ss_entry = self._sstable.get(key)
        if ss_entry is not None and ss_entry.value != "__TOMBSTONE__":
            return StoredValue(value=ss_entry.value, timestamp=ss_entry.timestamp)
        return None

    def delete(self, key: str) -> float:
        """Tombstone-delete *key* and return the timestamp."""
        ts = time.time()
        with self._lock:
            self._append_wal("DELETE", key, "__TOMBSTONE__", ts)
            self._memtable[key] = StoredValue(
                value="__TOMBSTONE__", timestamp=ts, deleted=True
            )
            self._maybe_flush()
        return ts

    def keys(self) -> list[str]:
        """Return all live keys visible in the memtable."""
        with self._lock:
            return [k for k, v in self._memtable.items() if not v.deleted]

    # -- WAL -------------------------------------------------------------

    def _append_wal(self, op: str, key: str, value: str, ts: float) -> None:
        record = json.dumps({"op": op, "key": key, "value": value, "ts": ts})
        with open(self._wal_path, "a", encoding="utf-8") as fh:
            fh.write(record + "\n")

    def _replay_wal(self) -> None:
        if not os.path.exists(self._wal_path):
            return
        with open(self._wal_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                key = record["key"]
                value = record["value"]
                ts = record["ts"]
                deleted = record["op"] == "DELETE"
                self._memtable[key] = StoredValue(
                    value=value, timestamp=ts, deleted=deleted
                )

    # -- flush -----------------------------------------------------------

    def _maybe_flush(self) -> None:
        if len(self._memtable) < self._memtable_threshold:
            return
        flush_data = {
            k: (v.value, v.timestamp) for k, v in self._memtable.items()
        }
        self._sstable.flush(flush_data)
        self._memtable.clear()
        # Truncate WAL after successful flush
        with open(self._wal_path, "w", encoding="utf-8"):
            pass
