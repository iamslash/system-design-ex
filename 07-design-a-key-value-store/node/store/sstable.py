"""Minimal SSTable-like sorted key-value file on disk.

Each SSTable file stores entries sorted by key.  Format (line-oriented for
simplicity):

    key\tvalue\ttimestamp\n

Files are named ``sstable_<epoch_ms>.dat`` and live under the configured
data directory.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass


@dataclass
class SSTableEntry:
    key: str
    value: str
    timestamp: float


class SSTable:
    """Flush sorted key-value pairs to disk and read them back."""

    def __init__(self, data_dir: str) -> None:
        self._data_dir = data_dir
        os.makedirs(self._data_dir, exist_ok=True)

    # -- write -----------------------------------------------------------

    def flush(self, memtable: dict[str, tuple[str, float]]) -> str | None:
        """Write *memtable* ``{key: (value, timestamp)}`` to a new SSTable file.

        Returns the file path or ``None`` if the memtable is empty.
        """
        if not memtable:
            return None

        filename = f"sstable_{int(time.time() * 1000)}.dat"
        filepath = os.path.join(self._data_dir, filename)

        sorted_items = sorted(memtable.items(), key=lambda kv: kv[0])
        with open(filepath, "w", encoding="utf-8") as fh:
            for key, (value, ts) in sorted_items:
                fh.write(f"{key}\t{value}\t{ts}\n")

        return filepath

    # -- read ------------------------------------------------------------

    def get(self, key: str) -> SSTableEntry | None:
        """Search all SSTables (newest first) for *key*.

        Uses sequential scan per file (sufficient for a teaching
        implementation).
        """
        files = self._list_files()
        for filepath in files:
            entry = self._scan_file(filepath, key)
            if entry is not None:
                return entry
        return None

    def _scan_file(self, filepath: str, key: str) -> SSTableEntry | None:
        try:
            with open(filepath, "r", encoding="utf-8") as fh:
                for line in fh:
                    parts = line.rstrip("\n").split("\t", 2)
                    if len(parts) != 3:
                        continue
                    k, v, ts = parts
                    if k == key:
                        return SSTableEntry(key=k, value=v, timestamp=float(ts))
        except FileNotFoundError:
            pass
        return None

    def _list_files(self) -> list[str]:
        """Return SSTable file paths sorted newest-first."""
        try:
            names = [
                n
                for n in os.listdir(self._data_dir)
                if n.startswith("sstable_") and n.endswith(".dat")
            ]
        except FileNotFoundError:
            return []
        names.sort(reverse=True)
        return [os.path.join(self._data_dir, n) for n in names]
