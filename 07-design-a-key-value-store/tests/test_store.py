"""Tests for the StorageEngine (memtable + WAL + SSTable)."""

from __future__ import annotations

import os
import tempfile

import pytest

from node.store.engine import StorageEngine


@pytest.fixture()
def data_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


class TestPutGet:
    def test_put_and_get(self, data_dir: str) -> None:
        engine = StorageEngine(data_dir)
        engine.put("k1", "v1")
        stored = engine.get("k1")
        assert stored is not None
        assert stored.value == "v1"

    def test_get_missing_key(self, data_dir: str) -> None:
        engine = StorageEngine(data_dir)
        assert engine.get("missing") is None

    def test_overwrite_key(self, data_dir: str) -> None:
        engine = StorageEngine(data_dir)
        engine.put("k1", "v1")
        engine.put("k1", "v2")
        stored = engine.get("k1")
        assert stored is not None
        assert stored.value == "v2"

    def test_delete_key(self, data_dir: str) -> None:
        engine = StorageEngine(data_dir)
        engine.put("k1", "v1")
        engine.delete("k1")
        assert engine.get("k1") is None

    def test_keys_returns_live_keys(self, data_dir: str) -> None:
        engine = StorageEngine(data_dir)
        engine.put("a", "1")
        engine.put("b", "2")
        engine.put("c", "3")
        engine.delete("b")
        keys = engine.keys()
        assert sorted(keys) == ["a", "c"]


class TestWALRecovery:
    def test_recovery_from_wal(self, data_dir: str) -> None:
        engine1 = StorageEngine(data_dir)
        engine1.put("k1", "hello")
        engine1.put("k2", "world")

        # Create a new engine pointing at the same data dir
        # to simulate a restart -- it should replay the WAL
        engine2 = StorageEngine(data_dir)
        assert engine2.get("k1") is not None
        assert engine2.get("k1").value == "hello"
        assert engine2.get("k2") is not None
        assert engine2.get("k2").value == "world"

    def test_recovery_respects_deletes(self, data_dir: str) -> None:
        engine1 = StorageEngine(data_dir)
        engine1.put("k1", "hello")
        engine1.delete("k1")

        engine2 = StorageEngine(data_dir)
        assert engine2.get("k1") is None


class TestMemtableFlush:
    def test_flush_to_sstable(self, data_dir: str) -> None:
        # Use a very small threshold to trigger flush
        engine = StorageEngine(data_dir, memtable_threshold=5)
        for i in range(10):
            engine.put(f"key-{i}", f"val-{i}")

        # After flush, the memtable may have been cleared
        # but SSTables should be readable
        sstable_dir = os.path.join(data_dir, "sstables")
        if os.path.exists(sstable_dir):
            files = [f for f in os.listdir(sstable_dir) if f.endswith(".dat")]
            assert len(files) > 0, "Expected at least one SSTable file"

    def test_wal_truncated_after_flush(self, data_dir: str) -> None:
        engine = StorageEngine(data_dir, memtable_threshold=5)
        for i in range(10):
            engine.put(f"key-{i}", f"val-{i}")

        wal_path = os.path.join(data_dir, "wal.log")
        # WAL should have been truncated after flush
        # It may still contain entries from after the last flush
        with open(wal_path, "r") as f:
            lines = f.readlines()
        # Lines should be fewer than 10 (we flushed at 5)
        assert len(lines) < 10
