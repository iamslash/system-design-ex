"""Tests for the Snowflake ID generator."""

from __future__ import annotations

import threading
import time
from unittest.mock import patch

import pytest

from src.snowflake import (
    EPOCH,
    MAX_DATACENTER_ID,
    MAX_MACHINE_ID,
    MAX_SEQUENCE,
    SnowflakeGenerator,
)


class TestSnowflakeInit:
    """Validation tests for constructor parameters."""

    def test_valid_init(self) -> None:
        gen = SnowflakeGenerator(datacenter_id=0, machine_id=0)
        assert gen.datacenter_id == 0
        assert gen.machine_id == 0

    def test_max_valid_ids(self) -> None:
        gen = SnowflakeGenerator(
            datacenter_id=MAX_DATACENTER_ID,
            machine_id=MAX_MACHINE_ID,
        )
        assert gen.datacenter_id == MAX_DATACENTER_ID
        assert gen.machine_id == MAX_MACHINE_ID

    def test_invalid_datacenter_id_negative(self) -> None:
        with pytest.raises(ValueError, match="datacenter_id"):
            SnowflakeGenerator(datacenter_id=-1, machine_id=0)

    def test_invalid_datacenter_id_too_large(self) -> None:
        with pytest.raises(ValueError, match="datacenter_id"):
            SnowflakeGenerator(datacenter_id=32, machine_id=0)

    def test_invalid_machine_id_negative(self) -> None:
        with pytest.raises(ValueError, match="machine_id"):
            SnowflakeGenerator(datacenter_id=0, machine_id=-1)

    def test_invalid_machine_id_too_large(self) -> None:
        with pytest.raises(ValueError, match="machine_id"):
            SnowflakeGenerator(datacenter_id=0, machine_id=32)


class TestGenerate:
    """Tests for ID generation."""

    def test_uniqueness_10000(self) -> None:
        """Generate 10,000 IDs; all must be unique."""
        gen = SnowflakeGenerator(datacenter_id=1, machine_id=1)
        ids = [gen.generate() for _ in range(10_000)]
        assert len(set(ids)) == 10_000

    def test_id_is_positive_64bit(self) -> None:
        """Each ID must be a positive integer fitting in 64 bits."""
        gen = SnowflakeGenerator(datacenter_id=0, machine_id=0)
        for _ in range(100):
            sid = gen.generate()
            assert sid > 0
            assert sid.bit_length() <= 63  # sign bit is 0

    def test_ids_sortable_by_time(self) -> None:
        """IDs generated later must be >= IDs generated earlier."""
        gen = SnowflakeGenerator(datacenter_id=0, machine_id=0)
        prev = gen.generate()
        for _ in range(1000):
            curr = gen.generate()
            assert curr >= prev
            prev = curr

    def test_ids_across_milliseconds_are_strictly_increasing(self) -> None:
        """IDs from different milliseconds are strictly increasing."""
        gen = SnowflakeGenerator(datacenter_id=0, machine_id=0)
        id1 = gen.generate()
        time.sleep(0.002)  # ensure at least 1 ms passes
        id2 = gen.generate()
        assert id2 > id1


class TestParse:
    """Tests for parse round-trip."""

    def test_parse_roundtrip(self) -> None:
        """generate -> parse -> verify components match."""
        gen = SnowflakeGenerator(datacenter_id=5, machine_id=17)
        sid = gen.generate()
        parsed = SnowflakeGenerator.parse(sid)

        assert parsed["datacenter_id"] == 5
        assert parsed["machine_id"] == 17
        assert parsed["sequence"] >= 0
        assert parsed["timestamp_ms"] > EPOCH
        assert "UTC" in parsed["datetime"]

    def test_parse_sequence_increments(self) -> None:
        """When generating fast enough, sequence should increment."""
        gen = SnowflakeGenerator(datacenter_id=0, machine_id=0)
        # Generate two IDs rapidly (likely same ms)
        id1 = gen.generate()
        id2 = gen.generate()
        p1 = SnowflakeGenerator.parse(id1)
        p2 = SnowflakeGenerator.parse(id2)

        if p1["timestamp_ms"] == p2["timestamp_ms"]:
            assert p2["sequence"] == p1["sequence"] + 1

    def test_parse_custom_epoch(self) -> None:
        """Parsing with a custom epoch recovers the correct timestamp."""
        custom_epoch = 1609459200000  # 2021-01-01 00:00:00 UTC
        gen = SnowflakeGenerator(datacenter_id=0, machine_id=0, epoch=custom_epoch)
        sid = gen.generate()
        parsed = SnowflakeGenerator.parse(sid, epoch=custom_epoch)

        assert parsed["datacenter_id"] == 0
        assert parsed["machine_id"] == 0
        # Timestamp should be close to now
        now_ms = int(time.time() * 1000)
        assert abs(parsed["timestamp_ms"] - now_ms) < 2000  # within 2 seconds


class TestClockBackward:
    """Tests for clock backward detection."""

    def test_clock_backward_raises(self) -> None:
        """If the clock moves backwards, generate() must raise RuntimeError."""
        gen = SnowflakeGenerator(datacenter_id=0, machine_id=0)

        # Generate one ID to set _last_timestamp
        gen.generate()

        # Mock time to return a value in the past
        future_ts = gen._last_timestamp
        with patch.object(gen, "_current_millis", return_value=future_ts - 100):
            with pytest.raises(RuntimeError, match="Clock moved backwards"):
                gen.generate()


class TestSequenceOverflow:
    """Tests for sequence overflow behavior."""

    def test_sequence_wraps_on_overflow(self) -> None:
        """When sequence hits 4095, the generator waits for next ms."""
        gen = SnowflakeGenerator(datacenter_id=0, machine_id=0)

        fixed_time = int(time.time() * 1000)
        call_count = 0

        def mock_millis() -> int:
            nonlocal call_count
            call_count += 1
            # After 4096 + a few calls, advance the clock by 1 ms
            if call_count > MAX_SEQUENCE + 2:
                return fixed_time + 1
            return fixed_time

        with patch.object(gen, "_current_millis", side_effect=mock_millis):
            ids = set()
            for _ in range(MAX_SEQUENCE + 2):  # 4097 IDs
                ids.add(gen.generate())

            # All IDs must be unique
            assert len(ids) == MAX_SEQUENCE + 2


class TestMultiWorker:
    """Tests for different worker combinations."""

    def test_different_workers_produce_different_ids(self) -> None:
        """Workers with different datacenter/machine IDs produce disjoint ID sets."""
        gen_a = SnowflakeGenerator(datacenter_id=0, machine_id=0)
        gen_b = SnowflakeGenerator(datacenter_id=0, machine_id=1)
        gen_c = SnowflakeGenerator(datacenter_id=1, machine_id=0)

        ids_a = {gen_a.generate() for _ in range(1000)}
        ids_b = {gen_b.generate() for _ in range(1000)}
        ids_c = {gen_c.generate() for _ in range(1000)}

        # No overlaps between any pair
        assert not (ids_a & ids_b)
        assert not (ids_a & ids_c)
        assert not (ids_b & ids_c)

    def test_concurrent_generation_unique(self) -> None:
        """Multiple threads using the same generator produce unique IDs."""
        gen = SnowflakeGenerator(datacenter_id=0, machine_id=0)
        results: list[list[int]] = [[] for _ in range(4)]

        def worker(idx: int) -> None:
            local = []
            for _ in range(2500):
                local.append(gen.generate())
            results[idx] = local

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        all_ids = [sid for r in results for sid in r]
        assert len(all_ids) == 10_000
        assert len(set(all_ids)) == 10_000


class TestCustomEpoch:
    """Tests for custom epoch support."""

    def test_custom_epoch_produces_valid_ids(self) -> None:
        custom_epoch = 1700000000000  # Nov 2023
        gen = SnowflakeGenerator(datacenter_id=0, machine_id=0, epoch=custom_epoch)

        sid = gen.generate()
        assert sid > 0
        assert sid.bit_length() <= 63

    def test_custom_epoch_smaller_ids(self) -> None:
        """A more recent epoch produces smaller IDs (smaller timestamp offset)."""
        old_epoch = EPOCH  # 2010
        new_epoch = 1700000000000  # 2023

        gen_old = SnowflakeGenerator(datacenter_id=0, machine_id=0, epoch=old_epoch)
        gen_new = SnowflakeGenerator(datacenter_id=0, machine_id=0, epoch=new_epoch)

        id_old = gen_old.generate()
        id_new = gen_new.generate()

        # The one with the older epoch has a larger timestamp offset
        assert id_old > id_new
