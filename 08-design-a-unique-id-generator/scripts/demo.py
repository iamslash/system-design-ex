#!/usr/bin/env python3
"""Snowflake ID Generator Demo.

Demonstrates:
1. Single generator usage with parsed output
2. Throughput benchmark
3. Multi-worker uniqueness simulation
4. ID bit-structure visualization
5. Comparison with other approaches (UUID, auto_increment)

Run:
    python scripts/demo.py
"""

from __future__ import annotations

import sys
import os
import threading
import time
import uuid

# Allow running from repo root or from the chapter directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.snowflake import (
    SnowflakeGenerator,
    DATACENTER_ID_SHIFT,
    MACHINE_ID_SHIFT,
    SEQUENCE_BITS,
    TIMESTAMP_SHIFT,
)


def demo_single_generator() -> None:
    """Generate 10 IDs and display each one parsed."""
    print("=" * 70)
    print("  1. Single Generator -- Generate & Parse IDs")
    print("=" * 70)
    print()

    gen = SnowflakeGenerator(datacenter_id=1, machine_id=1)

    for i in range(10):
        sid = gen.generate()
        parsed = SnowflakeGenerator.parse(sid)
        print(
            f"  [{i + 1:2d}] ID: {sid:<22d}  "
            f"ts={parsed['timestamp_ms']}  "
            f"dc={parsed['datacenter_id']}  "
            f"mc={parsed['machine_id']}  "
            f"seq={parsed['sequence']}"
        )

    print()


def demo_throughput() -> None:
    """Benchmark: generate 100,000 IDs and report throughput."""
    print("=" * 70)
    print("  2. Throughput Benchmark")
    print("=" * 70)
    print()

    gen = SnowflakeGenerator(datacenter_id=0, machine_id=0)
    count = 100_000

    start = time.perf_counter()
    ids = [gen.generate() for _ in range(count)]
    elapsed = time.perf_counter() - start

    throughput = count / elapsed
    unique_count = len(set(ids))

    print(f"  Generated : {count:,} IDs")
    print(f"  Elapsed   : {elapsed:.3f} s")
    print(f"  Throughput: {throughput:,.0f} IDs/sec")
    print(f"  Unique    : {unique_count:,} / {count:,}")
    print()


def demo_multi_worker() -> None:
    """Simulate 4 workers generating IDs in parallel, verify uniqueness."""
    print("=" * 70)
    print("  3. Multi-Worker Simulation (4 workers, 1000 IDs each)")
    print("=" * 70)
    print()

    workers = [
        SnowflakeGenerator(datacenter_id=0, machine_id=0),
        SnowflakeGenerator(datacenter_id=0, machine_id=1),
        SnowflakeGenerator(datacenter_id=1, machine_id=0),
        SnowflakeGenerator(datacenter_id=1, machine_id=1),
    ]

    results: list[list[int]] = [[] for _ in workers]
    ids_per_worker = 1000

    def worker_task(idx: int) -> None:
        gen = workers[idx]
        local_ids = []
        for _ in range(ids_per_worker):
            local_ids.append(gen.generate())
        results[idx] = local_ids

    threads = []
    for i in range(len(workers)):
        t = threading.Thread(target=worker_task, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    all_ids: list[int] = []
    for i, worker_ids in enumerate(results):
        w = workers[i]
        print(
            f"  Worker {i} (dc={w.datacenter_id}, mc={w.machine_id}): "
            f"{len(worker_ids):,} IDs  "
            f"range=[{min(worker_ids)}..{max(worker_ids)}]"
        )
        all_ids.extend(worker_ids)

    total = len(all_ids)
    unique = len(set(all_ids))
    print()
    print(f"  Total IDs : {total:,}")
    print(f"  Unique IDs: {unique:,}")
    print(f"  Duplicates: {total - unique}")
    assert unique == total, "DUPLICATE IDs DETECTED!"
    print("  Result    : ALL UNIQUE")
    print()


def demo_bit_visualization() -> None:
    """Show the binary structure of a snowflake ID with labeled sections."""
    print("=" * 70)
    print("  4. ID Bit-Structure Visualization")
    print("=" * 70)
    print()

    gen = SnowflakeGenerator(datacenter_id=7, machine_id=19)
    sid = gen.generate()
    parsed = SnowflakeGenerator.parse(sid)

    bits = format(sid, "064b")

    sign_bit = bits[0]
    timestamp_bits = bits[1:42]
    datacenter_bits = bits[42:47]
    machine_bits = bits[47:52]
    sequence_bits = bits[52:64]

    print(f"  ID (decimal) : {sid}")
    print(f"  ID (binary)  : {bits}")
    print()
    print(f"  {'Section':<14s}  {'Bits':<45s}  {'Value'}")
    print(f"  {'-' * 14}  {'-' * 45}  {'-' * 10}")
    print(f"  {'Sign':<14s}  {sign_bit:<45s}  {int(sign_bit, 2)}")
    print(f"  {'Timestamp':<14s}  {timestamp_bits:<45s}  {parsed['timestamp_ms']}")
    print(f"  {'Datacenter':<14s}  {datacenter_bits:<45s}  {parsed['datacenter_id']}")
    print(f"  {'Machine':<14s}  {machine_bits:<45s}  {parsed['machine_id']}")
    print(f"  {'Sequence':<14s}  {sequence_bits:<45s}  {parsed['sequence']}")
    print()
    print(f"  Datetime: {parsed['datetime']}")
    print()


def demo_comparison() -> None:
    """Compare Snowflake with UUID and auto_increment approaches."""
    print("=" * 70)
    print("  5. Comparison With Other Approaches")
    print("=" * 70)
    print()

    # --- UUID ---
    print("  [UUID v4] 128-bit, random, not sortable:")
    for i in range(5):
        u = uuid.uuid4()
        print(f"    {u}  ({u.int.bit_length()} bits)")
    print()

    # --- Snowflake ---
    print("  [Snowflake] 64-bit, time-sortable:")
    gen = SnowflakeGenerator(datacenter_id=0, machine_id=1)
    sf_ids = []
    for i in range(5):
        sid = gen.generate()
        sf_ids.append(sid)
        print(f"    {sid}  ({sid.bit_length()} bits)")
    print()

    is_sorted = all(sf_ids[i] <= sf_ids[i + 1] for i in range(len(sf_ids) - 1))
    print(f"  Snowflake IDs sortable by time? {is_sorted}")
    print()

    # --- Auto-increment problem ---
    print("  [Auto-Increment] Why it fails in distributed systems:")
    print()
    print("    Server A: INSERT -> id=1, id=2, id=3, ...")
    print("    Server B: INSERT -> id=1, id=2, id=3, ...")
    print("                        ^^ COLLISION! ^^")
    print()
    print("    Even with odd/even split (A=1,3,5.. B=2,4,6..):")
    print("    - Hard to scale beyond 2 servers")
    print("    - IDs don't encode time information")
    print("    - Adding a 3rd server requires reassignment")
    print()


def main() -> None:
    print()
    print("Snowflake ID Generator Demo")
    print("===========================")
    print()

    demo_single_generator()
    demo_throughput()
    demo_multi_worker()
    demo_bit_visualization()
    demo_comparison()

    print("Done.")
    print()


if __name__ == "__main__":
    main()
