#!/usr/bin/env python3
"""Interactive demo: consistent hashing vs modular hashing.

Run from the repository root:

    python 06-design-consistent-hashing/scripts/demo.py

Or from within the chapter directory:

    python scripts/demo.py
"""

from __future__ import annotations

import math
import sys
import os

# ---------------------------------------------------------------------------
# Make sure the ``src`` package is importable regardless of the working
# directory from which this script is invoked.
# ---------------------------------------------------------------------------
_CHAPTER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _CHAPTER_DIR)

from src.consistent_hash import ConsistentHashRing  # noqa: E402


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _modular_hash(key: str, num_servers: int) -> int:
    """Simple modular hash: hash(key) % N."""
    return hash(key) % num_servers


def _std_dev(values: list[int]) -> float:
    """Population standard deviation."""
    n = len(values)
    if n == 0:
        return 0.0
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    return math.sqrt(variance)


def _print_dist(dist: dict[str, int]) -> None:
    """Pretty-print a distribution dict."""
    for node, count in sorted(dist.items()):
        print(f"  {node}: {count}")


# ---------------------------------------------------------------------------
# Demo sections
# ---------------------------------------------------------------------------

NUM_KEYS = 10_000
SERVERS = [f"server-{i}" for i in range(5)]


def _generate_keys(n: int = NUM_KEYS) -> list[str]:
    return [f"key-{i}" for i in range(n)]


def demo_rehashing_problem() -> None:
    """Show that modular hashing remaps most keys when a server is removed."""
    print("=" * 60)
    print("  Rehashing Problem (Modular Hash)")
    print("=" * 60)

    keys = _generate_keys()
    servers_4 = SERVERS[:4]
    servers_3 = SERVERS[:3]

    mapping_4 = {k: servers_4[_modular_hash(k, 4)] for k in keys}
    mapping_3 = {k: servers_3[_modular_hash(k, 3)] for k in keys}

    dist_4: dict[str, int] = {s: 0 for s in servers_4}
    for s in mapping_4.values():
        dist_4[s] += 1

    dist_3: dict[str, int] = {s: 0 for s in servers_3}
    for s in mapping_3.values():
        dist_3[s] += 1

    remapped = sum(1 for k in keys if mapping_4[k] != mapping_3.get(k))

    print(f"\nModular hash with {len(servers_4)} servers:")
    _print_dist(dist_4)
    print(f"\nRemove {servers_4[-1]} -> modular hash with {len(servers_3)} servers:")
    _print_dist(dist_3)
    print(f"\nKeys remapped: {remapped}/{len(keys)} ({100 * remapped / len(keys):.1f}%)")
    print()


def demo_consistent_hashing() -> None:
    """Show that consistent hashing remaps far fewer keys."""
    print("=" * 60)
    print("  Consistent Hashing")
    print("=" * 60)

    keys = _generate_keys()
    ring = ConsistentHashRing(num_virtual_nodes=150)
    for s in SERVERS[:4]:
        ring.add_node(s)

    mapping_before = {k: ring.get_node(k) for k in keys}
    dist_before = ring.get_distribution(keys)

    print(f"\nRing with {len(ring)} servers, {ring.num_virtual_nodes} vnodes:")
    _print_dist(dist_before)
    vals = list(dist_before.values())
    print(f"  std dev: {_std_dev(vals):.1f}")

    # Remove a server
    removed = SERVERS[3]
    ring.remove_node(removed)
    mapping_after = {k: ring.get_node(k) for k in keys}
    remapped = sum(1 for k in keys if mapping_before[k] != mapping_after[k])

    dist_after = ring.get_distribution(keys)
    print(f"\nRemove {removed}:")
    _print_dist(dist_after)
    vals = list(dist_after.values())
    print(f"  std dev: {_std_dev(vals):.1f}")
    print(f"  Keys remapped: {remapped}/{len(keys)} ({100 * remapped / len(keys):.1f}%)")

    # Add a new server
    ring_full = ConsistentHashRing(num_virtual_nodes=150)
    for s in SERVERS[:4]:
        ring_full.add_node(s)
    mapping_full = {k: ring_full.get_node(k) for k in keys}

    ring_full.add_node("server-new")
    mapping_added = {k: ring_full.get_node(k) for k in keys}
    remapped_add = sum(1 for k in keys if mapping_full[k] != mapping_added[k])

    dist_added = ring_full.get_distribution(keys)
    print(f"\nAdd server-new (now {len(ring_full)} servers):")
    _print_dist(dist_added)
    vals = list(dist_added.values())
    print(f"  std dev: {_std_dev(vals):.1f}")
    print(f"  Keys remapped: {remapped_add}/{len(keys)} ({100 * remapped_add / len(keys):.1f}%)")
    print()


def demo_virtual_nodes_effect() -> None:
    """Show how the number of virtual nodes affects distribution evenness."""
    print("=" * 60)
    print("  Virtual Nodes Effect")
    print("=" * 60)

    keys = _generate_keys()
    servers = SERVERS[:5]
    vnode_counts = [3, 10, 50, 150, 500]

    print()
    print(f"  {'vnodes':>8}  {'std dev':>8}  {'min':>6}  {'max':>6}")
    print(f"  {'------':>8}  {'-------':>8}  {'---':>6}  {'---':>6}")

    for vn in vnode_counts:
        ring = ConsistentHashRing(num_virtual_nodes=vn)
        for s in servers:
            ring.add_node(s)
        dist = ring.get_distribution(keys)
        vals = list(dist.values())
        sd = _std_dev(vals)
        print(f"  {vn:>8}  {sd:>8.1f}  {min(vals):>6}  {max(vals):>6}")

    print()


def demo_add_remove_simulation() -> None:
    """Simulate adding and removing servers with 10 000 keys."""
    print("=" * 60)
    print("  Add / Remove Server Simulation")
    print("=" * 60)

    keys = _generate_keys()
    ring = ConsistentHashRing(num_virtual_nodes=150)
    for s in SERVERS:
        ring.add_node(s)

    mapping_before = {k: ring.get_node(k) for k in keys}
    dist = ring.get_distribution(keys)

    print(f"\nInitial ring ({len(ring)} servers):")
    _print_dist(dist)
    vals = list(dist.values())
    print(f"  std dev: {_std_dev(vals):.1f}")

    # Add server
    ring.add_node("server-5")
    mapping_add = {k: ring.get_node(k) for k in keys}
    remapped_add = sum(1 for k in keys if mapping_before[k] != mapping_add[k])
    dist_add = ring.get_distribution(keys)
    print(f"\nAfter adding server-5 ({len(ring)} servers):")
    _print_dist(dist_add)
    vals = list(dist_add.values())
    print(f"  std dev: {_std_dev(vals):.1f}")
    print(f"  Keys remapped: {remapped_add}/{len(keys)} ({100 * remapped_add / len(keys):.1f}%)")

    # Remove server
    ring2 = ConsistentHashRing(num_virtual_nodes=150)
    for s in SERVERS:
        ring2.add_node(s)
    mapping2_before = {k: ring2.get_node(k) for k in keys}

    ring2.remove_node("server-2")
    mapping2_after = {k: ring2.get_node(k) for k in keys}
    remapped_rm = sum(1 for k in keys if mapping2_before[k] != mapping2_after[k])
    dist_rm = ring2.get_distribution(keys)
    print(f"\nAfter removing server-2 ({len(ring2)} servers):")
    _print_dist(dist_rm)
    vals = list(dist_rm.values())
    print(f"  std dev: {_std_dev(vals):.1f}")
    print(f"  Keys remapped: {remapped_rm}/{len(keys)} ({100 * remapped_rm / len(keys):.1f}%)")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print()
    print("Consistent Hashing Demo")
    print("=======================")
    print()

    demo_rehashing_problem()
    demo_consistent_hashing()
    demo_virtual_nodes_effect()
    demo_add_remove_simulation()

    print("Done.")


if __name__ == "__main__":
    main()
