"""Tests for the ConsistentHashRing implementation."""

from __future__ import annotations

import math
import sys
import os

import pytest

# Ensure ``src`` is importable.
_CHAPTER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _CHAPTER_DIR)

from src.consistent_hash import ConsistentHashRing  # noqa: E402


# ---------------------------------------------------------------------------
# Empty ring
# ---------------------------------------------------------------------------

class TestEmptyRing:
    def test_get_node_raises_on_empty_ring(self) -> None:
        ring = ConsistentHashRing()
        with pytest.raises(RuntimeError, match="empty"):
            ring.get_node("any-key")

    def test_len_is_zero(self) -> None:
        ring = ConsistentHashRing()
        assert len(ring) == 0

    def test_nodes_is_empty(self) -> None:
        ring = ConsistentHashRing()
        assert ring.nodes == frozenset()


# ---------------------------------------------------------------------------
# Single node
# ---------------------------------------------------------------------------

class TestSingleNode:
    def test_all_keys_go_to_single_node(self) -> None:
        ring = ConsistentHashRing(num_virtual_nodes=50)
        ring.add_node("server-0")
        for i in range(1000):
            assert ring.get_node(f"key-{i}") == "server-0"

    def test_distribution_single_node(self) -> None:
        ring = ConsistentHashRing()
        ring.add_node("server-0")
        keys = [f"key-{i}" for i in range(500)]
        dist = ring.get_distribution(keys)
        assert dist == {"server-0": 500}


# ---------------------------------------------------------------------------
# Add / Remove nodes
# ---------------------------------------------------------------------------

class TestAddRemoveNode:
    def test_add_node_increases_length(self) -> None:
        ring = ConsistentHashRing()
        ring.add_node("a")
        ring.add_node("b")
        assert len(ring) == 2

    def test_add_duplicate_node_is_noop(self) -> None:
        ring = ConsistentHashRing()
        ring.add_node("a")
        ring.add_node("a")
        assert len(ring) == 1

    def test_remove_node_decreases_length(self) -> None:
        ring = ConsistentHashRing()
        ring.add_node("a")
        ring.add_node("b")
        ring.remove_node("a")
        assert len(ring) == 1
        assert "a" not in ring

    def test_remove_unknown_node_raises(self) -> None:
        ring = ConsistentHashRing()
        with pytest.raises(KeyError):
            ring.remove_node("nonexistent")

    def test_contains(self) -> None:
        ring = ConsistentHashRing()
        ring.add_node("x")
        assert "x" in ring
        assert "y" not in ring


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_same_key_always_maps_to_same_node(self) -> None:
        ring = ConsistentHashRing(num_virtual_nodes=100)
        for s in ("s1", "s2", "s3"):
            ring.add_node(s)

        results = [ring.get_node("stable-key") for _ in range(100)]
        assert len(set(results)) == 1, "Key should always resolve to the same node"

    def test_independent_rings_agree(self) -> None:
        """Two rings built identically must produce the same mapping."""
        nodes = ["alpha", "beta", "gamma"]
        keys = [f"k{i}" for i in range(200)]

        ring_a = ConsistentHashRing(num_virtual_nodes=80)
        ring_b = ConsistentHashRing(num_virtual_nodes=80)
        for n in nodes:
            ring_a.add_node(n)
            ring_b.add_node(n)

        for k in keys:
            assert ring_a.get_node(k) == ring_b.get_node(k)


# ---------------------------------------------------------------------------
# Distribution evenness
# ---------------------------------------------------------------------------

class TestDistribution:
    def test_distribution_is_roughly_even(self) -> None:
        """With 150 vnodes and 5 nodes, std-dev should be small."""
        ring = ConsistentHashRing(num_virtual_nodes=150)
        servers = [f"server-{i}" for i in range(5)]
        for s in servers:
            ring.add_node(s)

        keys = [f"key-{i}" for i in range(10_000)]
        dist = ring.get_distribution(keys)
        counts = list(dist.values())

        ideal = 10_000 / 5  # 2000
        std = math.sqrt(sum((c - ideal) ** 2 for c in counts) / len(counts))
        # With 150 vnodes the std-dev should be well under 500.
        assert std < 500, f"Distribution too uneven: std dev = {std:.1f}"

    def test_all_nodes_appear_in_distribution(self) -> None:
        ring = ConsistentHashRing(num_virtual_nodes=100)
        nodes = [f"n{i}" for i in range(4)]
        for n in nodes:
            ring.add_node(n)

        keys = [f"key-{i}" for i in range(5000)]
        dist = ring.get_distribution(keys)
        for n in nodes:
            assert n in dist
            assert dist[n] > 0


# ---------------------------------------------------------------------------
# Minimal remapping on node removal
# ---------------------------------------------------------------------------

class TestMinimalRemapping:
    def test_removing_node_only_remaps_its_keys(self) -> None:
        """After removing a node, only keys that were on that node move."""
        ring = ConsistentHashRing(num_virtual_nodes=150)
        nodes = [f"server-{i}" for i in range(5)]
        for n in nodes:
            ring.add_node(n)

        keys = [f"key-{i}" for i in range(10_000)]
        mapping_before = {k: ring.get_node(k) for k in keys}

        removed = "server-2"
        ring.remove_node(removed)
        mapping_after = {k: ring.get_node(k) for k in keys}

        for k in keys:
            if mapping_before[k] != removed:
                # Keys that were NOT on the removed node must stay put.
                assert mapping_before[k] == mapping_after[k], (
                    f"Key {k!r} moved from {mapping_before[k]} to "
                    f"{mapping_after[k]} even though it was not on {removed}"
                )

    def test_remap_fraction_is_bounded(self) -> None:
        """Roughly K/N keys should move when a node is removed."""
        ring = ConsistentHashRing(num_virtual_nodes=150)
        num_nodes = 5
        nodes = [f"server-{i}" for i in range(num_nodes)]
        for n in nodes:
            ring.add_node(n)

        keys = [f"key-{i}" for i in range(10_000)]
        mapping_before = {k: ring.get_node(k) for k in keys}

        ring.remove_node("server-0")
        mapping_after = {k: ring.get_node(k) for k in keys}

        remapped = sum(1 for k in keys if mapping_before[k] != mapping_after[k])
        fraction = remapped / len(keys)
        expected = 1.0 / num_nodes  # ~0.20

        # Allow generous tolerance: fraction should be less than 2x expected.
        assert fraction < expected * 2.0, (
            f"Too many keys remapped: {fraction:.2%} (expected ~{expected:.2%})"
        )
