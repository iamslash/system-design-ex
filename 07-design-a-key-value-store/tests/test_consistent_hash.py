"""Tests for the ConsistentHashRing implementation."""

from __future__ import annotations

from node.replication.consistent_hash import ConsistentHashRing


class TestBasicOperations:
    def test_add_node_and_get(self) -> None:
        ring = ConsistentHashRing(virtual_nodes=100)
        ring.add_node("node1:8000")
        assert ring.get_node("any-key") == "node1:8000"

    def test_empty_ring_returns_none(self) -> None:
        ring = ConsistentHashRing()
        assert ring.get_node("key") is None

    def test_add_duplicate_node_is_idempotent(self) -> None:
        ring = ConsistentHashRing(virtual_nodes=10)
        ring.add_node("node1:8000")
        ring.add_node("node1:8000")
        assert ring.nodes == {"node1:8000"}


class TestDistribution:
    def test_keys_distributed_across_nodes(self) -> None:
        ring = ConsistentHashRing(virtual_nodes=150)
        ring.add_node("node1:8000")
        ring.add_node("node2:8000")
        ring.add_node("node3:8000")

        counts: dict[str, int] = {"node1:8000": 0, "node2:8000": 0, "node3:8000": 0}
        for i in range(3000):
            node = ring.get_node(f"key-{i}")
            assert node is not None
            counts[node] += 1

        # Each node should get a meaningful share (at least 15% of keys)
        for node, count in counts.items():
            assert count > 450, f"{node} only got {count}/3000 keys"

    def test_minimal_redistribution_on_add(self) -> None:
        ring = ConsistentHashRing(virtual_nodes=150)
        ring.add_node("node1:8000")
        ring.add_node("node2:8000")

        # Record assignments
        assignments_before = {}
        for i in range(1000):
            key = f"key-{i}"
            assignments_before[key] = ring.get_node(key)

        # Add a third node
        ring.add_node("node3:8000")

        moved = 0
        for i in range(1000):
            key = f"key-{i}"
            if ring.get_node(key) != assignments_before[key]:
                moved += 1

        # Roughly 1/3 of keys should move (with some tolerance)
        assert moved < 600, f"Too many keys moved: {moved}/1000"


class TestRemoveNode:
    def test_remove_node(self) -> None:
        ring = ConsistentHashRing(virtual_nodes=50)
        ring.add_node("node1:8000")
        ring.add_node("node2:8000")
        ring.remove_node("node1:8000")

        assert ring.nodes == {"node2:8000"}
        # All keys should go to node2 now
        for i in range(100):
            assert ring.get_node(f"key-{i}") == "node2:8000"

    def test_remove_nonexistent_node_is_noop(self) -> None:
        ring = ConsistentHashRing(virtual_nodes=10)
        ring.add_node("node1:8000")
        ring.remove_node("node999:8000")  # Should not raise
        assert ring.nodes == {"node1:8000"}


class TestReplicaNodes:
    def test_get_replica_nodes(self) -> None:
        ring = ConsistentHashRing(virtual_nodes=150)
        ring.add_node("node1:8000")
        ring.add_node("node2:8000")
        ring.add_node("node3:8000")

        replicas = ring.get_replica_nodes("my-key", 3)
        assert len(replicas) == 3
        assert len(set(replicas)) == 3  # All distinct

    def test_replica_count_capped_at_available_nodes(self) -> None:
        ring = ConsistentHashRing(virtual_nodes=50)
        ring.add_node("node1:8000")
        ring.add_node("node2:8000")

        replicas = ring.get_replica_nodes("key", 5)
        assert len(replicas) == 2

    def test_empty_ring_replicas(self) -> None:
        ring = ConsistentHashRing()
        assert ring.get_replica_nodes("key", 3) == []


class TestRingInfo:
    def test_ring_info(self) -> None:
        ring = ConsistentHashRing(virtual_nodes=100)
        ring.add_node("node1:8000")
        ring.add_node("node2:8000")

        info = ring.ring_info()
        assert set(info["nodes"]) == {"node1:8000", "node2:8000"}
        assert info["virtual_nodes_per_node"] == 100
        assert info["total_ring_entries"] == 200
