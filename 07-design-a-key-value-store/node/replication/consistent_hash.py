"""Consistent hash ring with virtual nodes.

Maps keys to a set of responsible replica nodes.
"""

from __future__ import annotations

import hashlib
from bisect import bisect_right


class ConsistentHashRing:
    """A hash ring that distributes keys across nodes using virtual nodes."""

    def __init__(self, virtual_nodes: int = 150) -> None:
        self._virtual_nodes = virtual_nodes
        self._ring: list[tuple[int, str]] = []  # sorted by hash
        self._nodes: set[str] = set()

    # -- membership ------------------------------------------------------

    def add_node(self, node: str) -> None:
        if node in self._nodes:
            return
        self._nodes.add(node)
        for i in range(self._virtual_nodes):
            h = self._hash(f"{node}:{i}")
            self._ring.append((h, node))
        self._ring.sort(key=lambda x: x[0])

    def remove_node(self, node: str) -> None:
        if node not in self._nodes:
            return
        self._nodes.discard(node)
        self._ring = [(h, n) for h, n in self._ring if n != node]

    @property
    def nodes(self) -> set[str]:
        return set(self._nodes)

    # -- lookup ----------------------------------------------------------

    def get_node(self, key: str) -> str | None:
        """Return the primary node responsible for *key*."""
        if not self._ring:
            return None
        h = self._hash(key)
        hashes = [item[0] for item in self._ring]
        idx = bisect_right(hashes, h) % len(self._ring)
        return self._ring[idx][1]

    def get_replica_nodes(self, key: str, n: int) -> list[str]:
        """Return up to *n* distinct nodes responsible for *key*.

        Walks the ring clockwise from the key's position, collecting
        distinct physical nodes.
        """
        if not self._ring:
            return []

        h = self._hash(key)
        hashes = [item[0] for item in self._ring]
        idx = bisect_right(hashes, h) % len(self._ring)

        replicas: list[str] = []
        seen: set[str] = set()
        ring_len = len(self._ring)

        for offset in range(ring_len):
            _, node = self._ring[(idx + offset) % ring_len]
            if node not in seen:
                seen.add(node)
                replicas.append(node)
            if len(replicas) >= n:
                break

        return replicas

    # -- info ------------------------------------------------------------

    def ring_info(self) -> dict:
        """Return a summary of the ring for the cluster info endpoint."""
        return {
            "nodes": sorted(self._nodes),
            "virtual_nodes_per_node": self._virtual_nodes,
            "total_ring_entries": len(self._ring),
        }

    # -- internal --------------------------------------------------------

    @staticmethod
    def _hash(key: str) -> int:
        digest = hashlib.md5(key.encode("utf-8")).hexdigest()
        return int(digest, 16)
