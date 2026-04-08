"""Consistent Hashing Ring implementation.

A consistent hash ring distributes keys across nodes so that adding or
removing a node only remaps approximately K/N keys (where K is the total
number of keys and N is the number of nodes).  Virtual nodes improve
balance by giving each physical node multiple positions on the ring.
"""

from __future__ import annotations

import bisect
import hashlib


def _hash(key: str) -> int:
    """Return a deterministic integer hash for *key* using SHA-256."""
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return int(digest, 16)


class ConsistentHashRing:
    """A consistent hash ring with configurable virtual nodes.

    Parameters
    ----------
    num_virtual_nodes:
        Number of virtual nodes (replicas) created on the ring for each
        physical node.  More virtual nodes yield a more even distribution
        at the cost of slightly more memory.
    """

    def __init__(self, num_virtual_nodes: int = 150) -> None:
        self._num_virtual_nodes = num_virtual_nodes
        # Sorted list of hash values that sit on the ring.
        self._keys: list[int] = []
        # Mapping from a ring hash value to the physical node name.
        self._ring: dict[int, str] = {}
        # Set of physical node names currently in the ring.
        self._nodes: set[str] = set()

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    @property
    def nodes(self) -> frozenset[str]:
        """Return the set of physical nodes currently on the ring."""
        return frozenset(self._nodes)

    @property
    def num_virtual_nodes(self) -> int:
        """Return the configured number of virtual nodes per physical node."""
        return self._num_virtual_nodes

    def __len__(self) -> int:
        """Return the number of physical nodes on the ring."""
        return len(self._nodes)

    def __contains__(self, node: str) -> bool:
        return node in self._nodes

    # ------------------------------------------------------------------
    # Core API
    # ------------------------------------------------------------------

    def add_node(self, node: str) -> None:
        """Add a physical node (with its virtual nodes) to the ring.

        If *node* is already present the call is a no-op.
        """
        if node in self._nodes:
            return
        self._nodes.add(node)
        for i in range(self._num_virtual_nodes):
            virtual_key = f"{node}#vn{i}"
            h = _hash(virtual_key)
            self._ring[h] = node
            bisect.insort(self._keys, h)

    def remove_node(self, node: str) -> None:
        """Remove a physical node (and all its virtual nodes) from the ring.

        Raises ``KeyError`` if *node* is not on the ring.
        """
        if node not in self._nodes:
            raise KeyError(f"Node {node!r} is not on the ring")
        self._nodes.discard(node)
        for i in range(self._num_virtual_nodes):
            virtual_key = f"{node}#vn{i}"
            h = _hash(virtual_key)
            del self._ring[h]
            idx = bisect.bisect_left(self._keys, h)
            if idx < len(self._keys) and self._keys[idx] == h:
                self._keys.pop(idx)

    def get_node(self, key: str) -> str:
        """Return the physical node responsible for *key*.

        The lookup hashes *key*, then walks clockwise on the ring to find
        the first virtual node whose hash is >= the key hash.  If the key
        hash is larger than every entry on the ring, it wraps around to
        the first entry (index 0).

        Raises ``RuntimeError`` if the ring is empty.
        """
        if not self._ring:
            raise RuntimeError("The hash ring is empty (no nodes added)")

        h = _hash(key)
        idx = bisect.bisect_right(self._keys, h)
        # Wrap around to the beginning of the ring.
        if idx == len(self._keys):
            idx = 0
        return self._ring[self._keys[idx]]

    def get_distribution(self, keys: list[str]) -> dict[str, int]:
        """Return a mapping of physical node -> number of keys routed to it.

        Every physical node in the ring is included in the result even if
        its count is zero.
        """
        dist: dict[str, int] = {node: 0 for node in sorted(self._nodes)}
        for key in keys:
            node = self.get_node(key)
            dist[node] += 1
        return dist
