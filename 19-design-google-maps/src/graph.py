"""Road graph with nodes (intersections) and edges (roads).

Nodes represent intersections identified by string IDs with latitude/longitude.
Edges represent directed road segments with distance (km) and speed limit (km/h).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Node:
    """An intersection in the road network."""

    id: str
    lat: float
    lng: float


@dataclass(frozen=True)
class Edge:
    """A directed road segment between two intersections."""

    src: str
    dst: str
    distance_km: float
    speed_limit_kmh: float = 60.0


class RoadGraph:
    """Weighted directed graph representing a road network."""

    def __init__(self) -> None:
        self._nodes: dict[str, Node] = {}
        self._adj: dict[str, list[Edge]] = {}

    # -- mutation ----------------------------------------------------------

    def add_node(self, node_id: str, lat: float, lng: float) -> Node:
        """Add an intersection to the graph."""
        node = Node(id=node_id, lat=lat, lng=lng)
        self._nodes[node_id] = node
        self._adj.setdefault(node_id, [])
        return node

    def add_edge(
        self,
        src: str,
        dst: str,
        distance_km: float | None = None,
        speed_limit_kmh: float = 60.0,
        bidirectional: bool = True,
    ) -> Edge:
        """Add a road segment. If *distance_km* is None, compute from Haversine."""
        if src not in self._nodes:
            raise KeyError(f"Source node '{src}' not in graph")
        if dst not in self._nodes:
            raise KeyError(f"Destination node '{dst}' not in graph")

        if distance_km is None:
            distance_km = haversine(self._nodes[src], self._nodes[dst])

        edge = Edge(src=src, dst=dst, distance_km=distance_km, speed_limit_kmh=speed_limit_kmh)
        self._adj[src].append(edge)

        if bidirectional:
            reverse = Edge(src=dst, dst=src, distance_km=distance_km, speed_limit_kmh=speed_limit_kmh)
            self._adj[dst].append(reverse)

        return edge

    def remove_node(self, node_id: str) -> None:
        """Remove a node and all its incident edges."""
        if node_id not in self._nodes:
            raise KeyError(f"Node '{node_id}' not in graph")
        del self._nodes[node_id]
        del self._adj[node_id]
        for src in self._adj:
            self._adj[src] = [e for e in self._adj[src] if e.dst != node_id]

    def remove_edge(self, src: str, dst: str) -> None:
        """Remove directed edge(s) from *src* to *dst*."""
        if src not in self._adj:
            raise KeyError(f"Node '{src}' not in graph")
        before = len(self._adj[src])
        self._adj[src] = [e for e in self._adj[src] if e.dst != dst]
        if len(self._adj[src]) == before:
            raise KeyError(f"No edge from '{src}' to '{dst}'")

    # -- queries -----------------------------------------------------------

    def get_node(self, node_id: str) -> Node:
        return self._nodes[node_id]

    def get_neighbors(self, node_id: str) -> list[Edge]:
        return list(self._adj.get(node_id, []))

    @property
    def nodes(self) -> dict[str, Node]:
        return dict(self._nodes)

    @property
    def node_count(self) -> int:
        return len(self._nodes)

    @property
    def edge_count(self) -> int:
        return sum(len(edges) for edges in self._adj.values())

    def has_node(self, node_id: str) -> bool:
        return node_id in self._nodes

    def has_edge(self, src: str, dst: str) -> bool:
        return any(e.dst == dst for e in self._adj.get(src, []))

    def subgraph(self, node_ids: set[str]) -> "RoadGraph":
        """Return a new graph containing only the specified nodes and their mutual edges."""
        g = RoadGraph()
        for nid in node_ids:
            if nid in self._nodes:
                n = self._nodes[nid]
                g.add_node(n.id, n.lat, n.lng)
        for nid in node_ids:
            for edge in self._adj.get(nid, []):
                if edge.dst in node_ids:
                    g._adj[nid].append(edge)
        return g


# -- helpers ---------------------------------------------------------------

_EARTH_RADIUS_KM = 6371.0


def haversine(a: Node, b: Node) -> float:
    """Great-circle distance between two nodes in kilometres."""
    lat1, lat2 = math.radians(a.lat), math.radians(b.lat)
    dlat = math.radians(b.lat - a.lat)
    dlng = math.radians(b.lng - a.lng)

    h = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlng / 2) ** 2
    )
    return 2 * _EARTH_RADIUS_KM * math.asin(math.sqrt(h))
