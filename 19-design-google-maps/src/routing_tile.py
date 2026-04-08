"""Routing tiles -- break a large road graph into geographic tiles.

Each tile is identified by a geohash-like key derived from quantised
latitude/longitude.  The TileManager lazily materialises per-tile
sub-graphs so that pathfinding only loads the tiles it needs.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from .graph import Node, RoadGraph


def tile_key(lat: float, lng: float, precision: int = 2) -> str:
    """Compute a geohash-style tile key.

    *precision* controls how many decimal places are kept, which sets
    tile size.  precision=2 gives roughly 1 km tiles; precision=1
    gives ~11 km tiles.
    """
    factor = 10 ** precision
    qlat = math.floor(lat * factor) / factor
    qlng = math.floor(lng * factor) / factor
    return f"{qlat:.{precision}f},{qlng:.{precision}f}"


def neighbor_keys(lat: float, lng: float, precision: int = 2) -> list[str]:
    """Return the 9-cell neighbourhood (centre + 8 surrounding tiles)."""
    step = 1 / (10 ** precision)
    keys: list[str] = []
    for dlat in (-step, 0, step):
        for dlng in (-step, 0, step):
            keys.append(tile_key(lat + dlat, lng + dlng, precision))
    return keys


@dataclass
class RoutingTile:
    """A single routing tile containing a sub-graph."""

    key: str
    graph: RoadGraph = field(default_factory=RoadGraph)
    loaded: bool = False


class TileManager:
    """Manages a collection of routing tiles over a full road graph.

    Tiles are created lazily: the first time a tile is requested, the
    manager extracts the relevant nodes and edges from the master graph.
    """

    def __init__(self, master: RoadGraph, precision: int = 2) -> None:
        self._master = master
        self._precision = precision
        self._tiles: dict[str, RoutingTile] = {}
        self._node_tile: dict[str, str] = {}  # node_id -> tile_key

        # Pre-compute node-to-tile mapping
        for nid, node in master.nodes.items():
            key = tile_key(node.lat, node.lng, precision)
            self._node_tile[nid] = key

    @property
    def precision(self) -> int:
        return self._precision

    @property
    def tile_count(self) -> int:
        """Number of distinct tiles that contain at least one node."""
        return len(set(self._node_tile.values()))

    def tiles_loaded(self) -> int:
        """Number of tiles currently materialised in memory."""
        return sum(1 for t in self._tiles.values() if t.loaded)

    def get_tile(self, key: str) -> RoutingTile:
        """Retrieve (and lazily build) a routing tile."""
        if key in self._tiles and self._tiles[key].loaded:
            return self._tiles[key]

        tile = RoutingTile(key=key)
        node_ids = {nid for nid, tk in self._node_tile.items() if tk == key}

        for nid in node_ids:
            n = self._master.get_node(nid)
            tile.graph.add_node(n.id, n.lat, n.lng)

        # Include edges whose source is in this tile
        for nid in node_ids:
            for edge in self._master.get_neighbors(nid):
                # Ensure destination node exists in tile graph
                if not tile.graph.has_node(edge.dst):
                    dn = self._master.get_node(edge.dst)
                    tile.graph.add_node(dn.id, dn.lat, dn.lng)
                tile.graph._adj[nid].append(edge)

        tile.loaded = True
        self._tiles[key] = tile
        return tile

    def get_tile_for_node(self, node_id: str) -> RoutingTile:
        """Get the tile that contains the given node."""
        key = self._node_tile[node_id]
        return self.get_tile(key)

    def get_tiles_in_area(self, lat: float, lng: float) -> list[RoutingTile]:
        """Get the 3x3 neighbourhood of tiles around a coordinate."""
        keys = neighbor_keys(lat, lng, self._precision)
        tiles: list[RoutingTile] = []
        for k in keys:
            # Only load tiles that actually have nodes
            if any(tk == k for tk in self._node_tile.values()):
                tiles.append(self.get_tile(k))
        return tiles

    def merge_tiles(self, keys: list[str]) -> RoadGraph:
        """Merge multiple tiles into a single graph for cross-tile routing."""
        merged = RoadGraph()
        for key in keys:
            tile = self.get_tile(key)
            for nid, node in tile.graph.nodes.items():
                if not merged.has_node(nid):
                    merged.add_node(nid, node.lat, node.lng)
            for nid in tile.graph.nodes:
                for edge in tile.graph.get_neighbors(nid):
                    if not merged.has_node(edge.dst):
                        dn = tile.graph.get_node(edge.dst)
                        merged.add_node(dn.id, dn.lat, dn.lng)
                    if not merged.has_edge(edge.src, edge.dst):
                        merged._adj[edge.src].append(edge)
        return merged

    def node_tile_key(self, node_id: str) -> str:
        """Return the tile key for a given node."""
        return self._node_tile[node_id]
