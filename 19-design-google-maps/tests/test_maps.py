"""Tests for the simplified Google Maps implementation.

Covers: graph operations, A* vs Dijkstra, routing tiles, ETA, geocoding.
"""

from __future__ import annotations

import math
import pytest

from src.graph import RoadGraph, Node, Edge, haversine
from src.routing import astar, dijkstra, RouteResult
from src.routing_tile import TileManager, tile_key, neighbor_keys
from src.eta import TrafficModel, compute_eta
from src.geocoding import GeocodingService


# ── Fixtures ─────────────────────────────────────────────────────


@pytest.fixture
def simple_graph() -> RoadGraph:
    """A small 4-node graph for basic tests.

        B
       / \\
      A   D
       \\ /
        C
    """
    g = RoadGraph()
    g.add_node("A", 37.50, 127.00)
    g.add_node("B", 37.51, 127.01)
    g.add_node("C", 37.50, 127.01)
    g.add_node("D", 37.51, 127.02)
    g.add_edge("A", "B", distance_km=1.5, speed_limit_kmh=60.0)
    g.add_edge("A", "C", distance_km=1.0, speed_limit_kmh=40.0)
    g.add_edge("B", "D", distance_km=1.0, speed_limit_kmh=60.0)
    g.add_edge("C", "D", distance_km=1.5, speed_limit_kmh=40.0)
    return g


@pytest.fixture
def city_graph() -> RoadGraph:
    """A larger grid graph (4x4 = 16 nodes) for routing tests."""
    g = RoadGraph()
    rows = 4
    cols = 4
    for r in range(rows):
        for c in range(cols):
            nid = f"N{r}_{c}"
            g.add_node(nid, 37.50 + r * 0.01, 127.00 + c * 0.01)

    for r in range(rows):
        for c in range(cols - 1):
            g.add_edge(f"N{r}_{c}", f"N{r}_{c+1}", speed_limit_kmh=60.0)
    for r in range(rows - 1):
        for c in range(cols):
            g.add_edge(f"N{r}_{c}", f"N{r+1}_{c}", speed_limit_kmh=40.0)
    return g


# ══════════════════════════════════════════════════════════════════
#  GRAPH TESTS
# ══════════════════════════════════════════════════════════════════


class TestGraph:
    def test_add_node(self) -> None:
        g = RoadGraph()
        node = g.add_node("X", 37.5, 127.0)
        assert node.id == "X"
        assert g.node_count == 1
        assert g.has_node("X")

    def test_add_edge_bidirectional(self, simple_graph: RoadGraph) -> None:
        assert simple_graph.has_edge("A", "B")
        assert simple_graph.has_edge("B", "A")  # bidirectional

    def test_add_edge_auto_distance(self) -> None:
        g = RoadGraph()
        g.add_node("P", 37.50, 127.00)
        g.add_node("Q", 37.51, 127.01)
        edge = g.add_edge("P", "Q")
        assert edge.distance_km > 0  # computed via Haversine

    def test_add_edge_missing_node(self) -> None:
        g = RoadGraph()
        g.add_node("P", 37.5, 127.0)
        with pytest.raises(KeyError):
            g.add_edge("P", "MISSING")

    def test_remove_node(self, simple_graph: RoadGraph) -> None:
        simple_graph.remove_node("B")
        assert not simple_graph.has_node("B")
        assert not simple_graph.has_edge("A", "B")
        assert simple_graph.node_count == 3

    def test_remove_node_missing(self, simple_graph: RoadGraph) -> None:
        with pytest.raises(KeyError):
            simple_graph.remove_node("MISSING")

    def test_remove_edge(self, simple_graph: RoadGraph) -> None:
        simple_graph.remove_edge("A", "B")
        assert not simple_graph.has_edge("A", "B")
        assert simple_graph.has_edge("B", "A")  # reverse still exists

    def test_remove_edge_missing(self, simple_graph: RoadGraph) -> None:
        with pytest.raises(KeyError):
            simple_graph.remove_edge("A", "D")  # no direct edge

    def test_get_neighbors(self, simple_graph: RoadGraph) -> None:
        neighbors = simple_graph.get_neighbors("A")
        dst_ids = {e.dst for e in neighbors}
        assert "B" in dst_ids
        assert "C" in dst_ids

    def test_node_count(self, simple_graph: RoadGraph) -> None:
        assert simple_graph.node_count == 4

    def test_edge_count(self, simple_graph: RoadGraph) -> None:
        # 4 bidirectional edges = 8 directed edges
        assert simple_graph.edge_count == 8

    def test_subgraph(self, simple_graph: RoadGraph) -> None:
        sub = simple_graph.subgraph({"A", "B"})
        assert sub.node_count == 2
        assert sub.has_node("A")
        assert sub.has_node("B")
        assert not sub.has_node("C")


class TestHaversine:
    def test_same_point(self) -> None:
        n = Node("X", 37.5, 127.0)
        assert haversine(n, n) == pytest.approx(0.0, abs=1e-10)

    def test_known_distance(self) -> None:
        # Seoul City Hall to Gangnam Station is ~7.5 km
        a = Node("city_hall", 37.5662, 126.9780)
        b = Node("gangnam", 37.4979, 127.0276)
        d = haversine(a, b)
        assert 7.0 < d < 9.0  # rough check


# ══════════════════════════════════════════════════════════════════
#  ROUTING TESTS
# ══════════════════════════════════════════════════════════════════


class TestRouting:
    def test_astar_finds_path(self, simple_graph: RoadGraph) -> None:
        result = astar(simple_graph, "A", "D")
        assert result is not None
        assert result.path[0] == "A"
        assert result.path[-1] == "D"
        assert result.distance_km > 0

    def test_dijkstra_finds_path(self, simple_graph: RoadGraph) -> None:
        result = dijkstra(simple_graph, "A", "D")
        assert result is not None
        assert result.path[0] == "A"
        assert result.path[-1] == "D"

    def test_astar_same_distance_as_dijkstra(self, simple_graph: RoadGraph) -> None:
        ra = astar(simple_graph, "A", "D")
        rd = dijkstra(simple_graph, "A", "D")
        assert ra is not None and rd is not None
        assert ra.distance_km == pytest.approx(rd.distance_km, rel=1e-6)

    def test_astar_explores_fewer_nodes(self, city_graph: RoadGraph) -> None:
        ra = astar(city_graph, "N0_0", "N3_3")
        rd = dijkstra(city_graph, "N0_0", "N3_3")
        assert ra is not None and rd is not None
        assert ra.nodes_explored <= rd.nodes_explored

    def test_no_path(self) -> None:
        g = RoadGraph()
        g.add_node("X", 0, 0)
        g.add_node("Y", 1, 1)
        # No edge
        assert astar(g, "X", "Y") is None
        assert dijkstra(g, "X", "Y") is None

    def test_same_start_end(self, simple_graph: RoadGraph) -> None:
        result = astar(simple_graph, "A", "A")
        assert result is not None
        assert result.path == ["A"]
        assert result.distance_km == 0.0

    def test_invalid_node(self, simple_graph: RoadGraph) -> None:
        assert astar(simple_graph, "MISSING", "A") is None
        assert dijkstra(simple_graph, "A", "MISSING") is None

    def test_path_through_grid(self, city_graph: RoadGraph) -> None:
        result = astar(city_graph, "N0_0", "N3_3")
        assert result is not None
        # Path must be contiguous
        for i in range(len(result.path) - 1):
            assert city_graph.has_edge(result.path[i], result.path[i + 1])


# ══════════════════════════════════════════════════════════════════
#  ROUTING TILE TESTS
# ══════════════════════════════════════════════════════════════════


class TestRoutingTiles:
    def test_tile_key_basic(self) -> None:
        key = tile_key(37.495, 127.025, precision=2)
        assert key == "37.49,127.02"

    def test_tile_key_precision(self) -> None:
        k1 = tile_key(37.495, 127.025, precision=1)
        assert k1 == "37.4,127.0"

    def test_neighbor_keys_count(self) -> None:
        keys = neighbor_keys(37.5, 127.0, precision=2)
        assert len(keys) == 9

    def test_neighbor_keys_includes_center(self) -> None:
        center = tile_key(37.5, 127.0, precision=2)
        keys = neighbor_keys(37.5, 127.0, precision=2)
        assert center in keys

    def test_tile_manager_creates_tiles(self, city_graph: RoadGraph) -> None:
        tm = TileManager(city_graph, precision=2)
        assert tm.tile_count > 0
        assert tm.tiles_loaded() == 0

    def test_tile_manager_lazy_load(self, city_graph: RoadGraph) -> None:
        tm = TileManager(city_graph, precision=2)
        tile = tm.get_tile_for_node("N0_0")
        assert tile.loaded
        assert tm.tiles_loaded() >= 1

    def test_tile_contains_node(self, city_graph: RoadGraph) -> None:
        tm = TileManager(city_graph, precision=2)
        tile = tm.get_tile_for_node("N0_0")
        assert tile.graph.has_node("N0_0")

    def test_merge_tiles(self, city_graph: RoadGraph) -> None:
        tm = TileManager(city_graph, precision=2)
        all_keys = list({tm.node_tile_key(nid) for nid in city_graph.nodes})
        merged = tm.merge_tiles(all_keys)
        assert merged.node_count >= city_graph.node_count

    def test_cross_tile_routing(self, city_graph: RoadGraph) -> None:
        tm = TileManager(city_graph, precision=2)
        all_keys = list({tm.node_tile_key(nid) for nid in city_graph.nodes})
        merged = tm.merge_tiles(all_keys)
        result = astar(merged, "N0_0", "N3_3")
        assert result is not None
        assert result.path[0] == "N0_0"
        assert result.path[-1] == "N3_3"


# ══════════════════════════════════════════════════════════════════
#  ETA TESTS
# ══════════════════════════════════════════════════════════════════


class TestETA:
    def test_eta_basic(self, simple_graph: RoadGraph) -> None:
        result = astar(simple_graph, "A", "D")
        assert result is not None
        eta = compute_eta(simple_graph, result.path)
        assert eta.total_distance_km > 0
        assert eta.total_time_minutes > 0

    def test_eta_single_node(self, simple_graph: RoadGraph) -> None:
        eta = compute_eta(simple_graph, ["A"])
        assert eta.total_distance_km == 0.0
        assert eta.total_time_minutes == 0.0

    def test_eta_with_traffic(self, simple_graph: RoadGraph) -> None:
        result = astar(simple_graph, "A", "D")
        assert result is not None

        traffic = TrafficModel()
        eta_normal = compute_eta(simple_graph, result.path)

        # Double traffic on first segment
        traffic.set_traffic(result.path[0], result.path[1], 2.0)
        eta_heavy = compute_eta(simple_graph, result.path, traffic)

        assert eta_heavy.total_time_minutes > eta_normal.total_time_minutes

    def test_traffic_multiplier_default(self) -> None:
        tm = TrafficModel()
        assert tm.get_multiplier("X", "Y") == 1.0

    def test_traffic_multiplier_set(self) -> None:
        tm = TrafficModel()
        tm.set_traffic("X", "Y", 3.0)
        assert tm.get_multiplier("X", "Y") == 3.0

    def test_traffic_invalid_multiplier(self) -> None:
        tm = TrafficModel()
        with pytest.raises(ValueError):
            tm.set_traffic("X", "Y", 0.0)
        with pytest.raises(ValueError):
            tm.set_traffic("X", "Y", -1.0)

    def test_traffic_clear(self) -> None:
        tm = TrafficModel()
        tm.set_traffic("X", "Y", 2.0)
        tm.clear()
        assert tm.get_multiplier("X", "Y") == 1.0

    def test_congested_segments(self) -> None:
        tm = TrafficModel()
        tm.set_traffic("A", "B", 2.0)
        tm.set_traffic("C", "D", 1.5)
        assert len(tm.congested_segments) == 2

    def test_eta_average_speed(self, simple_graph: RoadGraph) -> None:
        result = astar(simple_graph, "A", "D")
        assert result is not None
        eta = compute_eta(simple_graph, result.path)
        assert eta.average_speed_kmh > 0

    def test_eta_invalid_path(self, simple_graph: RoadGraph) -> None:
        with pytest.raises(ValueError):
            compute_eta(simple_graph, ["A", "D"])  # no direct edge A->D

    def test_eta_segment_times(self, simple_graph: RoadGraph) -> None:
        result = astar(simple_graph, "A", "D")
        assert result is not None
        eta = compute_eta(simple_graph, result.path)
        assert len(eta.segment_times) == len(result.path) - 1
        assert all(t > 0 for t in eta.segment_times)


# ══════════════════════════════════════════════════════════════════
#  GEOCODING TESTS
# ══════════════════════════════════════════════════════════════════


class TestGeocoding:
    def test_register_and_geocode(self) -> None:
        geo = GeocodingService()
        geo.register("Seoul Station", 37.554, 126.972)
        loc = geo.geocode("Seoul Station")
        assert loc is not None
        assert loc.lat == 37.554

    def test_geocode_case_insensitive(self) -> None:
        geo = GeocodingService()
        geo.register("Seoul Station", 37.554, 126.972)
        assert geo.geocode("SEOUL STATION") is not None
        assert geo.geocode("seoul station") is not None

    def test_geocode_not_found(self) -> None:
        geo = GeocodingService()
        assert geo.geocode("Nowhere") is None

    def test_reverse_geocode(self) -> None:
        geo = GeocodingService()
        geo.register("Seoul Station", 37.554, 126.972)
        loc = geo.reverse_geocode(37.554, 126.972)
        assert loc is not None
        assert loc.name == "Seoul Station"

    def test_reverse_geocode_tolerance(self) -> None:
        geo = GeocodingService()
        geo.register("Seoul Station", 37.554, 126.972)
        loc = geo.reverse_geocode(37.5545, 126.9725, tolerance=0.01)
        assert loc is not None
        assert loc.name == "Seoul Station"

    def test_reverse_geocode_out_of_range(self) -> None:
        geo = GeocodingService()
        geo.register("Seoul Station", 37.554, 126.972)
        assert geo.reverse_geocode(38.0, 127.0, tolerance=0.001) is None

    def test_search(self) -> None:
        geo = GeocodingService()
        geo.register("Gangnam Station", 37.498, 127.028)
        geo.register("Seoul Station", 37.554, 126.972)
        geo.register("COEX Mall", 37.512, 127.059)
        results = geo.search("station")
        assert len(results) == 2

    def test_search_no_results(self) -> None:
        geo = GeocodingService()
        geo.register("Gangnam Station", 37.498, 127.028)
        assert geo.search("airport") == []

    def test_location_count(self) -> None:
        geo = GeocodingService()
        assert geo.location_count == 0
        geo.register("A", 0, 0)
        geo.register("B", 1, 1)
        assert geo.location_count == 2

    def test_all_locations(self) -> None:
        geo = GeocodingService()
        geo.register("A", 0, 0)
        geo.register("B", 1, 1)
        locs = geo.all_locations()
        assert len(locs) == 2
