#!/usr/bin/env python3
"""Demo: simplified Google Maps -- routing, ETA, tiles, geocoding.

Run:
    python scripts/demo.py
"""

from __future__ import annotations

import sys
import os

# Allow running from the chapter directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.graph import RoadGraph
from src.routing import astar, dijkstra
from src.routing_tile import TileManager, tile_key
from src.eta import TrafficModel, compute_eta
from src.geocoding import GeocodingService


def build_city_graph() -> RoadGraph:
    """Build a sample city with 24 intersections in a grid-like layout.

    The city is modeled after a ~4x6 grid in the Gangnam area of Seoul,
    with coordinates roughly between (37.49, 127.02) and (37.52, 127.07).
    """
    g = RoadGraph()

    # Row 0 (south) -- lat ~37.495
    g.add_node("A1", 37.495, 127.020)
    g.add_node("A2", 37.495, 127.030)
    g.add_node("A3", 37.495, 127.040)
    g.add_node("A4", 37.495, 127.050)
    g.add_node("A5", 37.495, 127.060)
    g.add_node("A6", 37.495, 127.070)

    # Row 1 -- lat ~37.502
    g.add_node("B1", 37.502, 127.020)
    g.add_node("B2", 37.502, 127.030)
    g.add_node("B3", 37.502, 127.040)
    g.add_node("B4", 37.502, 127.050)
    g.add_node("B5", 37.502, 127.060)
    g.add_node("B6", 37.502, 127.070)

    # Row 2 -- lat ~37.509
    g.add_node("C1", 37.509, 127.020)
    g.add_node("C2", 37.509, 127.030)
    g.add_node("C3", 37.509, 127.040)
    g.add_node("C4", 37.509, 127.050)
    g.add_node("C5", 37.509, 127.060)
    g.add_node("C6", 37.509, 127.070)

    # Row 3 (north) -- lat ~37.516
    g.add_node("D1", 37.516, 127.020)
    g.add_node("D2", 37.516, 127.030)
    g.add_node("D3", 37.516, 127.040)
    g.add_node("D4", 37.516, 127.050)
    g.add_node("D5", 37.516, 127.060)
    g.add_node("D6", 37.516, 127.070)

    # East-west roads (horizontal) -- main arterials at 60 km/h
    for row, prefix in [("A", 37.495), ("B", 37.502), ("C", 37.509), ("D", 37.516)]:
        for i in range(1, 6):
            src = f"{row}{i}"
            dst = f"{row}{i+1}"
            speed = 80.0 if row in ("A", "D") else 60.0  # outer roads faster
            g.add_edge(src, dst, speed_limit_kmh=speed)

    # North-south roads (vertical) -- local streets at 40 km/h
    rows = ["A", "B", "C", "D"]
    for col in range(1, 7):
        for ri in range(len(rows) - 1):
            src = f"{rows[ri]}{col}"
            dst = f"{rows[ri+1]}{col}"
            speed = 50.0 if col in (1, 6) else 40.0  # boundary roads a bit faster
            g.add_edge(src, dst, speed_limit_kmh=speed)

    # A few diagonal shortcuts
    g.add_edge("A1", "B2", speed_limit_kmh=50.0)
    g.add_edge("B2", "C3", speed_limit_kmh=50.0)
    g.add_edge("C3", "D4", speed_limit_kmh=50.0)
    g.add_edge("A6", "B5", speed_limit_kmh=50.0)

    return g


def build_geocoding() -> GeocodingService:
    """Register landmarks around the sample city."""
    geo = GeocodingService()
    geo.register("Gangnam Station", 37.498, 127.028)
    geo.register("COEX Mall", 37.512, 127.059)
    geo.register("Samsung Station", 37.509, 127.044)
    geo.register("Seolleung Station", 37.505, 127.049)
    geo.register("Yeoksam Station", 37.500, 127.037)
    geo.register("Hanti Station", 37.502, 127.063)
    geo.register("Dogok Station", 37.492, 127.047)
    geo.register("Daechi Station", 37.494, 127.063)
    return geo


def section(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def main() -> None:
    graph = build_city_graph()
    geo = build_geocoding()

    # ── 1. Graph info ────────────────────────────────────────
    section("1. Road Network")
    print(f"  Intersections : {graph.node_count}")
    print(f"  Road segments : {graph.edge_count}")

    # ── 2. Geocoding ─────────────────────────────────────────
    section("2. Geocoding")
    for name in ["Gangnam Station", "COEX Mall", "Samsung Station"]:
        loc = geo.geocode(name)
        if loc:
            print(f"  {name:25s} -> ({loc.lat}, {loc.lng})")

    results = geo.search("station")
    print(f"\n  Search 'station': {len(results)} results")
    for loc in results:
        print(f"    - {loc.name}")

    # ── 3. Routing: A* vs Dijkstra ──────────────────────────
    section("3. Routing: A1 -> D6")
    start, end = "A1", "D6"

    result_astar = astar(graph, start, end)
    result_dijkstra = dijkstra(graph, start, end)

    if result_astar:
        print(f"  A* path      : {' -> '.join(result_astar.path)}")
        print(f"  A* distance  : {result_astar.distance_km:.3f} km")
        print(f"  A* explored  : {result_astar.nodes_explored} nodes")

    if result_dijkstra:
        print(f"\n  Dijkstra path: {' -> '.join(result_dijkstra.path)}")
        print(f"  Dijkstra dist: {result_dijkstra.distance_km:.3f} km")
        print(f"  Dijkstra exp : {result_dijkstra.nodes_explored} nodes")

    if result_astar and result_dijkstra:
        saved = result_dijkstra.nodes_explored - result_astar.nodes_explored
        print(f"\n  A* explored {saved} fewer nodes than Dijkstra")

    # ── 4. ETA without traffic ───────────────────────────────
    section("4. ETA (no traffic)")
    if result_astar:
        eta = compute_eta(graph, result_astar.path)
        print(f"  Distance     : {eta.total_distance_km:.3f} km")
        print(f"  Time         : {eta.total_time_minutes:.1f} minutes")
        print(f"  Avg speed    : {eta.average_speed_kmh:.1f} km/h")

    # ── 5. ETA with traffic ──────────────────────────────────
    section("5. ETA (rush hour traffic)")
    traffic = TrafficModel()
    # Simulate congestion on popular routes
    if result_astar:
        path = result_astar.path
        for i in range(min(3, len(path) - 1)):
            traffic.set_traffic(path[i], path[i + 1], 2.5)
            print(f"  Congestion: {path[i]} -> {path[i+1]} (2.5x slower)")

        eta_traffic = compute_eta(graph, path, traffic)
        print(f"\n  Distance     : {eta_traffic.total_distance_km:.3f} km")
        print(f"  Time         : {eta_traffic.total_time_minutes:.1f} minutes")
        print(f"  Avg speed    : {eta_traffic.average_speed_kmh:.1f} km/h")
        print(f"  Slowdown     : +{eta_traffic.total_time_minutes - eta.total_time_minutes:.1f} min vs free-flow")

    # ── 6. Routing Tiles ─────────────────────────────────────
    section("6. Routing Tiles")
    tm = TileManager(graph, precision=2)
    print(f"  Total tiles  : {tm.tile_count}")
    print(f"  Loaded tiles : {tm.tiles_loaded()}")

    # Load tile for A1
    tile_a1 = tm.get_tile_for_node("A1")
    print(f"\n  Tile for A1  : key='{tile_a1.key}'")
    print(f"    Nodes      : {tile_a1.graph.node_count}")
    print(f"    Edges      : {tile_a1.graph.edge_count}")
    print(f"  Loaded tiles : {tm.tiles_loaded()}")

    # Load neighbourhood
    node_a1 = graph.get_node("A1")
    nearby = tm.get_tiles_in_area(node_a1.lat, node_a1.lng)
    print(f"\n  Tiles near A1: {len(nearby)} loaded")
    for t in nearby:
        print(f"    - {t.key} ({t.graph.node_count} nodes)")
    print(f"  Loaded tiles : {tm.tiles_loaded()}")

    # ── 7. Cross-tile routing ────────────────────────────────
    section("7. Cross-Tile Routing")
    src_key = tm.node_tile_key("A1")
    dst_key = tm.node_tile_key("D6")
    print(f"  Source tile   : {src_key}")
    print(f"  Dest tile     : {dst_key}")
    print(f"  Same tile?    : {src_key == dst_key}")

    # Merge needed tiles and route
    all_keys = list({tm.node_tile_key(nid) for nid in graph.nodes})
    merged = tm.merge_tiles(all_keys)
    cross_result = astar(merged, "A1", "D6")
    if cross_result:
        print(f"  Cross-tile path: {' -> '.join(cross_result.path)}")
        print(f"  Distance       : {cross_result.distance_km:.3f} km")

    print()


if __name__ == "__main__":
    main()
