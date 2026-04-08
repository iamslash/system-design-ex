#!/usr/bin/env python3
"""Proximity Service Demo.

Geohash, Quadtree, Proximity Service 의 주요 기능을 시연한다.

Run:
    python scripts/demo.py
"""

from __future__ import annotations

import os
import sys
import time

# Allow running from repo root or from the chapter directory.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.geohash import (
    bounding_box,
    decode,
    encode,
    neighbors,
    precision_for_radius_km,
    PRECISION_TABLE,
)
from src.quadtree import Business, QuadTree, haversine_km
from src.proximity import IndexType, ProximityService


# ---------------------------------------------------------------------------
# Sample businesses (San Francisco area)
# ---------------------------------------------------------------------------

BUSINESSES = [
    Business("b01", "Blue Bottle Coffee", 37.7825, -122.4082),
    Business("b02", "Tartine Bakery", 37.7614, -122.4241),
    Business("b03", "Bi-Rite Market", 37.7617, -122.4256),
    Business("b04", "Flour + Water", 37.7598, -122.4210),
    Business("b05", "Delfina Restaurant", 37.7612, -122.4242),
    Business("b06", "Sightglass Coffee", 37.7719, -122.4078),
    Business("b07", "Philz Coffee", 37.7643, -122.4219),
    Business("b08", "Chez Panisse", 37.8795, -122.2690),  # Berkeley
    Business("b09", "Nopa", 37.7745, -122.4373),
    Business("b10", "State Bird Provisions", 37.7872, -122.4394),
    Business("b11", "Zuni Cafe", 37.7758, -122.4214),
    Business("b12", "Liholiho Yacht Club", 37.7860, -122.4141),
    Business("b13", "La Taqueria", 37.7508, -122.4180),
    Business("b14", "Swan Oyster Depot", 37.7865, -122.4217),
    Business("b15", "House of Prime Rib", 37.7893, -122.4223),
]

USER_LAT = 37.7749
USER_LNG = -122.4194


def section(title: str) -> None:
    """Print a section header."""
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)
    print()


def demo_geohash_encoding() -> None:
    """1. Geohash encoding / decoding demo."""
    section("1. Geohash Encoding & Decoding")

    print("  Precision table (approximate cell dimensions):")
    print(f"  {'Precision':>9}  {'Width (km)':>10}  {'Height (km)':>11}")
    print(f"  {'---------':>9}  {'----------':>10}  {'-----------':>11}")
    for prec in range(1, 10):
        w, h = PRECISION_TABLE[prec]
        print(f"  {prec:>9}  {w:>10.3f}  {h:>11.3f}")
    print()

    # Encode some locations
    locations = [
        ("San Francisco", 37.7749, -122.4194),
        ("Tokyo", 35.6762, 139.6503),
        ("London", 51.5074, -0.1278),
        ("Sydney", -33.8688, 151.2093),
    ]

    for name, lat, lng in locations:
        for prec in [4, 6, 8]:
            gh = encode(lat, lng, prec)
            dlat, dlng = decode(gh)
            err = haversine_km(lat, lng, dlat, dlng)
            print(f"  {name:15s} prec={prec} -> {gh:12s}  "
                  f"decoded=({dlat:9.4f}, {dlng:10.4f})  "
                  f"error={err:.3f} km")
        print()


def demo_geohash_neighbors() -> None:
    """2. Geohash neighbor finding demo."""
    section("2. Geohash Neighbors")

    gh = encode(USER_LAT, USER_LNG, 6)
    print(f"  User location: ({USER_LAT}, {USER_LNG})")
    print(f"  Geohash (precision 6): {gh}")
    print()

    bb = bounding_box(gh)
    print(f"  Bounding box: lat=[{bb[0]:.6f}, {bb[2]:.6f}]  "
          f"lng=[{bb[1]:.6f}, {bb[3]:.6f}]")
    print()

    nbrs = neighbors(gh)
    dirs = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    print("  Neighbors:")
    for d, n in zip(dirs, nbrs):
        nlat, nlng = decode(n)
        print(f"    {d:2s}: {n}  -> ({nlat:.4f}, {nlng:.4f})")
    print()

    # Auto-select precision for various radii
    print("  Auto-precision for search radius:")
    for radius in [0.1, 0.5, 1.0, 5.0, 20.0, 100.0]:
        prec = precision_for_radius_km(radius)
        print(f"    {radius:6.1f} km -> precision {prec}")


def demo_quadtree() -> None:
    """3. Quadtree build & query demo."""
    section("3. Quadtree Build & Query")

    qt = QuadTree(max_points=3)
    qt.build(BUSINESSES)

    print(f"  Businesses inserted : {qt.size}")
    print(f"  Total nodes         : {qt.count_nodes()}")
    print(f"  Max depth           : {qt.max_depth()}")
    print()

    # k-nearest query
    print(f"  5 nearest to user ({USER_LAT}, {USER_LNG}):")
    results = qt.find_nearest(USER_LAT, USER_LNG, k=5)
    for i, (biz, dist) in enumerate(results, 1):
        print(f"    {i}. {biz.name:25s}  {dist:.3f} km")
    print()

    # Range query
    print("  Businesses within 2km bounding box:")
    from src.quadtree import _radius_to_box
    box = _radius_to_box(USER_LAT, USER_LNG, 2.0)
    in_range = qt.query_range(box)
    for biz in in_range:
        dist = haversine_km(USER_LAT, USER_LNG, biz.lat, biz.lng)
        print(f"    {biz.name:25s}  {dist:.3f} km")


def demo_proximity_service() -> None:
    """4. Proximity service demo with both backends."""
    section("4. Proximity Service - Geohash Backend")

    svc_gh = ProximityService(
        index_type=IndexType.GEOHASH,
        geohash_precision=6,
    )
    svc_gh.add_businesses(BUSINESSES)

    print(f"  Index type       : {svc_gh.index_type.value}")
    print(f"  Business count   : {svc_gh.business_count}")
    print()

    for radius in [0.5, 1.0, 2.0, 5.0]:
        results = svc_gh.search(USER_LAT, USER_LNG, radius_km=radius)
        print(f"  Within {radius:.1f} km ({len(results)} results):")
        for r in results[:5]:
            print(f"    {r.business.name:25s}  {r.distance_km:.3f} km")
        if len(results) > 5:
            print(f"    ... and {len(results) - 5} more")
        print()

    section("5. Proximity Service - Quadtree Backend")

    svc_qt = ProximityService(index_type=IndexType.QUADTREE)
    svc_qt.add_businesses(BUSINESSES)

    print(f"  Index type       : {svc_qt.index_type.value}")
    print(f"  Business count   : {svc_qt.business_count}")
    print()

    for radius in [0.5, 1.0, 2.0, 5.0]:
        results = svc_qt.search(USER_LAT, USER_LNG, radius_km=radius)
        print(f"  Within {radius:.1f} km ({len(results)} results):")
        for r in results[:5]:
            print(f"    {r.business.name:25s}  {r.distance_km:.3f} km")
        if len(results) > 5:
            print(f"    ... and {len(results) - 5} more")
        print()

    # find_nearest comparison
    print("  find_nearest (top 5):")
    nearest = svc_qt.find_nearest(USER_LAT, USER_LNG, k=5)
    for i, r in enumerate(nearest, 1):
        print(f"    {i}. {r.business.name:25s}  {r.distance_km:.3f} km")


def demo_comparison() -> None:
    """6. Compare geohash vs quadtree approaches."""
    section("6. Approach Comparison")

    # Build both indexes
    svc_gh = ProximityService(
        index_type=IndexType.GEOHASH,
        geohash_precision=6,
    )
    svc_qt = ProximityService(index_type=IndexType.QUADTREE)
    svc_gh.add_businesses(BUSINESSES)
    svc_qt.add_businesses(BUSINESSES)

    # Search with both
    radius = 2.0
    results_gh = svc_gh.search(USER_LAT, USER_LNG, radius_km=radius)
    results_qt = svc_qt.search(USER_LAT, USER_LNG, radius_km=radius)

    ids_gh = {r.business.id for r in results_gh}
    ids_qt = {r.business.id for r in results_qt}

    print(f"  Search radius: {radius} km")
    print(f"  Geohash results : {len(results_gh)}")
    print(f"  Quadtree results: {len(results_qt)}")
    print(f"  Same results    : {ids_gh == ids_qt}")
    print()

    # Benchmark
    num_searches = 10_000
    print(f"  Benchmark: {num_searches:,} searches each")

    start = time.perf_counter()
    for _ in range(num_searches):
        svc_gh.search(USER_LAT, USER_LNG, radius_km=radius)
    gh_time = time.perf_counter() - start

    start = time.perf_counter()
    for _ in range(num_searches):
        svc_qt.search(USER_LAT, USER_LNG, radius_km=radius)
    qt_time = time.perf_counter() - start

    print(f"  Geohash : {gh_time:.3f}s ({num_searches / gh_time:,.0f} QPS)")
    print(f"  Quadtree: {qt_time:.3f}s ({num_searches / qt_time:,.0f} QPS)")
    print()

    print("  +------------------+------------------+------------------+")
    print("  |   Approach       |   Pros           |   Cons           |")
    print("  +------------------+------------------+------------------+")
    print("  |   Geohash        | Simple, DB-      | Boundary issues, |")
    print("  |                  | friendly, easy   | fixed precision  |")
    print("  |                  | to cache/shard   |                  |")
    print("  +------------------+------------------+------------------+")
    print("  |   Quadtree       | Adaptive density,| In-memory only,  |")
    print("  |                  | exact k-nearest, | harder to shard  |")
    print("  |                  | no boundary gap  |                  |")
    print("  +------------------+------------------+------------------+")


def main() -> None:
    print()
    print("Proximity Service Demo")
    print("======================")

    demo_geohash_encoding()
    demo_geohash_neighbors()
    demo_quadtree()
    demo_proximity_service()
    demo_comparison()

    section("Done")
    print("  All demonstrations completed successfully.")
    print()


if __name__ == "__main__":
    main()
