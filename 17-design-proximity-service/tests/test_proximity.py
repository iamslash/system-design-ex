"""Tests for Proximity Service."""

from __future__ import annotations

import math
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.geohash import (
    bounding_box,
    decode,
    encode,
    neighbors,
    precision_for_radius_km,
    PRECISION_TABLE,
)
from src.quadtree import (
    BoundingBox,
    Business,
    QuadTree,
    QuadTreeNode,
    haversine_km,
)
from src.proximity import IndexType, ProximityService, SearchResult


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_BUSINESSES = [
    Business("b1", "Cafe Alpha", 37.7749, -122.4194),
    Business("b2", "Bakery Beta", 37.7751, -122.4180),
    Business("b3", "Shop Gamma", 37.7760, -122.4210),
    Business("b4", "Bar Delta", 37.7740, -122.4170),
    Business("b5", "Gym Epsilon", 37.7730, -122.4200),
    Business("b6", "Spa Zeta", 37.7780, -122.4150),
    Business("b7", "Diner Eta", 37.7700, -122.4250),
    Business("b8", "Salon Theta", 37.7800, -122.4100),
    Business("b9", "Market Iota", 37.7650, -122.4300),
    Business("b10", "Pub Kappa", 37.7850, -122.4050),
]


# ---------------------------------------------------------------------------
# Geohash: encode / decode
# ---------------------------------------------------------------------------


class TestGeohashEncode:
    """Geohash encoding tests."""

    def test_encode_basic(self) -> None:
        """Known coordinate encodes to expected geohash prefix."""
        gh = encode(37.7749, -122.4194, 6)
        assert isinstance(gh, str)
        assert len(gh) == 6

    def test_encode_precision(self) -> None:
        """Output length matches requested precision."""
        for prec in range(1, 10):
            gh = encode(0.0, 0.0, prec)
            assert len(gh) == prec

    def test_encode_origin(self) -> None:
        """Origin (0, 0) should encode to a known geohash."""
        gh = encode(0.0, 0.0, 6)
        assert gh.startswith("s")  # (0,0) is in the 's' quadrant

    def test_encode_invalid_precision(self) -> None:
        """Out-of-range precision raises ValueError."""
        try:
            encode(0, 0, 0)
            assert False, "Expected ValueError"
        except ValueError:
            pass
        try:
            encode(0, 0, 13)
            assert False, "Expected ValueError"
        except ValueError:
            pass

    def test_encode_invalid_latitude(self) -> None:
        """Out-of-range latitude raises ValueError."""
        try:
            encode(91.0, 0.0)
            assert False, "Expected ValueError"
        except ValueError:
            pass

    def test_encode_invalid_longitude(self) -> None:
        """Out-of-range longitude raises ValueError."""
        try:
            encode(0.0, 181.0)
            assert False, "Expected ValueError"
        except ValueError:
            pass


class TestGeohashDecode:
    """Geohash decoding tests."""

    def test_decode_basic(self) -> None:
        """Decoding returns a (lat, lng) tuple."""
        lat, lng = decode("9q8yyk")
        assert isinstance(lat, float)
        assert isinstance(lng, float)

    def test_decode_empty_raises(self) -> None:
        """Decoding empty string raises ValueError."""
        try:
            decode("")
            assert False, "Expected ValueError"
        except ValueError:
            pass

    def test_decode_invalid_char_raises(self) -> None:
        """Decoding invalid character raises ValueError."""
        try:
            decode("abc!@#")
            assert False, "Expected ValueError"
        except ValueError:
            pass


class TestGeohashRoundtrip:
    """Geohash encode/decode roundtrip tests."""

    def test_roundtrip_precision_6(self) -> None:
        """Encode -> decode roundtrip within precision-6 cell error."""
        lat, lng = 37.7749, -122.4194
        gh = encode(lat, lng, 6)
        dlat, dlng = decode(gh)
        # Precision 6 has ~1.2km x 0.6km cells, so error < 0.01 degrees
        assert abs(dlat - lat) < 0.01
        assert abs(dlng - lng) < 0.01

    def test_roundtrip_precision_8(self) -> None:
        """Higher precision gives smaller roundtrip error."""
        lat, lng = 37.7749, -122.4194
        gh = encode(lat, lng, 8)
        dlat, dlng = decode(gh)
        assert abs(dlat - lat) < 0.001
        assert abs(dlng - lng) < 0.001

    def test_roundtrip_multiple_locations(self) -> None:
        """Roundtrip works for various global locations."""
        locations = [
            (0.0, 0.0),
            (37.7749, -122.4194),   # San Francisco
            (-33.8688, 151.2093),   # Sydney
            (51.5074, -0.1278),     # London
            (35.6762, 139.6503),    # Tokyo
        ]
        for lat, lng in locations:
            gh = encode(lat, lng, 7)
            dlat, dlng = decode(gh)
            assert abs(dlat - lat) < 0.005, f"Failed for ({lat}, {lng})"
            assert abs(dlng - lng) < 0.005, f"Failed for ({lat}, {lng})"

    def test_roundtrip_boundary_values(self) -> None:
        """Roundtrip works at coordinate boundaries."""
        for lat, lng in [(-90, -180), (-90, 180), (90, -180), (90, 180)]:
            gh = encode(lat, lng, 4)
            dlat, dlng = decode(gh)
            assert abs(dlat - lat) < 1.0
            assert abs(dlng - lng) < 1.0


# ---------------------------------------------------------------------------
# Geohash: neighbors
# ---------------------------------------------------------------------------


class TestGeohashNeighbors:
    """Geohash neighbor finding tests."""

    def test_neighbors_count(self) -> None:
        """Neighbors returns exactly 8 geohashes."""
        nbrs = neighbors("9q8yyk")
        assert len(nbrs) == 8

    def test_neighbors_same_precision(self) -> None:
        """All neighbors have the same precision as the input."""
        gh = encode(37.7749, -122.4194, 6)
        nbrs = neighbors(gh)
        for n in nbrs:
            assert len(n) == len(gh)

    def test_neighbors_all_unique(self) -> None:
        """Neighbors should be 8 distinct cells (non-polar)."""
        gh = encode(37.7749, -122.4194, 6)
        nbrs = neighbors(gh)
        assert len(set(nbrs)) == 8

    def test_neighbors_do_not_include_self(self) -> None:
        """The center cell should not appear in its own neighbors."""
        gh = encode(37.7749, -122.4194, 6)
        nbrs = neighbors(gh)
        assert gh not in nbrs

    def test_neighbors_at_equator(self) -> None:
        """Neighbors work correctly at the equator."""
        gh = encode(0.0, 0.0, 4)
        nbrs = neighbors(gh)
        assert len(nbrs) == 8
        assert len(set(nbrs)) == 8

    def test_neighbors_empty_raises(self) -> None:
        """Empty geohash raises ValueError."""
        try:
            neighbors("")
            assert False, "Expected ValueError"
        except ValueError:
            pass

    def test_neighbors_near_date_line(self) -> None:
        """Neighbors near the international date line wrap correctly."""
        gh = encode(0.0, 179.9, 4)
        nbrs = neighbors(gh)
        assert len(nbrs) == 8
        # Some neighbors should wrap to negative longitude
        decoded_lngs = [decode(n)[1] for n in nbrs]
        has_negative = any(lng < 0 for lng in decoded_lngs)
        has_positive = any(lng > 0 for lng in decoded_lngs)
        assert has_negative and has_positive


# ---------------------------------------------------------------------------
# Geohash: bounding box & precision
# ---------------------------------------------------------------------------


class TestGeohashBoundingBox:
    """Geohash bounding box tests."""

    def test_bounding_box_contains_center(self) -> None:
        """Bounding box should contain the decoded center."""
        gh = encode(37.7749, -122.4194, 6)
        min_lat, min_lng, max_lat, max_lng = bounding_box(gh)
        lat, lng = decode(gh)
        assert min_lat <= lat <= max_lat
        assert min_lng <= lng <= max_lng

    def test_bounding_box_shrinks_with_precision(self) -> None:
        """Higher precision produces smaller bounding boxes."""
        lat, lng = 37.7749, -122.4194
        prev_area = float("inf")
        for prec in range(1, 9):
            gh = encode(lat, lng, prec)
            min_lat, min_lng, max_lat, max_lng = bounding_box(gh)
            area = (max_lat - min_lat) * (max_lng - min_lng)
            assert area < prev_area
            prev_area = area


class TestGeohashPrecision:
    """Geohash precision selection tests."""

    def test_precision_for_small_radius(self) -> None:
        """Small radius selects high precision."""
        p = precision_for_radius_km(0.01)
        assert p >= 7

    def test_precision_for_large_radius(self) -> None:
        """Large radius selects low precision."""
        p = precision_for_radius_km(100.0)
        assert p <= 3

    def test_precision_table_completeness(self) -> None:
        """Precision table covers levels 1-9."""
        for i in range(1, 10):
            assert i in PRECISION_TABLE


# ---------------------------------------------------------------------------
# Quadtree
# ---------------------------------------------------------------------------


class TestQuadtreeBasic:
    """Quadtree basic operations tests."""

    def test_insert_and_size(self) -> None:
        """Inserting businesses increments size."""
        qt = QuadTree(max_points=4)
        for biz in SAMPLE_BUSINESSES[:5]:
            qt.insert(biz)
        assert qt.size == 5

    def test_build_from_list(self) -> None:
        """Bulk build populates the tree."""
        qt = QuadTree(max_points=4)
        qt.build(SAMPLE_BUSINESSES)
        assert qt.size == len(SAMPLE_BUSINESSES)

    def test_empty_tree_query(self) -> None:
        """Querying an empty tree returns no results."""
        qt = QuadTree()
        box = BoundingBox(37.0, -123.0, 38.0, -122.0)
        assert qt.query_range(box) == []

    def test_query_range_finds_all(self) -> None:
        """A sufficiently large box finds all businesses."""
        qt = QuadTree(max_points=2)
        qt.build(SAMPLE_BUSINESSES)
        box = BoundingBox(37.0, -123.0, 38.0, -122.0)
        results = qt.query_range(box)
        assert len(results) == len(SAMPLE_BUSINESSES)

    def test_query_range_finds_subset(self) -> None:
        """A smaller box finds only businesses within it."""
        qt = QuadTree(max_points=2)
        qt.build(SAMPLE_BUSINESSES)
        # Tight box around first few businesses
        box = BoundingBox(37.774, -122.420, 37.776, -122.417)
        results = qt.query_range(box)
        assert 0 < len(results) < len(SAMPLE_BUSINESSES)


class TestQuadtreeNearest:
    """Quadtree k-nearest search tests."""

    def test_find_nearest_basic(self) -> None:
        """find_nearest returns the requested number of results."""
        qt = QuadTree(max_points=2)
        qt.build(SAMPLE_BUSINESSES)
        results = qt.find_nearest(37.7749, -122.4194, k=3)
        assert len(results) == 3

    def test_find_nearest_sorted_by_distance(self) -> None:
        """Results are sorted by ascending distance."""
        qt = QuadTree(max_points=2)
        qt.build(SAMPLE_BUSINESSES)
        results = qt.find_nearest(37.7749, -122.4194, k=5)
        distances = [d for _, d in results]
        assert distances == sorted(distances)

    def test_find_nearest_returns_businesses(self) -> None:
        """Each result is a (Business, float) tuple."""
        qt = QuadTree(max_points=2)
        qt.build(SAMPLE_BUSINESSES)
        results = qt.find_nearest(37.7749, -122.4194, k=2)
        for biz, dist in results:
            assert isinstance(biz, Business)
            assert isinstance(dist, float)
            assert dist >= 0


class TestQuadtreeSubdivision:
    """Quadtree subdivision tests."""

    def test_subdivision_creates_children(self) -> None:
        """Inserting more than max_points triggers subdivision."""
        qt = QuadTree(max_points=2)
        qt.build(SAMPLE_BUSINESSES)
        # Root should have children after inserting 10 businesses with max 2
        assert qt.root.children is not None

    def test_node_count_increases(self) -> None:
        """Node count is greater than 1 after subdivision."""
        qt = QuadTree(max_points=2)
        qt.build(SAMPLE_BUSINESSES)
        assert qt.count_nodes() > 1

    def test_max_depth_reasonable(self) -> None:
        """Max depth stays within bounds."""
        qt = QuadTree(max_points=2)
        qt.build(SAMPLE_BUSINESSES)
        assert 0 < qt.max_depth() <= 20


# ---------------------------------------------------------------------------
# Quadtree: haversine
# ---------------------------------------------------------------------------


class TestHaversine:
    """Haversine distance tests."""

    def test_same_point_zero_distance(self) -> None:
        """Distance from a point to itself is zero."""
        assert haversine_km(37.7749, -122.4194, 37.7749, -122.4194) == 0.0

    def test_known_distance(self) -> None:
        """Distance between SF and LA is approximately 559 km."""
        dist = haversine_km(37.7749, -122.4194, 34.0522, -118.2437)
        assert 550 < dist < 570


# ---------------------------------------------------------------------------
# Proximity Service: geohash backend
# ---------------------------------------------------------------------------


class TestProximityServiceGeohash:
    """Proximity service with geohash index."""

    def test_search_returns_results(self) -> None:
        """Search near known businesses returns matches."""
        svc = ProximityService(index_type=IndexType.GEOHASH, geohash_precision=6)
        svc.add_businesses(SAMPLE_BUSINESSES)
        results = svc.search(37.7749, -122.4194, radius_km=1.0)
        assert len(results) > 0

    def test_search_results_sorted_by_distance(self) -> None:
        """Results are sorted by ascending distance."""
        svc = ProximityService(index_type=IndexType.GEOHASH, geohash_precision=6)
        svc.add_businesses(SAMPLE_BUSINESSES)
        results = svc.search(37.7749, -122.4194, radius_km=5.0)
        distances = [r.distance_km for r in results]
        assert distances == sorted(distances)

    def test_search_respects_radius(self) -> None:
        """All results are within the search radius."""
        svc = ProximityService(index_type=IndexType.GEOHASH, geohash_precision=6)
        svc.add_businesses(SAMPLE_BUSINESSES)
        radius = 0.5
        results = svc.search(37.7749, -122.4194, radius_km=radius)
        for r in results:
            assert r.distance_km <= radius

    def test_search_empty_area(self) -> None:
        """Search in an empty area returns no results."""
        svc = ProximityService(index_type=IndexType.GEOHASH, geohash_precision=6)
        svc.add_businesses(SAMPLE_BUSINESSES)
        # Middle of the Pacific Ocean
        results = svc.search(0.0, -150.0, radius_km=1.0)
        assert len(results) == 0

    def test_search_limit(self) -> None:
        """Search respects the limit parameter."""
        svc = ProximityService(index_type=IndexType.GEOHASH, geohash_precision=6)
        svc.add_businesses(SAMPLE_BUSINESSES)
        results = svc.search(37.7749, -122.4194, radius_km=5.0, limit=3)
        assert len(results) <= 3


# ---------------------------------------------------------------------------
# Proximity Service: quadtree backend
# ---------------------------------------------------------------------------


class TestProximityServiceQuadtree:
    """Proximity service with quadtree index."""

    def test_search_returns_results(self) -> None:
        """Search near known businesses returns matches."""
        svc = ProximityService(index_type=IndexType.QUADTREE)
        svc.add_businesses(SAMPLE_BUSINESSES)
        results = svc.search(37.7749, -122.4194, radius_km=1.0)
        assert len(results) > 0

    def test_search_results_sorted_by_distance(self) -> None:
        """Results are sorted by ascending distance."""
        svc = ProximityService(index_type=IndexType.QUADTREE)
        svc.add_businesses(SAMPLE_BUSINESSES)
        results = svc.search(37.7749, -122.4194, radius_km=5.0)
        distances = [r.distance_km for r in results]
        assert distances == sorted(distances)

    def test_search_respects_radius(self) -> None:
        """All results are within the search radius."""
        svc = ProximityService(index_type=IndexType.QUADTREE)
        svc.add_businesses(SAMPLE_BUSINESSES)
        radius = 0.5
        results = svc.search(37.7749, -122.4194, radius_km=radius)
        for r in results:
            assert r.distance_km <= radius

    def test_search_empty_area(self) -> None:
        """Search in an empty area returns no results."""
        svc = ProximityService(index_type=IndexType.QUADTREE)
        svc.add_businesses(SAMPLE_BUSINESSES)
        results = svc.search(0.0, -150.0, radius_km=1.0)
        assert len(results) == 0

    def test_find_nearest(self) -> None:
        """find_nearest returns k results."""
        svc = ProximityService(index_type=IndexType.QUADTREE)
        svc.add_businesses(SAMPLE_BUSINESSES)
        results = svc.find_nearest(37.7749, -122.4194, k=3)
        assert len(results) == 3
        for r in results:
            assert isinstance(r, SearchResult)


# ---------------------------------------------------------------------------
# Proximity Service: general
# ---------------------------------------------------------------------------


class TestProximityServiceGeneral:
    """General proximity service tests."""

    def test_business_count(self) -> None:
        """business_count reflects added businesses."""
        svc = ProximityService()
        assert svc.business_count == 0
        svc.add_businesses(SAMPLE_BUSINESSES[:3])
        assert svc.business_count == 3
        svc.add_businesses(SAMPLE_BUSINESSES[3:])
        assert svc.business_count == len(SAMPLE_BUSINESSES)

    def test_index_type_property(self) -> None:
        """index_type property returns configured type."""
        svc_gh = ProximityService(index_type=IndexType.GEOHASH)
        assert svc_gh.index_type == IndexType.GEOHASH
        svc_qt = ProximityService(index_type=IndexType.QUADTREE)
        assert svc_qt.index_type == IndexType.QUADTREE

    def test_both_indexes_find_same_businesses(self) -> None:
        """Geohash and quadtree indexes return the same businesses for large radius."""
        svc_gh = ProximityService(index_type=IndexType.GEOHASH, geohash_precision=4)
        svc_qt = ProximityService(index_type=IndexType.QUADTREE)
        svc_gh.add_businesses(SAMPLE_BUSINESSES)
        svc_qt.add_businesses(SAMPLE_BUSINESSES)

        results_gh = svc_gh.search(37.7749, -122.4194, radius_km=5.0)
        results_qt = svc_qt.search(37.7749, -122.4194, radius_km=5.0)

        ids_gh = {r.business.id for r in results_gh}
        ids_qt = {r.business.id for r in results_qt}
        assert ids_gh == ids_qt

    def test_search_result_contains_business_and_distance(self) -> None:
        """Each SearchResult has business and distance_km attributes."""
        svc = ProximityService(index_type=IndexType.QUADTREE)
        svc.add_businesses(SAMPLE_BUSINESSES)
        results = svc.search(37.7749, -122.4194, radius_km=5.0)
        for r in results:
            assert hasattr(r, "business")
            assert hasattr(r, "distance_km")
            assert isinstance(r.business, Business)
            assert isinstance(r.distance_km, float)


# ---------------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------------


class TestPerformance:
    """Performance and scalability tests."""

    def test_geohash_encode_performance(self) -> None:
        """Encoding 10k coordinates completes in under 1 second."""
        start = time.perf_counter()
        for i in range(10_000):
            lat = (i % 180) - 90.0 + 0.001
            lng = (i % 360) - 180.0 + 0.001
            encode(lat, lng, 6)
        elapsed = time.perf_counter() - start
        assert elapsed < 1.0, f"Encoding 10k took {elapsed:.2f}s"

    def test_quadtree_build_performance(self) -> None:
        """Building a quadtree with 1000 businesses completes quickly."""
        businesses = [
            Business(
                f"biz-{i}",
                f"Business {i}",
                37.7 + (i % 100) * 0.001,
                -122.4 + (i // 100) * 0.001,
            )
            for i in range(1000)
        ]
        start = time.perf_counter()
        qt = QuadTree(max_points=4)
        qt.build(businesses)
        elapsed = time.perf_counter() - start
        assert elapsed < 2.0, f"Build 1000 took {elapsed:.2f}s"
        assert qt.size == 1000

    def test_proximity_search_performance(self) -> None:
        """100 proximity searches over 1000 businesses complete quickly."""
        businesses = [
            Business(
                f"biz-{i}",
                f"Business {i}",
                37.7 + (i % 100) * 0.001,
                -122.4 + (i // 100) * 0.001,
            )
            for i in range(1000)
        ]
        svc = ProximityService(index_type=IndexType.QUADTREE)
        svc.add_businesses(businesses)

        start = time.perf_counter()
        for _ in range(100):
            svc.search(37.75, -122.40, radius_km=2.0)
        elapsed = time.perf_counter() - start
        assert elapsed < 2.0, f"100 searches took {elapsed:.2f}s"
