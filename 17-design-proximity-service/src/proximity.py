"""Proximity Service using geohash or quadtree.

Given a user's location (lat/lng) and a search radius, return nearby
businesses.  Two indexing strategies are supported:

- **Geohash**: Encode the query point, find the geohash cell + its 8
  neighbors, then filter businesses within the radius.
- **Quadtree**: Use the quadtree's expanding-box nearest search.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum

from src.geohash import encode as geohash_encode
from src.geohash import neighbors as geohash_neighbors
from src.geohash import precision_for_radius_km
from src.quadtree import (
    BoundingBox,
    Business,
    QuadTree,
    haversine_km,
    _radius_to_box,
)


class IndexType(Enum):
    """Spatial index strategy."""
    GEOHASH = "geohash"
    QUADTREE = "quadtree"


@dataclass
class SearchResult:
    """A single proximity search result.

    Attributes:
        business: The matched business.
        distance_km: Great-circle distance from the query point in km.
    """

    business: Business
    distance_km: float


class ProximityService:
    """Proximity service backed by geohash or quadtree index.

    Args:
        index_type: Which spatial index to use.
        geohash_precision: Fixed geohash precision (if None, auto-select).
        quadtree_max_points: Max businesses per quadtree leaf node.
    """

    def __init__(
        self,
        index_type: IndexType = IndexType.GEOHASH,
        geohash_precision: int | None = None,
        quadtree_max_points: int = 4,
    ) -> None:
        self._index_type = index_type
        self._geohash_precision = geohash_precision
        self._quadtree_max_points = quadtree_max_points

        # Geohash index: geohash_prefix -> [Business, ...]
        self._geohash_index: dict[str, list[Business]] = {}
        # Quadtree index
        self._quadtree: QuadTree | None = None

        self._businesses: list[Business] = []

    @property
    def index_type(self) -> IndexType:
        return self._index_type

    @property
    def business_count(self) -> int:
        return len(self._businesses)

    def add_businesses(self, businesses: list[Business]) -> None:
        """Add businesses and rebuild the spatial index."""
        self._businesses.extend(businesses)
        self._rebuild_index()

    def _rebuild_index(self) -> None:
        """Rebuild the active spatial index from scratch."""
        if self._index_type == IndexType.GEOHASH:
            self._build_geohash_index()
        else:
            self._build_quadtree_index()

    def _build_geohash_index(self) -> None:
        """Build the geohash inverted index."""
        # Use the configured precision or a sensible default.
        precision = self._geohash_precision or 6
        self._geohash_index.clear()
        for biz in self._businesses:
            gh = geohash_encode(biz.lat, biz.lng, precision)
            self._geohash_index.setdefault(gh, []).append(biz)

    def _build_quadtree_index(self) -> None:
        """Build the quadtree index."""
        self._quadtree = QuadTree(max_points=self._quadtree_max_points)
        self._quadtree.build(self._businesses)

    def search(
        self,
        lat: float,
        lng: float,
        radius_km: float,
        limit: int = 20,
    ) -> list[SearchResult]:
        """Search for businesses near a point.

        Args:
            lat: Query latitude.
            lng: Query longitude.
            radius_km: Search radius in kilometers.
            limit: Maximum results to return.

        Returns:
            List of SearchResult sorted by distance (ascending).
        """
        if self._index_type == IndexType.GEOHASH:
            return self._search_geohash(lat, lng, radius_km, limit)
        else:
            return self._search_quadtree(lat, lng, radius_km, limit)

    def _search_geohash(
        self,
        lat: float,
        lng: float,
        radius_km: float,
        limit: int,
    ) -> list[SearchResult]:
        """Search using geohash index."""
        # Determine precision from radius if not explicitly set.
        precision = self._geohash_precision or precision_for_radius_km(radius_km)

        # Encode query point and find its cell + 8 neighbors.
        query_gh = geohash_encode(lat, lng, precision)
        cells = [query_gh] + geohash_neighbors(query_gh)

        # Collect candidate businesses from all relevant cells.
        # We need to re-hash businesses at the query precision if the index
        # precision differs.
        candidates: list[Business] = []
        if precision == (self._geohash_precision or 6):
            for cell in cells:
                candidates.extend(self._geohash_index.get(cell, []))
        else:
            # Fallback: scan businesses and check prefix match.
            for biz in self._businesses:
                biz_gh = geohash_encode(biz.lat, biz.lng, precision)
                if biz_gh in cells:
                    candidates.append(biz)

        # Filter by actual distance and sort.
        results: list[SearchResult] = []
        for biz in candidates:
            dist = haversine_km(lat, lng, biz.lat, biz.lng)
            if dist <= radius_km:
                results.append(SearchResult(business=biz, distance_km=dist))

        results.sort(key=lambda r: r.distance_km)
        return results[:limit]

    def _search_quadtree(
        self,
        lat: float,
        lng: float,
        radius_km: float,
        limit: int,
    ) -> list[SearchResult]:
        """Search using quadtree index."""
        if self._quadtree is None:
            return []

        # Query the bounding box, then filter by actual radius.
        box = _radius_to_box(lat, lng, radius_km)
        candidates = self._quadtree.query_range(box)

        results: list[SearchResult] = []
        for biz in candidates:
            dist = haversine_km(lat, lng, biz.lat, biz.lng)
            if dist <= radius_km:
                results.append(SearchResult(business=biz, distance_km=dist))

        results.sort(key=lambda r: r.distance_km)
        return results[:limit]

    def find_nearest(
        self,
        lat: float,
        lng: float,
        k: int = 5,
    ) -> list[SearchResult]:
        """Find the k nearest businesses regardless of radius.

        Only available with QUADTREE index.  Falls back to a large-radius
        geohash search when using GEOHASH index.

        Args:
            lat: Query latitude.
            lng: Query longitude.
            k: Number of nearest results.

        Returns:
            List of SearchResult sorted by distance, up to k items.
        """
        if self._index_type == IndexType.QUADTREE and self._quadtree:
            pairs = self._quadtree.find_nearest(lat, lng, k=k)
            return [
                SearchResult(business=biz, distance_km=dist)
                for biz, dist in pairs
            ]

        # Geohash fallback: expand radius until we find k results.
        radius = 1.0
        while radius <= 500.0:
            results = self.search(lat, lng, radius, limit=k)
            if len(results) >= k:
                return results[:k]
            radius *= 2
        return self.search(lat, lng, 500.0, limit=k)
