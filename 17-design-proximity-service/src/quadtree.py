"""Quadtree for spatial indexing.

A quadtree recursively subdivides a 2-D region into four quadrants.  Each
leaf node holds at most *max_points* business locations.  When a leaf
overflows it is split into four children.  This gives efficient spatial
queries with O(log n) average lookup for well-distributed data.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field


@dataclass
class Business:
    """A business with a geographic location.

    Attributes:
        id: Unique business identifier.
        name: Human-readable name.
        lat: Latitude in degrees.
        lng: Longitude in degrees.
    """

    id: str
    name: str
    lat: float
    lng: float


@dataclass
class BoundingBox:
    """Axis-aligned bounding box (lat/lng rectangle).

    Attributes:
        min_lat: Southern boundary.
        min_lng: Western boundary.
        max_lat: Northern boundary.
        max_lng: Eastern boundary.
    """

    min_lat: float
    min_lng: float
    max_lat: float
    max_lng: float

    def contains(self, lat: float, lng: float) -> bool:
        """Check whether a point lies inside (or on the border of) this box."""
        return (
            self.min_lat <= lat <= self.max_lat
            and self.min_lng <= lng <= self.max_lng
        )

    def intersects(self, other: BoundingBox) -> bool:
        """Check whether two bounding boxes overlap."""
        return not (
            other.min_lat > self.max_lat
            or other.max_lat < self.min_lat
            or other.min_lng > self.max_lng
            or other.max_lng < self.min_lng
        )

    @property
    def center(self) -> tuple[float, float]:
        """Return the center point of the bounding box."""
        return (
            (self.min_lat + self.max_lat) / 2,
            (self.min_lng + self.max_lng) / 2,
        )


@dataclass
class QuadTreeNode:
    """A single node in the quadtree.

    Leaf nodes store business locations directly.  Internal nodes have exactly
    four children (NW, NE, SW, SE).
    """

    boundary: BoundingBox
    max_points: int
    businesses: list[Business] = field(default_factory=list)
    children: list[QuadTreeNode] | None = None  # [NW, NE, SW, SE]
    depth: int = 0

    @property
    def is_leaf(self) -> bool:
        return self.children is None

    def _subdivide(self) -> None:
        """Split this leaf into four children and redistribute points."""
        mid_lat = (self.boundary.min_lat + self.boundary.max_lat) / 2
        mid_lng = (self.boundary.min_lng + self.boundary.max_lng) / 2
        b = self.boundary
        d = self.depth + 1
        mp = self.max_points

        self.children = [
            QuadTreeNode(BoundingBox(mid_lat, b.min_lng, b.max_lat, mid_lng), mp, depth=d),  # NW
            QuadTreeNode(BoundingBox(mid_lat, mid_lng, b.max_lat, b.max_lng), mp, depth=d),  # NE
            QuadTreeNode(BoundingBox(b.min_lat, b.min_lng, mid_lat, mid_lng), mp, depth=d),  # SW
            QuadTreeNode(BoundingBox(b.min_lat, mid_lng, mid_lat, b.max_lng), mp, depth=d),  # SE
        ]

        # Redistribute existing businesses into children.
        for biz in self.businesses:
            for child in self.children:
                if child.boundary.contains(biz.lat, biz.lng):
                    child.insert(biz)
                    break
        self.businesses = []

    def insert(self, business: Business) -> None:
        """Insert a business into this subtree."""
        if not self.boundary.contains(business.lat, business.lng):
            return

        if self.is_leaf:
            self.businesses.append(business)
            # Subdivide if over capacity (limit depth to avoid infinite split
            # when many points share the exact same coordinate).
            if len(self.businesses) > self.max_points and self.depth < 20:
                self._subdivide()
        else:
            for child in self.children:  # type: ignore[union-attr]
                if child.boundary.contains(business.lat, business.lng):
                    child.insert(business)
                    return

    def query_range(self, box: BoundingBox) -> list[Business]:
        """Return all businesses within the given bounding box."""
        results: list[Business] = []
        if not self.boundary.intersects(box):
            return results

        if self.is_leaf:
            for biz in self.businesses:
                if box.contains(biz.lat, biz.lng):
                    results.append(biz)
        else:
            for child in self.children:  # type: ignore[union-attr]
                results.extend(child.query_range(box))

        return results


class QuadTree:
    """Quadtree spatial index for business locations.

    Args:
        boundary: The world bounding box (defaults to full lat/lng range).
        max_points: Maximum businesses per leaf before subdivision.
    """

    def __init__(
        self,
        boundary: BoundingBox | None = None,
        max_points: int = 4,
    ) -> None:
        if boundary is None:
            boundary = BoundingBox(-90.0, -180.0, 90.0, 180.0)
        self.root = QuadTreeNode(boundary, max_points)
        self._size = 0

    @property
    def size(self) -> int:
        """Total number of businesses in the tree."""
        return self._size

    def insert(self, business: Business) -> None:
        """Insert a business into the quadtree."""
        self.root.insert(business)
        self._size += 1

    def build(self, businesses: list[Business]) -> None:
        """Bulk-insert a list of businesses."""
        for biz in businesses:
            self.insert(biz)

    def query_range(self, box: BoundingBox) -> list[Business]:
        """Return all businesses within a bounding box."""
        return self.root.query_range(box)

    def find_nearest(
        self,
        lat: float,
        lng: float,
        k: int = 5,
        initial_radius_km: float = 1.0,
        max_radius_km: float = 50.0,
    ) -> list[tuple[Business, float]]:
        """Find the k nearest businesses to a point.

        Uses an expanding-box strategy: start with *initial_radius_km* and
        double until at least k candidates are found or *max_radius_km* is
        reached.

        Args:
            lat: Query latitude.
            lng: Query longitude.
            k: Number of nearest results desired.
            initial_radius_km: Starting search radius in km.
            max_radius_km: Maximum search radius in km.

        Returns:
            List of (Business, distance_km) sorted by distance, up to k items.
        """
        radius = initial_radius_km
        candidates: list[Business] = []

        while radius <= max_radius_km:
            box = _radius_to_box(lat, lng, radius)
            candidates = self.query_range(box)
            if len(candidates) >= k:
                break
            radius *= 2

        # If still short, try one final search at max radius.
        if len(candidates) < k and radius < max_radius_km * 2:
            box = _radius_to_box(lat, lng, max_radius_km)
            candidates = self.query_range(box)

        # Sort by actual distance and return top-k.
        with_dist = [
            (biz, haversine_km(lat, lng, biz.lat, biz.lng))
            for biz in candidates
        ]
        with_dist.sort(key=lambda x: x[1])
        return with_dist[:k]

    def count_nodes(self) -> int:
        """Count total nodes in the tree (for diagnostics)."""
        return self._count(self.root)

    def _count(self, node: QuadTreeNode) -> int:
        total = 1
        if node.children:
            for child in node.children:
                total += self._count(child)
        return total

    def max_depth(self) -> int:
        """Return the maximum depth of the tree."""
        return self._depth(self.root)

    def _depth(self, node: QuadTreeNode) -> int:
        if node.is_leaf:
            return node.depth
        return max(self._depth(c) for c in node.children)  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

_EARTH_RADIUS_KM = 6371.0


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Compute the great-circle distance between two points in kilometers."""
    lat1_r, lat2_r = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlng / 2) ** 2
    )
    return _EARTH_RADIUS_KM * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _radius_to_box(
    lat: float, lng: float, radius_km: float
) -> BoundingBox:
    """Convert a center point + radius into an approximate bounding box."""
    # 1 degree of latitude ~ 111 km
    delta_lat = radius_km / 111.0
    # 1 degree of longitude varies with latitude
    delta_lng = radius_km / (111.0 * max(math.cos(math.radians(lat)), 1e-10))

    return BoundingBox(
        min_lat=lat - delta_lat,
        min_lng=lng - delta_lng,
        max_lat=lat + delta_lat,
        max_lng=lng + delta_lng,
    )
