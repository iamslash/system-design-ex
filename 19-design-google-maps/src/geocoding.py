"""Simple geocoding service -- name to lat/lng lookup.

In a real system this would hit an external geocoding API or a spatial
index.  Here we use a plain dictionary for demonstration purposes.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Location:
    """A named geographic location."""

    name: str
    lat: float
    lng: float


class GeocodingService:
    """Dict-based geocoding: name -> coordinates."""

    def __init__(self) -> None:
        self._forward: dict[str, Location] = {}  # normalised name -> Location
        self._reverse: dict[tuple[float, float], Location] = {}

    def register(self, name: str, lat: float, lng: float) -> Location:
        """Register a named location."""
        loc = Location(name=name, lat=lat, lng=lng)
        self._forward[name.lower()] = loc
        self._reverse[(lat, lng)] = loc
        return loc

    def geocode(self, name: str) -> Location | None:
        """Forward geocode: name -> Location."""
        return self._forward.get(name.lower())

    def reverse_geocode(self, lat: float, lng: float, tolerance: float = 0.001) -> Location | None:
        """Reverse geocode: lat/lng -> nearest Location within tolerance."""
        # Exact match first
        exact = self._reverse.get((lat, lng))
        if exact is not None:
            return exact

        # Nearest within tolerance
        best: Location | None = None
        best_dist = float("inf")
        for (rlat, rlng), loc in self._reverse.items():
            d = abs(rlat - lat) + abs(rlng - lng)  # Manhattan for speed
            if d < best_dist and d <= tolerance:
                best_dist = d
                best = loc
        return best

    def search(self, query: str) -> list[Location]:
        """Search for locations whose name contains the query string."""
        q = query.lower()
        return [loc for key, loc in self._forward.items() if q in key]

    @property
    def location_count(self) -> int:
        return len(self._forward)

    def all_locations(self) -> list[Location]:
        return list(self._forward.values())
