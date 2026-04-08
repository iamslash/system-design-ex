"""Nearby friend computation using the Haversine formula.

The Haversine formula calculates the great-circle distance between two
points on a sphere given their latitude and longitude in decimal degrees.

    a = sin^2(dlat/2) + cos(lat1) * cos(lat2) * sin^2(dlon/2)
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    d = R * c

where R is the Earth's radius (3958.8 miles / 6371 km).
"""

from __future__ import annotations

import math

import redis.asyncio as aioredis

from config import settings
from location.tracker import LocationTracker

# Earth's radius in miles
EARTH_RADIUS_MILES = 3958.8


def haversine_distance(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> float:
    """Calculate the great-circle distance between two points in miles.

    Parameters are in decimal degrees.
    """
    lat1_r, lon1_r = math.radians(lat1), math.radians(lon1)
    lat2_r, lon2_r = math.radians(lat2), math.radians(lon2)

    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return EARTH_RADIUS_MILES * c


class NearbyFinder:
    """Finds friends within a given radius of a user's current location."""

    def __init__(self, redis: aioredis.Redis, tracker: LocationTracker) -> None:
        self._redis = redis
        self._tracker = tracker

    async def get_friends(self, user_id: str) -> list[str]:
        """Return the friend list for a user from Redis set."""
        return list(await self._redis.smembers(f"friends:{user_id}"))

    async def add_friendship(self, user_a: str, user_b: str) -> None:
        """Create a bidirectional friendship."""
        await self._redis.sadd(f"friends:{user_a}", user_b)
        await self._redis.sadd(f"friends:{user_b}", user_a)

    async def remove_friendship(self, user_a: str, user_b: str) -> None:
        """Remove a bidirectional friendship."""
        await self._redis.srem(f"friends:{user_a}", user_b)
        await self._redis.srem(f"friends:{user_b}", user_a)

    async def find_nearby(
        self, user_id: str, radius_miles: float | None = None
    ) -> list[dict]:
        """Find all friends within *radius_miles* of *user_id*.

        Returns a list of dicts sorted by distance (nearest first), each
        containing user_id, latitude, longitude, distance_miles, timestamp.

        Friends whose location cache has expired (TTL elapsed) are excluded.
        """
        radius = radius_miles if radius_miles is not None else settings.NEARBY_RADIUS_MILES

        my_loc = await self._tracker.get(user_id)
        if my_loc is None:
            return []

        friends = await self.get_friends(user_id)
        if not friends:
            return []

        nearby: list[dict] = []
        for friend_id in friends:
            friend_loc = await self._tracker.get(friend_id)
            if friend_loc is None:
                continue  # Location expired or never set

            dist = haversine_distance(
                my_loc["latitude"],
                my_loc["longitude"],
                friend_loc["latitude"],
                friend_loc["longitude"],
            )
            if dist <= radius:
                nearby.append({
                    "user_id": friend_id,
                    "latitude": friend_loc["latitude"],
                    "longitude": friend_loc["longitude"],
                    "distance_miles": round(dist, 4),
                    "timestamp": friend_loc["timestamp"],
                })

        nearby.sort(key=lambda x: x["distance_miles"])
        return nearby
