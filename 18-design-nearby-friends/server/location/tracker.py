"""Location update and cache with TTL.

Each user's latest location is stored in Redis as a hash with a TTL.
When the TTL expires the user is considered inactive and their location
is automatically evicted from the cache.

Redis keys:
    location:{user_id} -> {latitude, longitude, timestamp}   (TTL = LOCATION_TTL)
"""

from __future__ import annotations

import time

import redis.asyncio as aioredis

from config import settings


class LocationTracker:
    """Manages real-time user location updates with automatic TTL expiry."""

    def __init__(self, redis: aioredis.Redis, ttl: int | None = None) -> None:
        self._redis = redis
        self._ttl = ttl if ttl is not None else settings.LOCATION_TTL

    async def update(self, user_id: str, latitude: float, longitude: float) -> dict:
        """Store or refresh a user's location with TTL.

        Returns the stored location dict.
        """
        now = time.time()
        key = f"location:{user_id}"
        mapping = {
            "latitude": str(latitude),
            "longitude": str(longitude),
            "timestamp": str(now),
        }
        await self._redis.hset(key, mapping=mapping)
        await self._redis.expire(key, self._ttl)
        return {
            "user_id": user_id,
            "latitude": latitude,
            "longitude": longitude,
            "timestamp": now,
        }

    async def get(self, user_id: str) -> dict | None:
        """Retrieve a user's cached location.

        Returns None if the key has expired or was never set.
        """
        key = f"location:{user_id}"
        data = await self._redis.hgetall(key)
        if not data:
            return None
        return {
            "user_id": user_id,
            "latitude": float(data["latitude"]),
            "longitude": float(data["longitude"]),
            "timestamp": float(data["timestamp"]),
        }

    async def remove(self, user_id: str) -> None:
        """Explicitly remove a user's location from the cache."""
        await self._redis.delete(f"location:{user_id}")

    async def get_ttl(self, user_id: str) -> int:
        """Return remaining TTL in seconds (-2 if key missing, -1 if no TTL)."""
        return await self._redis.ttl(f"location:{user_id}")
