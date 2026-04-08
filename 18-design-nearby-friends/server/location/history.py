"""Location history storage.

Stores a time-series of location updates per user in a Redis sorted set.
Score is the UNIX timestamp so entries are naturally ordered chronologically.

Redis key:
    location_history:{user_id}  (Sorted Set, score=timestamp, member=JSON)
"""

from __future__ import annotations

import json

import redis.asyncio as aioredis


class LocationHistory:
    """Append-only location history backed by a Redis sorted set."""

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    async def append(
        self, user_id: str, latitude: float, longitude: float, timestamp: float
    ) -> None:
        """Record a location data point."""
        key = f"location_history:{user_id}"
        entry = json.dumps({
            "latitude": latitude,
            "longitude": longitude,
            "timestamp": timestamp,
        })
        await self._redis.zadd(key, {entry: timestamp})

    async def get_range(
        self,
        user_id: str,
        start: float = 0,
        end: float = float("inf"),
        limit: int = 100,
    ) -> list[dict]:
        """Retrieve location history within a time range.

        Returns entries ordered oldest-first, capped by *limit*.
        """
        key = f"location_history:{user_id}"
        # Use +inf string representation for Redis
        end_str = "+inf" if end == float("inf") else str(end)
        raw = await self._redis.zrangebyscore(
            key, min=str(start), max=end_str, start=0, num=limit
        )
        return [json.loads(entry) for entry in raw]

    async def count(self, user_id: str) -> int:
        """Return the total number of location history entries for a user."""
        return await self._redis.zcard(f"location_history:{user_id}")
