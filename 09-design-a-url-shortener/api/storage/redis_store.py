"""Redis-based URL storage.

Stores and retrieves short URL mappings in Redis.

Key structure:
  - url:short:{code}  — hash: {long_url, created_at, clicks}
  - url:long:{hash}   — dedup: long URL hash -> short code
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass

from redis.asyncio import Redis


# Redis key prefixes
SHORT_PREFIX = "url:short:"
LONG_PREFIX = "url:long:"


@dataclass
class URLEntry:
    """A stored URL entry."""

    short_code: str
    long_url: str
    created_at: float
    clicks: int


class RedisURLStore:
    """Redis-backed URL store."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def save(self, short_code: str, long_url: str) -> URLEntry:
        """Save a short URL mapping.

        Uses SETNX to atomically set the reverse mapping.
        Even if concurrent requests shorten the same long URL, only one
        succeeds and the rest receive the existing code.

        Args:
            short_code: The short code.
            long_url: The original URL.

        Returns:
            The saved URLEntry.
        """
        now = time.time()

        # Atomic dedup: secure the reverse mapping first with SETNX
        long_hash = self._hash_url(long_url)
        long_key = f"{LONG_PREFIX}{long_hash}"
        was_set = await self._redis.setnx(long_key, short_code)

        if not was_set:
            # Another request saved first -> use the existing code
            existing_code = await self._redis.get(long_key)
            if existing_code and existing_code != short_code:
                existing = await self.get_by_code(existing_code)
                if existing:
                    return existing

        # Save forward mapping: short_code -> long_url
        short_key = f"{SHORT_PREFIX}{short_code}"
        await self._redis.hset(
            short_key,
            mapping={
                "long_url": long_url,
                "created_at": str(now),
                "clicks": "0",
            },
        )

        return URLEntry(
            short_code=short_code,
            long_url=long_url,
            created_at=now,
            clicks=0,
        )

    async def get_by_code(self, short_code: str) -> URLEntry | None:
        """Retrieve a URL entry by short code.

        Args:
            short_code: The short code.

        Returns:
            A URLEntry, or None if it does not exist.
        """
        short_key = f"{SHORT_PREFIX}{short_code}"
        data = await self._redis.hgetall(short_key)

        if not data:
            return None

        return URLEntry(
            short_code=short_code,
            long_url=data["long_url"],
            created_at=float(data["created_at"]),
            clicks=int(data["clicks"]),
        )

    async def get_code_by_long_url(self, long_url: str) -> str | None:
        """Look up an existing short code for the given long URL (dedup).

        Args:
            long_url: The original URL.

        Returns:
            The existing short code, or None if not found.
        """
        long_hash = self._hash_url(long_url)
        code = await self._redis.get(f"{LONG_PREFIX}{long_hash}")
        return code

    async def increment_clicks(self, short_code: str) -> int:
        """Increment the click count by 1.

        Args:
            short_code: The short code.

        Returns:
            The incremented click count.
        """
        short_key = f"{SHORT_PREFIX}{short_code}"
        return await self._redis.hincrby(short_key, "clicks", 1)

    async def code_exists(self, short_code: str) -> bool:
        """Check whether a short code exists.

        Args:
            short_code: The short code.

        Returns:
            True if it exists.
        """
        return await self._redis.exists(f"{SHORT_PREFIX}{short_code}") > 0

    @staticmethod
    def _hash_url(url: str) -> str:
        """Return the SHA-256 hash of the URL (used for dedup lookup)."""
        return hashlib.sha256(url.encode("utf-8")).hexdigest()
