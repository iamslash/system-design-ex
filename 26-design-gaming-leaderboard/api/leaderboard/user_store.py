"""User profile storage using Redis hashes.

Each user profile is stored in a Redis hash at key ``user:{user_id}`` with
fields ``display_name`` and ``created_at``.  This keeps profile data separate
from leaderboard sorted-set scores so that both can scale independently.
"""

from __future__ import annotations

from datetime import datetime, timezone

from redis.asyncio import Redis


class UserStore:
    """CRUD operations for user profiles backed by Redis hashes."""

    _KEY_PREFIX = "user"

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    def _key(self, user_id: str) -> str:
        return f"{self._KEY_PREFIX}:{user_id}"

    async def upsert(self, user_id: str, display_name: str | None = None) -> dict[str, str]:
        """Create or update a user profile.

        If *display_name* is ``None`` the field is left unchanged for existing
        users and defaults to *user_id* for new users.
        """
        key = self._key(user_id)
        exists = await self._redis.exists(key)

        if not exists:
            now = datetime.now(timezone.utc).isoformat()
            await self._redis.hset(key, mapping={
                "user_id": user_id,
                "display_name": display_name or user_id,
                "created_at": now,
            })
        elif display_name is not None:
            await self._redis.hset(key, "display_name", display_name)

        return await self.get(user_id)  # type: ignore[return-value]

    async def get(self, user_id: str) -> dict[str, str] | None:
        """Return the user profile or ``None`` if not found."""
        data = await self._redis.hgetall(self._key(user_id))
        return data if data else None
