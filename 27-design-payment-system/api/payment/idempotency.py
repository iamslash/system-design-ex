"""Idempotency key handling using Redis.

Prevents duplicate payment processing by storing idempotency keys with
their associated payment IDs. Each key has a TTL (default 24h) to avoid
unbounded storage growth.
"""

from __future__ import annotations

import json

import redis.asyncio as aioredis


class IdempotencyStore:
    """Manages idempotency keys in Redis."""

    PREFIX = "idempotency:"
    TTL_SECONDS = 86400  # 24 hours

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    def _key(self, idempotency_key: str) -> str:
        return f"{self.PREFIX}{idempotency_key}"

    async def check_and_set(
        self, idempotency_key: str, payment_data: dict
    ) -> dict | None:
        """Check if an idempotency key exists.

        Returns:
            None if the key is new (and sets it).
            The stored payment data if the key already exists.
        """
        rkey = self._key(idempotency_key)
        existing = await self._redis.get(rkey)
        if existing is not None:
            return json.loads(existing)
        # Set the key with TTL — NX ensures atomicity
        was_set = await self._redis.set(
            rkey, json.dumps(payment_data), ex=self.TTL_SECONDS, nx=True
        )
        if not was_set:
            # Another request set it between our GET and SET
            existing = await self._redis.get(rkey)
            return json.loads(existing) if existing else None
        return None

    async def update(self, idempotency_key: str, payment_data: dict) -> None:
        """Update the stored data for an existing idempotency key."""
        rkey = self._key(idempotency_key)
        ttl = await self._redis.ttl(rkey)
        if ttl < 0:
            ttl = self.TTL_SECONDS
        await self._redis.set(rkey, json.dumps(payment_data), ex=ttl)

    async def get(self, idempotency_key: str) -> dict | None:
        """Retrieve stored data for an idempotency key."""
        rkey = self._key(idempotency_key)
        data = await self._redis.get(rkey)
        return json.loads(data) if data else None

    async def delete(self, idempotency_key: str) -> None:
        """Remove an idempotency key (used in tests)."""
        await self._redis.delete(self._key(idempotency_key))
