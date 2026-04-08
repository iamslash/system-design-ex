"""Sliding Window Counter rate limiting algorithm backed by Redis sorted sets."""

from __future__ import annotations

import math
import time
import uuid

import redis.asyncio as aioredis

from algorithms import RateLimitResult

# Lua script executed atomically in Redis.
# Keys: [sorted_set_key]
# Args: [window_size, max_requests, now, unique_member]
# Returns: [allowed (0/1), remaining_requests]
_LUA_SCRIPT = """
local key = KEYS[1]
local window_size = tonumber(ARGV[1])
local max_requests = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local member = ARGV[4]

-- Remove entries outside the current window
local window_start = now - window_size
redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

-- Count current requests in window
local current_count = redis.call('ZCARD', key)

if current_count < max_requests then
    -- Add the new request with the current timestamp as score
    redis.call('ZADD', key, now, member)
    redis.call('EXPIRE', key, window_size + 1)
    local remaining = max_requests - current_count - 1
    return {1, remaining}
else
    redis.call('EXPIRE', key, window_size + 1)
    return {0, 0}
end
"""


class SlidingWindowCounter:
    """Sliding Window Counter using Redis sorted sets.

    Each request is stored as a member in a sorted set with the timestamp
    as the score.  Expired entries (outside the window) are pruned on
    every check.
    """

    def __init__(
        self,
        redis_client: aioredis.Redis,
        max_requests: int = 10,
        window_size: int = 60,
    ) -> None:
        self._redis = redis_client
        self._max_requests = max_requests
        self._window_size = window_size
        self._script: object | None = None

    async def _get_script(self) -> object:
        if self._script is None:
            self._script = self._redis.register_script(_LUA_SCRIPT)
        return self._script

    async def is_allowed(self, client_id: str) -> RateLimitResult:
        """Check whether *client_id* is allowed to make a request."""
        key = f"rate_limit:sliding_window:{client_id}"
        now = time.time()
        member = f"{now}:{uuid.uuid4().hex[:8]}"

        script = await self._get_script()
        allowed_int, remaining = await script(keys=[key], args=[self._window_size, self._max_requests, now, member])  # type: ignore[operator]

        allowed = bool(allowed_int)
        retry_after = 0
        if not allowed:
            # Estimate time until the oldest entry expires from the window
            oldest_score = await self._redis.zrange(key, 0, 0, withscores=True)
            if oldest_score:
                _, oldest_ts = oldest_score[0]
                retry_after = math.ceil((float(oldest_ts) + self._window_size) - now)
                retry_after = max(retry_after, 1)
            else:
                retry_after = self._window_size

        return RateLimitResult(
            allowed=allowed,
            limit=self._max_requests,
            remaining=int(remaining),
            retry_after=retry_after,
        )
