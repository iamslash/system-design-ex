"""Token Bucket rate limiting algorithm backed by Redis."""

from __future__ import annotations

import math
import time

import redis.asyncio as aioredis

from algorithms import RateLimitResult

# Lua script executed atomically in Redis.
# Keys: [bucket_key]
# Args: [bucket_size, refill_rate, now]
# Returns: [allowed (0/1), tokens_remaining]
_LUA_SCRIPT = """
local key = KEYS[1]
local bucket_size = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])

if tokens == nil then
    tokens = bucket_size
    last_refill = now
end

-- Refill tokens based on elapsed time
local elapsed = now - last_refill
local refill = elapsed * refill_rate
tokens = math.min(bucket_size, tokens + refill)
last_refill = now

-- Try to consume one token
if tokens >= 1 then
    tokens = tokens - 1
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', last_refill)
    redis.call('EXPIRE', key, math.ceil(bucket_size / refill_rate) + 1)
    return {1, math.floor(tokens)}
else
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', last_refill)
    redis.call('EXPIRE', key, math.ceil(bucket_size / refill_rate) + 1)
    return {0, 0}
end
"""


class TokenBucket:
    """Token Bucket algorithm using Redis for distributed state.

    Each client IP gets a bucket that holds up to ``bucket_size`` tokens.
    Tokens are refilled at ``refill_rate`` tokens per second.  Each
    allowed request consumes one token.
    """

    def __init__(
        self,
        redis_client: aioredis.Redis,
        bucket_size: int = 10,
        refill_rate: float = 1.0,
    ) -> None:
        self._redis = redis_client
        self._bucket_size = bucket_size
        self._refill_rate = refill_rate
        self._script: object | None = None

    async def _get_script(self) -> object:
        if self._script is None:
            self._script = self._redis.register_script(_LUA_SCRIPT)
        return self._script

    async def is_allowed(self, client_id: str) -> RateLimitResult:
        """Check whether *client_id* is allowed to make a request."""
        key = f"rate_limit:token_bucket:{client_id}"
        now = time.time()

        script = await self._get_script()
        allowed_int, remaining = await script(keys=[key], args=[self._bucket_size, self._refill_rate, now])  # type: ignore[operator]

        allowed = bool(allowed_int)
        retry_after = 0
        if not allowed:
            retry_after = math.ceil(1.0 / self._refill_rate)

        return RateLimitResult(
            allowed=allowed,
            limit=self._bucket_size,
            remaining=int(remaining),
            retry_after=retry_after,
        )
