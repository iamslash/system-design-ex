"""Per-user, per-channel notification rate limiter.

Uses a Redis Sorted Set to enforce sliding-window rate limits
per user per channel.
Default limits: push 10/h, sms 5/h, email 20/h (configurable via environment variables).
"""

from __future__ import annotations

import time

from redis.asyncio import Redis

from config import settings

# Maximum sends per hour per channel
RATE_LIMITS: dict[str, int] = {
    "push": settings.RATE_LIMIT_PUSH,
    "sms": settings.RATE_LIMIT_SMS,
    "email": settings.RATE_LIMIT_EMAIL,
}

# Window size in seconds — 1 hour
WINDOW_SIZE: int = 3600


# Lua script: atomically cleans up the window, checks the count, and inserts
_LUA_RATE_LIMIT = """
local key = KEYS[1]
local window_size = tonumber(ARGV[1])
local max_count = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local member = ARGV[4]

-- Remove stale records outside the window
local window_start = now - window_size
redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

-- Get the count within the current window
local current_count = redis.call('ZCARD', key)

if current_count >= max_count then
    return 0
end

-- Allowed: record the current timestamp
redis.call('ZADD', key, now, member)
redis.call('EXPIRE', key, window_size + 1)
return 1
"""

_script_cache: dict[int, object] = {}


async def check_rate_limit(
    redis: Redis,
    user_id: str,
    channel: str,
    window_size: int = WINDOW_SIZE,
    limits: dict[str, int] | None = None,
) -> bool:
    """Check the per-channel rate limit for a user.

    Uses a Lua script to atomically clean up the window, check the count, and insert,
    ensuring accurate rate limiting even under concurrent requests.

    Args:
        redis: Redis client.
        user_id: User ID.
        channel: Notification channel (push/sms/email).
        window_size: Sliding window size in seconds.
        limits: Per-channel max send count override (for testing).

    Returns:
        True if the send is allowed, False if the rate limit is exceeded.
    """
    effective_limits = limits or RATE_LIMITS
    max_count = effective_limits.get(channel, 10)
    key = f"rate_limit:{user_id}:{channel}"
    now = time.time()
    member = f"{now}:{user_id}:{channel}"

    client_id = id(redis)
    if client_id not in _script_cache:
        _script_cache[client_id] = redis.register_script(_LUA_RATE_LIMIT)

    result = await _script_cache[client_id](keys=[key], args=[window_size, max_count, now, member])
    return bool(result)
