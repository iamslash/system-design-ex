"""Counter-based ID generator using Redis INCR.

Implements an atomic auto-increment counter using Redis INCR.
The generated ID is encoded in Base62 and used as the short code.
"""

from __future__ import annotations

from redis.asyncio import Redis

# Redis counter key
COUNTER_KEY = "url:id_counter"

# Start value (starting from 100 million ensures at least 5 characters in the code)
START_VALUE = 100_000_000


async def next_id(redis: Redis) -> int:
    """Generate the next unique ID.

    Uses Redis INCR to atomically increment the counter.
    On the first call, starts from START_VALUE.

    Args:
        redis: Redis client.

    Returns:
        A new unique integer ID.
    """
    # Initialize to START_VALUE if the counter does not exist
    exists = await redis.exists(COUNTER_KEY)
    if not exists:
        await redis.set(COUNTER_KEY, START_VALUE)

    return await redis.incr(COUNTER_KEY)
