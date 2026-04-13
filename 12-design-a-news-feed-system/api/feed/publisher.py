"""Post creation and fanout triggering.

Creates a post, stores it in Redis, then pushes it to all followers'
feeds via fanout.

Redis data structures:
  post:{post_id} — HASH: {user_id, content, created_at, likes}
  user:{user_id} — HASH: {name, created_at}
"""

from __future__ import annotations

import threading
import time
from typing import Any

from redis.asyncio import Redis

from feed.fanout import fanout_to_followers

# Sequence counter to guarantee uniqueness within the same millisecond
_counter_lock = threading.Lock()
_last_ts: int = 0
_sequence: int = 0


def _generate_post_id() -> str:
    """Generate a timestamp-based post ID (Snowflake-like).

    Uses millisecond-precision timestamp + sequence number to guarantee uniqueness.
    If multiple posts are created within the same millisecond, the sequence number increments.
    """
    global _last_ts, _sequence
    with _counter_lock:
        ts = int(time.time() * 1000)
        if ts == _last_ts:
            _sequence += 1
        else:
            _last_ts = ts
            _sequence = 0
        return f"{ts}{_sequence:04d}"


async def create_post(
    redis: Redis,
    user_id: str,
    content: str,
) -> dict[str, Any]:
    """Create a post and fanout to follower feeds.

    Processing flow:
      1. Generate a timestamp-based post_id
      2. Store post data in the post:{post_id} hash
      3. Call fanout_to_followers to push to follower feeds
    """
    post_id = _generate_post_id()
    # Use post_id itself as the score to guarantee sort order
    created_at = float(post_id)

    # Store post data in a Redis Hash
    await redis.hset(
        f"post:{post_id}",
        mapping={
            "post_id": post_id,
            "user_id": user_id,
            "content": content,
            "created_at": str(created_at),
            "likes": "0",
        },
    )

    # Also add to the author's own feed
    await redis.zadd(f"feed:{user_id}", {post_id: created_at})

    # Fanout to follower feeds
    follower_count = await fanout_to_followers(redis, user_id, post_id, created_at)

    return {
        "post_id": post_id,
        "user_id": user_id,
        "content": content,
        "created_at": created_at,
        "fanout_count": follower_count,
    }


async def get_post(redis: Redis, post_id: str) -> dict[str, str] | None:
    """Retrieve post data by post_id."""
    data = await redis.hgetall(f"post:{post_id}")
    return data if data else None
