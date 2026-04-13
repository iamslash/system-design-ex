"""Fanout on write — push post to all followers' feed caches.

When a post is created, looks up the author's follower list and
adds post_id to each follower's feed:{user_id} Sorted Set.

Redis data structures:
  feed:{user_id}      — ZSET: score=timestamp, member=post_id
  followers:{user_id} — SET:  set of user IDs that follow this user
"""

from __future__ import annotations

import logging

from redis.asyncio import Redis

from config import settings

logger = logging.getLogger(__name__)


async def fanout_to_followers(
    redis: Redis,
    author_id: str,
    post_id: str,
    timestamp: float,
) -> int:
    """Push post_id to the feeds of all the author's followers.

    Processing flow:
      1. Fetch the follower list from followers:{author_id}
      2. ZADD post_id to each follower's feed:{follower_id} Sorted Set
      3. If the feed size exceeds FEED_MAX_SIZE, remove the oldest entries (ZREMRANGEBYRANK)
    """
    # Fetch the follower list
    followers = await redis.smembers(f"followers:{author_id}")

    if not followers:
        return 0

    # Push the post to each follower's feed
    pipe = redis.pipeline()
    for follower_id in followers:
        feed_key = f"feed:{follower_id}"
        pipe.zadd(feed_key, {post_id: timestamp})
        # Enforce feed size limit — remove oldest entries first
        pipe.zremrangebyrank(feed_key, 0, -(settings.FEED_MAX_SIZE + 1))
    await pipe.execute()

    logger.info(
        "Fanout post %s from %s to %d followers",
        post_id, author_id, len(followers),
    )

    return len(followers)
