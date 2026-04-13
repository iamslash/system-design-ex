"""Social graph — follow/unfollow and follower/following queries.

Redis data structures:
  following:{user_id} — SET: set of user IDs this user follows
  followers:{user_id} — SET: set of user IDs that follow this user
"""

from __future__ import annotations

from typing import Any

from redis.asyncio import Redis


async def follow(redis: Redis, follower_id: str, followee_id: str) -> dict[str, Any]:
    """follower_id follows followee_id.

    Updates Redis Sets bidirectionally:
      - Adds followee_id to following:{follower_id}
      - Adds follower_id to followers:{followee_id}
    """
    if follower_id == followee_id:
        return {"status": "error", "message": "Cannot follow yourself"}

    # Check if already following
    already = await redis.sismember(f"following:{follower_id}", followee_id)
    if already:
        return {"status": "already_following", "message": f"{follower_id} already follows {followee_id}"}

    pipe = redis.pipeline()
    pipe.sadd(f"following:{follower_id}", followee_id)
    pipe.sadd(f"followers:{followee_id}", follower_id)
    await pipe.execute()

    return {"status": "ok", "follower": follower_id, "followee": followee_id}


async def unfollow(redis: Redis, follower_id: str, followee_id: str) -> dict[str, Any]:
    """follower_id unfollows followee_id.

    Updates Redis Sets bidirectionally:
      - Removes followee_id from following:{follower_id}
      - Removes follower_id from followers:{followee_id}
    """
    removed = await redis.srem(f"following:{follower_id}", followee_id)
    if not removed:
        return {"status": "not_following", "message": f"{follower_id} does not follow {followee_id}"}

    await redis.srem(f"followers:{followee_id}", follower_id)

    return {"status": "ok", "follower": follower_id, "followee": followee_id}


async def get_followers(redis: Redis, user_id: str) -> list[str]:
    """Return the list of users who follow user_id."""
    members = await redis.smembers(f"followers:{user_id}")
    return sorted(members)


async def get_following(redis: Redis, user_id: str) -> list[str]:
    """Return the list of users that user_id follows."""
    members = await redis.smembers(f"following:{user_id}")
    return sorted(members)
