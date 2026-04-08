"""Social graph — follow/unfollow and follower/following queries.

Redis 데이터 구조:
  following:{user_id} — SET: 이 사용자가 팔로우하는 사용자 ID 집합
  followers:{user_id} — SET: 이 사용자를 팔로우하는 사용자 ID 집합
"""

from __future__ import annotations

from typing import Any

from redis.asyncio import Redis


async def follow(redis: Redis, follower_id: str, followee_id: str) -> dict[str, Any]:
    """follower_id 가 followee_id 를 팔로우한다.

    양방향으로 Redis Set 을 갱신한다:
      - following:{follower_id} 에 followee_id 추가
      - followers:{followee_id} 에 follower_id 추가
    """
    if follower_id == followee_id:
        return {"status": "error", "message": "Cannot follow yourself"}

    # 이미 팔로우 중인지 확인
    already = await redis.sismember(f"following:{follower_id}", followee_id)
    if already:
        return {"status": "already_following", "message": f"{follower_id} already follows {followee_id}"}

    pipe = redis.pipeline()
    pipe.sadd(f"following:{follower_id}", followee_id)
    pipe.sadd(f"followers:{followee_id}", follower_id)
    await pipe.execute()

    return {"status": "ok", "follower": follower_id, "followee": followee_id}


async def unfollow(redis: Redis, follower_id: str, followee_id: str) -> dict[str, Any]:
    """follower_id 가 followee_id 를 언팔로우한다.

    양방향으로 Redis Set 을 갱신한다:
      - following:{follower_id} 에서 followee_id 제거
      - followers:{followee_id} 에서 follower_id 제거
    """
    removed = await redis.srem(f"following:{follower_id}", followee_id)
    if not removed:
        return {"status": "not_following", "message": f"{follower_id} does not follow {followee_id}"}

    await redis.srem(f"followers:{followee_id}", follower_id)

    return {"status": "ok", "follower": follower_id, "followee": followee_id}


async def get_followers(redis: Redis, user_id: str) -> list[str]:
    """user_id 를 팔로우하는 사용자 목록을 반환한다."""
    members = await redis.smembers(f"followers:{user_id}")
    return sorted(members)


async def get_following(redis: Redis, user_id: str) -> list[str]:
    """user_id 가 팔로우하는 사용자 목록을 반환한다."""
    members = await redis.smembers(f"following:{user_id}")
    return sorted(members)
