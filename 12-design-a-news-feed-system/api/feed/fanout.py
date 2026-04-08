"""Fanout on write — push post to all followers' feed caches.

포스트가 생성되면 작성자의 팔로워 목록을 조회하고,
각 팔로워의 feed:{user_id} Sorted Set 에 post_id 를 추가한다.

Redis 데이터 구조:
  feed:{user_id}      — ZSET: score=timestamp, member=post_id
  followers:{user_id} — SET:  이 사용자를 팔로우하는 사용자 ID 집합
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
    """작성자의 모든 팔로워 피드에 post_id 를 push 한다.

    처리 흐름:
      1. followers:{author_id} 에서 팔로워 목록 조회
      2. 각 팔로워의 feed:{follower_id} Sorted Set 에 ZADD
      3. 피드 크기가 FEED_MAX_SIZE 를 초과하면 오래된 항목 제거 (ZREMRANGEBYRANK)
    """
    # 팔로워 목록 조회
    followers = await redis.smembers(f"followers:{author_id}")

    if not followers:
        return 0

    # 각 팔로워의 피드에 포스트 push
    pipe = redis.pipeline()
    for follower_id in followers:
        feed_key = f"feed:{follower_id}"
        pipe.zadd(feed_key, {post_id: timestamp})
        # 피드 크기 제한 — 가장 오래된 항목부터 제거
        pipe.zremrangebyrank(feed_key, 0, -(settings.FEED_MAX_SIZE + 1))
    await pipe.execute()

    logger.info(
        "Fanout post %s from %s to %d followers",
        post_id, author_id, len(followers),
    )

    return len(followers)
