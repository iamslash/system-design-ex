"""Post creation and fanout triggering.

포스트를 생성하고 Redis 에 저장한 뒤, fanout 을 통해
모든 팔로워의 피드에 push 한다.

Redis 데이터 구조:
  post:{post_id} — HASH: {user_id, content, created_at, likes}
  user:{user_id} — HASH: {name, created_at}
"""

from __future__ import annotations

import threading
import time
from typing import Any

from redis.asyncio import Redis

from feed.fanout import fanout_to_followers

# 동일 밀리초 내 고유성을 보장하기 위한 시퀀스 카운터
_counter_lock = threading.Lock()
_last_ts: int = 0
_sequence: int = 0


def _generate_post_id() -> str:
    """타임스탬프 기반 포스트 ID 를 생성한다 (Snowflake-like).

    밀리초 단위 타임스탬프 + 시퀀스 번호로 고유성을 보장한다.
    같은 밀리초에 여러 포스트가 생성되면 시퀀스 번호가 증가한다.
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
    """포스트를 생성하고 팔로워 피드에 fanout 한다.

    처리 흐름:
      1. 타임스탬프 기반 post_id 생성
      2. post:{post_id} 해시에 포스트 데이터 저장
      3. fanout_to_followers 호출 → 팔로워 피드에 push
    """
    post_id = _generate_post_id()
    # post_id 자체를 score 로 사용하여 정렬 순서를 보장한다
    created_at = float(post_id)

    # 포스트 데이터를 Redis Hash 에 저장
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

    # 작성자 본인의 피드에도 추가
    await redis.zadd(f"feed:{user_id}", {post_id: created_at})

    # 팔로워 피드에 fanout
    follower_count = await fanout_to_followers(redis, user_id, post_id, created_at)

    return {
        "post_id": post_id,
        "user_id": user_id,
        "content": content,
        "created_at": created_at,
        "fanout_count": follower_count,
    }


async def get_post(redis: Redis, post_id: str) -> dict[str, str] | None:
    """post_id 로 포스트 데이터를 조회한다."""
    data = await redis.hgetall(f"post:{post_id}")
    return data if data else None
