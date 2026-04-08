"""News feed retrieval and hydration.

사용자의 피드를 조회하고, 각 포스트와 작성자 정보를 hydrate 하여
완전한 피드 항목을 반환한다.

Redis 데이터 구조:
  feed:{user_id} — ZSET: score=timestamp, member=post_id
  post:{post_id} — HASH: {user_id, content, created_at, likes}
  user:{user_id} — HASH: {name, created_at}
"""

from __future__ import annotations

from typing import Any

from redis.asyncio import Redis


async def get_feed(
    redis: Redis,
    user_id: str,
    offset: int = 0,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """사용자의 뉴스 피드를 역시간순으로 조회한다.

    처리 흐름:
      1. feed:{user_id} Sorted Set 에서 ZREVRANGE 로 post_id 목록 조회
      2. 각 post_id 로 post:{post_id} 해시에서 포스트 데이터 조회 (hydration)
      3. 각 포스트의 user_id 로 user:{user_id} 해시에서 작성자 정보 조회
      4. 포스트 + 작성자 정보를 합쳐 반환
    """
    feed_key = f"feed:{user_id}"

    # 1. 피드에서 post_id 목록을 역시간순으로 조회
    post_ids: list[str] = await redis.zrevrange(feed_key, offset, offset + limit - 1)

    if not post_ids:
        return []

    # 2. 각 포스트 데이터를 파이프라인으로 일괄 조회
    pipe = redis.pipeline()
    for post_id in post_ids:
        pipe.hgetall(f"post:{post_id}")
    post_results = await pipe.execute()

    # 3. 작성자 정보를 일괄 조회 (중복 제거)
    author_ids: set[str] = set()
    posts: list[dict[str, str]] = []
    for post_data in post_results:
        if post_data:
            posts.append(post_data)
            author_ids.add(post_data.get("user_id", ""))

    # 작성자 정보 파이프라인 조회
    author_map: dict[str, dict[str, str]] = {}
    if author_ids:
        pipe = redis.pipeline()
        ordered_ids = list(author_ids)
        for author_id in ordered_ids:
            pipe.hgetall(f"user:{author_id}")
        author_results = await pipe.execute()
        for aid, adata in zip(ordered_ids, author_results):
            if adata:
                author_map[aid] = adata

    # 4. 포스트 + 작성자 정보를 합쳐 반환
    feed_items: list[dict[str, Any]] = []
    for post in posts:
        author_id = post.get("user_id", "")
        author_info = author_map.get(author_id, {})
        feed_items.append({
            "post_id": post.get("post_id", ""),
            "user_id": author_id,
            "author_name": author_info.get("name", author_id),
            "content": post.get("content", ""),
            "created_at": post.get("created_at", ""),
            "likes": int(post.get("likes", "0")),
        })

    return feed_items
