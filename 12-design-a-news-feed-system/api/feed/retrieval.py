"""News feed retrieval and hydration.

Retrieves a user's feed, hydrates each post with author information,
and returns complete feed items.

Redis data structures:
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
    """Retrieve a user's news feed in reverse chronological order.

    Processing flow:
      1. Fetch post_id list from feed:{user_id} Sorted Set using ZREVRANGE
      2. Fetch post data for each post_id from the post:{post_id} hash (hydration)
      3. Fetch author information for each post from the user:{user_id} hash
      4. Merge post data with author information and return
    """
    feed_key = f"feed:{user_id}"

    # 1. Fetch post_id list from the feed in reverse chronological order
    post_ids: list[str] = await redis.zrevrange(feed_key, offset, offset + limit - 1)

    if not post_ids:
        return []

    # 2. Batch-fetch each post's data via pipeline
    pipe = redis.pipeline()
    for post_id in post_ids:
        pipe.hgetall(f"post:{post_id}")
    post_results = await pipe.execute()

    # 3. Batch-fetch author information (deduplicated)
    author_ids: set[str] = set()
    posts: list[dict[str, str]] = []
    for post_data in post_results:
        if post_data:
            posts.append(post_data)
            author_ids.add(post_data.get("user_id", ""))

    # Fetch author information via pipeline
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

    # 4. Merge post data with author information and return
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
