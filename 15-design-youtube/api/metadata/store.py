"""Video metadata storage in Redis.

Stores and manages video metadata in Redis Hashes.
Each video is stored as a hash at key video:{video_id}.

Redis data structure:
  video:{video_id} (Hash)
    - video_id: unique video ID
    - title: video title
    - description: video description
    - status: uploading | transcoding | ready
    - resolutions: available resolutions (comma-separated)
    - created_at: creation timestamp
    - views: view count
    - thumbnail: thumbnail path

  video_list (Sorted Set)
    - member = video_id, score = created_at
    - used to retrieve the latest video list
"""

from __future__ import annotations

import time
from typing import Any

from redis.asyncio import Redis


async def create_video_metadata(
    redis: Redis,
    video_id: str,
    title: str,
    description: str = "",
) -> dict[str, Any]:
    """Create video metadata.

    Args:
        redis: Redis client
        video_id: Video ID
        title: Video title
        description: Video description

    Returns:
        Created metadata
    """
    created_at = time.time()
    video_key = f"video:{video_id}"

    metadata = {
        "video_id": video_id,
        "title": title,
        "description": description,
        "status": "uploading",
        "resolutions": "",
        "created_at": str(created_at),
        "views": "0",
        "thumbnail": "",
    }

    await redis.hset(video_key, mapping=metadata)

    # Add to video list Sorted Set (score = created_at)
    await redis.zadd("video_list", {video_id: created_at})

    return {
        "video_id": video_id,
        "title": title,
        "description": description,
        "status": "uploading",
        "created_at": created_at,
        "views": 0,
    }


async def get_video_metadata(
    redis: Redis,
    video_id: str,
) -> dict[str, Any] | None:
    """Retrieve video metadata.

    Increments views by 1 on each retrieval (view count tracking).

    Args:
        redis: Redis client
        video_id: Video ID

    Returns:
        Video metadata, or None if not found
    """
    video_key = f"video:{video_id}"
    data = await redis.hgetall(video_key)

    if not data:
        return None

    # Increment view count
    await redis.hincrby(video_key, "views", 1)

    resolutions = data.get("resolutions", "")
    resolution_list = [r for r in resolutions.split(",") if r] if resolutions else []

    return {
        "video_id": data.get("video_id", ""),
        "title": data.get("title", ""),
        "description": data.get("description", ""),
        "status": data.get("status", "unknown"),
        "resolutions": resolution_list,
        "created_at": data.get("created_at", ""),
        "views": int(data.get("views", "0")) + 1,  # reflect the just-incremented value
        "thumbnail": data.get("thumbnail", ""),
    }


async def update_video_status(
    redis: Redis,
    video_id: str,
    status: str,
    **extra_fields: str,
) -> bool:
    """Update video status.

    Args:
        redis: Redis client
        video_id: Video ID
        status: New status (uploading, transcoding, ready)
        **extra_fields: Additional fields

    Returns:
        True if update succeeded, False otherwise
    """
    video_key = f"video:{video_id}"
    exists = await redis.exists(video_key)
    if not exists:
        return False

    mapping: dict[str, str] = {"status": status}
    mapping.update(extra_fields)
    await redis.hset(video_key, mapping=mapping)
    return True


async def list_videos(
    redis: Redis,
    offset: int = 0,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Retrieve video list in reverse chronological order.

    Fetches video_id list from the video_list Sorted Set in reverse time order,
    then batch-retrieves each video's metadata using a pipeline.

    Args:
        redis: Redis client
        offset: Start position
        limit: Number of results to retrieve

    Returns:
        List of video metadata
    """
    video_ids = await redis.zrevrange("video_list", offset, offset + limit - 1)

    if not video_ids:
        return []

    # Batch retrieve using pipeline
    pipe = redis.pipeline()
    for vid in video_ids:
        pipe.hgetall(f"video:{vid}")
    results = await pipe.execute()

    videos: list[dict[str, Any]] = []
    for data in results:
        if data:
            resolutions = data.get("resolutions", "")
            resolution_list = (
                [r for r in resolutions.split(",") if r] if resolutions else []
            )
            videos.append({
                "video_id": data.get("video_id", ""),
                "title": data.get("title", ""),
                "description": data.get("description", ""),
                "status": data.get("status", "unknown"),
                "resolutions": resolution_list,
                "created_at": data.get("created_at", ""),
                "views": int(data.get("views", "0")),
            })

    return videos


async def delete_video_metadata(
    redis: Redis,
    video_id: str,
) -> bool:
    """Delete video metadata.

    Args:
        redis: Redis client
        video_id: Video ID

    Returns:
        True if deletion succeeded, False otherwise
    """
    video_key = f"video:{video_id}"
    deleted = await redis.delete(video_key)
    await redis.zrem("video_list", video_id)
    return deleted > 0
