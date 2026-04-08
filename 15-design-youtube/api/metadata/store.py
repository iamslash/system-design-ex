"""Video metadata storage in Redis.

비디오 메타데이터를 Redis Hash 에 저장하고 관리한다.
각 비디오는 video:{video_id} 키에 해시 형태로 저장된다.

Redis 데이터 구조:
  video:{video_id} (Hash)
    - video_id: 비디오 고유 ID
    - title: 비디오 제목
    - description: 비디오 설명
    - status: uploading | transcoding | ready
    - resolutions: 사용 가능한 해상도 (쉼표 구분)
    - created_at: 생성 시간
    - views: 조회수
    - thumbnail: 썸네일 경로

  video_list (Sorted Set)
    - member = video_id, score = created_at
    - 최신 비디오 목록 조회에 사용
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
    """비디오 메타데이터를 생성한다.

    Args:
        redis: Redis 클라이언트
        video_id: 비디오 ID
        title: 비디오 제목
        description: 비디오 설명

    Returns:
        생성된 메타데이터
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

    # 비디오 목록 Sorted Set 에 추가 (score = created_at)
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
    """비디오 메타데이터를 조회한다.

    조회 시 views 를 1 증가시킨다 (조회수 카운트).

    Args:
        redis: Redis 클라이언트
        video_id: 비디오 ID

    Returns:
        비디오 메타데이터, 없으면 None
    """
    video_key = f"video:{video_id}"
    data = await redis.hgetall(video_key)

    if not data:
        return None

    # 조회수 증가
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
        "views": int(data.get("views", "0")) + 1,  # 방금 증가한 값 반영
        "thumbnail": data.get("thumbnail", ""),
    }


async def update_video_status(
    redis: Redis,
    video_id: str,
    status: str,
    **extra_fields: str,
) -> bool:
    """비디오 상태를 갱신한다.

    Args:
        redis: Redis 클라이언트
        video_id: 비디오 ID
        status: 새 상태 (uploading, transcoding, ready)
        **extra_fields: 추가 필드

    Returns:
        갱신 성공 여부
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
    """비디오 목록을 최신순으로 조회한다.

    video_list Sorted Set 에서 역시간순으로 video_id 목록을 가져온 뒤,
    각 비디오의 메타데이터를 파이프라인으로 일괄 조회한다.

    Args:
        redis: Redis 클라이언트
        offset: 시작 위치
        limit: 조회 개수

    Returns:
        비디오 메타데이터 목록
    """
    video_ids = await redis.zrevrange("video_list", offset, offset + limit - 1)

    if not video_ids:
        return []

    # 파이프라인으로 일괄 조회
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
    """비디오 메타데이터를 삭제한다.

    Args:
        redis: Redis 클라이언트
        video_id: 비디오 ID

    Returns:
        삭제 성공 여부
    """
    video_key = f"video:{video_id}"
    deleted = await redis.delete(video_key)
    await redis.zrem("video_list", video_id)
    return deleted > 0
