"""File version history management.

각 업로드마다 새 버전을 생성하고, 이전 버전을 보존한다.
특정 버전으로 복원(restore) 할 수 있다.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from redis.asyncio import Redis

from sync.notification import publish_sync_event


async def create_version(
    redis: Redis,
    file_id: str,
    version: int,
    block_hashes: list[str],
    size: int,
) -> None:
    """파일의 새 버전을 저장한다.

    Args:
        redis: Redis 클라이언트
        file_id: 파일 ID
        version: 버전 번호
        block_hashes: 이 버전을 구성하는 블록 해시 목록
        size: 파일 크기 (바이트)
    """
    now = datetime.now(timezone.utc).isoformat()
    await redis.hset(
        f"file_version:{file_id}:{version}",
        mapping={
            "version": str(version),
            "block_hashes": json.dumps(block_hashes),
            "size": str(size),
            "block_count": str(len(block_hashes)),
            "created_at": now,
        },
    )


async def get_version(
    redis: Redis,
    file_id: str,
    version: int,
) -> dict[str, str] | None:
    """특정 버전의 정보를 조회한다.

    Returns:
        버전 데이터 딕셔너리 또는 None (버전이 없을 때)
    """
    data = await redis.hgetall(f"file_version:{file_id}:{version}")
    return data if data else None


async def get_revisions(
    redis: Redis,
    file_id: str,
) -> list[dict[str, Any]]:
    """파일의 모든 버전 히스토리를 조회한다.

    Returns:
        버전 정보 리스트 (오래된 순)
    """
    file_meta = await redis.hgetall(f"file:{file_id}")
    if not file_meta:
        return []

    latest_version = int(file_meta.get("latest_version", "0"))
    revisions: list[dict[str, Any]] = []

    for v in range(1, latest_version + 1):
        version_data = await get_version(redis, file_id, v)
        if version_data:
            revisions.append({
                "version": int(version_data["version"]),
                "size": int(version_data["size"]),
                "block_count": int(version_data["block_count"]),
                "created_at": version_data["created_at"],
            })

    return revisions


async def restore_version(
    redis: Redis,
    file_id: str,
    target_version: int,
) -> dict[str, Any]:
    """파일을 특정 버전으로 복원한다.

    복원은 대상 버전의 블록 해시 목록을 새 버전으로 복사하는 방식으로 동작한다.
    기존 버전 히스토리는 유지되며, 복원 자체도 새 버전으로 기록된다.

    Args:
        redis: Redis 클라이언트
        file_id: 파일 ID
        target_version: 복원할 대상 버전 번호

    Returns:
        복원 결과 정보

    Raises:
        ValueError: 파일이나 대상 버전이 없을 때
    """
    file_meta = await redis.hgetall(f"file:{file_id}")
    if not file_meta:
        raise ValueError(f"File not found: {file_id}")

    # 복원 대상 버전 조회
    target_data = await get_version(redis, file_id, target_version)
    if target_data is None:
        raise ValueError(f"Version {target_version} not found for file {file_id}")

    # 새 버전 번호
    current_latest = int(file_meta["latest_version"])
    new_version = current_latest + 1

    # 대상 버전의 블록 해시를 새 버전으로 복사
    block_hashes = json.loads(target_data["block_hashes"])
    size = int(target_data["size"])
    await create_version(redis, file_id, new_version, block_hashes, size)

    # 파일 메타데이터 갱신
    now = datetime.now(timezone.utc).isoformat()
    await redis.hset(
        f"file:{file_id}",
        mapping={
            "latest_version": str(new_version),
            "size": str(size),
            "updated_at": now,
        },
    )

    # 동기화 이벤트 발행
    await publish_sync_event(redis, file_meta["user_id"], {
        "event_type": "restore",
        "file_id": file_id,
        "filename": file_meta["filename"],
        "user_id": file_meta["user_id"],
        "version": new_version,
        "timestamp": now,
    })

    return {
        "file_id": file_id,
        "filename": file_meta["filename"],
        "restored_from": target_version,
        "new_version": new_version,
        "size": size,
        "message": f"Restored {file_meta['filename']} from v{target_version} as v{new_version}",
    }
