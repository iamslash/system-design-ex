"""File manager for upload and download operations.

업로드 시 파일을 블록으로 분할 → 각 블록 저장(dedup) → 메타데이터 저장.
다운로드 시 메타데이터에서 블록 해시 목록 조회 → 블록 재조립 → 파일 반환.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from redis.asyncio import Redis

from storage.block_server import load_block, split_into_blocks, store_block
from storage.versioning import create_version, get_version
from sync.notification import publish_sync_event


async def upload_file(
    redis: Redis,
    filename: str,
    data: bytes,
    user_id: str,
    storage_path: str | None = None,
) -> dict[str, Any]:
    """파일을 업로드한다.

    처리 흐름:
      1. 파일을 블록으로 분할
      2. 각 블록을 저장 (dedup — 동일 해시의 블록은 건너뜀)
      3. 새 버전 생성 (블록 해시 목록 저장)
      4. 파일 메타데이터 갱신
      5. 동기화 이벤트 발행

    Returns:
        업로드 결과 (file_id, version, new/reused block 수 등)
    """
    now = datetime.now(timezone.utc).isoformat()

    # 1. 파일을 블록으로 분할
    blocks = split_into_blocks(data)

    # 2. 각 블록 저장 (dedup 적용)
    block_hashes: list[str] = []
    new_blocks = 0
    reused_blocks = 0
    for block in blocks:
        block_hash, is_new = await store_block(redis, block, storage_path)
        block_hashes.append(block_hash)
        if is_new:
            new_blocks += 1
        else:
            reused_blocks += 1

    # 3. 파일 ID 결정 (기존 파일이면 기존 ID 사용)
    file_id = await _find_file_id(redis, filename, user_id)
    if file_id is None:
        file_id = str(uuid.uuid4())
        # 사용자 파일 목록에 추가
        await redis.sadd(f"user_files:{user_id}", file_id)
        version = 1
        await redis.hset(
            f"file:{file_id}",
            mapping={
                "file_id": file_id,
                "filename": filename,
                "user_id": user_id,
                "latest_version": str(version),
                "size": str(len(data)),
                "created_at": now,
                "updated_at": now,
            },
        )
    else:
        # 기존 파일: 버전 증가
        version = int(await redis.hget(f"file:{file_id}", "latest_version") or "0") + 1
        await redis.hset(
            f"file:{file_id}",
            mapping={
                "latest_version": str(version),
                "size": str(len(data)),
                "updated_at": now,
            },
        )

    # 4. 버전 정보 저장
    await create_version(redis, file_id, version, block_hashes, len(data))

    # 5. 동기화 이벤트 발행
    await publish_sync_event(redis, user_id, {
        "event_type": "upload",
        "file_id": file_id,
        "filename": filename,
        "user_id": user_id,
        "version": version,
        "timestamp": now,
    })

    return {
        "file_id": file_id,
        "filename": filename,
        "version": version,
        "size": len(data),
        "total_blocks": len(block_hashes),
        "new_blocks": new_blocks,
        "reused_blocks": reused_blocks,
        "message": f"Uploaded {filename} v{version}: "
        f"{new_blocks} new blocks, {reused_blocks} reused blocks",
    }


async def download_file(
    redis: Redis,
    file_id: str,
    version: int | None = None,
    storage_path: str | None = None,
) -> tuple[str, bytes]:
    """파일을 다운로드한다.

    메타데이터에서 블록 해시 목록을 조회하고, 각 블록을 읽어
    원본 파일로 재조립한다.

    Args:
        redis: Redis 클라이언트
        file_id: 파일 ID
        version: 다운로드할 버전 (None 이면 최신 버전)
        storage_path: 블록 저장 경로

    Returns:
        (filename, file_data) 튜플

    Raises:
        ValueError: 파일 또는 버전이 없을 때
    """
    # 파일 메타데이터 조회
    file_meta = await redis.hgetall(f"file:{file_id}")
    if not file_meta:
        raise ValueError(f"File not found: {file_id}")

    filename = file_meta["filename"]

    # 버전 결정
    if version is None:
        version = int(file_meta["latest_version"])

    # 버전의 블록 해시 목록 조회
    version_data = await get_version(redis, file_id, version)
    if version_data is None:
        raise ValueError(f"Version {version} not found for file {file_id}")

    block_hashes = json.loads(version_data["block_hashes"])

    # 블록을 순서대로 읽어 재조립
    chunks: list[bytes] = []
    for block_hash in block_hashes:
        chunk = await load_block(block_hash, storage_path)
        chunks.append(chunk)

    return filename, b"".join(chunks)


async def delete_file(
    redis: Redis,
    file_id: str,
) -> dict[str, Any]:
    """파일을 삭제한다 (메타데이터만 삭제, 블록은 유지).

    블록은 다른 파일에서도 참조될 수 있으므로 삭제하지 않는다.
    가비지 컬렉션은 별도 배치 작업으로 수행한다.
    """
    file_meta = await redis.hgetall(f"file:{file_id}")
    if not file_meta:
        raise ValueError(f"File not found: {file_id}")

    user_id = file_meta["user_id"]
    filename = file_meta["filename"]
    latest_version = int(file_meta["latest_version"])

    # 모든 버전 메타데이터 삭제
    for v in range(1, latest_version + 1):
        await redis.delete(f"file_version:{file_id}:{v}")

    # 파일 메타데이터 삭제
    await redis.delete(f"file:{file_id}")

    # 사용자 파일 목록에서 제거
    await redis.srem(f"user_files:{user_id}", file_id)

    # 동기화 이벤트 발행
    now = datetime.now(timezone.utc).isoformat()
    await publish_sync_event(redis, user_id, {
        "event_type": "delete",
        "file_id": file_id,
        "filename": filename,
        "user_id": user_id,
        "version": 0,
        "timestamp": now,
    })

    return {
        "file_id": file_id,
        "filename": filename,
        "message": f"Deleted {filename}",
    }


async def list_files(
    redis: Redis,
    user_id: str,
) -> list[dict[str, Any]]:
    """사용자의 파일 목록을 조회한다."""
    file_ids = await redis.smembers(f"user_files:{user_id}")
    files: list[dict[str, Any]] = []
    for fid in sorted(file_ids):
        meta = await redis.hgetall(f"file:{fid}")
        if meta:
            files.append(meta)
    return files


async def _find_file_id(redis: Redis, filename: str, user_id: str) -> str | None:
    """사용자의 기존 파일 중 같은 이름의 파일 ID 를 찾는다."""
    file_ids = await redis.smembers(f"user_files:{user_id}")
    for fid in file_ids:
        stored_name = await redis.hget(f"file:{fid}", "filename")
        if stored_name == filename:
            return fid
    return None
