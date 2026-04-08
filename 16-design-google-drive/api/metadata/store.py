"""File and user metadata store backed by Redis.

Redis 에 파일 메타데이터, 사용자 파일 목록 등을 저장하고 조회한다.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from redis.asyncio import Redis


async def get_file_metadata(
    redis: Redis,
    file_id: str,
) -> dict[str, Any] | None:
    """파일 메타데이터를 조회한다.

    Redis key: file:{file_id}
    Fields: file_id, filename, user_id, latest_version, size, created_at, updated_at
    """
    data = await redis.hgetall(f"file:{file_id}")
    return data if data else None


async def get_user_file_ids(
    redis: Redis,
    user_id: str,
) -> set[str]:
    """사용자의 파일 ID 목록을 조회한다.

    Redis key: user_files:{user_id} (Set)
    """
    return await redis.smembers(f"user_files:{user_id}")


async def get_block_info(
    redis: Redis,
    block_hash: str,
) -> dict[str, Any] | None:
    """블록 메타데이터를 조회한다.

    Redis key: block:{hash}
    Fields: original_size, compressed_size
    """
    data = await redis.hgetall(f"block:{block_hash}")
    return data if data else None
