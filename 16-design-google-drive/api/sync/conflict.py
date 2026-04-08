"""Sync conflict resolution.

동시에 같은 파일을 수정하면 충돌이 발생한다.
"first writer wins" 전략: 먼저 업로드한 버전이 반영되고,
나중에 업로드한 사용자에게는 충돌 응답을 반환한다.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from redis.asyncio import Redis


async def check_conflict(
    redis: Redis,
    file_id: str,
    expected_version: int,
) -> dict[str, Any] | None:
    """파일의 현재 버전과 클라이언트가 기대하는 버전을 비교하여 충돌을 감지한다.

    클라이언트가 "나는 v2 기반으로 수정했다" 라고 보냈는데
    서버의 최신 버전이 v3 이면 충돌이 발생한 것이다.

    Args:
        redis: Redis 클라이언트
        file_id: 파일 ID
        expected_version: 클라이언트가 기대하는 (마지막으로 알고 있는) 버전

    Returns:
        충돌 정보 딕셔너리 (충돌 없으면 None)
    """
    file_meta = await redis.hgetall(f"file:{file_id}")
    if not file_meta:
        return None  # 파일이 없으면 충돌 아님 (새 파일)

    server_version = int(file_meta.get("latest_version", "0"))

    if server_version > expected_version:
        # 충돌 발생: 서버에 더 최신 버전이 있다
        return {
            "conflict": True,
            "file_id": file_id,
            "message": f"Conflict: server has v{server_version}, "
            f"you expected v{expected_version}",
            "your_version": expected_version,
            "server_version": server_version,
            "server_updated_at": file_meta.get("updated_at", ""),
            "server_updated_by": file_meta.get("user_id", ""),
        }

    return None


async def resolve_conflict_first_writer_wins(
    redis: Redis,
    file_id: str,
    expected_version: int,
) -> dict[str, Any] | None:
    """First-writer-wins 충돌 해결.

    파일의 최신 버전이 expected_version 과 같으면 충돌 없음 (업로드 진행).
    최신 버전이 expected_version 보다 크면 충돌 반환.

    이 함수는 업로드 전에 호출되어야 한다.

    Returns:
        충돌이 없으면 None, 충돌이면 충돌 정보 딕셔너리
    """
    return await check_conflict(redis, file_id, expected_version)
