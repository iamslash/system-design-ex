"""Sync conflict resolution.

When the same file is modified concurrently, a conflict occurs.
"First writer wins" strategy: the first uploaded version is accepted,
and a conflict response is returned to the user who uploads later.
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
    """Detect a conflict by comparing the server's current version with the client's expected version.

    If the client sends "I modified based on v2" but the server's latest version
    is v3, a conflict has occurred.

    Args:
        redis: Redis client
        file_id: File ID
        expected_version: The version the client expects (last known version)

    Returns:
        Conflict info dictionary, or None if there is no conflict
    """
    file_meta = await redis.hgetall(f"file:{file_id}")
    if not file_meta:
        return None  # No conflict if the file doesn't exist yet (new file)

    server_version = int(file_meta.get("latest_version", "0"))

    if server_version > expected_version:
        # Conflict: the server has a newer version
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
    """First-writer-wins conflict resolution.

    No conflict if the file's latest version equals expected_version (upload proceeds).
    Returns a conflict if the latest version is greater than expected_version.

    This function must be called before performing the upload.

    Returns:
        None if no conflict, or a conflict info dictionary if conflicted
    """
    return await check_conflict(redis, file_id, expected_version)
