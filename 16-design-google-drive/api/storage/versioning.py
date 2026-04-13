"""File version history management.

Creates a new version on each upload and preserves previous versions.
Supports restoring to a specific version.
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
    """Save a new version of a file.

    Args:
        redis: Redis client
        file_id: File ID
        version: Version number
        block_hashes: List of block hashes that make up this version
        size: File size in bytes
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
    """Retrieve information for a specific version.

    Returns:
        Version data dictionary, or None if the version does not exist
    """
    data = await redis.hgetall(f"file_version:{file_id}:{version}")
    return data if data else None


async def get_revisions(
    redis: Redis,
    file_id: str,
) -> list[dict[str, Any]]:
    """Retrieve the full version history of a file.

    Returns:
        List of version info dicts in chronological order (oldest first)
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
    """Restore a file to a specific version.

    Restoration works by copying the target version's block hash list as a new version.
    The existing version history is preserved; the restore itself is recorded as a new version.

    Args:
        redis: Redis client
        file_id: File ID
        target_version: The version number to restore to

    Returns:
        Restore result information

    Raises:
        ValueError: When the file or target version does not exist
    """
    file_meta = await redis.hgetall(f"file:{file_id}")
    if not file_meta:
        raise ValueError(f"File not found: {file_id}")

    # Look up the target version to restore
    target_data = await get_version(redis, file_id, target_version)
    if target_data is None:
        raise ValueError(f"Version {target_version} not found for file {file_id}")

    # Determine the new version number
    current_latest = int(file_meta["latest_version"])
    new_version = current_latest + 1

    # Copy the target version's block hashes into the new version
    block_hashes = json.loads(target_data["block_hashes"])
    size = int(target_data["size"])
    await create_version(redis, file_id, new_version, block_hashes, size)

    # Update file metadata
    now = datetime.now(timezone.utc).isoformat()
    await redis.hset(
        f"file:{file_id}",
        mapping={
            "latest_version": str(new_version),
            "size": str(size),
            "updated_at": now,
        },
    )

    # Publish a sync event
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
