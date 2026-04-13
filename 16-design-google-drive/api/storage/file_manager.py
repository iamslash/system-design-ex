"""File manager for upload and download operations.

On upload: split file into blocks -> store each block (dedup) -> save metadata.
On download: retrieve block hash list from metadata -> reassemble blocks -> return file.
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
    """Upload a file.

    Processing flow:
      1. Split file into blocks
      2. Store each block (dedup — skip blocks with duplicate hashes)
      3. Create new version (store block hash list)
      4. Update file metadata
      5. Publish sync event

    Returns:
        Upload result (file_id, version, new/reused block counts, etc.)
    """
    now = datetime.now(timezone.utc).isoformat()

    # 1. Split file into blocks
    blocks = split_into_blocks(data)

    # 2. Store each block (dedup applied)
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

    # 3. Determine file ID (reuse existing ID if file already exists)
    file_id = await _find_file_id(redis, filename, user_id)
    if file_id is None:
        file_id = str(uuid.uuid4())
        # Add to user's file list
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
        # Existing file: increment version
        version = int(await redis.hget(f"file:{file_id}", "latest_version") or "0") + 1
        await redis.hset(
            f"file:{file_id}",
            mapping={
                "latest_version": str(version),
                "size": str(len(data)),
                "updated_at": now,
            },
        )

    # 4. Save version info
    await create_version(redis, file_id, version, block_hashes, len(data))

    # 5. Publish sync event
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
    """Download a file.

    Retrieves the block hash list from metadata and reads each block
    to reassemble the original file.

    Args:
        redis: Redis client
        file_id: File ID
        version: Version to download (None means latest version)
        storage_path: Block storage path

    Returns:
        (filename, file_data) tuple

    Raises:
        ValueError: When the file or version is not found
    """
    # Retrieve file metadata
    file_meta = await redis.hgetall(f"file:{file_id}")
    if not file_meta:
        raise ValueError(f"File not found: {file_id}")

    filename = file_meta["filename"]

    # Determine version
    if version is None:
        version = int(file_meta["latest_version"])

    # Retrieve block hash list for the version
    version_data = await get_version(redis, file_id, version)
    if version_data is None:
        raise ValueError(f"Version {version} not found for file {file_id}")

    block_hashes = json.loads(version_data["block_hashes"])

    # Read blocks in order and reassemble
    chunks: list[bytes] = []
    for block_hash in block_hashes:
        chunk = await load_block(block_hash, storage_path)
        chunks.append(chunk)

    return filename, b"".join(chunks)


async def delete_file(
    redis: Redis,
    file_id: str,
) -> dict[str, Any]:
    """Delete a file (metadata only; blocks are retained).

    Blocks are not deleted because other files may reference them.
    Garbage collection is performed as a separate batch job.
    """
    file_meta = await redis.hgetall(f"file:{file_id}")
    if not file_meta:
        raise ValueError(f"File not found: {file_id}")

    user_id = file_meta["user_id"]
    filename = file_meta["filename"]
    latest_version = int(file_meta["latest_version"])

    # Delete all version metadata
    for v in range(1, latest_version + 1):
        await redis.delete(f"file_version:{file_id}:{v}")

    # Delete file metadata
    await redis.delete(f"file:{file_id}")

    # Remove from user's file list
    await redis.srem(f"user_files:{user_id}", file_id)

    # Publish sync event
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
    """List files belonging to the user."""
    file_ids = await redis.smembers(f"user_files:{user_id}")
    files: list[dict[str, Any]] = []
    for fid in sorted(file_ids):
        meta = await redis.hgetall(f"file:{fid}")
        if meta:
            files.append(meta)
    return files


async def _find_file_id(redis: Redis, filename: str, user_id: str) -> str | None:
    """Find the file ID of an existing file with the same name for the user."""
    file_ids = await redis.smembers(f"user_files:{user_id}")
    for fid in file_ids:
        stored_name = await redis.hget(f"file:{fid}", "filename")
        if stored_name == filename:
            return fid
    return None
