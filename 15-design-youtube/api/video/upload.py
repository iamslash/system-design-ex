"""Video upload with chunked/resumable support.

Supports chunk-based upload for reliably uploading large video files.
Each chunk is stored in a temporary directory and merged into a single file
once all chunks have been uploaded.

Flow:
  1. Client sends upload initiation request -> upload_id issued
  2. Client uploads chunks sequentially or in parallel
  3. After all chunks uploaded, client sends complete request -> merge -> transcode trigger
"""

from __future__ import annotations

import os
import uuid
from typing import Any

from redis.asyncio import Redis

from config import settings


async def initiate_upload(
    redis: Redis,
    title: str,
    description: str,
    total_chunks: int,
) -> dict[str, Any]:
    """Initiate an upload and issue an upload_id.

    Pre-signed URL simulation: in a real system, an S3 pre-signed URL would be
    issued so the client uploads directly to storage.
    Here, upload_id is used as a token equivalent.

    Args:
        redis: Redis client
        title: Video title
        description: Video description
        total_chunks: Total number of chunks

    Returns:
        Upload info including upload_id and pre-signed URL (simulated)
    """
    upload_id = str(uuid.uuid4())
    video_id = str(uuid.uuid4())

    # Store upload state in Redis
    upload_key = f"upload:{upload_id}"
    await redis.hset(
        upload_key,
        mapping={
            "upload_id": upload_id,
            "video_id": video_id,
            "title": title,
            "description": description,
            "total_chunks": str(total_chunks),
            "uploaded_chunks": "0",
            "status": "uploading",
        },
    )

    # Create chunk storage directory
    chunk_dir = os.path.join(settings.VIDEO_STORAGE_PATH, "chunks", upload_id)
    os.makedirs(chunk_dir, exist_ok=True)

    return {
        "upload_id": upload_id,
        "video_id": video_id,
        "presigned_url": f"/api/v1/videos/upload/{upload_id}/chunk/{{chunk_index}}",
        "total_chunks": total_chunks,
        "status": "uploading",
    }


async def upload_chunk(
    redis: Redis,
    upload_id: str,
    chunk_index: int,
    chunk_data: bytes,
) -> dict[str, Any]:
    """Upload a single chunk.

    Resumable upload: already-uploaded chunks are overwritten.
    The client only needs to re-upload failed chunks.

    Args:
        redis: Redis client
        upload_id: Upload ID
        chunk_index: Chunk index (0-based)
        chunk_data: Chunk binary data

    Returns:
        Upload status info
    """
    upload_key = f"upload:{upload_id}"
    upload_info = await redis.hgetall(upload_key)

    if not upload_info:
        return {"error": "Upload not found", "upload_id": upload_id}

    if upload_info.get("status") != "uploading":
        return {"error": "Upload is not in uploading state", "upload_id": upload_id}

    total_chunks = int(upload_info["total_chunks"])
    if chunk_index < 0 or chunk_index >= total_chunks:
        return {"error": f"Invalid chunk index: {chunk_index}", "upload_id": upload_id}

    # Save chunk to file
    chunk_dir = os.path.join(settings.VIDEO_STORAGE_PATH, "chunks", upload_id)
    os.makedirs(chunk_dir, exist_ok=True)
    chunk_path = os.path.join(chunk_dir, f"chunk_{chunk_index:05d}")

    with open(chunk_path, "wb") as f:
        f.write(chunk_data)

    # Track uploaded chunks using a Redis Set
    chunk_set_key = f"upload_chunks:{upload_id}"
    await redis.sadd(chunk_set_key, str(chunk_index))
    uploaded_count = await redis.scard(chunk_set_key)

    # Update upload progress
    await redis.hset(upload_key, "uploaded_chunks", str(uploaded_count))

    return {
        "upload_id": upload_id,
        "chunk_index": chunk_index,
        "uploaded_chunks": uploaded_count,
        "total_chunks": total_chunks,
        "status": "uploading",
    }


async def complete_upload(
    redis: Redis,
    upload_id: str,
) -> dict[str, Any]:
    """Complete the upload and merge chunks into a single file.

    After verifying all chunks have been uploaded:
      1. Read chunk files in order and merge into one file
      2. Change video status to 'transcoding'
      3. Clean up temporary chunk directory

    Args:
        redis: Redis client
        upload_id: Upload ID

    Returns:
        Completed video info
    """
    upload_key = f"upload:{upload_id}"
    upload_info = await redis.hgetall(upload_key)

    if not upload_info:
        return {"error": "Upload not found", "upload_id": upload_id}

    if upload_info.get("status") != "uploading":
        return {"error": "Upload is not in uploading state", "upload_id": upload_id}

    total_chunks = int(upload_info["total_chunks"])
    chunk_set_key = f"upload_chunks:{upload_id}"
    uploaded_count = await redis.scard(chunk_set_key)

    if uploaded_count < total_chunks:
        return {
            "error": "Not all chunks uploaded",
            "uploaded_chunks": uploaded_count,
            "total_chunks": total_chunks,
        }

    video_id = upload_info["video_id"]

    # Create video storage directory
    video_dir = os.path.join(settings.VIDEO_STORAGE_PATH, "originals")
    os.makedirs(video_dir, exist_ok=True)

    # Merge chunks into a single file
    chunk_dir = os.path.join(settings.VIDEO_STORAGE_PATH, "chunks", upload_id)
    output_path = os.path.join(video_dir, f"{video_id}.mp4")

    with open(output_path, "wb") as outfile:
        for i in range(total_chunks):
            chunk_path = os.path.join(chunk_dir, f"chunk_{i:05d}")
            if os.path.exists(chunk_path):
                with open(chunk_path, "rb") as chunk_file:
                    outfile.write(chunk_file.read())

    # Update upload status
    await redis.hset(upload_key, "status", "completed")

    # Clean up chunk tracking
    await redis.delete(chunk_set_key)

    # Clean up chunk files
    for i in range(total_chunks):
        chunk_path = os.path.join(chunk_dir, f"chunk_{i:05d}")
        if os.path.exists(chunk_path):
            os.remove(chunk_path)
    if os.path.exists(chunk_dir):
        try:
            os.rmdir(chunk_dir)
        except OSError:
            pass

    return {
        "upload_id": upload_id,
        "video_id": video_id,
        "title": upload_info["title"],
        "status": "completed",
        "file_path": output_path,
    }


async def get_upload_status(
    redis: Redis,
    upload_id: str,
) -> dict[str, Any] | None:
    """Get upload status."""
    upload_key = f"upload:{upload_id}"
    info = await redis.hgetall(upload_key)
    if not info:
        return None
    return {
        "upload_id": info.get("upload_id", ""),
        "video_id": info.get("video_id", ""),
        "title": info.get("title", ""),
        "total_chunks": int(info.get("total_chunks", "0")),
        "uploaded_chunks": int(info.get("uploaded_chunks", "0")),
        "status": info.get("status", "unknown"),
    }
