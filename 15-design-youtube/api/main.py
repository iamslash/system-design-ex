"""FastAPI video streaming server entry point.

Provides the HTTP API for a YouTube-like video streaming system.
Uses Redis as the metadata store and the filesystem as video storage.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Request, Response, UploadFile

from config import settings
from metadata.store import (
    create_video_metadata,
    get_video_metadata,
    list_videos,
)
from models import UploadCompleteRequest, UploadInitRequest
from video.streaming import build_stream_response_info, get_video_path
from video.transcode import transcode_video
from video.upload import complete_upload, get_upload_status, initiate_upload, upload_chunk

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# Global Redis client
redis_client: aioredis.Redis | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage Redis connection on application startup/shutdown."""
    global redis_client

    redis_client = aioredis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        decode_responses=True,
    )
    logger.info(
        "Video streaming service started (Redis=%s:%d)",
        settings.REDIS_HOST,
        settings.REDIS_PORT,
    )

    yield

    if redis_client:
        await redis_client.aclose()
    logger.info("Video streaming service stopped")


app = FastAPI(
    title="Video Streaming System",
    version="1.0.0",
    lifespan=lifespan,
)


def _get_redis() -> aioredis.Redis:
    """Return the Redis client, raising 503 if not connected."""
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis not connected")
    return redis_client


# ---------------------------------------------------------------------------
# Health Check
# ---------------------------------------------------------------------------


@app.get("/health")
async def health() -> dict[str, Any]:
    """Health check."""
    r = _get_redis()
    info = await r.info("server")
    return {
        "status": "ok",
        "redis_version": info.get("redis_version", "unknown"),
    }


# ---------------------------------------------------------------------------
# Upload Endpoints
# ---------------------------------------------------------------------------


@app.post("/api/v1/videos/upload")
async def api_initiate_upload(request: UploadInitRequest) -> dict[str, Any]:
    """Initiate a video upload.

    Returns an upload_id and pre-signed URL (simulated).
    The client uses this upload_id to upload chunks.
    """
    r = _get_redis()
    result = await initiate_upload(
        r,
        title=request.title,
        description=request.description,
        total_chunks=request.total_chunks,
    )

    # Create video metadata
    await create_video_metadata(
        r,
        video_id=result["video_id"],
        title=request.title,
        description=request.description,
    )

    return result


@app.put("/api/v1/videos/upload/{upload_id}/chunk/{chunk_index}")
async def api_upload_chunk(
    upload_id: str,
    chunk_index: int,
    file: UploadFile,
) -> dict[str, Any]:
    """Upload a video chunk.

    File data is sent as multipart/form-data.
    Re-sending the same chunk_index overwrites the previous chunk (resumable).
    """
    r = _get_redis()
    chunk_data = await file.read()
    result = await upload_chunk(r, upload_id, chunk_index, chunk_data)

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    return result


@app.post("/api/v1/videos/upload/{upload_id}/complete")
async def api_complete_upload(upload_id: str) -> dict[str, Any]:
    """Complete the upload and start transcoding.

    After verifying all chunks have been uploaded:
      1. Merge chunks into a single file
      2. Run the transcoding pipeline
      3. Update video status to 'ready'
    """
    r = _get_redis()

    # Complete upload (merge chunks)
    upload_result = await complete_upload(r, upload_id)
    if "error" in upload_result:
        raise HTTPException(status_code=400, detail=upload_result["error"])

    video_id = upload_result["video_id"]
    file_path = upload_result["file_path"]

    # Run transcoding
    transcode_result = await transcode_video(r, video_id, file_path)

    return {
        "upload_id": upload_id,
        "video_id": video_id,
        "title": upload_result["title"],
        "status": transcode_result["status"],
        "resolutions": transcode_result["resolutions"],
    }


# ---------------------------------------------------------------------------
# Video Metadata Endpoints
# ---------------------------------------------------------------------------


@app.get("/api/v1/videos/{video_id}")
async def api_get_video(video_id: str) -> dict[str, Any]:
    """Retrieve video metadata."""
    r = _get_redis()
    metadata = await get_video_metadata(r, video_id)
    if not metadata:
        raise HTTPException(status_code=404, detail="Video not found")
    return metadata


@app.get("/api/v1/videos")
async def api_list_videos(offset: int = 0, limit: int = 20) -> dict[str, Any]:
    """Retrieve video list in reverse chronological order."""
    r = _get_redis()
    videos = await list_videos(r, offset=offset, limit=limit)
    return {
        "count": len(videos),
        "videos": videos,
    }


# ---------------------------------------------------------------------------
# Streaming Endpoint
# ---------------------------------------------------------------------------


@app.get("/api/v1/videos/{video_id}/stream")
async def api_stream_video(
    video_id: str,
    request: Request,
    resolution: str = "720p",
) -> Response:
    """Stream a video.

    Supports HTTP Range headers to enable seeking.
    Returns 206 Partial Content when Range header is present,
    or 200 OK with the full file when no Range header is given.
    """
    r = _get_redis()

    # Check video existence
    video_key = f"video:{video_id}"
    exists = await r.exists(video_key)
    if not exists:
        raise HTTPException(status_code=404, detail="Video not found")

    # Find video file path
    file_path = get_video_path(video_id, resolution)
    if not file_path:
        raise HTTPException(status_code=404, detail="Video file not found")

    # Parse Range header and build response
    range_header = request.headers.get("range")
    stream_info = build_stream_response_info(file_path, range_header)

    return Response(
        content=stream_info["data"],
        status_code=stream_info["status_code"],
        headers=stream_info["headers"],
    )
