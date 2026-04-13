"""FastAPI file sync server entry point.

Provides a Google Drive-style file synchronization service.
Supports block-level storage, deduplication, version management, and long-polling sync.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query, UploadFile

from config import settings
from metadata.store import get_file_metadata
from storage.file_manager import delete_file, download_file, list_files, upload_file
from storage.versioning import get_revisions, restore_version
from sync.conflict import resolve_conflict_first_writer_wins
from sync.notification import poll_sync_events

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
        "File sync service started (Redis=%s:%d)",
        settings.REDIS_HOST,
        settings.REDIS_PORT,
    )

    yield

    if redis_client:
        await redis_client.aclose()
    logger.info("File sync service stopped")


app = FastAPI(
    title="Google Drive File Sync Service",
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
    try:
        await r.ping()
        return {"status": "ok", "redis": "connected"}
    except Exception:
        return {"status": "degraded", "redis": "disconnected"}


# ---------------------------------------------------------------------------
# File Upload
# ---------------------------------------------------------------------------


@app.post("/api/v1/files/upload")
async def api_upload(
    file: UploadFile,
    user_id: str = Query(default="anonymous"),
    expected_version: int | None = Query(default=None),
) -> dict[str, Any]:
    """Upload a file (multipart).

    Splits into blocks -> stores with dedup -> records metadata.
    If expected_version is provided, performs conflict detection.
    """
    r = _get_redis()

    data = await file.read()
    filename = file.filename or "unnamed"

    # Conflict check (only when expected_version is specified)
    if expected_version is not None:
        from storage.file_manager import _find_file_id

        file_id = await _find_file_id(r, filename, user_id)
        if file_id:
            conflict = await resolve_conflict_first_writer_wins(
                r, file_id, expected_version,
            )
            if conflict:
                raise HTTPException(status_code=409, detail=conflict)

    result = await upload_file(r, filename, data, user_id)
    return result


# ---------------------------------------------------------------------------
# File Download
# ---------------------------------------------------------------------------


@app.get("/api/v1/files/{file_id}/download")
async def api_download(
    file_id: str,
    version: int | None = Query(default=None),
) -> Any:
    """Download a file (reassembled from blocks)."""
    from fastapi.responses import Response

    r = _get_redis()
    try:
        filename, data = await download_file(r, file_id, version)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# File Metadata
# ---------------------------------------------------------------------------


@app.get("/api/v1/files/{file_id}")
async def api_get_file_metadata(file_id: str) -> dict[str, Any]:
    """Retrieve file metadata."""
    r = _get_redis()
    meta = await get_file_metadata(r, file_id)
    if not meta:
        raise HTTPException(status_code=404, detail="File not found")
    return meta


# ---------------------------------------------------------------------------
# Version History
# ---------------------------------------------------------------------------


@app.get("/api/v1/files/{file_id}/revisions")
async def api_get_revisions(file_id: str) -> dict[str, Any]:
    """Retrieve the version history of a file."""
    r = _get_redis()
    revisions = await get_revisions(r, file_id)
    if not revisions:
        raise HTTPException(status_code=404, detail="File not found")
    return {"file_id": file_id, "revisions": revisions}


# ---------------------------------------------------------------------------
# Restore Version
# ---------------------------------------------------------------------------


@app.post("/api/v1/files/{file_id}/restore/{version}")
async def api_restore_version(file_id: str, version: int) -> dict[str, Any]:
    """Restore a file to a specific version."""
    r = _get_redis()
    try:
        result = await restore_version(r, file_id, version)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return result


# ---------------------------------------------------------------------------
# Delete File
# ---------------------------------------------------------------------------


@app.delete("/api/v1/files/{file_id}")
async def api_delete_file(file_id: str) -> dict[str, Any]:
    """Delete a file."""
    r = _get_redis()
    try:
        result = await delete_file(r, file_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return result


# ---------------------------------------------------------------------------
# List Files
# ---------------------------------------------------------------------------


@app.get("/api/v1/files")
async def api_list_files(
    user_id: str = Query(default="anonymous"),
) -> dict[str, Any]:
    """List all files belonging to a user."""
    r = _get_redis()
    files = await list_files(r, user_id)
    return {"user_id": user_id, "count": len(files), "files": files}


# ---------------------------------------------------------------------------
# Sync (Long Polling)
# ---------------------------------------------------------------------------


@app.get("/api/v1/sync/poll")
async def api_sync_poll(
    user_id: str = Query(default="anonymous"),
    timeout: int = Query(default=None),
) -> dict[str, Any]:
    """Retrieve file change events via long-polling.

    Returns immediately if events are available; otherwise waits up to timeout seconds.
    """
    r = _get_redis()
    events = await poll_sync_events(r, user_id, timeout)
    return {"user_id": user_id, "events": events, "count": len(events)}
