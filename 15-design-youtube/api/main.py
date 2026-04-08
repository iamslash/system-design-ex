"""FastAPI video streaming server entry point.

YouTube 와 유사한 비디오 스트리밍 시스템의 HTTP API 를 제공한다.
Redis 를 메타데이터 저장소로, 파일시스템을 비디오 스토리지로 사용한다.
"""

from __future__ import annotations

import logging
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

# 전역 Redis 클라이언트
redis_client: aioredis.Redis | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 시작/종료 시 Redis 연결을 관리한다."""
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
    """Redis 클라이언트를 반환한다. 연결되지 않았으면 503 을 발생시킨다."""
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
    """비디오 업로드를 시작한다.

    upload_id 와 pre-signed URL (시뮬레이션)을 반환한다.
    클라이언트는 이 upload_id 를 사용하여 청크를 업로드한다.
    """
    r = _get_redis()
    result = await initiate_upload(
        r,
        title=request.title,
        description=request.description,
        total_chunks=request.total_chunks,
    )

    # 비디오 메타데이터 생성
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
    """비디오 청크를 업로드한다.

    multipart/form-data 형식으로 파일 데이터를 전송한다.
    동일한 chunk_index 를 다시 보내면 덮어쓴다 (resumable).
    """
    r = _get_redis()
    chunk_data = await file.read()
    result = await upload_chunk(r, upload_id, chunk_index, chunk_data)

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    return result


@app.post("/api/v1/videos/upload/{upload_id}/complete")
async def api_complete_upload(upload_id: str) -> dict[str, Any]:
    """업로드를 완료하고 트랜스코딩을 시작한다.

    모든 청크가 업로드되었는지 확인한 뒤:
      1. 청크를 하나의 파일로 병합
      2. 트랜스코딩 파이프라인 실행
      3. 비디오 상태를 'ready' 로 갱신
    """
    r = _get_redis()

    # 업로드 완료 (청크 병합)
    upload_result = await complete_upload(r, upload_id)
    if "error" in upload_result:
        raise HTTPException(status_code=400, detail=upload_result["error"])

    video_id = upload_result["video_id"]
    file_path = upload_result["file_path"]

    # 트랜스코딩 실행
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
    """비디오 메타데이터를 조회한다."""
    r = _get_redis()
    metadata = await get_video_metadata(r, video_id)
    if not metadata:
        raise HTTPException(status_code=404, detail="Video not found")
    return metadata


@app.get("/api/v1/videos")
async def api_list_videos(offset: int = 0, limit: int = 20) -> dict[str, Any]:
    """비디오 목록을 최신순으로 조회한다."""
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
    """비디오를 스트리밍한다.

    HTTP Range 헤더를 지원하여 시킹(seeking)이 가능하다.
    Range 헤더가 있으면 206 Partial Content,
    없으면 200 OK 로 전체 파일을 전송한다.
    """
    r = _get_redis()

    # 비디오 존재 여부 확인
    video_key = f"video:{video_id}"
    exists = await r.exists(video_key)
    if not exists:
        raise HTTPException(status_code=404, detail="Video not found")

    # 비디오 파일 경로 찾기
    file_path = get_video_path(video_id, resolution)
    if not file_path:
        raise HTTPException(status_code=404, detail="Video file not found")

    # Range 헤더 파싱 및 응답 구성
    range_header = request.headers.get("range")
    stream_info = build_stream_response_info(file_path, range_header)

    return Response(
        content=stream_info["data"],
        status_code=stream_info["status_code"],
        headers=stream_info["headers"],
    )
