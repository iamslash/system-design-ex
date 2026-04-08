"""Video upload with chunked/resumable support.

청크 단위 업로드를 지원하여 대용량 비디오 파일을 안정적으로 업로드한다.
각 청크는 임시 디렉토리에 저장되며, 모든 청크가 업로드되면 하나의 파일로 합친다.

흐름:
  1. 클라이언트가 업로드 시작 요청 → upload_id 발급
  2. 클라이언트가 청크를 순차적 또는 병렬로 업로드
  3. 모든 청크 업로드 후 완료 요청 → 청크 병합 → 트랜스코딩 트리거
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
    """업로드를 시작하고 upload_id 를 발급한다.

    Pre-signed URL 시뮬레이션: 실제 시스템에서는 S3 pre-signed URL 을
    발급하여 클라이언트가 직접 스토리지에 업로드하도록 한다.
    여기서는 upload_id 를 토큰처럼 사용한다.

    Args:
        redis: Redis 클라이언트
        title: 비디오 제목
        description: 비디오 설명
        total_chunks: 총 청크 수

    Returns:
        upload_id, pre-signed URL (시뮬레이션) 등의 업로드 정보
    """
    upload_id = str(uuid.uuid4())
    video_id = str(uuid.uuid4())

    # 업로드 상태를 Redis 에 저장
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

    # 청크 저장 디렉토리 생성
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
    """청크 하나를 업로드한다.

    Resumable upload: 이미 업로드된 청크는 덮어쓴다.
    클라이언트는 실패한 청크만 다시 업로드하면 된다.

    Args:
        redis: Redis 클라이언트
        upload_id: 업로드 ID
        chunk_index: 청크 인덱스 (0-based)
        chunk_data: 청크 바이너리 데이터

    Returns:
        업로드 상태 정보
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

    # 청크를 파일로 저장
    chunk_dir = os.path.join(settings.VIDEO_STORAGE_PATH, "chunks", upload_id)
    os.makedirs(chunk_dir, exist_ok=True)
    chunk_path = os.path.join(chunk_dir, f"chunk_{chunk_index:05d}")

    with open(chunk_path, "wb") as f:
        f.write(chunk_data)

    # 업로드된 청크 추적 (Redis Set 사용)
    chunk_set_key = f"upload_chunks:{upload_id}"
    await redis.sadd(chunk_set_key, str(chunk_index))
    uploaded_count = await redis.scard(chunk_set_key)

    # 업로드 진행률 갱신
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
    """업로드를 완료하고 청크를 하나의 파일로 병합한다.

    모든 청크가 업로드되었는지 확인한 뒤:
      1. 청크 파일들을 순서대로 읽어 하나의 파일로 합침
      2. 비디오 상태를 'transcoding' 으로 변경
      3. 임시 청크 디렉토리 정리

    Args:
        redis: Redis 클라이언트
        upload_id: 업로드 ID

    Returns:
        완료된 비디오 정보
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

    # 비디오 저장 디렉토리 생성
    video_dir = os.path.join(settings.VIDEO_STORAGE_PATH, "originals")
    os.makedirs(video_dir, exist_ok=True)

    # 청크를 하나의 파일로 병합
    chunk_dir = os.path.join(settings.VIDEO_STORAGE_PATH, "chunks", upload_id)
    output_path = os.path.join(video_dir, f"{video_id}.mp4")

    with open(output_path, "wb") as outfile:
        for i in range(total_chunks):
            chunk_path = os.path.join(chunk_dir, f"chunk_{i:05d}")
            if os.path.exists(chunk_path):
                with open(chunk_path, "rb") as chunk_file:
                    outfile.write(chunk_file.read())

    # 업로드 상태 갱신
    await redis.hset(upload_key, "status", "completed")

    # 청크 추적 정리
    await redis.delete(chunk_set_key)

    # 청크 파일 정리
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
    """업로드 상태를 조회한다."""
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
