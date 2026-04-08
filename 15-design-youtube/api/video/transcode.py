"""Video transcoding simulation with DAG model.

실제 트랜스코딩 파이프라인은 DAG(Directed Acyclic Graph) 형태로 구성된다:

    원본 비디오
        │
        ▼
    ┌─────────┐
    │  Split   │  ── 비디오를 세그먼트로 분할
    └────┬────┘
         │
    ┌────┴────────────────┬──────────────┐
    ▼                     ▼              ▼
┌─────────┐         ┌─────────┐    ┌──────────┐
│Encode   │         │Thumbnail│    │Watermark │
│360/720/ │         │ Extract │    │  Overlay │
│1080p    │         └────┬────┘    └────┬─────┘
└────┬────┘              │              │
     │              ┌────┘              │
     ▼              ▼                   ▼
    ┌─────────────────────────────────────┐
    │            Assemble                  │
    │  다양한 해상도 + 썸네일 + 워터마크    │
    └──────────────────────────────────────┘

이 시뮬레이션에서는 실제 인코딩 대신 각 해상도별로
텍스트 파일을 생성하여 파이프라인 단계를 보여준다.
"""

from __future__ import annotations

import os
import time
from typing import Any

from redis.asyncio import Redis

from config import settings


# ---------------------------------------------------------------------------
# DAG 노드 정의 — 각 단계는 독립적 태스크이다
# ---------------------------------------------------------------------------


def _dag_split(source_path: str, work_dir: str) -> dict[str, Any]:
    """Split 단계: 원본 비디오를 세그먼트로 분할한다.

    실제로는 GOP(Group of Pictures) 단위로 비디오를 분할한다.
    시뮬레이션에서는 원본 파일의 내용을 읽어 작업 디렉토리에 복사한다.
    """
    split_dir = os.path.join(work_dir, "segments")
    os.makedirs(split_dir, exist_ok=True)

    # 원본 파일 내용 읽기
    content = b""
    if os.path.exists(source_path):
        with open(source_path, "rb") as f:
            content = f.read()

    # 세그먼트로 분할 (시뮬레이션: 단일 세그먼트)
    segment_path = os.path.join(split_dir, "segment_000.dat")
    with open(segment_path, "wb") as f:
        f.write(content)

    return {
        "stage": "split",
        "segments": [segment_path],
        "segment_count": 1,
    }


def _dag_encode(
    segments: list[str],
    work_dir: str,
    resolution: str,
) -> dict[str, Any]:
    """Encode 단계: 세그먼트를 특정 해상도로 인코딩한다.

    실제로는 H.264/H.265 코덱으로 인코딩한다:
      - 컨테이너 포맷: MP4, HLS(.m3u8 + .ts), DASH(.mpd)
      - 비디오 코덱: H.264 (호환성), H.265/HEVC (효율성), VP9, AV1
      - 오디오 코덱: AAC, Opus

    시뮬레이션에서는 해상도 정보가 포함된 텍스트 파일을 생성한다.
    """
    encode_dir = os.path.join(work_dir, "encoded")
    os.makedirs(encode_dir, exist_ok=True)

    # 해상도별 비트레이트 매핑 (실제 시스템)
    bitrate_map = {
        "360p": "800kbps",
        "720p": "2500kbps",
        "1080p": "5000kbps",
    }

    encoded_path = os.path.join(encode_dir, f"video_{resolution}.mp4")

    # 원본 세그먼트 내용을 기반으로 인코딩된 파일 생성
    original_content = b""
    for seg in segments:
        if os.path.exists(seg):
            with open(seg, "rb") as f:
                original_content = f.read()

    header = (
        f"[Encoded Video]\n"
        f"Resolution: {resolution}\n"
        f"Bitrate: {bitrate_map.get(resolution, 'unknown')}\n"
        f"Codec: H.264 (simulated)\n"
        f"Container: MP4\n"
        f"---\n"
    ).encode()

    with open(encoded_path, "wb") as f:
        f.write(header + original_content)

    return {
        "stage": "encode",
        "resolution": resolution,
        "bitrate": bitrate_map.get(resolution, "unknown"),
        "output_path": encoded_path,
    }


def _dag_thumbnail(source_path: str, work_dir: str) -> dict[str, Any]:
    """Thumbnail 단계: 비디오에서 썸네일 이미지를 추출한다.

    실제로는 비디오의 특정 프레임을 추출하여 JPEG/WebP 로 저장한다.
    시뮬레이션에서는 텍스트 파일로 썸네일을 생성한다.
    """
    thumb_dir = os.path.join(work_dir, "thumbnails")
    os.makedirs(thumb_dir, exist_ok=True)

    thumb_path = os.path.join(thumb_dir, "thumbnail.jpg")
    with open(thumb_path, "w") as f:
        f.write("[Thumbnail Image]\nExtracted from video frame at 00:00:01\n")

    return {
        "stage": "thumbnail",
        "output_path": thumb_path,
    }


def _dag_watermark(encoded_paths: list[str], work_dir: str) -> dict[str, Any]:
    """Watermark 단계: 인코딩된 비디오에 워터마크를 오버레이한다.

    DRM/저작권 보호의 일환으로 비디오에 워터마크를 삽입한다.
    시뮬레이션에서는 워터마크 메타데이터를 기록한다.
    """
    watermark_dir = os.path.join(work_dir, "watermarked")
    os.makedirs(watermark_dir, exist_ok=True)

    watermark_info = os.path.join(watermark_dir, "watermark_info.txt")
    with open(watermark_info, "w") as f:
        f.write("[Watermark Applied]\n")
        f.write(f"Timestamp: {time.time()}\n")
        for path in encoded_paths:
            f.write(f"Applied to: {os.path.basename(path)}\n")

    return {
        "stage": "watermark",
        "applied_to": [os.path.basename(p) for p in encoded_paths],
        "info_path": watermark_info,
    }


def _dag_assemble(
    work_dir: str,
    output_dir: str,
    video_id: str,
    resolutions: list[str],
) -> dict[str, Any]:
    """Assemble 단계: 모든 산출물을 최종 디렉토리로 취합한다.

    인코딩된 비디오, 썸네일, 워터마크 정보를 비디오 ID 디렉토리에 모은다.
    실제로는 CDN 에 업로드하는 단계이기도 하다.
    """
    video_output_dir = os.path.join(output_dir, video_id)
    os.makedirs(video_output_dir, exist_ok=True)

    assembled_files: list[str] = []

    # 인코딩된 파일 복사
    encode_dir = os.path.join(work_dir, "encoded")
    for res in resolutions:
        src = os.path.join(encode_dir, f"video_{res}.mp4")
        dst = os.path.join(video_output_dir, f"{res}.mp4")
        if os.path.exists(src):
            with open(src, "rb") as sf, open(dst, "wb") as df:
                df.write(sf.read())
            assembled_files.append(dst)

    # 썸네일 복사
    thumb_src = os.path.join(work_dir, "thumbnails", "thumbnail.jpg")
    thumb_dst = os.path.join(video_output_dir, "thumbnail.jpg")
    if os.path.exists(thumb_src):
        with open(thumb_src, "rb") as sf, open(thumb_dst, "wb") as df:
            df.write(sf.read())
        assembled_files.append(thumb_dst)

    return {
        "stage": "assemble",
        "video_dir": video_output_dir,
        "files": assembled_files,
    }


# ---------------------------------------------------------------------------
# DAG 실행기 — 전체 파이프라인을 순서대로 실행한다
# ---------------------------------------------------------------------------


async def transcode_video(
    redis: Redis,
    video_id: str,
    source_path: str,
) -> dict[str, Any]:
    """DAG 기반 트랜스코딩 파이프라인을 실행한다.

    파이프라인 단계:
      1. Split   — 원본을 세그먼트로 분할
      2. Encode  — 각 해상도(360p, 720p, 1080p)로 병렬 인코딩
      3. Thumbnail — 썸네일 추출 (Encode 와 병렬)
      4. Watermark — 워터마크 삽입
      5. Assemble  — 최종 산출물 취합

    Args:
        redis: Redis 클라이언트
        video_id: 비디오 ID
        source_path: 원본 비디오 파일 경로

    Returns:
        트랜스코딩 결과 (해상도 목록, 파일 경로 등)
    """
    resolutions = settings.TRANSCODE_RESOLUTIONS

    # 작업 디렉토리 설정
    work_dir = os.path.join(settings.VIDEO_STORAGE_PATH, "work", video_id)
    output_dir = os.path.join(settings.VIDEO_STORAGE_PATH, "transcoded")
    os.makedirs(work_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    # 비디오 메타데이터 상태를 'transcoding' 으로 갱신
    video_key = f"video:{video_id}"
    await redis.hset(video_key, "status", "transcoding")

    dag_results: list[dict[str, Any]] = []

    # Step 1: Split
    split_result = _dag_split(source_path, work_dir)
    dag_results.append(split_result)

    # Step 2: Encode (각 해상도별) + Thumbnail (병렬 가능)
    encode_results: list[dict[str, Any]] = []
    encoded_paths: list[str] = []
    for res in resolutions:
        enc = _dag_encode(split_result["segments"], work_dir, res)
        encode_results.append(enc)
        encoded_paths.append(enc["output_path"])
    dag_results.extend(encode_results)

    # Step 3: Thumbnail (Encode 와 독립적으로 실행 가능)
    thumb_result = _dag_thumbnail(source_path, work_dir)
    dag_results.append(thumb_result)

    # Step 4: Watermark
    watermark_result = _dag_watermark(encoded_paths, work_dir)
    dag_results.append(watermark_result)

    # Step 5: Assemble
    assemble_result = _dag_assemble(work_dir, output_dir, video_id, resolutions)
    dag_results.append(assemble_result)

    # 비디오 메타데이터를 'ready' 로 갱신
    video_output_dir = os.path.join(output_dir, video_id)
    await redis.hset(
        video_key,
        mapping={
            "status": "ready",
            "resolutions": ",".join(resolutions),
            "thumbnail": os.path.join(video_output_dir, "thumbnail.jpg"),
            "transcoded_at": str(time.time()),
        },
    )

    return {
        "video_id": video_id,
        "status": "ready",
        "resolutions": resolutions,
        "output_dir": video_output_dir,
        "dag_stages": [r["stage"] for r in dag_results],
    }
