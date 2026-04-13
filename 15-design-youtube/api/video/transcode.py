"""Video transcoding simulation with DAG model.

The actual transcoding pipeline is structured as a DAG (Directed Acyclic Graph):

    Original Video
        │
        ▼
    ┌─────────┐
    │  Split   │  ── Split video into segments
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
    │  Multiple resolutions + thumbnail + watermark    │
    └──────────────────────────────────────┘

In this simulation, text files are created for each resolution
instead of actual encoding to illustrate the pipeline stages.
"""

from __future__ import annotations

import os
import time
from typing import Any

from redis.asyncio import Redis

from config import settings


# ---------------------------------------------------------------------------
# DAG node definitions — each stage is an independent task
# ---------------------------------------------------------------------------


def _dag_split(source_path: str, work_dir: str) -> dict[str, Any]:
    """Split stage: split the original video into segments.

    In a real system, the video is split in GOP (Group of Pictures) units.
    In this simulation, the original file contents are read and copied to the work directory.
    """
    split_dir = os.path.join(work_dir, "segments")
    os.makedirs(split_dir, exist_ok=True)

    # Read original file contents
    content = b""
    if os.path.exists(source_path):
        with open(source_path, "rb") as f:
            content = f.read()

    # Split into segments (simulation: single segment)
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
    """Encode stage: encode segments to a specific resolution.

    In a real system, encodes with H.264/H.265 codec:
      - Container formats: MP4, HLS (.m3u8 + .ts), DASH (.mpd)
      - Video codecs: H.264 (compatibility), H.265/HEVC (efficiency), VP9, AV1
      - Audio codecs: AAC, Opus

    In this simulation, a text file containing resolution info is created.
    """
    encode_dir = os.path.join(work_dir, "encoded")
    os.makedirs(encode_dir, exist_ok=True)

    # Bitrate mapping per resolution (real system)
    bitrate_map = {
        "360p": "800kbps",
        "720p": "2500kbps",
        "1080p": "5000kbps",
    }

    encoded_path = os.path.join(encode_dir, f"video_{resolution}.mp4")

    # Create encoded file based on original segment contents
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
    """Thumbnail stage: extract a thumbnail image from the video.

    In a real system, a specific frame is extracted from the video and saved as JPEG/WebP.
    In this simulation, a text file is created as the thumbnail.
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
    """Watermark stage: overlay a watermark on encoded videos.

    Watermark is embedded in the video as part of DRM/copyright protection.
    In this simulation, watermark metadata is recorded.
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
    """Assemble stage: collect all outputs into the final directory.

    Gathers encoded videos, thumbnails, and watermark info into the video ID directory.
    In a real system, this stage also uploads to a CDN.
    """
    video_output_dir = os.path.join(output_dir, video_id)
    os.makedirs(video_output_dir, exist_ok=True)

    assembled_files: list[str] = []

    # Copy encoded files
    encode_dir = os.path.join(work_dir, "encoded")
    for res in resolutions:
        src = os.path.join(encode_dir, f"video_{res}.mp4")
        dst = os.path.join(video_output_dir, f"{res}.mp4")
        if os.path.exists(src):
            with open(src, "rb") as sf, open(dst, "wb") as df:
                df.write(sf.read())
            assembled_files.append(dst)

    # Copy thumbnail
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
# DAG executor — runs the full pipeline in order
# ---------------------------------------------------------------------------


async def transcode_video(
    redis: Redis,
    video_id: str,
    source_path: str,
) -> dict[str, Any]:
    """Execute the DAG-based transcoding pipeline.

    Pipeline stages:
      1. Split   — split original into segments
      2. Encode  — parallel encoding at each resolution (360p, 720p, 1080p)
      3. Thumbnail — extract thumbnail (can run in parallel with Encode)
      4. Watermark — apply watermark
      5. Assemble  — collect final outputs

    Args:
        redis: Redis client
        video_id: Video ID
        source_path: Path to the original video file

    Returns:
        Transcoding result (resolution list, file paths, etc.)
    """
    resolutions = settings.TRANSCODE_RESOLUTIONS

    # Set up work directory
    work_dir = os.path.join(settings.VIDEO_STORAGE_PATH, "work", video_id)
    output_dir = os.path.join(settings.VIDEO_STORAGE_PATH, "transcoded")
    os.makedirs(work_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    # Update video metadata status to 'transcoding'
    video_key = f"video:{video_id}"
    await redis.hset(video_key, "status", "transcoding")

    dag_results: list[dict[str, Any]] = []

    # Step 1: Split
    split_result = _dag_split(source_path, work_dir)
    dag_results.append(split_result)

    # Step 2: Encode (per resolution) + Thumbnail (can run in parallel)
    encode_results: list[dict[str, Any]] = []
    encoded_paths: list[str] = []
    for res in resolutions:
        enc = _dag_encode(split_result["segments"], work_dir, res)
        encode_results.append(enc)
        encoded_paths.append(enc["output_path"])
    dag_results.extend(encode_results)

    # Step 3: Thumbnail (can run independently of Encode)
    thumb_result = _dag_thumbnail(source_path, work_dir)
    dag_results.append(thumb_result)

    # Step 4: Watermark
    watermark_result = _dag_watermark(encoded_paths, work_dir)
    dag_results.append(watermark_result)

    # Step 5: Assemble
    assemble_result = _dag_assemble(work_dir, output_dir, video_id, resolutions)
    dag_results.append(assemble_result)

    # Update video metadata to 'ready'
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
