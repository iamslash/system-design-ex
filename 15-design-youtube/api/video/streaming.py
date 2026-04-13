"""Video streaming with HTTP byte-range support.

Supports HTTP Range requests to enable video seeking.
The browser's <video> tag uses Range headers to request only specific byte ranges.

Range request flow:
  1. Client: GET /stream?range=bytes=0-999999
  2. Server: 206 Partial Content + Content-Range: bytes 0-999999/5000000
  3. Client: user seeks → GET /stream?range=bytes=3000000-3999999
  4. Server: 206 Partial Content + Content-Range: bytes 3000000-3999999/5000000

This allows playback of any desired range without downloading the entire file.
"""

from __future__ import annotations

import os
from typing import Any

from config import settings


# Default chunk size: send entire file when no Range header is present
DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1MB


def get_video_path(video_id: str, resolution: str = "720p") -> str | None:
    """Return the video file path.

    Searches in order: transcoded file → original.

    Args:
        video_id: Video ID
        resolution: Requested resolution (default: 720p)

    Returns:
        Video file path, or None if not found
    """
    # Search transcoded file first
    transcoded_path = os.path.join(
        settings.VIDEO_STORAGE_PATH, "transcoded", video_id, f"{resolution}.mp4"
    )
    if os.path.exists(transcoded_path):
        return transcoded_path

    # Fall back to original file
    original_path = os.path.join(
        settings.VIDEO_STORAGE_PATH, "originals", f"{video_id}.mp4"
    )
    if os.path.exists(original_path):
        return original_path

    return None


def parse_range_header(range_header: str | None, file_size: int) -> tuple[int, int]:
    """Parse a Range header and return the (start, end) byte range.

    Supported formats:
      - bytes=0-999        → (0, 999)
      - bytes=500-         → (500, file_size-1)
      - bytes=-500         → (file_size-500, file_size-1)
      - None (no header)   → (0, file_size-1)

    Args:
        range_header: HTTP Range header value (e.g. "bytes=0-999")
        file_size: Total file size

    Returns:
        (start, end) byte range (inclusive)
    """
    if not range_header or not range_header.startswith("bytes="):
        return 0, file_size - 1

    range_spec = range_header[6:]  # remove "bytes="
    parts = range_spec.split("-")

    if len(parts) != 2:
        return 0, file_size - 1

    start_str, end_str = parts

    if not start_str:
        # bytes=-500 → last 500 bytes
        suffix_length = int(end_str)
        start = max(0, file_size - suffix_length)
        end = file_size - 1
    elif not end_str:
        # bytes=500- → from 500 to end
        start = int(start_str)
        end = file_size - 1
    else:
        # bytes=0-999
        start = int(start_str)
        end = min(int(end_str), file_size - 1)

    # Validate range
    start = max(0, start)
    end = min(end, file_size - 1)

    if start > end:
        return 0, file_size - 1

    return start, end


def read_video_range(file_path: str, start: int, end: int) -> bytes:
    """Read the specified byte range from a video file.

    Args:
        file_path: Video file path
        start: Start byte (inclusive)
        end: End byte (inclusive)

    Returns:
        Byte data for the requested range
    """
    with open(file_path, "rb") as f:
        f.seek(start)
        length = end - start + 1
        return f.read(length)


def build_stream_response_info(
    file_path: str,
    range_header: str | None,
) -> dict[str, Any]:
    """Construct the information needed for a streaming response.

    Args:
        file_path: Video file path
        range_header: HTTP Range header value

    Returns:
        Response info including status_code, headers, data
    """
    file_size = os.path.getsize(file_path)
    start, end = parse_range_header(range_header, file_size)
    data = read_video_range(file_path, start, end)
    content_length = end - start + 1

    if range_header and range_header.startswith("bytes="):
        # 206 Partial Content
        return {
            "status_code": 206,
            "data": data,
            "headers": {
                "Content-Range": f"bytes {start}-{end}/{file_size}",
                "Accept-Ranges": "bytes",
                "Content-Length": str(content_length),
                "Content-Type": "video/mp4",
            },
        }
    else:
        # 200 OK — full file
        return {
            "status_code": 200,
            "data": data,
            "headers": {
                "Accept-Ranges": "bytes",
                "Content-Length": str(file_size),
                "Content-Type": "video/mp4",
            },
        }
