"""Video streaming with HTTP byte-range support.

HTTP Range 요청을 지원하여 비디오 시킹(seeking)을 가능하게 한다.
브라우저의 <video> 태그는 Range 헤더를 사용하여 특정 구간만 요청한다.

Range 요청 흐름:
  1. 클라이언트: GET /stream?range=bytes=0-999999
  2. 서버: 206 Partial Content + Content-Range: bytes 0-999999/5000000
  3. 클라이언트: 사용자가 시킹 → GET /stream?range=bytes=3000000-3999999
  4. 서버: 206 Partial Content + Content-Range: bytes 3000000-3999999/5000000

이 방식으로 전체 파일을 다운로드하지 않고도 원하는 구간을 재생할 수 있다.
"""

from __future__ import annotations

import os
from typing import Any

from config import settings


# 기본 청크 크기: Range 헤더가 없을 때 전체 파일을 전송
DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1MB


def get_video_path(video_id: str, resolution: str = "720p") -> str | None:
    """비디오 파일 경로를 반환한다.

    트랜스코딩된 파일 → 원본 순으로 탐색한다.

    Args:
        video_id: 비디오 ID
        resolution: 요청 해상도 (기본: 720p)

    Returns:
        비디오 파일 경로, 없으면 None
    """
    # 트랜스코딩된 파일 우선 탐색
    transcoded_path = os.path.join(
        settings.VIDEO_STORAGE_PATH, "transcoded", video_id, f"{resolution}.mp4"
    )
    if os.path.exists(transcoded_path):
        return transcoded_path

    # 원본 파일 폴백
    original_path = os.path.join(
        settings.VIDEO_STORAGE_PATH, "originals", f"{video_id}.mp4"
    )
    if os.path.exists(original_path):
        return original_path

    return None


def parse_range_header(range_header: str | None, file_size: int) -> tuple[int, int]:
    """Range 헤더를 파싱하여 (start, end) 바이트 범위를 반환한다.

    지원하는 형식:
      - bytes=0-999        → (0, 999)
      - bytes=500-         → (500, file_size-1)
      - bytes=-500         → (file_size-500, file_size-1)
      - None (헤더 없음)   → (0, file_size-1)

    Args:
        range_header: HTTP Range 헤더 값 (예: "bytes=0-999")
        file_size: 전체 파일 크기

    Returns:
        (start, end) 바이트 범위 (inclusive)
    """
    if not range_header or not range_header.startswith("bytes="):
        return 0, file_size - 1

    range_spec = range_header[6:]  # "bytes=" 제거
    parts = range_spec.split("-")

    if len(parts) != 2:
        return 0, file_size - 1

    start_str, end_str = parts

    if not start_str:
        # bytes=-500 → 마지막 500바이트
        suffix_length = int(end_str)
        start = max(0, file_size - suffix_length)
        end = file_size - 1
    elif not end_str:
        # bytes=500- → 500부터 끝까지
        start = int(start_str)
        end = file_size - 1
    else:
        # bytes=0-999
        start = int(start_str)
        end = min(int(end_str), file_size - 1)

    # 범위 유효성 검증
    start = max(0, start)
    end = min(end, file_size - 1)

    if start > end:
        return 0, file_size - 1

    return start, end


def read_video_range(file_path: str, start: int, end: int) -> bytes:
    """비디오 파일에서 지정된 바이트 범위를 읽는다.

    Args:
        file_path: 비디오 파일 경로
        start: 시작 바이트 (inclusive)
        end: 끝 바이트 (inclusive)

    Returns:
        요청 범위의 바이트 데이터
    """
    with open(file_path, "rb") as f:
        f.seek(start)
        length = end - start + 1
        return f.read(length)


def build_stream_response_info(
    file_path: str,
    range_header: str | None,
) -> dict[str, Any]:
    """스트리밍 응답에 필요한 정보를 구성한다.

    Args:
        file_path: 비디오 파일 경로
        range_header: HTTP Range 헤더 값

    Returns:
        status_code, headers, data 등의 응답 정보
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
        # 200 OK — 전체 파일
        return {
            "status_code": 200,
            "data": data,
            "headers": {
                "Accept-Ranges": "bytes",
                "Content-Length": str(file_size),
                "Content-Type": "video/mp4",
            },
        }
