"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os


class Settings:
    """YouTube-like video streaming system settings."""

    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))

    # 비디오 파일 저장 경로
    VIDEO_STORAGE_PATH: str = os.getenv("VIDEO_STORAGE_PATH", "/data/videos")

    # 청크 업로드 최대 크기 (10MB)
    MAX_CHUNK_SIZE: int = int(os.getenv("MAX_CHUNK_SIZE", "10485760"))

    # 트랜스코딩 해상도 목록
    TRANSCODE_RESOLUTIONS: list[str] = ["360p", "720p", "1080p"]


settings = Settings()
