"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os


class Settings:
    """Google Drive file sync service settings."""

    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))

    # 블록 크기 (바이트) — 파일을 이 크기로 분할한다
    BLOCK_SIZE: int = int(os.getenv("BLOCK_SIZE", "4096"))

    # Long polling 타임아웃 (초)
    POLL_TIMEOUT: int = int(os.getenv("POLL_TIMEOUT", "30"))

    # 블록 저장 경로
    BLOCK_STORAGE_PATH: str = os.getenv("BLOCK_STORAGE_PATH", "/data/blocks")


settings = Settings()
