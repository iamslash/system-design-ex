"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os


class Settings:
    """Google Drive file sync service settings."""

    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))

    # Block size in bytes — files are split into chunks of this size
    BLOCK_SIZE: int = int(os.getenv("BLOCK_SIZE", "4096"))

    # Long-polling timeout in seconds
    POLL_TIMEOUT: int = int(os.getenv("POLL_TIMEOUT", "30"))

    # Directory path for block storage
    BLOCK_STORAGE_PATH: str = os.getenv("BLOCK_STORAGE_PATH", "/data/blocks")


settings = Settings()
