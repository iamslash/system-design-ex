"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os


class Settings:
    """S3-like object storage settings populated from environment variables."""

    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))

    # Data directory for append-only object files
    DATA_DIR: str = os.getenv("DATA_DIR", "/tmp/s3data")

    # API port
    API_PORT: int = int(os.getenv("API_PORT", "8025"))

    # Max append-only file size before rotation (bytes): 64 MB
    MAX_FILE_SIZE: int = int(os.getenv("MAX_FILE_SIZE", str(64 * 1024 * 1024)))


settings = Settings()
