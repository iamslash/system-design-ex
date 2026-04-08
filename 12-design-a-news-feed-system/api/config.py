"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os


class Settings:
    """News feed system settings populated from environment variables."""

    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))

    # 피드에 보관할 최대 포스트 수
    FEED_MAX_SIZE: int = int(os.getenv("FEED_MAX_SIZE", "200"))


settings = Settings()
