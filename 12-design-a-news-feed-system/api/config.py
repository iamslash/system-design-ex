"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os


class Settings:
    """News feed system settings populated from environment variables."""

    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))

    # Maximum number of posts to retain in a feed
    FEED_MAX_SIZE: int = int(os.getenv("FEED_MAX_SIZE", "200"))


settings = Settings()
