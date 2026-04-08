"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os


class Settings:
    """URL shortener settings populated from environment variables."""

    # Shortening approach: "base62" or "hash"
    SHORTENER_APPROACH: str = os.getenv("SHORTENER_APPROACH", "base62")

    # Base URL for generated short links
    BASE_URL: str = os.getenv("BASE_URL", "http://localhost:8009")

    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))


settings = Settings()
