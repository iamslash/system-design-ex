"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os


class Settings:
    """Payment system settings populated from environment variables."""

    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    PSP_FAILURE_RATE: float = float(os.getenv("PSP_FAILURE_RATE", "0.0"))


settings = Settings()
