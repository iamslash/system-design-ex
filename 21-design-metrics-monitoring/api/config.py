"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os


class Settings:
    """Metrics monitoring settings populated from environment variables."""

    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))

    # API
    API_PORT: int = int(os.getenv("API_PORT", "8021"))

    # Alerting
    ALERT_CHECK_INTERVAL: int = int(os.getenv("ALERT_CHECK_INTERVAL", "10"))
    ALERT_EVALUATION_WINDOW: int = int(os.getenv("ALERT_EVALUATION_WINDOW", "60"))


settings = Settings()
