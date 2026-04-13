"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os


class Settings:
    """Notification system settings populated from environment variables."""

    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))

    # Per-channel rate limits (maximum sends per hour)
    RATE_LIMIT_PUSH: int = int(os.getenv("RATE_LIMIT_PUSH", "10"))
    RATE_LIMIT_SMS: int = int(os.getenv("RATE_LIMIT_SMS", "5"))
    RATE_LIMIT_EMAIL: int = int(os.getenv("RATE_LIMIT_EMAIL", "20"))

    # Simulated failure rate (0.0 ~ 1.0, used for retry testing)
    FAILURE_RATE: float = float(os.getenv("FAILURE_RATE", "0.1"))

    # Worker settings
    WORKER_POLL_INTERVAL: int = int(os.getenv("WORKER_POLL_INTERVAL", "1"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))


settings = Settings()
