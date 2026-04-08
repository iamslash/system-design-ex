"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os


class Settings:
    """Rate limiter settings populated from environment variables."""

    # Algorithm selection
    RATE_LIMIT_ALGORITHM: str = os.getenv("RATE_LIMIT_ALGORITHM", "token_bucket")

    # Sliding Window Counter
    RATE_LIMIT_REQUESTS: int = int(os.getenv("RATE_LIMIT_REQUESTS", "10"))
    RATE_LIMIT_WINDOW: int = int(os.getenv("RATE_LIMIT_WINDOW", "60"))

    # Token Bucket
    BUCKET_SIZE: int = int(os.getenv("BUCKET_SIZE", "10"))
    REFILL_RATE: float = float(os.getenv("REFILL_RATE", "1"))

    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))


settings = Settings()
