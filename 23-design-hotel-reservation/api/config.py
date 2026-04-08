"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os


class Settings:
    """Hotel reservation settings populated from environment variables."""

    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))

    # API
    API_PORT: int = int(os.getenv("API_PORT", "8023"))

    # Overbooking: 1.1 means 110% of total_inventory can be reserved
    OVERBOOKING_RATIO: float = float(os.getenv("OVERBOOKING_RATIO", "1.1"))


settings = Settings()
