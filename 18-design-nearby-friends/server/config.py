"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os


class Settings:
    """Nearby friends system settings populated from environment variables."""

    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))

    # Location
    LOCATION_TTL: int = int(os.getenv("LOCATION_TTL", "60"))
    NEARBY_RADIUS_MILES: float = float(os.getenv("NEARBY_RADIUS_MILES", "5"))


settings = Settings()
