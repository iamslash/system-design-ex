"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os


class Settings:
    """Notification system settings populated from environment variables."""

    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))

    # 채널별 rate limit (시간당 최대 전송 수)
    RATE_LIMIT_PUSH: int = int(os.getenv("RATE_LIMIT_PUSH", "10"))
    RATE_LIMIT_SMS: int = int(os.getenv("RATE_LIMIT_SMS", "5"))
    RATE_LIMIT_EMAIL: int = int(os.getenv("RATE_LIMIT_EMAIL", "20"))

    # 시뮬레이션 실패율 (0.0 ~ 1.0, retry 테스트용)
    FAILURE_RATE: float = float(os.getenv("FAILURE_RATE", "0.1"))

    # Worker 설정
    WORKER_POLL_INTERVAL: int = int(os.getenv("WORKER_POLL_INTERVAL", "1"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))


settings = Settings()
