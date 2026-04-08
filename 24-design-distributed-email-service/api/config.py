"""Application configuration loaded from environment variables."""

import os


REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
API_PORT: int = int(os.getenv("API_PORT", "8024"))

# SMTP settings (simulated by default)
SMTP_HOST: str = os.getenv("SMTP_HOST", "smtp.example.com")
SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER: str = os.getenv("SMTP_USER", "noreply@example.com")
SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "changeme")

# Worker polling interval in seconds
WORKER_POLL_INTERVAL: float = float(os.getenv("WORKER_POLL_INTERVAL", "1.0"))
