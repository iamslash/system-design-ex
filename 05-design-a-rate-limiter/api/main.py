"""FastAPI application entry point with rate limiter middleware."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from config import settings
from middleware.rate_limiter import RateLimiterMiddleware

app = FastAPI(title="Rate Limiter Example", version="1.0.0")

app.add_middleware(RateLimiterMiddleware)


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/api/limited")
async def limited(request: Request) -> dict[str, str]:
    """Rate-limited endpoint for testing."""
    return {"message": "This endpoint is rate-limited.", "client_ip": request.client.host if request.client else "unknown"}


@app.get("/api/unlimited")
async def unlimited() -> dict[str, str]:
    """Unlimited endpoint for comparison."""
    return {"message": "This endpoint has no rate limit."}


@app.get("/api/config")
async def config() -> dict[str, object]:
    """Show current rate limit configuration."""
    return {
        "algorithm": settings.RATE_LIMIT_ALGORITHM,
        "rate_limit_requests": settings.RATE_LIMIT_REQUESTS,
        "rate_limit_window": settings.RATE_LIMIT_WINDOW,
        "bucket_size": settings.BUCKET_SIZE,
        "refill_rate": settings.REFILL_RATE,
        "redis_host": settings.REDIS_HOST,
        "redis_port": settings.REDIS_PORT,
    }


@app.exception_handler(429)
async def rate_limit_handler(request: Request, exc: Exception) -> JSONResponse:
    """Custom handler for 429 responses."""
    return JSONResponse(
        status_code=429,
        content={"detail": str(exc)},
    )
