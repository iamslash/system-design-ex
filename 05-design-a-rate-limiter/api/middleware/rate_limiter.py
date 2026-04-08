"""Rate limiter middleware for FastAPI."""

from __future__ import annotations

import redis.asyncio as aioredis
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from algorithms.sliding_window_counter import SlidingWindowCounter
from algorithms.token_bucket import TokenBucket
from config import settings

# Paths that bypass rate limiting
_UNLIMITED_PATHS: set[str] = {"/health", "/api/unlimited", "/api/config", "/docs", "/openapi.json"}


class RateLimiterMiddleware(BaseHTTPMiddleware):
    """ASGI middleware that applies per-IP rate limiting."""

    def __init__(self, app: object) -> None:
        super().__init__(app)  # type: ignore[arg-type]
        self._redis: aioredis.Redis | None = None
        self._algorithm: TokenBucket | SlidingWindowCounter | None = None

    async def _get_redis(self) -> aioredis.Redis:
        if self._redis is None:
            self._redis = aioredis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                decode_responses=True,
            )
        return self._redis

    async def _get_algorithm(self) -> TokenBucket | SlidingWindowCounter:
        if self._algorithm is None:
            r = await self._get_redis()
            if settings.RATE_LIMIT_ALGORITHM == "sliding_window_counter":
                self._algorithm = SlidingWindowCounter(
                    redis_client=r,
                    max_requests=settings.RATE_LIMIT_REQUESTS,
                    window_size=settings.RATE_LIMIT_WINDOW,
                )
            else:
                self._algorithm = TokenBucket(
                    redis_client=r,
                    bucket_size=settings.BUCKET_SIZE,
                    refill_rate=settings.REFILL_RATE,
                )
        return self._algorithm

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP from X-Forwarded-For header or remote address."""
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()
        if request.client:
            return request.client.host
        return "unknown"

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        """Check rate limit before forwarding the request."""
        if request.url.path in _UNLIMITED_PATHS:
            return await call_next(request)

        client_ip = self._get_client_ip(request)
        algorithm = await self._get_algorithm()
        result = await algorithm.is_allowed(client_ip)

        if not result.allowed:
            return JSONResponse(
                status_code=429,
                content={
                    "detail": f"Rate limit exceeded. Try again in {result.retry_after} seconds.",
                },
                headers={
                    "X-Ratelimit-Limit": str(result.limit),
                    "X-Ratelimit-Remaining": "0",
                    "X-Ratelimit-Retry-After": str(result.retry_after),
                },
            )

        response = await call_next(request)
        response.headers["X-Ratelimit-Limit"] = str(result.limit)
        response.headers["X-Ratelimit-Remaining"] = str(result.remaining)
        return response
