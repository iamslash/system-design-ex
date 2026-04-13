"""FastAPI application for URL shortening service."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, HttpUrl
from redis.asyncio import Redis

from config import settings
from shortener.base62 import encode as base62_encode
from shortener.hash_approach import generate_short_code as hash_generate
from shortener.id_generator import next_id
from storage.redis_store import RedisURLStore


# ---------------------------------------------------------------------------
# Redis connection & lifespan
# ---------------------------------------------------------------------------

redis_client: Redis | None = None
store: RedisURLStore | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage Redis connection lifecycle."""
    global redis_client, store
    redis_client = Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        decode_responses=True,
    )
    store = RedisURLStore(redis_client)
    yield
    await redis_client.aclose()


app = FastAPI(
    title="URL Shortener",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------


class ShortenRequest(BaseModel):
    """POST /api/v1/shorten request body."""

    url: HttpUrl


class ShortenResponse(BaseModel):
    """POST /api/v1/shorten response body."""

    short_url: str
    short_code: str


class StatsResponse(BaseModel):
    """GET /api/v1/stats/{short_code} response body."""

    short_code: str
    long_url: str
    short_url: str
    clicks: int
    created_at: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}


@app.post("/api/v1/shorten", response_model=ShortenResponse)
async def shorten(body: ShortenRequest) -> ShortenResponse:
    """Shorten a long URL.

    1. Check dedup: if the same long URL was already shortened, return it.
    2. Generate a short code using the configured approach (base62 or hash).
    3. Save the mapping to Redis.
    """
    long_url = str(body.url)

    # Dedup check: return existing code if same URL was already shortened
    existing_code = await store.get_code_by_long_url(long_url)
    if existing_code:
        return ShortenResponse(
            short_url=f"{settings.BASE_URL}/{existing_code}",
            short_code=existing_code,
        )

    # Generate short code
    if settings.SHORTENER_APPROACH == "hash":
        # Hash approach: first 7 chars of CRC32 hash + collision resolution
        short_code = hash_generate(long_url)

        # Check and resolve collisions
        attempt = 0
        candidate_url = long_url
        suffixes = ["!", "@", "#", "$", "%", "^", "&", "*"]
        while await store.code_exists(short_code):
            if attempt >= len(suffixes):
                raise HTTPException(
                    status_code=500,
                    detail="Hash collision could not be resolved",
                )
            candidate_url = long_url + suffixes[attempt]
            short_code = hash_generate(candidate_url)
            attempt += 1
    else:
        # Base62 approach: Redis counter -> Base62 encoding
        uid = await next_id(redis_client)
        short_code = base62_encode(uid)

    # Save to Redis
    await store.save(short_code, long_url)

    return ShortenResponse(
        short_url=f"{settings.BASE_URL}/{short_code}",
        short_code=short_code,
    )


@app.get("/api/v1/stats/{short_code}", response_model=StatsResponse)
async def stats(short_code: str) -> StatsResponse:
    """Get statistics for a shortened URL."""
    entry = await store.get_by_code(short_code)
    if not entry:
        raise HTTPException(status_code=404, detail="Short URL not found")

    created_dt = datetime.fromtimestamp(entry.created_at, tz=timezone.utc)

    return StatsResponse(
        short_code=entry.short_code,
        long_url=entry.long_url,
        short_url=f"{settings.BASE_URL}/{entry.short_code}",
        clicks=entry.clicks,
        created_at=created_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
    )


@app.get("/{short_code}")
async def redirect(short_code: str) -> RedirectResponse:
    """Redirect short URL to the original long URL (301).

    301 Moved Permanently: browser caches the redirect, reducing server load.
    Click count is incremented atomically in Redis.
    """
    entry = await store.get_by_code(short_code)
    if not entry:
        raise HTTPException(status_code=404, detail="Short URL not found")

    # Increment click count (async, no impact on redirect performance)
    await store.increment_clicks(short_code)

    return RedirectResponse(url=entry.long_url, status_code=301)
