"""FastAPI application for URL shortening service."""

from __future__ import annotations

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
async def lifespan(app: FastAPI):
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

    # 중복 확인: 같은 URL 이 이미 단축되었으면 기존 코드 반환
    existing_code = await store.get_code_by_long_url(long_url)
    if existing_code:
        return ShortenResponse(
            short_url=f"{settings.BASE_URL}/{existing_code}",
            short_code=existing_code,
        )

    # 단축 코드 생성
    if settings.SHORTENER_APPROACH == "hash":
        # 해시 방식: CRC32 해시의 앞 7자 + 충돌 해결
        short_code = hash_generate(long_url)

        # 충돌 확인 및 해결
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
        # Base62 방식: Redis 카운터 → Base62 인코딩
        uid = await next_id(redis_client)
        short_code = base62_encode(uid)

    # Redis 에 저장
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

    301 Moved Permanently: 브라우저가 리다이렉트를 캐시하므로 서버 부하가 줄어든다.
    클릭 카운트는 Redis 에서 원자적으로 증가시킨다.
    """
    entry = await store.get_by_code(short_code)
    if not entry:
        raise HTTPException(status_code=404, detail="Short URL not found")

    # 클릭 카운트 증가 (비동기, 리다이렉트 성능에 영향 없음)
    await store.increment_clicks(short_code)

    return RedirectResponse(url=entry.long_url, status_code=301)
