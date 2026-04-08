"""FastAPI application entry point for the gaming leaderboard."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException

from config import settings
from models import LeaderboardEntry, ScoreRequest, ScoreResponse, UserRankResponse
from leaderboard.service import LeaderboardService


# ---------------------------------------------------------------------------
# Application lifespan — manage Redis connection pool
# ---------------------------------------------------------------------------

_redis: aioredis.Redis | None = None
_service: LeaderboardService | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open and close the Redis connection pool around the app lifetime."""
    global _redis, _service
    _redis = aioredis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        decode_responses=True,
    )
    _service = LeaderboardService(_redis)
    yield
    await _redis.aclose()


app = FastAPI(title="Gaming Leaderboard", version="1.0.0", lifespan=lifespan)


def _svc() -> LeaderboardService:
    """Return the singleton service; raises if the app is not started."""
    assert _service is not None, "Application not started"
    return _service


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Leaderboard endpoints
# ---------------------------------------------------------------------------


@app.post("/v1/scores", response_model=ScoreResponse, status_code=201)
async def score_point(body: ScoreRequest) -> ScoreResponse:
    """Increment a user's score."""
    svc = _svc()
    new_score = await svc.score_point(body.user_id, body.points)
    return ScoreResponse(
        user_id=body.user_id,
        new_score=new_score,
        leaderboard_key=svc.current_key(),
    )


@app.get("/v1/scores", response_model=list[LeaderboardEntry])
async def top_scores() -> list[LeaderboardEntry]:
    """Return the top 10 leaderboard."""
    entries = await _svc().top(10)
    return [LeaderboardEntry(**e) for e in entries]


@app.get("/v1/scores/{user_id}", response_model=UserRankResponse)
async def user_rank(user_id: str) -> UserRankResponse:
    """Return a user's rank and score."""
    data = await _svc().user_rank(user_id)
    if data["rank"] is None:
        raise HTTPException(status_code=404, detail=f"User '{user_id}' not found on leaderboard")
    return UserRankResponse(**data)


@app.get("/v1/scores/{user_id}/around", response_model=list[LeaderboardEntry])
async def around_user(user_id: str) -> list[LeaderboardEntry]:
    """Return 4 entries above and 4 below the user."""
    entries = await _svc().around_user(user_id, span=4)
    if not entries:
        raise HTTPException(status_code=404, detail=f"User '{user_id}' not found on leaderboard")
    return [LeaderboardEntry(**e) for e in entries]
