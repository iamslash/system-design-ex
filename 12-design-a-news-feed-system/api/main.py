"""FastAPI news feed server entry point.

뉴스 피드 시스템의 HTTP API 를 제공한다.
Redis 를 캐시 겸 저장소로 사용한다.
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from typing import Any

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException

from config import settings
from feed.publisher import create_post, get_post
from feed.retrieval import get_feed
from models import CreatePostRequest, CreateUserRequest, FollowRequest, UnfollowRequest
from social.graph import follow, get_followers, get_following, unfollow

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# 전역 Redis 클라이언트
redis_client: aioredis.Redis | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 시작/종료 시 Redis 연결을 관리한다."""
    global redis_client

    redis_client = aioredis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        decode_responses=True,
    )
    logger.info("News feed service started (Redis=%s:%d)", settings.REDIS_HOST, settings.REDIS_PORT)

    yield

    if redis_client:
        await redis_client.aclose()
    logger.info("News feed service stopped")


app = FastAPI(
    title="News Feed System",
    version="1.0.0",
    lifespan=lifespan,
)


def _get_redis() -> aioredis.Redis:
    """Redis 클라이언트를 반환한다. 연결되지 않았으면 503 을 발생시킨다."""
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis not connected")
    return redis_client


# ---------------------------------------------------------------------------
# Health Check
# ---------------------------------------------------------------------------


@app.get("/health")
async def health() -> dict[str, Any]:
    """Health check."""
    r = _get_redis()
    info = await r.info("server")
    return {
        "status": "ok",
        "redis_version": info.get("redis_version", "unknown"),
    }


# ---------------------------------------------------------------------------
# User Endpoints
# ---------------------------------------------------------------------------


@app.post("/api/v1/users")
async def create_user(request: CreateUserRequest) -> dict[str, Any]:
    """사용자를 생성한다."""
    r = _get_redis()
    user_key = f"user:{request.user_id}"

    exists = await r.exists(user_key)
    if exists:
        raise HTTPException(status_code=409, detail="User already exists")

    await r.hset(
        user_key,
        mapping={
            "user_id": request.user_id,
            "name": request.name,
            "created_at": str(time.time()),
        },
    )
    return {"status": "ok", "user_id": request.user_id, "name": request.name}


# ---------------------------------------------------------------------------
# Post Endpoints
# ---------------------------------------------------------------------------


@app.post("/api/v1/posts")
async def api_create_post(request: CreatePostRequest) -> dict[str, Any]:
    """포스트를 생성한다."""
    r = _get_redis()
    result = await create_post(r, request.user_id, request.content)
    return result


@app.get("/api/v1/posts/{post_id}")
async def api_get_post(post_id: str) -> dict[str, Any]:
    """포스트를 조회한다."""
    r = _get_redis()
    post = await get_post(r, post_id)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return post


# ---------------------------------------------------------------------------
# Feed Endpoints
# ---------------------------------------------------------------------------


@app.get("/api/v1/feed/{user_id}")
async def api_get_feed(user_id: str, offset: int = 0, limit: int = 20) -> dict[str, Any]:
    """사용자의 뉴스 피드를 조회한다."""
    r = _get_redis()
    feed_items = await get_feed(r, user_id, offset=offset, limit=limit)
    return {
        "user_id": user_id,
        "count": len(feed_items),
        "feed": feed_items,
    }


# ---------------------------------------------------------------------------
# Social Endpoints
# ---------------------------------------------------------------------------


@app.post("/api/v1/follow")
async def api_follow(request: FollowRequest) -> dict[str, Any]:
    """사용자를 팔로우한다."""
    r = _get_redis()
    result = await follow(r, request.follower_id, request.followee_id)
    return result


@app.post("/api/v1/unfollow")
async def api_unfollow(request: UnfollowRequest) -> dict[str, Any]:
    """사용자를 언팔로우한다."""
    r = _get_redis()
    result = await unfollow(r, request.follower_id, request.followee_id)
    return result


@app.get("/api/v1/friends/{user_id}")
async def api_friends(user_id: str) -> dict[str, Any]:
    """사용자의 친구 목록(팔로잉/팔로워)을 조회한다."""
    r = _get_redis()
    following_list = await get_following(r, user_id)
    followers_list = await get_followers(r, user_id)
    return {
        "user_id": user_id,
        "following": following_list,
        "following_count": len(following_list),
        "followers": followers_list,
        "followers_count": len(followers_list),
    }
