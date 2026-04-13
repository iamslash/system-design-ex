"""FastAPI news feed server entry point.

Provides the HTTP API for the news feed system.
Redis is used as both a cache and data store.
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncGenerator
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

# Global Redis client
redis_client: aioredis.Redis | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage Redis connection on application startup/shutdown."""
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
    """Return the Redis client, raising 503 if not connected."""
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
    """Create a user."""
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
    """Create a post."""
    r = _get_redis()
    result = await create_post(r, request.user_id, request.content)
    return result


@app.get("/api/v1/posts/{post_id}")
async def api_get_post(post_id: str) -> dict[str, Any]:
    """Retrieve a post by ID."""
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
    """Retrieve the news feed for a user."""
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
    """Follow a user."""
    r = _get_redis()
    result = await follow(r, request.follower_id, request.followee_id)
    return result


@app.post("/api/v1/unfollow")
async def api_unfollow(request: UnfollowRequest) -> dict[str, Any]:
    """Unfollow a user."""
    r = _get_redis()
    result = await unfollow(r, request.follower_id, request.followee_id)
    return result


@app.get("/api/v1/friends/{user_id}")
async def api_friends(user_id: str) -> dict[str, Any]:
    """Retrieve the friends list (following/followers) for a user."""
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
