"""FastAPI notification server entry point.

Provides the HTTP API for the notification system and runs a background
worker (consumer).  Redis is used as both a message queue and data store.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException

from config import settings
from models import (
    BatchNotificationRequest,
    NotificationRequest,
    UserPreferences,
)
from notification.dispatcher import (
    dispatch_notification,
    get_user_preferences,
    save_user_preferences,
)
from worker.consumer import consume_queues

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# Global Redis client and worker task
redis_client: aioredis.Redis | None = None
worker_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage Redis connection and worker on application startup/shutdown."""
    global redis_client, worker_task

    # Startup: connect to Redis and launch worker
    redis_client = aioredis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        decode_responses=True,
    )
    worker_task = asyncio.create_task(consume_queues(redis_client))
    logger.info("Notification service started (Redis=%s:%d)", settings.REDIS_HOST, settings.REDIS_PORT)

    yield

    # Shutdown: stop worker and close Redis connection
    if worker_task:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
    if redis_client:
        await redis_client.aclose()
    logger.info("Notification service stopped")


app = FastAPI(
    title="Notification System",
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
    """Health check with queue stats."""
    r = _get_redis()
    push_len = await r.llen("queue:push")
    sms_len = await r.llen("queue:sms")
    email_len = await r.llen("queue:email")
    return {
        "status": "ok",
        "queues": {
            "push": push_len,
            "sms": sms_len,
            "email": email_len,
        },
    }


# ---------------------------------------------------------------------------
# Notification Endpoints
# ---------------------------------------------------------------------------


@app.post("/api/v1/notify")
async def notify(request: NotificationRequest) -> dict[str, Any]:
    """Send a notification."""
    r = _get_redis()
    result = await dispatch_notification(r, request)
    return result


@app.post("/api/v1/notify/batch")
async def notify_batch(request: BatchNotificationRequest) -> dict[str, Any]:
    """Send notifications to multiple users simultaneously."""
    r = _get_redis()
    results = []
    for user_id in request.user_ids:
        single_request = NotificationRequest(
            user_id=user_id,
            channel=request.channel,
            template=request.template,
            params=request.params,
            priority=request.priority,
        )
        result = await dispatch_notification(r, single_request)
        results.append(result)
    return {
        "total": len(request.user_ids),
        "results": results,
    }


@app.get("/api/v1/notifications/{user_id}")
async def get_notification_history(user_id: str) -> dict[str, Any]:
    """Retrieve notification history for a user."""
    r = _get_redis()
    notification_ids = await r.lrange(f"user_notifications:{user_id}", 0, 49)
    notifications = []
    for nid in notification_ids:
        data = await r.hgetall(f"notification:{nid}")
        if data:
            notifications.append(data)
    return {
        "user_id": user_id,
        "count": len(notifications),
        "notifications": notifications,
    }


@app.get("/api/v1/notifications/{notification_id}/status")
async def get_notification_status(notification_id: str) -> dict[str, Any]:
    """Retrieve the status of a specific notification."""
    r = _get_redis()
    data = await r.hgetall(f"notification:{notification_id}")
    if not data:
        raise HTTPException(status_code=404, detail="Notification not found")
    return data


# ---------------------------------------------------------------------------
# User Preferences Endpoints
# ---------------------------------------------------------------------------


@app.get("/api/v1/settings/{user_id}")
async def get_settings(user_id: str) -> dict[str, Any]:
    """Retrieve notification preferences for a user."""
    r = _get_redis()
    prefs = await get_user_preferences(r, user_id)
    return {
        "user_id": user_id,
        "preferences": prefs.model_dump(),
    }


@app.put("/api/v1/settings/{user_id}")
async def update_settings(user_id: str, prefs: UserPreferences) -> dict[str, Any]:
    """Update notification preferences for a user."""
    r = _get_redis()
    await save_user_preferences(r, user_id, prefs)
    return {
        "user_id": user_id,
        "preferences": prefs.model_dump(),
        "message": "Preferences updated",
    }
