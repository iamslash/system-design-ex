"""Notification dispatcher — routes notifications to the correct channel queue.

Validates notification requests (user preferences, rate limit, dedup) then
pushes them into Redis List-based message queues.
Per-channel queues: queue:push, queue:sms, queue:email
"""

from __future__ import annotations

import json
import logging
from typing import Any

from redis.asyncio import Redis

from models import (
    Channel,
    NotificationRecord,
    NotificationRequest,
    NotificationStatus,
    UserPreferences,
)
from notification.rate_limiter import check_rate_limit
from notification.template import render_template

logger = logging.getLogger(__name__)


async def get_user_preferences(redis: Redis, user_id: str) -> UserPreferences:
    """Retrieve user notification preferences from Redis."""
    data = await redis.get(f"preferences:{user_id}")
    if data:
        return UserPreferences.model_validate_json(data)
    return UserPreferences()  # Default: all channels enabled


async def save_user_preferences(
    redis: Redis, user_id: str, prefs: UserPreferences
) -> None:
    """Save user notification preferences to Redis."""
    await redis.set(f"preferences:{user_id}", prefs.model_dump_json())


async def dispatch_notification(
    redis: Redis,
    request: NotificationRequest,
) -> dict[str, Any]:
    """Validate a notification request and push it to the appropriate channel queue.

    Processing flow:
      1. Check user preferences (opt-out status)
      2. Check rate limit
      3. Render template
      4. Create notification record and store in Redis
      5. LPUSH to channel queue

    Args:
        redis: Redis client.
        request: Notification send request.

    Returns:
        Result dict with keys {"notification_id": "...", "status": "...", "message": "..."}.
    """
    # 1. Check user preferences — skip if the channel is opted out
    prefs = await get_user_preferences(redis, request.user_id)
    channel_enabled = getattr(prefs, request.channel.value, True)
    if not channel_enabled:
        logger.info(
            "User %s opted out of %s notifications",
            request.user_id,
            request.channel.value,
        )
        return {
            "notification_id": None,
            "status": "skipped",
            "message": f"User opted out of {request.channel.value} notifications",
        }

    # 2. Check rate limit
    allowed = await check_rate_limit(redis, request.user_id, request.channel.value)
    if not allowed:
        logger.warning(
            "Rate limit exceeded for user %s on channel %s",
            request.user_id,
            request.channel.value,
        )
        return {
            "notification_id": None,
            "status": "rate_limited",
            "message": f"Rate limit exceeded for {request.channel.value} channel",
        }

    # 3. Render template
    rendered = render_template(request.template, request.params)

    # 4. Create notification record
    record = NotificationRecord(
        user_id=request.user_id,
        channel=request.channel,
        template=request.template,
        params=request.params,
        priority=request.priority,
        status=NotificationStatus.PENDING,
    )

    # 5. Store notification log in Redis (Hash)
    # Redis HSET requires str/int/float values, so complex types are JSON-serialized
    record_dict = json.loads(record.model_dump_json())
    flat: dict[str, str] = {k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) for k, v in record_dict.items()}
    await redis.hset(
        f"notification:{record.notification_id}",
        mapping=flat,
    )
    # Append to the per-user notification list
    await redis.lpush(f"user_notifications:{request.user_id}", record.notification_id)

    # 6. Insert message into channel queue (LPUSH)
    queue_name = f"queue:{request.channel.value}"
    message = json.dumps(
        {
            "notification_id": record.notification_id,
            "user_id": request.user_id,
            "channel": request.channel.value,
            "title": rendered["title"],
            "body": rendered["body"],
            "priority": request.priority.value,
            "retry_count": 0,
        }
    )
    await redis.lpush(queue_name, message)
    logger.info(
        "Dispatched notification %s to %s queue",
        record.notification_id,
        queue_name,
    )

    return {
        "notification_id": record.notification_id,
        "status": "pending",
        "message": f"Notification queued to {request.channel.value}",
    }
