"""Queue consumer (worker) — pulls messages from Redis queues and sends via channel handlers.

Uses BRPOP to dequeue messages from each channel queue (queue:push, queue:sms, queue:email)
and delivers them via the corresponding channel handler.
On failure, retries with exponential backoff up to MAX_RETRIES times.
Checks notification_id to prevent duplicate processing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from redis.asyncio import Redis

from channels.email import send_email
from channels.push import send_push
from channels.sms import send_sms
from config import settings

logger = logging.getLogger(__name__)

# Mapping of channel names to their send functions
CHANNEL_HANDLERS = {
    "push": send_push,
    "sms": send_sms,
    "email": send_email,
}

# List of queues to monitor
QUEUES = ["queue:push", "queue:sms", "queue:email"]


async def update_notification_status(
    redis: Redis,
    notification_id: str,
    status: str,
    retry_count: int | None = None,
) -> None:
    """Update the status of a notification record stored in Redis."""
    key = f"notification:{notification_id}"
    updates: dict[str, Any] = {
        "status": status,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if retry_count is not None:
        updates["retry_count"] = str(retry_count)
    await redis.hset(key, mapping=updates)


async def is_duplicate(redis: Redis, notification_id: str) -> bool:
    """Check whether a notification has already been processed (sent/delivered) to prevent duplicates."""
    key = f"notification:{notification_id}"
    status = await redis.hget(key, "status")
    return status in ("sent", "delivered")


async def process_message(redis: Redis, raw_message: str) -> None:
    """Process a single message dequeued from a channel queue.

    Processing flow:
      1. Parse the message
      2. Check for duplicates (skip if already sent/delivered)
      3. Call the channel handler
      4. On success — update status to sent
      5. On failure — if retry_count < MAX_RETRIES, wait with exponential backoff and requeue
                    — if exceeded, update status to failed
    """
    message: dict[str, Any] = json.loads(raw_message)
    notification_id = message["notification_id"]
    channel = message["channel"]
    user_id = message["user_id"]
    title = message["title"]
    body = message["body"]
    retry_count = message.get("retry_count", 0)

    # 1. Check for duplicate
    if await is_duplicate(redis, notification_id):
        logger.info(
            "Skipping duplicate notification %s (already processed)",
            notification_id,
        )
        return

    # 2. Call the channel handler
    handler = CHANNEL_HANDLERS.get(channel)
    if handler is None:
        logger.error("Unknown channel: %s", channel)
        await update_notification_status(redis, notification_id, "failed")
        return

    success = await handler(user_id, title, body)

    if success:
        # 3a. Send succeeded
        await update_notification_status(redis, notification_id, "sent", retry_count)
        # Event tracking: record sent timestamp
        await redis.hset(
            f"notification:{notification_id}",
            "sent_at",
            datetime.now(timezone.utc).isoformat(),
        )
        logger.info("Notification %s sent successfully", notification_id)
    else:
        # 3b. Send failed — retry or mark as failed
        retry_count += 1
        if retry_count <= settings.MAX_RETRIES:
            # Exponential backoff: wait 2^(retry_count-1) seconds before requeuing
            backoff = 2 ** (retry_count - 1)
            logger.warning(
                "Notification %s failed (attempt %d/%d), retrying in %ds...",
                notification_id,
                retry_count,
                settings.MAX_RETRIES,
                backoff,
            )
            await update_notification_status(
                redis, notification_id, "pending", retry_count
            )
            await asyncio.sleep(backoff)
            # Requeue: increment retry_count and push back to the queue
            message["retry_count"] = retry_count
            await redis.lpush(f"queue:{channel}", json.dumps(message))
        else:
            # Max retries exceeded — mark as failed
            logger.error(
                "Notification %s failed after %d retries, marking as failed",
                notification_id,
                settings.MAX_RETRIES,
            )
            await update_notification_status(
                redis, notification_id, "failed", retry_count
            )


async def consume_queues(redis: Redis) -> None:
    """Worker loop that continuously consumes messages from all channel queues.

    Blocks on BRPOP waiting for messages, then calls process_message when one arrives.
    Runs as a background task within the API service.
    """
    logger.info("Worker started — listening on queues: %s", QUEUES)
    while True:
        try:
            # BRPOP: monitor multiple queues simultaneously, wait up to timeout seconds
            result = await redis.brpop(QUEUES, timeout=settings.WORKER_POLL_INTERVAL)
            if result is None:
                # Timeout — no message, wait again
                continue
            _queue_name, raw_message = result
            await process_message(redis, raw_message)
        except asyncio.CancelledError:
            logger.info("Worker shutting down...")
            break
        except Exception:
            logger.exception("Worker error — will retry in 1s")
            await asyncio.sleep(1)
