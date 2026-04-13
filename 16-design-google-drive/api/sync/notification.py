"""Long-polling notification service for file change events.

When a client requests /sync/poll, it waits up to POLL_TIMEOUT seconds
for new events. Returns immediately when an event occurs.
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from redis.asyncio import Redis

from config import settings


async def publish_sync_event(
    redis: Redis,
    user_id: str,
    event: dict[str, Any],
) -> None:
    """Publish a sync event to a user's event queue.

    Args:
        redis: Redis client
        user_id: ID of the user who will receive the event
        event: Event data
    """
    await redis.lpush(f"sync_events:{user_id}", json.dumps(event))
    # Keep at most 1000 events
    await redis.ltrim(f"sync_events:{user_id}", 0, 999)


async def poll_sync_events(
    redis: Redis,
    user_id: str,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """Retrieve a user's sync events via long-polling.

    Returns immediately if events are already available.
    Otherwise waits up to timeout seconds for new events.

    Args:
        redis: Redis client
        user_id: User ID
        timeout: Wait duration in seconds. Uses settings.POLL_TIMEOUT if None.

    Returns:
        List of events (empty list if none)
    """
    if timeout is None:
        timeout = settings.POLL_TIMEOUT

    key = f"sync_events:{user_id}"

    # Return immediately if events are already available
    events = await _drain_events(redis, key)
    if events:
        return events

    # No events yet — poll at intervals until timeout
    elapsed = 0
    poll_interval = 1  # check every 1 second
    while elapsed < timeout:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
        events = await _drain_events(redis, key)
        if events:
            return events

    return []


async def _drain_events(redis: Redis, key: str) -> list[dict[str, Any]]:
    """Drain and return all events from the queue.

    Repeatedly calls RPOP to empty the queue (returns oldest events first).
    """
    events: list[dict[str, Any]] = []
    while True:
        raw = await redis.rpop(key)
        if raw is None:
            break
        events.append(json.loads(raw))
    return events
