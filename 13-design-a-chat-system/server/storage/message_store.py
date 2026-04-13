"""Message persistence layer using Redis Sorted Sets.

Each channel has a sorted set where messages are scored by timestamp,
enabling efficient retrieval of the latest N messages.

Redis key pattern:
  messages:{channel_id} -> sorted set (score=timestamp, member=message JSON)
"""

from __future__ import annotations

import json
from typing import Any

import redis.asyncio as aioredis


class MessageStore:
    """Redis Sorted Set-based message storage.

    Messages are stored per channel using the timestamp as the score,
    providing a time-ordered message history.

    Storage structure:
      - key: messages:{channel_id}
      - score: timestamp (float)
      - member: message JSON string
    """

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._redis = redis_client

    async def save_message(self, channel_id: str, message: dict[str, Any]) -> None:
        """Save a message to a Redis Sorted Set.

        Args:
            channel_id: Channel identifier (e.g. "dm:alice:bob", "group:team1")
            message: Message dictionary to store (must include a timestamp field)
        """
        key = f"messages:{channel_id}"
        ts = message.get("timestamp", 0)
        await self._redis.zadd(key, {json.dumps(message): ts})

    async def get_messages(
        self, channel_id: str, limit: int = 100, offset: int = 0
    ) -> list[dict[str, Any]]:
        """Retrieve the latest messages for a channel.

        Uses ZREVRANGE to fetch messages in reverse-chronological order, then
        reverses the result so messages are returned in chronological order
        (oldest first).

        Args:
            channel_id: Channel identifier
            limit: Maximum number of messages to return (default 100)
            offset: Number of messages to skip for pagination

        Returns:
            List of messages sorted in chronological order
        """
        key = f"messages:{channel_id}"
        start = offset
        end = offset + limit - 1
        raw_messages = await self._redis.zrevrange(key, start, end)
        # Fetched in reverse order, so reverse again to return chronologically
        messages = [json.loads(m) for m in reversed(raw_messages)]
        return messages

    async def get_max_message_id(self, channel_id: str) -> str | None:
        """Return the latest message ID for a channel (used for cross-device sync).

        Clients request only messages after this ID to avoid receiving duplicates.
        """
        key = f"messages:{channel_id}"
        latest = await self._redis.zrevrange(key, 0, 0)
        if latest:
            msg = json.loads(latest[0])
            return msg.get("message_id")
        return None
