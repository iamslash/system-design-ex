"""Message routing handler for 1:1 and group chat.

Receives parsed WebSocket messages, generates message IDs, persists them,
and routes to the appropriate recipients via the ConnectionManager.
"""

from __future__ import annotations

import time
from typing import Any

import redis.asyncio as aioredis

from chat.connection_manager import ConnectionManager
from chat.id_generator import id_generator
from storage.message_store import MessageStore


def make_dm_channel(user_a: str, user_b: str) -> str:
    """Generate a channel ID for a 1:1 DM conversation.

    Users are sorted so the same pair always maps to the same channel ID.
    Example: dm:alice:bob == dm:bob:alice
    """
    a, b = sorted([user_a, user_b])
    return f"dm:{a}:{b}"


class MessageHandler:
    """Route 1:1 and group messages.

    Message flow:
      1. Generate message ID (IdGenerator)
      2. Persist message in Redis (MessageStore)
      3. Deliver to recipients via WebSocket (ConnectionManager)
    """

    def __init__(
        self,
        conn_manager: ConnectionManager,
        message_store: MessageStore,
        redis_client: aioredis.Redis,
    ) -> None:
        self._conn = conn_manager
        self._store = message_store
        self._redis = redis_client

    async def handle_dm(self, from_user: str, to_user: str, content: str) -> dict[str, Any]:
        """Process a 1:1 direct message.

        1. Generate channel ID (sorted user pair)
        2. Generate message ID and persist to Redis
        3. Deliver to recipient in real time
        """
        channel_id = make_dm_channel(from_user, to_user)
        msg_id = id_generator.generate()
        ts = time.time()

        message_data: dict[str, Any] = {
            "type": "message",
            "message_id": msg_id,
            "from": from_user,
            "to": to_user,
            "content": content,
            "channel_id": channel_id,
            "timestamp": ts,
        }

        # Persist message to Redis
        await self._store.save_message(channel_id, message_data)

        # Deliver to recipient in real time
        await self._conn.send_to_user(to_user, message_data)
        # Also deliver to sender's other devices
        await self._conn.send_to_user(from_user, message_data)

        return message_data

    async def handle_group_message(
        self, from_user: str, group_id: str, content: str
    ) -> dict[str, Any] | None:
        """Process a group message.

        1. Fetch group member list from Redis
        2. Generate message ID and persist to Redis
        3. Deliver to all group members in real time
        """
        import json as _json

        # Fetch group info
        group_data = await self._redis.hgetall(f"group:{group_id}")
        if not group_data:
            return None

        members: list[str] = _json.loads(group_data.get("members", "[]"))
        channel_id = f"group:{group_id}"
        msg_id = id_generator.generate()
        ts = time.time()

        message_data: dict[str, Any] = {
            "type": "group_message",
            "message_id": msg_id,
            "from": from_user,
            "group_id": group_id,
            "content": content,
            "channel_id": channel_id,
            "timestamp": ts,
        }

        # Persist message to Redis
        await self._store.save_message(channel_id, message_data)

        # Deliver to all group members
        await self._conn.broadcast(members, message_data)

        return message_data
