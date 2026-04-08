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
    """1:1 채팅의 채널 ID 를 생성한다.

    두 사용자를 정렬하여 동일한 쌍은 항상 같은 채널 ID 를 갖도록 한다.
    예: dm:alice:bob == dm:bob:alice
    """
    a, b = sorted([user_a, user_b])
    return f"dm:{a}:{b}"


class MessageHandler:
    """1:1 메시지와 그룹 메시지를 라우팅한다.

    메시지 흐름:
      1. 메시지 ID 생성 (IdGenerator)
      2. Redis 에 메시지 저장 (MessageStore)
      3. 수신자에게 WebSocket 으로 전달 (ConnectionManager)
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
        """1:1 메시지를 처리한다.

        1. 채널 ID 생성 (정렬된 사용자 쌍)
        2. 메시지 ID 생성 및 Redis 저장
        3. 수신자에게 실시간 전달
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

        # Redis 에 메시지 저장
        await self._store.save_message(channel_id, message_data)

        # 수신자에게 실시간 전달
        await self._conn.send_to_user(to_user, message_data)
        # 발신자의 다른 디바이스에도 전달
        await self._conn.send_to_user(from_user, message_data)

        return message_data

    async def handle_group_message(
        self, from_user: str, group_id: str, content: str
    ) -> dict[str, Any] | None:
        """그룹 메시지를 처리한다.

        1. Redis 에서 그룹 멤버 목록 조회
        2. 메시지 ID 생성 및 Redis 저장
        3. 모든 그룹 멤버에게 실시간 전달
        """
        import json as _json

        # 그룹 정보 조회
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

        # Redis 에 메시지 저장
        await self._store.save_message(channel_id, message_data)

        # 모든 그룹 멤버에게 전달
        await self._conn.broadcast(members, message_data)

        return message_data
