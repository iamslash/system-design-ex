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
    """Redis Sorted Set 기반 메시지 저장소.

    메시지를 채널별로 저장하며, 타임스탬프를 score 로 사용하여
    시간순 정렬된 메시지 히스토리를 제공한다.

    저장 구조:
      - key: messages:{channel_id}
      - score: timestamp (float)
      - member: message JSON string
    """

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._redis = redis_client

    async def save_message(self, channel_id: str, message: dict[str, Any]) -> None:
        """메시지를 Redis Sorted Set 에 저장한다.

        Args:
            channel_id: 채널 식별자 (예: "dm:alice:bob", "group:team1")
            message: 저장할 메시지 딕셔너리 (timestamp 포함)
        """
        key = f"messages:{channel_id}"
        ts = message.get("timestamp", 0)
        await self._redis.zadd(key, {json.dumps(message): ts})

    async def get_messages(
        self, channel_id: str, limit: int = 100, offset: int = 0
    ) -> list[dict[str, Any]]:
        """채널의 최신 메시지를 조회한다.

        ZREVRANGE 를 사용하여 최신순으로 가져온 뒤 시간순(오래된 것 먼저)으로 반환한다.

        Args:
            channel_id: 채널 식별자
            limit: 가져올 최대 메시지 수 (기본 100)
            offset: 건너뛸 메시지 수 (페이지네이션)

        Returns:
            시간순 정렬된 메시지 리스트
        """
        key = f"messages:{channel_id}"
        start = offset
        end = offset + limit - 1
        raw_messages = await self._redis.zrevrange(key, start, end)
        # 최신순으로 가져왔으므로 역순으로 뒤집어 시간순 반환
        messages = [json.loads(m) for m in reversed(raw_messages)]
        return messages

    async def get_max_message_id(self, channel_id: str) -> str | None:
        """채널의 최신 메시지 ID 를 반환한다 (디바이스 간 동기화용).

        클라이언트는 이 ID 이후의 메시지만 요청하여 중복 수신을 방지한다.
        """
        key = f"messages:{channel_id}"
        latest = await self._redis.zrevrange(key, 0, 0)
        if latest:
            msg = json.loads(latest[0])
            return msg.get("message_id")
        return None
