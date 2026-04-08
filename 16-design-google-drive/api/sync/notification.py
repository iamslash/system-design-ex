"""Long-polling notification service for file change events.

클라이언트가 /sync/poll 에 요청하면 새 이벤트가 있을 때까지
최대 POLL_TIMEOUT 초 동안 대기한다. 이벤트가 발생하면 즉시 반환한다.
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
    """동기화 이벤트를 사용자의 이벤트 큐에 발행한다.

    Args:
        redis: Redis 클라이언트
        user_id: 이벤트를 수신할 사용자 ID
        event: 이벤트 데이터
    """
    await redis.lpush(f"sync_events:{user_id}", json.dumps(event))
    # 최대 1000개 이벤트만 유지
    await redis.ltrim(f"sync_events:{user_id}", 0, 999)


async def poll_sync_events(
    redis: Redis,
    user_id: str,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """사용자의 동기화 이벤트를 long-polling 으로 조회한다.

    이미 이벤트가 있으면 즉시 반환한다.
    이벤트가 없으면 최대 timeout 초 동안 새 이벤트를 기다린다.

    Args:
        redis: Redis 클라이언트
        user_id: 사용자 ID
        timeout: 대기 시간 (초). None 이면 settings.POLL_TIMEOUT 사용.

    Returns:
        이벤트 리스트 (없으면 빈 리스트)
    """
    if timeout is None:
        timeout = settings.POLL_TIMEOUT

    key = f"sync_events:{user_id}"

    # 이미 이벤트가 있으면 즉시 반환
    events = await _drain_events(redis, key)
    if events:
        return events

    # 이벤트가 없으면 polling 으로 대기
    elapsed = 0
    poll_interval = 1  # 1초 간격으로 확인
    while elapsed < timeout:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
        events = await _drain_events(redis, key)
        if events:
            return events

    return []


async def _drain_events(redis: Redis, key: str) -> list[dict[str, Any]]:
    """큐에 있는 모든 이벤트를 꺼내 반환한다.

    RPOP 을 반복하여 큐를 비운다 (가장 오래된 이벤트부터 반환).
    """
    events: list[dict[str, Any]] = []
    while True:
        raw = await redis.rpop(key)
        if raw is None:
            break
        events.append(json.loads(raw))
    return events
