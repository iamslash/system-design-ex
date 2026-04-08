"""Notification dispatcher — routes notifications to the correct channel queue.

알림 요청을 검증(사용자 설정, rate limit, 중복)한 뒤
Redis List 기반 메시지 큐에 넣는다.
채널별 큐: queue:push, queue:sms, queue:email
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
    """Redis 에서 사용자 알림 설정을 조회한다."""
    data = await redis.get(f"preferences:{user_id}")
    if data:
        return UserPreferences.model_validate_json(data)
    return UserPreferences()  # 기본값: 모든 채널 활성화


async def save_user_preferences(
    redis: Redis, user_id: str, prefs: UserPreferences
) -> None:
    """사용자 알림 설정을 Redis 에 저장한다."""
    await redis.set(f"preferences:{user_id}", prefs.model_dump_json())


async def dispatch_notification(
    redis: Redis,
    request: NotificationRequest,
) -> dict[str, Any]:
    """알림 요청을 검증하고 적절한 채널 큐에 넣는다.

    처리 흐름:
      1. 사용자 설정 확인 (opt-out 여부)
      2. Rate limit 확인
      3. 템플릿 렌더링
      4. 알림 레코드 생성 및 Redis 에 저장
      5. 채널 큐에 LPUSH

    Args:
        redis: Redis 클라이언트.
        request: 알림 전송 요청.

    Returns:
        {"notification_id": "...", "status": "...", "message": "..."} 형태의 결과.
    """
    # 1. 사용자 설정 확인 — opt-out 채널이면 전송하지 않음
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

    # 2. Rate limit 확인
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

    # 3. 템플릿 렌더링
    rendered = render_template(request.template, request.params)

    # 4. 알림 레코드 생성
    record = NotificationRecord(
        user_id=request.user_id,
        channel=request.channel,
        template=request.template,
        params=request.params,
        priority=request.priority,
        status=NotificationStatus.PENDING,
    )

    # 5. Redis 에 알림 로그 저장 (Hash)
    # Redis HSET 은 값이 str/int/float 이어야 하므로 복합 타입은 JSON 직렬화
    record_dict = json.loads(record.model_dump_json())
    flat: dict[str, str] = {k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) for k, v in record_dict.items()}
    await redis.hset(
        f"notification:{record.notification_id}",
        mapping=flat,
    )
    # 사용자별 알림 목록에 추가
    await redis.lpush(f"user_notifications:{request.user_id}", record.notification_id)

    # 6. 채널 큐에 메시지 삽입 (LPUSH)
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
