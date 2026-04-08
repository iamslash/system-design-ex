"""Queue consumer (worker) — pulls messages from Redis queues and sends via channel handlers.

각 채널 큐(queue:push, queue:sms, queue:email)에서 BRPOP 으로 메시지를 꺼내
해당 채널 핸들러를 통해 전송한다.
실패 시 exponential backoff 로 재시도하며, 최대 MAX_RETRIES 회까지 시도한다.
중복 처리 방지를 위해 notification_id 를 확인한다.
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

# 채널별 전송 함수 매핑
CHANNEL_HANDLERS = {
    "push": send_push,
    "sms": send_sms,
    "email": send_email,
}

# 모니터링할 큐 목록
QUEUES = ["queue:push", "queue:sms", "queue:email"]


async def update_notification_status(
    redis: Redis,
    notification_id: str,
    status: str,
    retry_count: int | None = None,
) -> None:
    """Redis 에 저장된 알림 레코드의 상태를 갱신한다."""
    key = f"notification:{notification_id}"
    updates: dict[str, Any] = {
        "status": status,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if retry_count is not None:
        updates["retry_count"] = str(retry_count)
    await redis.hset(key, mapping=updates)


async def is_duplicate(redis: Redis, notification_id: str) -> bool:
    """이미 처리 완료(sent/delivered)된 알림인지 확인한다 (중복 방지)."""
    key = f"notification:{notification_id}"
    status = await redis.hget(key, "status")
    return status in ("sent", "delivered")


async def process_message(redis: Redis, raw_message: str) -> None:
    """큐에서 꺼낸 메시지 하나를 처리한다.

    처리 흐름:
      1. 메시지 파싱
      2. 중복 확인 (이미 sent/delivered 이면 스킵)
      3. 채널 핸들러 호출
      4. 성공 → 상태를 sent 로 갱신
      5. 실패 → retry_count < MAX_RETRIES 이면 exponential backoff 후 재큐잉
              → 초과 시 상태를 failed 로 갱신
    """
    message: dict[str, Any] = json.loads(raw_message)
    notification_id = message["notification_id"]
    channel = message["channel"]
    user_id = message["user_id"]
    title = message["title"]
    body = message["body"]
    retry_count = message.get("retry_count", 0)

    # 1. 중복 확인
    if await is_duplicate(redis, notification_id):
        logger.info(
            "Skipping duplicate notification %s (already processed)",
            notification_id,
        )
        return

    # 2. 채널 핸들러 호출
    handler = CHANNEL_HANDLERS.get(channel)
    if handler is None:
        logger.error("Unknown channel: %s", channel)
        await update_notification_status(redis, notification_id, "failed")
        return

    success = await handler(user_id, title, body)

    if success:
        # 3a. 전송 성공
        await update_notification_status(redis, notification_id, "sent", retry_count)
        # 이벤트 추적: sent 타임스탬프 기록
        await redis.hset(
            f"notification:{notification_id}",
            "sent_at",
            datetime.now(timezone.utc).isoformat(),
        )
        logger.info("Notification %s sent successfully", notification_id)
    else:
        # 3b. 전송 실패 → retry 또는 failed
        retry_count += 1
        if retry_count <= settings.MAX_RETRIES:
            # Exponential backoff: 2^(retry_count-1) 초 대기 후 재큐잉
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
            # 재큐잉: retry_count 를 증가시켜 다시 큐에 넣음
            message["retry_count"] = retry_count
            await redis.lpush(f"queue:{channel}", json.dumps(message))
        else:
            # 최대 재시도 초과 → failed
            logger.error(
                "Notification %s failed after %d retries, marking as failed",
                notification_id,
                settings.MAX_RETRIES,
            )
            await update_notification_status(
                redis, notification_id, "failed", retry_count
            )


async def consume_queues(redis: Redis) -> None:
    """모든 채널 큐에서 메시지를 지속적으로 소비하는 워커 루프.

    BRPOP 으로 블로킹 대기하며, 메시지가 도착하면 process_message 를 호출한다.
    API 서비스 내 백그라운드 태스크로 실행된다.
    """
    logger.info("Worker started — listening on queues: %s", QUEUES)
    while True:
        try:
            # BRPOP: 여러 큐를 동시에 모니터링, timeout 초 대기
            result = await redis.brpop(QUEUES, timeout=settings.WORKER_POLL_INTERVAL)
            if result is None:
                # 타임아웃 — 메시지 없음, 다시 대기
                continue
            _queue_name, raw_message = result
            await process_message(redis, raw_message)
        except asyncio.CancelledError:
            logger.info("Worker shutting down...")
            break
        except Exception:
            logger.exception("Worker error — will retry in 1s")
            await asyncio.sleep(1)
