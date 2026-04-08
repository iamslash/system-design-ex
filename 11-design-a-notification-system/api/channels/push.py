"""Push notification channel handler (simulated).

실제 환경에서는 APNs (iOS) 또는 FCM (Android) API 를 호출하지만,
여기서는 로그 출력으로 시뮬레이션한다.
설정 가능한 실패율(FAILURE_RATE)을 통해 retry 메커니즘을 테스트할 수 있다.
"""

from __future__ import annotations

import logging
import random

from config import settings

logger = logging.getLogger(__name__)


async def send_push(
    user_id: str,
    title: str,
    body: str,
    failure_rate: float | None = None,
) -> bool:
    """Push 알림을 전송한다 (시뮬레이션).

    Args:
        user_id: 대상 사용자 ID.
        title: 알림 제목.
        body: 알림 본문.
        failure_rate: 실패 확률 오버라이드 (테스트용).

    Returns:
        True 이면 전송 성공, False 이면 실패.
    """
    rate = failure_rate if failure_rate is not None else settings.FAILURE_RATE

    # 설정된 확률로 실패를 시뮬레이션
    if random.random() < rate:
        logger.error(
            "[PUSH FAILED] user=%s title='%s' (simulated failure)",
            user_id,
            title,
        )
        return False

    logger.info(
        "[PUSH SENT] user=%s title='%s' body='%s'",
        user_id,
        title,
        body,
    )
    return True
