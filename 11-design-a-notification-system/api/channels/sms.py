"""SMS channel handler (simulated).

실제 환경에서는 Twilio, Nexmo 등의 SMS API 를 호출하지만,
여기서는 로그 출력으로 시뮬레이션한다.
"""

from __future__ import annotations

import logging
import random

from config import settings

logger = logging.getLogger(__name__)


async def send_sms(
    user_id: str,
    title: str,
    body: str,
    failure_rate: float | None = None,
) -> bool:
    """SMS 를 전송한다 (시뮬레이션).

    Args:
        user_id: 대상 사용자 ID.
        title: 알림 제목.
        body: 알림 본문.
        failure_rate: 실패 확률 오버라이드 (테스트용).

    Returns:
        True 이면 전송 성공, False 이면 실패.
    """
    rate = failure_rate if failure_rate is not None else settings.FAILURE_RATE

    if random.random() < rate:
        logger.error(
            "[SMS FAILED] user=%s title='%s' (simulated failure)",
            user_id,
            title,
        )
        return False

    logger.info(
        "[SMS SENT] user=%s title='%s' body='%s'",
        user_id,
        title,
        body,
    )
    return True
