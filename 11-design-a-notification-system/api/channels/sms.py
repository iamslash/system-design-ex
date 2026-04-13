"""SMS channel handler (simulated).

In a real environment this would call Twilio, Nexmo, or another SMS API;
here it simulates delivery via log output.
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
    """Send an SMS (simulated).

    Args:
        user_id: Target user ID.
        title: Notification title.
        body: Notification body.
        failure_rate: Failure probability override (for testing).

    Returns:
        True if the send succeeded, False if it failed.
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
