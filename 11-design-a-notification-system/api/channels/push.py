"""Push notification channel handler (simulated).

In a real environment this would call APNs (iOS) or FCM (Android) APIs;
here it simulates delivery via log output.
The configurable failure rate (FAILURE_RATE) allows testing the retry mechanism.
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
    """Send a push notification (simulated).

    Args:
        user_id: Target user ID.
        title: Notification title.
        body: Notification body.
        failure_rate: Failure probability override (for testing).

    Returns:
        True if the send succeeded, False if it failed.
    """
    rate = failure_rate if failure_rate is not None else settings.FAILURE_RATE

    # Simulate failure at the configured probability
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
