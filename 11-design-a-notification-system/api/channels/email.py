"""Email channel handler (simulated).

In a real environment this would call an email API such as SendGrid, Mailgun,
or Amazon SES.  Here we simulate delivery with log output.
"""

from __future__ import annotations

import logging
import random

from config import settings

logger = logging.getLogger(__name__)


async def send_email(
    user_id: str,
    title: str,
    body: str,
    failure_rate: float | None = None,
) -> bool:
    """Send an email (simulated).

    Args:
        user_id: Target user ID.
        title: Notification subject line.
        body: Notification body text.
        failure_rate: Override failure probability (for testing).

    Returns:
        True if delivery succeeded, False otherwise.
    """
    rate = failure_rate if failure_rate is not None else settings.FAILURE_RATE

    if random.random() < rate:
        logger.error(
            "[EMAIL FAILED] user=%s subject='%s' (simulated failure)",
            user_id,
            title,
        )
        return False

    logger.info(
        "[EMAIL SENT] user=%s subject='%s' body='%s'",
        user_id,
        title,
        body,
    )
    return True
