"""Alert notification channels (simulated).

In production these would integrate with real email/webhook/PagerDuty
services. Here we log the notification and store it in Redis so tests
can verify delivery.
"""

from __future__ import annotations

import json
import logging
import time

import redis.asyncio as aioredis

from models import Alert

logger = logging.getLogger(__name__)

NOTIFICATIONS_KEY = "alerting:notifications"


class Notifier:
    """Send alert notifications through configured channels."""

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._redis = redis_client

    async def notify(self, alert: Alert) -> list[dict]:
        """Dispatch notifications for an alert on all its channels.

        Returns a list of notification records (one per channel).
        """
        records: list[dict] = []
        for channel in alert.notification_channels:
            record = await self._send(channel, alert)
            records.append(record)
        return records

    async def _send(self, channel: str, alert: Alert) -> dict:
        """Simulate sending through a specific channel."""
        record = {
            "channel": channel,
            "alert_id": alert.id,
            "rule_name": alert.rule_name,
            "metric_name": alert.metric_name,
            "value": alert.value,
            "threshold": alert.threshold,
            "severity": alert.severity.value,
            "status": alert.status.value,
            "sent_at": time.time(),
        }

        if channel == "email":
            logger.info(
                "[EMAIL] Alert '%s' (%s): %s = %s (threshold %s)",
                alert.rule_name,
                alert.severity.value,
                alert.metric_name,
                alert.value,
                alert.threshold,
            )
        elif channel == "webhook":
            logger.info(
                "[WEBHOOK] POST alert payload for '%s' -> %s",
                alert.rule_name,
                json.dumps(record),
            )
        elif channel == "slack":
            logger.info(
                "[SLACK] #alerts: %s is %s (value=%s, threshold=%s)",
                alert.rule_name,
                alert.status.value,
                alert.value,
                alert.threshold,
            )
        else:
            logger.warning("[UNKNOWN CHANNEL] %s for alert %s", channel, alert.id)

        # Persist for verification / history
        await self._redis.rpush(NOTIFICATIONS_KEY, json.dumps(record))
        return record

    async def list_notifications(self, limit: int = 100) -> list[dict]:
        """Return recent notification records."""
        raw = await self._redis.lrange(NOTIFICATIONS_KEY, -limit, -1)
        return [json.loads(r) for r in raw]
