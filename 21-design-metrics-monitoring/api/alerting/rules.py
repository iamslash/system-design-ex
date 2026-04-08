"""Alert rule evaluation engine.

Rules compare aggregated metric values against thresholds using the
configured comparison operator over an evaluation window.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Optional

import redis.asyncio as aioredis

from models import (
    Alert,
    AlertRule,
    AlertSeverity,
    AlertStatus,
    ComparisonOperator,
)
from storage.timeseries import TimeSeriesStorage


RULES_KEY = "alerting:rules"
ALERTS_KEY = "alerting:alerts"


def _compare(value: float, operator: ComparisonOperator, threshold: float) -> bool:
    """Evaluate a comparison expression."""
    if operator == ComparisonOperator.GT:
        return value > threshold
    if operator == ComparisonOperator.GTE:
        return value >= threshold
    if operator == ComparisonOperator.LT:
        return value < threshold
    if operator == ComparisonOperator.LTE:
        return value <= threshold
    if operator == ComparisonOperator.EQ:
        return value == threshold
    if operator == ComparisonOperator.NEQ:
        return value != threshold
    raise ValueError(f"Unknown operator: {operator}")


class AlertRuleEngine:
    """Manage alert rules and evaluate them against time-series data."""

    def __init__(
        self,
        redis_client: aioredis.Redis,
        storage: TimeSeriesStorage,
    ) -> None:
        self._redis = redis_client
        self._storage = storage

    # ------------------------------------------------------------------
    # Rule CRUD
    # ------------------------------------------------------------------

    async def add_rule(self, rule: AlertRule) -> AlertRule:
        """Persist a new alert rule and return it with a generated id."""
        rule.id = rule.id or str(uuid.uuid4())[:8]
        await self._redis.hset(RULES_KEY, rule.id, rule.model_dump_json())
        return rule

    async def get_rule(self, rule_id: str) -> Optional[AlertRule]:
        raw = await self._redis.hget(RULES_KEY, rule_id)
        if raw is None:
            return None
        return AlertRule.model_validate_json(raw)

    async def list_rules(self) -> list[AlertRule]:
        raw = await self._redis.hgetall(RULES_KEY)
        return [AlertRule.model_validate_json(v) for v in raw.values()]

    async def delete_rule(self, rule_id: str) -> bool:
        return bool(await self._redis.hdel(RULES_KEY, rule_id))

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------

    async def evaluate_rule(self, rule: AlertRule) -> Optional[Alert]:
        """Evaluate a single rule against recent data.

        Returns an Alert if the rule fires, otherwise None.
        """
        now = time.time()
        start = now - rule.duration
        points = await self._storage.query_range(
            metric_name=rule.metric_name,
            labels=rule.labels,
            start=start,
            end=now,
        )

        if not points:
            return None

        values = [p["value"] for p in points]
        avg_value = sum(values) / len(values)

        if _compare(avg_value, rule.operator, rule.threshold):
            alert = Alert(
                id=str(uuid.uuid4())[:8],
                rule_id=rule.id or "",
                rule_name=rule.name,
                metric_name=rule.metric_name,
                labels=rule.labels,
                status=AlertStatus.FIRING,
                value=round(avg_value, 4),
                threshold=rule.threshold,
                severity=rule.severity,
                fired_at=now,
                notification_channels=rule.notification_channels,
            )
            await self._redis.hset(ALERTS_KEY, alert.id, alert.model_dump_json())
            return alert

        return None

    async def evaluate_all(self) -> list[Alert]:
        """Evaluate every registered rule. Returns newly fired alerts."""
        rules = await self.list_rules()
        fired: list[Alert] = []
        for rule in rules:
            alert = await self.evaluate_rule(rule)
            if alert is not None:
                fired.append(alert)
        return fired

    # ------------------------------------------------------------------
    # Active alerts
    # ------------------------------------------------------------------

    async def list_alerts(self, status: Optional[AlertStatus] = None) -> list[Alert]:
        raw = await self._redis.hgetall(ALERTS_KEY)
        alerts = [Alert.model_validate_json(v) for v in raw.values()]
        if status is not None:
            alerts = [a for a in alerts if a.status == status]
        return sorted(alerts, key=lambda a: a.fired_at, reverse=True)

    async def resolve_alert(self, alert_id: str) -> Optional[Alert]:
        raw = await self._redis.hget(ALERTS_KEY, alert_id)
        if raw is None:
            return None
        alert = Alert.model_validate_json(raw)
        alert.status = AlertStatus.RESOLVED
        alert.resolved_at = time.time()
        await self._redis.hset(ALERTS_KEY, alert.id, alert.model_dump_json())
        return alert
