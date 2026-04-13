"""Tests for the metrics monitoring system (20+ tests)."""

from __future__ import annotations

import sys
import os
import time

import fakeredis.aioredis
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

from storage.timeseries import TimeSeriesStorage, label_hash, ts_key
from collector.metrics import MetricsCollector
from query.service import QueryService
from alerting.rules import AlertRuleEngine, _compare
from alerting.notifier import Notifier
from models import (
    AggregationType,
    AlertRule,
    AlertSeverity,
    AlertStatus,
    ComparisonOperator,
    MetricBatch,
    MetricPoint,
    QueryRequest,
)


# ---------------------------------------------------------------------------
# Time-series storage
# ---------------------------------------------------------------------------


class TestTimeSeriesStorage:
    """Tests for Redis sorted-set time-series storage."""

    @pytest.mark.asyncio
    async def test_add_and_query_single_point(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """A single stored point should be retrievable by range query."""
        storage = TimeSeriesStorage(redis_client)
        now = time.time()
        await storage.add("cpu.load", {"host": "s1"}, 0.75, now)

        points = await storage.query_range("cpu.load", {"host": "s1"}, now - 1, now + 1)
        assert len(points) == 1
        assert points[0]["value"] == 0.75

    @pytest.mark.asyncio
    async def test_add_multiple_points(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Multiple points with different timestamps should all be stored."""
        storage = TimeSeriesStorage(redis_client)
        base = time.time()
        for i in range(5):
            await storage.add("mem.used", {"host": "s1"}, float(i), base + i)

        points = await storage.query_range("mem.used", {"host": "s1"}, base - 1, base + 10)
        assert len(points) == 5

    @pytest.mark.asyncio
    async def test_query_range_filters_by_time(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Range query should only return points within [start, end]."""
        storage = TimeSeriesStorage(redis_client)
        base = 1000000.0
        for i in range(10):
            await storage.add("disk.io", {}, float(i), base + i)

        points = await storage.query_range("disk.io", {}, base + 3, base + 6)
        assert len(points) == 4
        values = [p["value"] for p in points]
        assert values == [3.0, 4.0, 5.0, 6.0]

    @pytest.mark.asyncio
    async def test_query_latest(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """query_latest should return the N most recent points."""
        storage = TimeSeriesStorage(redis_client)
        base = time.time()
        for i in range(10):
            await storage.add("net.rx", {}, float(i), base + i)

        latest = await storage.query_latest("net.rx", {}, count=3)
        assert len(latest) == 3
        assert latest[-1]["value"] == 9.0

    @pytest.mark.asyncio
    async def test_label_isolation(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Different label sets should be stored in separate time-series."""
        storage = TimeSeriesStorage(redis_client)
        now = time.time()
        await storage.add("cpu.load", {"host": "s1"}, 0.5, now)
        await storage.add("cpu.load", {"host": "s2"}, 0.9, now)

        s1 = await storage.query_range("cpu.load", {"host": "s1"}, now - 1, now + 1)
        s2 = await storage.query_range("cpu.load", {"host": "s2"}, now - 1, now + 1)
        assert len(s1) == 1 and s1[0]["value"] == 0.5
        assert len(s2) == 1 and s2[0]["value"] == 0.9

    @pytest.mark.asyncio
    async def test_batch_add(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Batch add should store all points efficiently."""
        storage = TimeSeriesStorage(redis_client)
        base = time.time()
        tuples = [("cpu.load", {"host": "s1"}, float(i), base + i) for i in range(5)]
        count = await storage.add_batch(tuples)
        assert count == 5

        points = await storage.query_range("cpu.load", {"host": "s1"}, base - 1, base + 10)
        assert len(points) == 5

    @pytest.mark.asyncio
    async def test_list_metrics(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """list_metrics should return all known metric names."""
        storage = TimeSeriesStorage(redis_client)
        now = time.time()
        await storage.add("cpu.load", {}, 0.5, now)
        await storage.add("mem.used", {}, 1024, now)

        names = await storage.list_metrics()
        assert "cpu.load" in names
        assert "mem.used" in names

    @pytest.mark.asyncio
    async def test_delete_range(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """delete_range should remove points within the given window."""
        storage = TimeSeriesStorage(redis_client)
        base = 2000000.0
        for i in range(5):
            await storage.add("temp", {}, float(i), base + i)

        removed = await storage.delete_range("temp", {}, base, base + 2)
        assert removed == 3  # points at base+0, base+1, base+2

        remaining = await storage.query_range("temp", {}, base - 1, base + 10)
        assert len(remaining) == 2


# ---------------------------------------------------------------------------
# Downsampling
# ---------------------------------------------------------------------------


class TestDownsampling:
    """Tests for time-series downsampling."""

    @pytest.mark.asyncio
    async def test_downsample_basic(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Downsampling should aggregate points into fixed buckets."""
        storage = TimeSeriesStorage(redis_client)
        # Use a base aligned to a 60s boundary to get predictable buckets
        base = 1000020.0  # aligned so first 3 points fall in [1000020, 1000080)
        # 6 points: base+0, base+20, base+40, base+60, base+80, base+100
        for i in range(6):
            await storage.add("cpu.load", {}, float(i + 1), base + i * 20)

        buckets = await storage.downsample("cpu.load", {}, bucket_seconds=60, start=base - 1, end=base + 200)
        assert len(buckets) == 2
        # First bucket [1000020, 1000080): values 1,2,3 -> avg=2.0
        assert buckets[0]["count"] == 3
        assert buckets[0]["avg"] == pytest.approx(2.0)
        assert buckets[0]["max"] == 3.0
        assert buckets[0]["min"] == 1.0

    @pytest.mark.asyncio
    async def test_downsample_single_bucket(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """All points in one bucket should produce a single result."""
        storage = TimeSeriesStorage(redis_client)
        base = 1000000.0
        for i in range(4):
            await storage.add("mem", {}, 100.0 + i, base + i)

        buckets = await storage.downsample("mem", {}, bucket_seconds=60, start=base - 1, end=base + 10)
        assert len(buckets) == 1
        assert buckets[0]["count"] == 4
        assert buckets[0]["sum"] == pytest.approx(406.0)

    @pytest.mark.asyncio
    async def test_downsample_empty(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Downsampling with no data should return empty."""
        storage = TimeSeriesStorage(redis_client)
        buckets = await storage.downsample("nonexistent", {}, bucket_seconds=60, start=0, end=9999999)
        assert buckets == []


# ---------------------------------------------------------------------------
# Metrics collector
# ---------------------------------------------------------------------------


class TestMetricsCollector:
    """Tests for the push-based metrics collector."""

    @pytest.mark.asyncio
    async def test_push_single(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Pushing a single metric should store it."""
        storage = TimeSeriesStorage(redis_client)
        collector = MetricsCollector(storage)
        now = time.time()
        point = MetricPoint(name="cpu.load", labels={"host": "s1"}, value=0.75, timestamp=now)
        await collector.push(point)

        data = await storage.query_range("cpu.load", {"host": "s1"}, now - 1, now + 1)
        assert len(data) == 1
        assert data[0]["value"] == 0.75

    @pytest.mark.asyncio
    async def test_push_batch(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Pushing a batch should store all points."""
        storage = TimeSeriesStorage(redis_client)
        collector = MetricsCollector(storage)
        now = time.time()
        batch = MetricBatch(metrics=[
            MetricPoint(name="cpu.load", labels={"host": "s1"}, value=float(i), timestamp=now + i)
            for i in range(5)
        ])
        count = await collector.push_batch(batch)
        assert count == 5

    @pytest.mark.asyncio
    async def test_push_auto_timestamp(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Omitting timestamp should auto-fill with current time."""
        storage = TimeSeriesStorage(redis_client)
        collector = MetricsCollector(storage)
        before = time.time()
        point = MetricPoint(name="auto.ts", labels={}, value=42.0)
        await collector.push(point)
        after = time.time()

        data = await storage.query_range("auto.ts", {}, before - 1, after + 1)
        assert len(data) == 1
        assert before <= data[0]["timestamp"] <= after


# ---------------------------------------------------------------------------
# Query service
# ---------------------------------------------------------------------------


class TestQueryService:
    """Tests for the query service with aggregation."""

    @pytest.mark.asyncio
    async def test_query_avg(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """AVG aggregation should compute the mean."""
        storage = TimeSeriesStorage(redis_client)
        qs = QueryService(storage)
        base = 5000000.0
        for i, v in enumerate([10.0, 20.0, 30.0]):
            await storage.add("test.metric", {}, v, base + i)

        req = QueryRequest(name="test.metric", labels={}, start=base - 1, end=base + 10, aggregation=AggregationType.AVG)
        result = await qs.query(req)
        assert result.aggregated_value == pytest.approx(20.0)

    @pytest.mark.asyncio
    async def test_query_max(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """MAX aggregation should return the highest value."""
        storage = TimeSeriesStorage(redis_client)
        qs = QueryService(storage)
        base = 5000000.0
        for i, v in enumerate([5.0, 15.0, 10.0]):
            await storage.add("test.max", {}, v, base + i)

        req = QueryRequest(name="test.max", labels={}, start=base - 1, end=base + 10, aggregation=AggregationType.MAX)
        result = await qs.query(req)
        assert result.aggregated_value == 15.0

    @pytest.mark.asyncio
    async def test_query_min(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """MIN aggregation should return the lowest value."""
        storage = TimeSeriesStorage(redis_client)
        qs = QueryService(storage)
        base = 5000000.0
        for i, v in enumerate([5.0, 15.0, 10.0]):
            await storage.add("test.min", {}, v, base + i)

        req = QueryRequest(name="test.min", labels={}, start=base - 1, end=base + 10, aggregation=AggregationType.MIN)
        result = await qs.query(req)
        assert result.aggregated_value == 5.0

    @pytest.mark.asyncio
    async def test_query_sum(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """SUM aggregation should add all values."""
        storage = TimeSeriesStorage(redis_client)
        qs = QueryService(storage)
        base = 5000000.0
        for i, v in enumerate([1.0, 2.0, 3.0]):
            await storage.add("test.sum", {}, v, base + i)

        req = QueryRequest(name="test.sum", labels={}, start=base - 1, end=base + 10, aggregation=AggregationType.SUM)
        result = await qs.query(req)
        assert result.aggregated_value == pytest.approx(6.0)

    @pytest.mark.asyncio
    async def test_query_count(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """COUNT aggregation should return the number of points."""
        storage = TimeSeriesStorage(redis_client)
        qs = QueryService(storage)
        base = 5000000.0
        for i, v in enumerate([1.0, 2.0, 3.0, 4.0]):
            await storage.add("test.count", {}, v, base + i)

        req = QueryRequest(name="test.count", labels={}, start=base - 1, end=base + 10, aggregation=AggregationType.COUNT)
        result = await qs.query(req)
        assert result.aggregated_value == 4.0

    @pytest.mark.asyncio
    async def test_query_with_downsample(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Query with downsample should return bucketed results."""
        storage = TimeSeriesStorage(redis_client)
        qs = QueryService(storage)
        base = 1000000.0
        for i in range(10):
            await storage.add("ds.metric", {}, float(i), base + i * 10)

        req = QueryRequest(name="ds.metric", labels={}, start=base - 1, end=base + 200, downsample=60)
        result = await qs.query(req)
        assert len(result.data_points) >= 1
        assert "avg" in result.data_points[0]


# ---------------------------------------------------------------------------
# Alert rule evaluation
# ---------------------------------------------------------------------------


class TestAlertRules:
    """Tests for alert rule engine."""

    @pytest.mark.asyncio
    async def test_add_and_list_rules(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Rules should be persistable and listable."""
        storage = TimeSeriesStorage(redis_client)
        engine = AlertRuleEngine(redis_client, storage)

        rule = AlertRule(
            name="High CPU",
            metric_name="cpu.load",
            operator=ComparisonOperator.GT,
            threshold=0.8,
        )
        created = await engine.add_rule(rule)
        assert created.id is not None

        rules = await engine.list_rules()
        assert len(rules) == 1
        assert rules[0].name == "High CPU"

    @pytest.mark.asyncio
    async def test_delete_rule(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Deleting a rule should remove it."""
        storage = TimeSeriesStorage(redis_client)
        engine = AlertRuleEngine(redis_client, storage)

        rule = AlertRule(
            name="Temp Rule",
            metric_name="cpu.load",
            operator=ComparisonOperator.GT,
            threshold=0.5,
        )
        created = await engine.add_rule(rule)
        assert await engine.delete_rule(created.id)
        assert len(await engine.list_rules()) == 0

    @pytest.mark.asyncio
    async def test_rule_fires_on_threshold_breach(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """An alert should fire when the metric exceeds the threshold."""
        storage = TimeSeriesStorage(redis_client)
        engine = AlertRuleEngine(redis_client, storage)
        now = time.time()

        # Push high values
        for i in range(5):
            await storage.add("cpu.load", {"host": "s1"}, 0.95, now - i)

        rule = AlertRule(
            name="High CPU",
            metric_name="cpu.load",
            labels={"host": "s1"},
            operator=ComparisonOperator.GT,
            threshold=0.8,
            duration=60,
            severity=AlertSeverity.CRITICAL,
        )
        await engine.add_rule(rule)
        alert = await engine.evaluate_rule(rule)
        assert alert is not None
        assert alert.status == AlertStatus.FIRING
        assert alert.value > 0.8

    @pytest.mark.asyncio
    async def test_rule_does_not_fire_below_threshold(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """No alert when metric is below threshold."""
        storage = TimeSeriesStorage(redis_client)
        engine = AlertRuleEngine(redis_client, storage)
        now = time.time()

        for i in range(5):
            await storage.add("cpu.load", {}, 0.3, now - i)

        rule = AlertRule(
            name="High CPU",
            metric_name="cpu.load",
            operator=ComparisonOperator.GT,
            threshold=0.8,
        )
        alert = await engine.evaluate_rule(rule)
        assert alert is None

    @pytest.mark.asyncio
    async def test_evaluate_all(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """evaluate_all should check every registered rule."""
        storage = TimeSeriesStorage(redis_client)
        engine = AlertRuleEngine(redis_client, storage)
        now = time.time()

        await storage.add("cpu.load", {}, 0.95, now)
        await storage.add("mem.used", {}, 50.0, now)

        await engine.add_rule(AlertRule(
            name="High CPU", metric_name="cpu.load",
            operator=ComparisonOperator.GT, threshold=0.8,
        ))
        await engine.add_rule(AlertRule(
            name="Low Mem", metric_name="mem.used",
            operator=ComparisonOperator.LT, threshold=100.0,
        ))

        fired = await engine.evaluate_all()
        assert len(fired) == 2

    @pytest.mark.asyncio
    async def test_resolve_alert(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Resolving an alert should update its status."""
        storage = TimeSeriesStorage(redis_client)
        engine = AlertRuleEngine(redis_client, storage)
        now = time.time()

        await storage.add("cpu.load", {}, 0.95, now)
        rule = AlertRule(
            name="High CPU", metric_name="cpu.load",
            operator=ComparisonOperator.GT, threshold=0.8,
        )
        await engine.add_rule(rule)
        alert = await engine.evaluate_rule(rule)
        assert alert is not None

        resolved = await engine.resolve_alert(alert.id)
        assert resolved is not None
        assert resolved.status == AlertStatus.RESOLVED
        assert resolved.resolved_at is not None


# ---------------------------------------------------------------------------
# Alert triggering / notifications
# ---------------------------------------------------------------------------


class TestAlertTriggering:
    """Tests for alert notification delivery."""

    @pytest.mark.asyncio
    async def test_notify_email(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Email notification should be recorded."""
        storage = TimeSeriesStorage(redis_client)
        engine = AlertRuleEngine(redis_client, storage)
        notifier = Notifier(redis_client)
        now = time.time()

        await storage.add("cpu.load", {}, 0.95, now)
        rule = AlertRule(
            name="High CPU", metric_name="cpu.load",
            operator=ComparisonOperator.GT, threshold=0.8,
            notification_channels=["email"],
        )
        await engine.add_rule(rule)
        alert = await engine.evaluate_rule(rule)
        assert alert is not None

        records = await notifier.notify(alert)
        assert len(records) == 1
        assert records[0]["channel"] == "email"

    @pytest.mark.asyncio
    async def test_notify_multiple_channels(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Notifications should go to all configured channels."""
        storage = TimeSeriesStorage(redis_client)
        engine = AlertRuleEngine(redis_client, storage)
        notifier = Notifier(redis_client)
        now = time.time()

        await storage.add("cpu.load", {}, 0.99, now)
        rule = AlertRule(
            name="CPU Critical", metric_name="cpu.load",
            operator=ComparisonOperator.GT, threshold=0.8,
            notification_channels=["email", "webhook", "slack"],
        )
        await engine.add_rule(rule)
        alert = await engine.evaluate_rule(rule)
        assert alert is not None

        records = await notifier.notify(alert)
        assert len(records) == 3
        channels = {r["channel"] for r in records}
        assert channels == {"email", "webhook", "slack"}

    @pytest.mark.asyncio
    async def test_notification_history(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Sent notifications should be retrievable from history."""
        storage = TimeSeriesStorage(redis_client)
        engine = AlertRuleEngine(redis_client, storage)
        notifier = Notifier(redis_client)
        now = time.time()

        await storage.add("disk.full", {}, 95.0, now)
        rule = AlertRule(
            name="Disk Full", metric_name="disk.full",
            operator=ComparisonOperator.GT, threshold=90.0,
            notification_channels=["email"],
        )
        await engine.add_rule(rule)
        alert = await engine.evaluate_rule(rule)
        assert alert is not None
        await notifier.notify(alert)

        history = await notifier.list_notifications()
        assert len(history) >= 1
        assert history[-1]["alert_id"] == alert.id


# ---------------------------------------------------------------------------
# Comparison operators
# ---------------------------------------------------------------------------


class TestComparisonOperators:
    """Tests for the _compare helper."""

    def test_gt(self) -> None:
        assert _compare(10.0, ComparisonOperator.GT, 5.0) is True
        assert _compare(5.0, ComparisonOperator.GT, 10.0) is False

    def test_gte(self) -> None:
        assert _compare(5.0, ComparisonOperator.GTE, 5.0) is True
        assert _compare(4.0, ComparisonOperator.GTE, 5.0) is False

    def test_lt(self) -> None:
        assert _compare(3.0, ComparisonOperator.LT, 5.0) is True
        assert _compare(5.0, ComparisonOperator.LT, 3.0) is False

    def test_lte(self) -> None:
        assert _compare(5.0, ComparisonOperator.LTE, 5.0) is True
        assert _compare(6.0, ComparisonOperator.LTE, 5.0) is False

    def test_eq(self) -> None:
        assert _compare(5.0, ComparisonOperator.EQ, 5.0) is True
        assert _compare(5.0, ComparisonOperator.EQ, 6.0) is False

    def test_neq(self) -> None:
        assert _compare(5.0, ComparisonOperator.NEQ, 6.0) is True
        assert _compare(5.0, ComparisonOperator.NEQ, 5.0) is False


# ---------------------------------------------------------------------------
# Label hashing / key format
# ---------------------------------------------------------------------------


class TestKeyFormat:
    """Tests for label hashing and key generation."""

    def test_label_hash_deterministic(self) -> None:
        """Same labels should always produce the same hash."""
        h1 = label_hash({"host": "s1", "region": "us"})
        h2 = label_hash({"region": "us", "host": "s1"})
        assert h1 == h2

    def test_label_hash_empty(self) -> None:
        assert label_hash({}) == "_"

    def test_ts_key_format(self) -> None:
        key = ts_key("cpu.load", {"host": "s1"})
        assert key.startswith("ts:cpu.load:")
        assert len(key) > len("ts:cpu.load:")
