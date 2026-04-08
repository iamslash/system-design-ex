"""FastAPI application entry point for the metrics monitoring system."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query

from alerting.notifier import Notifier
from alerting.rules import AlertRuleEngine
from collector.metrics import MetricsCollector
from config import settings
from models import (
    AlertRule,
    AlertStatus,
    MetricBatch,
    MetricPoint,
    AggregationType,
    QueryRequest,
)
from query.service import QueryService
from storage.timeseries import TimeSeriesStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global service instances (initialized during lifespan)
_redis: Optional[aioredis.Redis] = None
_storage: Optional[TimeSeriesStorage] = None
_collector: Optional[MetricsCollector] = None
_query_service: Optional[QueryService] = None
_rule_engine: Optional[AlertRuleEngine] = None
_notifier: Optional[Notifier] = None
_alert_task: Optional[asyncio.Task] = None


async def _alert_loop() -> None:
    """Background loop that periodically evaluates alert rules."""
    while True:
        try:
            assert _rule_engine is not None and _notifier is not None
            fired = await _rule_engine.evaluate_all()
            for alert in fired:
                await _notifier.notify(alert)
                logger.info("Alert fired: %s (value=%s)", alert.rule_name, alert.value)
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("Error in alert evaluation loop")
        await asyncio.sleep(settings.ALERT_CHECK_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle."""
    global _redis, _storage, _collector, _query_service, _rule_engine, _notifier, _alert_task

    _redis = aioredis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        decode_responses=True,
    )
    _storage = TimeSeriesStorage(_redis)
    _collector = MetricsCollector(_storage)
    _query_service = QueryService(_storage)
    _rule_engine = AlertRuleEngine(_redis, _storage)
    _notifier = Notifier(_redis)
    _alert_task = asyncio.create_task(_alert_loop())

    logger.info("Metrics monitoring system started (port=%s)", settings.API_PORT)
    yield

    _alert_task.cancel()
    try:
        await _alert_task
    except asyncio.CancelledError:
        pass
    await _redis.aclose()
    logger.info("Metrics monitoring system stopped")


app = FastAPI(title="Metrics Monitoring System", version="1.0.0", lifespan=lifespan)


# -----------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------

@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}


# -----------------------------------------------------------------------
# Metrics collection (push)
# -----------------------------------------------------------------------

@app.post("/api/v1/metrics")
async def push_metric(point: MetricPoint) -> dict:
    """Push a single metric data point."""
    assert _collector is not None
    await _collector.push(point)
    return {"status": "accepted", "metric": point.name}


@app.post("/api/v1/metrics/batch")
async def push_batch(batch: MetricBatch) -> dict:
    """Push a batch of metric data points."""
    assert _collector is not None
    count = await _collector.push_batch(batch)
    return {"status": "accepted", "count": count}


# -----------------------------------------------------------------------
# Query
# -----------------------------------------------------------------------

@app.get("/api/v1/query")
async def query_metrics(
    name: str,
    labels: str = Query(default="", description="Comma-separated key=value pairs"),
    start: float = Query(..., description="Start timestamp"),
    end: Optional[float] = Query(default=None, description="End timestamp"),
    aggregation: Optional[AggregationType] = Query(default=None),
    downsample: Optional[int] = Query(default=None, description="Bucket size in seconds"),
) -> dict:
    """Query time-series data with optional aggregation."""
    assert _query_service is not None
    parsed_labels = _parse_labels(labels)
    req = QueryRequest(
        name=name,
        labels=parsed_labels,
        start=start,
        end=end,
        aggregation=aggregation,
        downsample=downsample,
    )
    result = await _query_service.query(req)
    return result.model_dump()


@app.get("/api/v1/metrics/list")
async def list_metrics() -> dict:
    """List all known metric names."""
    assert _query_service is not None
    names = await _query_service.list_metrics()
    return {"metrics": names}


# -----------------------------------------------------------------------
# Alerting -- rules
# -----------------------------------------------------------------------

@app.post("/api/v1/rules")
async def create_rule(rule: AlertRule) -> dict:
    """Create an alert rule."""
    assert _rule_engine is not None
    created = await _rule_engine.add_rule(rule)
    return {"status": "created", "rule": created.model_dump()}


@app.get("/api/v1/rules")
async def get_rules() -> dict:
    """List all alert rules."""
    assert _rule_engine is not None
    rules = await _rule_engine.list_rules()
    return {"rules": [r.model_dump() for r in rules]}


@app.delete("/api/v1/rules/{rule_id}")
async def delete_rule(rule_id: str) -> dict:
    """Delete an alert rule."""
    assert _rule_engine is not None
    deleted = await _rule_engine.delete_rule(rule_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Rule not found")
    return {"status": "deleted", "rule_id": rule_id}


# -----------------------------------------------------------------------
# Alerting -- alerts
# -----------------------------------------------------------------------

@app.get("/api/v1/alerts")
async def get_alerts(status: Optional[str] = Query(default=None)) -> dict:
    """List alerts, optionally filtered by status."""
    assert _rule_engine is not None
    alert_status = AlertStatus(status) if status else None
    alerts = await _rule_engine.list_alerts(status=alert_status)
    return {"alerts": [a.model_dump() for a in alerts]}


@app.post("/api/v1/alerts/{alert_id}/resolve")
async def resolve_alert(alert_id: str) -> dict:
    """Resolve an active alert."""
    assert _rule_engine is not None
    alert = await _rule_engine.resolve_alert(alert_id)
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    return {"status": "resolved", "alert": alert.model_dump()}


@app.post("/api/v1/alerts/evaluate")
async def evaluate_alerts() -> dict:
    """Manually trigger alert evaluation."""
    assert _rule_engine is not None and _notifier is not None
    fired = await _rule_engine.evaluate_all()
    for alert in fired:
        await _notifier.notify(alert)
    return {"fired": len(fired), "alerts": [a.model_dump() for a in fired]}


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def _parse_labels(raw: str) -> dict[str, str]:
    """Parse 'key1=val1,key2=val2' into a dict."""
    if not raw:
        return {}
    result: dict[str, str] = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if "=" in pair:
            k, v = pair.split("=", 1)
            result[k.strip()] = v.strip()
    return result
