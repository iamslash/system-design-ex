"""Metrics collection service -- receives pushed metric data points."""

from __future__ import annotations

from models import MetricPoint, MetricBatch
from storage.timeseries import TimeSeriesStorage


class MetricsCollector:
    """Accepts metric data points and stores them in the time-series backend."""

    def __init__(self, storage: TimeSeriesStorage) -> None:
        self._storage = storage

    async def push(self, point: MetricPoint) -> None:
        """Ingest a single metric data point."""
        ts = point.effective_timestamp()
        await self._storage.add(
            metric_name=point.name,
            labels=point.labels,
            value=point.value,
            timestamp=ts,
        )

    async def push_batch(self, batch: MetricBatch) -> int:
        """Ingest a batch of metric data points. Returns count stored."""
        tuples = [
            (p.name, p.labels, p.value, p.effective_timestamp())
            for p in batch.metrics
        ]
        return await self._storage.add_batch(tuples)
