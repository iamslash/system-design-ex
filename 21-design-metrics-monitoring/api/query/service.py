"""Query service -- range queries with aggregation and downsampling."""

from __future__ import annotations

import time
from typing import Optional

from models import AggregationType, QueryRequest, QueryResult
from storage.timeseries import TimeSeriesStorage


class QueryService:
    """Execute time-series queries with optional aggregation."""

    def __init__(self, storage: TimeSeriesStorage) -> None:
        self._storage = storage

    async def query(self, req: QueryRequest) -> QueryResult:
        """Run a range query, optionally aggregating or downsampling."""
        end = req.end if req.end is not None else time.time()

        if req.downsample:
            data_points = await self._storage.downsample(
                metric_name=req.name,
                labels=req.labels,
                bucket_seconds=req.downsample,
                start=req.start,
                end=end,
            )
            return QueryResult(
                name=req.name,
                labels=req.labels,
                data_points=data_points,
            )

        raw_points = await self._storage.query_range(
            metric_name=req.name,
            labels=req.labels,
            start=req.start,
            end=end,
        )

        aggregated_value: Optional[float] = None
        if req.aggregation and raw_points:
            aggregated_value = self._aggregate(raw_points, req.aggregation)

        return QueryResult(
            name=req.name,
            labels=req.labels,
            data_points=raw_points,
            aggregation=req.aggregation,
            aggregated_value=aggregated_value,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _aggregate(points: list[dict], agg: AggregationType) -> float:
        """Apply an aggregation function to a list of data points."""
        values = [p["value"] for p in points]
        if agg == AggregationType.AVG:
            return sum(values) / len(values)
        if agg == AggregationType.MAX:
            return max(values)
        if agg == AggregationType.MIN:
            return min(values)
        if agg == AggregationType.SUM:
            return sum(values)
        if agg == AggregationType.COUNT:
            return float(len(values))
        raise ValueError(f"Unknown aggregation: {agg}")

    async def list_metrics(self) -> list[str]:
        """Proxy to storage metadata."""
        return await self._storage.list_metrics()

    async def list_label_sets(self, metric_name: str) -> list[dict[str, str]]:
        """Proxy to storage metadata."""
        return await self._storage.list_label_sets(metric_name)
