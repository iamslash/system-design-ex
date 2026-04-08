"""Time-series storage using Redis sorted sets.

Key format: ts:{metric_name}:{label_hash}
  - score  = unix timestamp
  - member = JSON string {"value": ..., "timestamp": ..., "labels": ...}

This mirrors how real TSDB systems store data but uses Redis as the
backing store for simplicity.
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Optional

import redis.asyncio as aioredis


def label_hash(labels: dict[str, str]) -> str:
    """Deterministic hash of label key-value pairs."""
    if not labels:
        return "_"
    canonical = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
    return hashlib.md5(canonical.encode()).hexdigest()[:12]


def ts_key(metric_name: str, labels: dict[str, str]) -> str:
    """Build the Redis key for a time-series."""
    return f"ts:{metric_name}:{label_hash(labels)}"


class TimeSeriesStorage:
    """Read/write time-series data points in Redis sorted sets."""

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._redis = redis_client

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def add(
        self,
        metric_name: str,
        labels: dict[str, str],
        value: float,
        timestamp: Optional[float] = None,
    ) -> None:
        """Store a single data point."""
        ts = timestamp if timestamp is not None else time.time()
        key = ts_key(metric_name, labels)
        member = json.dumps({"value": value, "timestamp": ts, "labels": labels})
        await self._redis.zadd(key, {member: ts})
        # Track known metric names and label combinations
        await self._redis.sadd("metrics:names", metric_name)
        await self._redis.sadd(f"metrics:labels:{metric_name}", json.dumps(labels, sort_keys=True))

    async def add_batch(
        self,
        points: list[tuple[str, dict[str, str], float, float]],
    ) -> int:
        """Store multiple data points. Each tuple: (name, labels, value, timestamp).

        Returns the number of points stored.
        """
        pipe = self._redis.pipeline()
        for name, labels, value, ts in points:
            key = ts_key(name, labels)
            member = json.dumps({"value": value, "timestamp": ts, "labels": labels})
            pipe.zadd(key, {member: ts})
            pipe.sadd("metrics:names", name)
            pipe.sadd(f"metrics:labels:{name}", json.dumps(labels, sort_keys=True))
        await pipe.execute()
        return len(points)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def query_range(
        self,
        metric_name: str,
        labels: dict[str, str],
        start: float,
        end: Optional[float] = None,
    ) -> list[dict]:
        """Return data points within [start, end] ordered by timestamp."""
        end = end if end is not None else time.time()
        key = ts_key(metric_name, labels)
        raw = await self._redis.zrangebyscore(key, min=start, max=end)
        return [json.loads(m) for m in raw]

    async def query_latest(
        self,
        metric_name: str,
        labels: dict[str, str],
        count: int = 1,
    ) -> list[dict]:
        """Return the most recent *count* data points."""
        key = ts_key(metric_name, labels)
        raw = await self._redis.zrange(key, -count, -1)
        return [json.loads(m) for m in raw]

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    async def list_metrics(self) -> list[str]:
        """Return all known metric names."""
        return sorted([m for m in await self._redis.smembers("metrics:names")])

    async def list_label_sets(self, metric_name: str) -> list[dict[str, str]]:
        """Return all known label combinations for a metric."""
        raw = await self._redis.smembers(f"metrics:labels:{metric_name}")
        return [json.loads(m) for m in raw]

    # ------------------------------------------------------------------
    # Downsampling
    # ------------------------------------------------------------------

    async def downsample(
        self,
        metric_name: str,
        labels: dict[str, str],
        bucket_seconds: int,
        start: float,
        end: Optional[float] = None,
    ) -> list[dict]:
        """Aggregate data points into fixed-size time buckets.

        Returns a list of {bucket_start, bucket_end, avg, max, min, sum, count}.
        """
        points = await self.query_range(metric_name, labels, start, end)
        if not points:
            return []

        buckets: dict[float, list[float]] = {}
        for p in points:
            ts = p["timestamp"]
            bucket_start = (ts // bucket_seconds) * bucket_seconds
            buckets.setdefault(bucket_start, []).append(p["value"])

        result = []
        for bucket_start in sorted(buckets):
            values = buckets[bucket_start]
            result.append({
                "bucket_start": bucket_start,
                "bucket_end": bucket_start + bucket_seconds,
                "avg": sum(values) / len(values),
                "max": max(values),
                "min": min(values),
                "sum": sum(values),
                "count": len(values),
            })
        return result

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    async def delete_range(
        self,
        metric_name: str,
        labels: dict[str, str],
        start: float,
        end: float,
    ) -> int:
        """Remove data points within [start, end]. Returns count removed."""
        key = ts_key(metric_name, labels)
        return await self._redis.zremrangebyscore(key, min=start, max=end)
