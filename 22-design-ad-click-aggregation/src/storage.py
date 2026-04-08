"""In-memory aggregated data store with time-series support.

Stores pre-aggregated click counts keyed by (ad_id, minute_ts) and
provides efficient queries for:
  - Click count for a specific ad_id within a time range.
  - Top-N ads within the last M minutes.
  - Filtered queries by country, ip, user_id.
"""

from __future__ import annotations

import bisect
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Sequence

from .event import AdClickEvent
from .aggregator import (
    FilterFn,
    MinuteCount,
    aggregate_counts,
    map_partition,
)


@dataclass(slots=True)
class _TimeSeriesEntry:
    """A single point in the per-ad time series."""

    minute_ts: float
    count: int


class AggregatedStore:
    """In-memory store for aggregated ad click counts.

    Internally maintains:
      - Per-ad sorted time series of minute-level counts.
      - A raw event buffer (optional) for late-event reprocessing.
    """

    def __init__(self) -> None:
        # ad_id -> sorted list of _TimeSeriesEntry (by minute_ts)
        self._series: dict[str, list[_TimeSeriesEntry]] = defaultdict(list)
        # Watermark: events with ts < watermark are considered late
        self._watermark: float = 0.0

    # -- Ingestion ------------------------------------------------------------

    def ingest(
        self,
        events: Sequence[AdClickEvent],
        filters: Sequence[FilterFn] | None = None,
    ) -> int:
        """Ingest raw events, aggregate, and store counts.

        Returns the number of events that were late (below watermark).
        """
        late_count = 0
        partitions = map_partition(events, filters=filters)
        aggregated = aggregate_counts(partitions)

        for ad_id, buckets in aggregated.items():
            for minute_ts, mc in buckets.items():
                if minute_ts < self._watermark:
                    late_count += mc.count
                self._upsert(ad_id, minute_ts, mc.count)

        return late_count

    def _upsert(self, ad_id: str, minute_ts: float, count: int) -> None:
        """Insert or add to an existing time-series entry."""
        series = self._series[ad_id]
        # Binary search for the correct position
        timestamps = [e.minute_ts for e in series]
        idx = bisect.bisect_left(timestamps, minute_ts)
        if idx < len(series) and series[idx].minute_ts == minute_ts:
            series[idx].count += count
        else:
            series.insert(idx, _TimeSeriesEntry(minute_ts=minute_ts, count=count))

    # -- Watermark ------------------------------------------------------------

    def set_watermark(self, ts: float) -> None:
        """Advance the watermark. Events older than this are late."""
        self._watermark = max(self._watermark, ts)

    @property
    def watermark(self) -> float:
        return self._watermark

    # -- Queries --------------------------------------------------------------

    def count_by_ad(
        self,
        ad_id: str,
        start_ts: float,
        end_ts: float,
    ) -> int:
        """Return total click count for *ad_id* in [start_ts, end_ts)."""
        series = self._series.get(ad_id, [])
        total = 0
        for entry in series:
            if start_ts <= entry.minute_ts < end_ts:
                total += entry.count
        return total

    def count_all_in_range(
        self,
        start_ts: float,
        end_ts: float,
    ) -> dict[str, int]:
        """Return {ad_id: total_count} for all ads in [start_ts, end_ts)."""
        result: dict[str, int] = {}
        for ad_id, series in self._series.items():
            total = 0
            for entry in series:
                if start_ts <= entry.minute_ts < end_ts:
                    total += entry.count
            if total > 0:
                result[ad_id] = total
        return result

    def top_n(
        self,
        n: int,
        start_ts: float,
        end_ts: float,
    ) -> list[tuple[str, int]]:
        """Return top-N ads by click count in [start_ts, end_ts).

        Returns:
            List of (ad_id, count) tuples sorted descending by count.
        """
        counts = self.count_all_in_range(start_ts, end_ts)
        ranked = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        return ranked[:n]

    def top_n_last_m_minutes(
        self,
        n: int,
        m: int,
        now: float | None = None,
    ) -> list[tuple[str, int]]:
        """Return top-N ads in the last M minutes."""
        if now is None:
            now = time.time()
        start = now - m * 60
        return self.top_n(n, start_ts=start, end_ts=now + 60)

    def time_series_for_ad(
        self,
        ad_id: str,
        start_ts: float,
        end_ts: float,
    ) -> list[tuple[float, int]]:
        """Return minute-level time series for an ad in [start_ts, end_ts).

        Returns:
            List of (minute_ts, count) tuples sorted by time.
        """
        series = self._series.get(ad_id, [])
        return [
            (e.minute_ts, e.count)
            for e in series
            if start_ts <= e.minute_ts < end_ts
        ]

    def all_ad_ids(self) -> list[str]:
        """Return all ad_ids that have at least one entry."""
        return list(self._series.keys())
