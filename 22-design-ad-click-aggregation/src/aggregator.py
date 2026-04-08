"""MapReduce-style ad click aggregation pipeline.

Pipeline stages:
  1. **Map** -- partition raw events by ad_id
  2. **Aggregate** -- count clicks per (ad_id, minute) within each partition
  3. **Reduce** -- merge partitions and extract top-N ads

Filtering (country / ip / user_id) is applied at the Map stage so that
downstream stages only process relevant events.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Sequence

from .event import AdClickEvent


# -- Filter helpers -----------------------------------------------------------

FilterFn = Callable[[AdClickEvent], bool]


def filter_by_country(country: str) -> FilterFn:
    """Return a predicate that keeps only events from *country*."""
    def _pred(e: AdClickEvent) -> bool:
        return e.country == country
    return _pred


def filter_by_ip(ip: str) -> FilterFn:
    """Return a predicate that keeps only events from *ip*."""
    def _pred(e: AdClickEvent) -> bool:
        return e.ip == ip
    return _pred


def filter_by_user(user_id: str) -> FilterFn:
    """Return a predicate that keeps only events from *user_id*."""
    def _pred(e: AdClickEvent) -> bool:
        return e.user_id == user_id
    return _pred


def compose_filters(*filters: FilterFn) -> FilterFn:
    """AND-compose multiple filter predicates."""
    def _pred(e: AdClickEvent) -> bool:
        return all(f(e) for f in filters)
    return _pred


# -- Map stage ----------------------------------------------------------------

def map_partition(
    events: Sequence[AdClickEvent],
    filters: Sequence[FilterFn] | None = None,
) -> dict[str, list[AdClickEvent]]:
    """Partition events by ad_id, optionally applying filters.

    Returns:
        A dict mapping ad_id -> list of events for that ad.
    """
    combined: FilterFn | None = None
    if filters:
        combined = compose_filters(*filters)

    partitions: dict[str, list[AdClickEvent]] = defaultdict(list)
    for event in events:
        if combined and not combined(event):
            continue
        partitions[event.ad_id].append(event)
    return dict(partitions)


# -- Aggregate stage ----------------------------------------------------------

@dataclass(slots=True)
class MinuteCount:
    """Click count for a single (ad_id, minute) bucket."""

    ad_id: str
    minute_ts: float  # floored minute timestamp
    count: int = 0


def aggregate_counts(
    partitions: dict[str, list[AdClickEvent]],
) -> dict[str, dict[float, MinuteCount]]:
    """Count clicks per (ad_id, minute) within each partition.

    Returns:
        {ad_id: {minute_ts: MinuteCount}}
    """
    result: dict[str, dict[float, MinuteCount]] = {}
    for ad_id, events in partitions.items():
        buckets: dict[float, MinuteCount] = {}
        for event in events:
            mk = event.minute_key()
            if mk not in buckets:
                buckets[mk] = MinuteCount(ad_id=ad_id, minute_ts=mk)
            buckets[mk].count += 1
        result[ad_id] = buckets
    return result


# -- Reduce stage -------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class AdTotal:
    """Total click count for one ad across all minute buckets."""

    ad_id: str
    total_clicks: int


def reduce_top_n(
    aggregated: dict[str, dict[float, MinuteCount]],
    n: int = 10,
) -> list[AdTotal]:
    """Reduce per-minute counts to total per ad and return the top *n*.

    Returns:
        A list of AdTotal sorted descending by total_clicks, length <= n.
    """
    totals: list[AdTotal] = []
    for ad_id, buckets in aggregated.items():
        total = sum(mc.count for mc in buckets.values())
        totals.append(AdTotal(ad_id=ad_id, total_clicks=total))
    totals.sort(key=lambda t: t.total_clicks, reverse=True)
    return totals[:n]


# -- Full pipeline convenience ------------------------------------------------

def run_pipeline(
    events: Sequence[AdClickEvent],
    *,
    top_n: int = 10,
    filters: Sequence[FilterFn] | None = None,
) -> list[AdTotal]:
    """Execute the full Map -> Aggregate -> Reduce pipeline.

    Args:
        events: Raw click events.
        top_n: How many top ads to return.
        filters: Optional filter predicates applied at the Map stage.

    Returns:
        Top-N ads by total click count.
    """
    partitions = map_partition(events, filters=filters)
    aggregated = aggregate_counts(partitions)
    return reduce_top_n(aggregated, n=top_n)
