"""Tumbling and sliding window implementations for ad click aggregation.

A **tumbling window** divides time into fixed, non-overlapping intervals
(e.g., every 1 minute).  Each event falls into exactly one window.

A **sliding window** uses a fixed duration that advances by a configurable
step.  Events may belong to multiple overlapping windows.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Sequence

from .event import AdClickEvent


@dataclass(frozen=True, slots=True)
class WindowBucket:
    """Aggregated click count for one window interval."""

    window_start: float
    window_end: float
    ad_id: str
    count: int


# -- Tumbling window ----------------------------------------------------------

def tumbling_window(
    events: Sequence[AdClickEvent],
    window_seconds: float = 60.0,
) -> dict[str, list[WindowBucket]]:
    """Aggregate events into fixed, non-overlapping tumbling windows.

    Args:
        events: Sorted click events.
        window_seconds: Width of each window in seconds.

    Returns:
        {ad_id: [WindowBucket, ...]} sorted by window_start within each ad.
    """
    # (ad_id, window_start) -> count
    buckets: dict[tuple[str, float], int] = defaultdict(int)

    for event in events:
        w_start = event.timestamp - (event.timestamp % window_seconds)
        buckets[(event.ad_id, w_start)] += 1

    result: dict[str, list[WindowBucket]] = defaultdict(list)
    for (ad_id, w_start), count in buckets.items():
        result[ad_id].append(
            WindowBucket(
                window_start=w_start,
                window_end=w_start + window_seconds,
                ad_id=ad_id,
                count=count,
            )
        )

    # Sort each ad's buckets by window_start
    for ad_id in result:
        result[ad_id].sort(key=lambda b: b.window_start)

    return dict(result)


# -- Sliding window -----------------------------------------------------------

def sliding_window(
    events: Sequence[AdClickEvent],
    window_seconds: float = 300.0,
    step_seconds: float = 60.0,
) -> dict[str, list[WindowBucket]]:
    """Aggregate events into overlapping sliding windows.

    The window of size *window_seconds* advances by *step_seconds*.
    An event belongs to every window whose [start, end) range contains
    its timestamp.

    Args:
        events: Sorted click events.
        window_seconds: Width of each sliding window.
        step_seconds: How far the window advances each step.

    Returns:
        {ad_id: [WindowBucket, ...]} sorted by window_start within each ad.
    """
    if not events:
        return {}

    # Determine the global time range
    t_min = min(e.timestamp for e in events)
    t_max = max(e.timestamp for e in events)

    # The earliest window that could contain t_min starts at
    # t_min aligned down to step, minus (window_seconds - step_seconds)
    # so the window [start, start+window_seconds) still covers t_min.
    first_start = t_min - (t_min % step_seconds) - (window_seconds - step_seconds)

    # Generate all window starts up to the last one that could contain t_max
    window_starts: list[float] = []
    ws = first_start
    while ws <= t_max:
        window_starts.append(ws)
        ws += step_seconds

    # (ad_id, window_start) -> count
    buckets: dict[tuple[str, float], int] = defaultdict(int)

    for event in events:
        for ws in window_starts:
            if ws <= event.timestamp < ws + window_seconds:
                buckets[(event.ad_id, ws)] += 1

    result: dict[str, list[WindowBucket]] = defaultdict(list)
    for (ad_id, ws), count in buckets.items():
        result[ad_id].append(
            WindowBucket(
                window_start=ws,
                window_end=ws + window_seconds,
                ad_id=ad_id,
                count=count,
            )
        )

    for ad_id in result:
        result[ad_id].sort(key=lambda b: b.window_start)

    return dict(result)
