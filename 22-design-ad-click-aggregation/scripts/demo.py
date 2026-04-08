#!/usr/bin/env python3
"""Demo: ad click event aggregation pipeline.

Generates 100K synthetic ad click events, runs the MapReduce aggregation
pipeline, and demonstrates top-N queries, country filtering, and
tumbling vs sliding window comparisons.

Usage:
    python scripts/demo.py
"""

from __future__ import annotations

import os
import sys
import time

# Allow running from the chapter root directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.event import generate_events
from src.aggregator import (
    filter_by_country,
    map_partition,
    aggregate_counts,
    reduce_top_n,
    run_pipeline,
)
from src.window import tumbling_window, sliding_window
from src.storage import AggregatedStore


def _header(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def main() -> None:
    # -- Generate events ------------------------------------------------------
    _header("1. Generate 100K ad click events")
    base_time = 1_700_000_000.0  # Fixed base for reproducibility
    events = generate_events(
        100_000,
        num_ads=200,
        num_users=50_000,
        time_span_seconds=600.0,
        base_time=base_time,
    )
    print(f"  Generated {len(events):,} events")
    print(f"  Time span : {events[0].timestamp:.0f} ~ {events[-1].timestamp:.0f}")
    print(f"  Sample    : {events[0]}")

    # -- Full pipeline: top-10 -----------------------------------------------
    _header("2. MapReduce pipeline -> Top 10 ads (all events)")
    top10 = run_pipeline(events, top_n=10)
    for rank, ad in enumerate(top10, 1):
        print(f"  #{rank:2d}  {ad.ad_id}  clicks={ad.total_clicks:,}")

    # -- Filter by country ---------------------------------------------------
    _header("3. Top 10 ads filtered by country='US'")
    top10_us = run_pipeline(events, top_n=10, filters=[filter_by_country("US")])
    for rank, ad in enumerate(top10_us, 1):
        print(f"  #{rank:2d}  {ad.ad_id}  clicks={ad.total_clicks:,}")

    # -- Step-by-step pipeline -----------------------------------------------
    _header("4. Step-by-step pipeline demonstration")
    partitions = map_partition(events)
    print(f"  Map stage     : {len(partitions)} ad partitions")
    aggregated = aggregate_counts(partitions)
    total_buckets = sum(len(b) for b in aggregated.values())
    print(f"  Aggregate stage: {total_buckets} (ad_id, minute) buckets")
    top5 = reduce_top_n(aggregated, n=5)
    print(f"  Reduce stage  : top 5 = {[(a.ad_id, a.total_clicks) for a in top5]}")

    # -- Tumbling vs Sliding windows -----------------------------------------
    _header("5. Tumbling window (1-min) vs Sliding window (5-min, 1-min step)")
    # Pick the top ad for analysis
    top_ad = top10[0].ad_id
    ad_events = [e for e in events if e.ad_id == top_ad]

    tw = tumbling_window(ad_events, window_seconds=60.0)
    sw = sliding_window(ad_events, window_seconds=300.0, step_seconds=60.0)

    tw_buckets = tw.get(top_ad, [])
    sw_buckets = sw.get(top_ad, [])

    print(f"\n  Ad: {top_ad} ({len(ad_events):,} clicks)")
    print(f"\n  Tumbling windows (1-min): {len(tw_buckets)} windows")
    for b in tw_buckets[:5]:
        print(f"    [{b.window_start:.0f} ~ {b.window_end:.0f}) count={b.count}")
    if len(tw_buckets) > 5:
        print(f"    ... and {len(tw_buckets) - 5} more")

    print(f"\n  Sliding windows (5-min, step 1-min): {len(sw_buckets)} windows")
    for b in sw_buckets[:5]:
        print(f"    [{b.window_start:.0f} ~ {b.window_end:.0f}) count={b.count}")
    if len(sw_buckets) > 5:
        print(f"    ... and {len(sw_buckets) - 5} more")

    # -- Storage queries -----------------------------------------------------
    _header("6. AggregatedStore: ingest + query")
    store = AggregatedStore()
    late = store.ingest(events)
    print(f"  Ingested {len(events):,} events, late events: {late}")

    # Query: count for top ad in first 5 minutes
    count = store.count_by_ad(top_ad, base_time, base_time + 300)
    print(f"  {top_ad} clicks in first 5 min: {count:,}")

    # Query: top 10 in last 10 minutes
    top10_store = store.top_n(10, start_ts=base_time, end_ts=base_time + 600 + 60)
    print(f"  Top 10 from store:")
    for rank, (ad_id, cnt) in enumerate(top10_store, 1):
        print(f"    #{rank:2d}  {ad_id}  clicks={cnt:,}")

    # Time series for top ad
    ts_data = store.time_series_for_ad(top_ad, base_time, base_time + 600 + 60)
    print(f"\n  Time series for {top_ad}: {len(ts_data)} minute-buckets")
    for minute_ts, cnt in ts_data[:5]:
        print(f"    minute={minute_ts:.0f}  count={cnt}")
    if len(ts_data) > 5:
        print(f"    ... and {len(ts_data) - 5} more")

    _header("Done")


if __name__ == "__main__":
    main()
