"""Tests for the ad click aggregation system (25+ tests)."""

from __future__ import annotations

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.event import AdClickEvent, generate_events, generate_events_for_ad
from src.aggregator import (
    AdTotal,
    MinuteCount,
    aggregate_counts,
    compose_filters,
    filter_by_country,
    filter_by_ip,
    filter_by_user,
    map_partition,
    reduce_top_n,
    run_pipeline,
)
from src.window import WindowBucket, tumbling_window, sliding_window
from src.storage import AggregatedStore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

BASE_TIME = 1_700_000_000.0  # Fixed epoch for deterministic tests


def _make_event(
    ad_id: str = "ad_0001",
    ts: float = BASE_TIME,
    user_id: str = "user_000001",
    ip: str = "10.0.0.1",
    country: str = "US",
) -> AdClickEvent:
    return AdClickEvent(ad_id=ad_id, timestamp=ts, user_id=user_id, ip=ip, country=country)


# ---------------------------------------------------------------------------
# Event generation
# ---------------------------------------------------------------------------


class TestEventGeneration:
    """Tests for event model and generation utilities."""

    def test_event_immutable(self) -> None:
        """AdClickEvent should be immutable (frozen dataclass)."""
        e = _make_event()
        with pytest.raises(AttributeError):
            e.ad_id = "ad_9999"  # type: ignore[misc]

    def test_minute_key_floors_to_minute(self) -> None:
        """minute_key should floor the timestamp to the nearest minute."""
        e = _make_event(ts=BASE_TIME + 37.5)
        assert e.minute_key() == BASE_TIME + 37.5 - ((BASE_TIME + 37.5) % 60)

    def test_generate_events_count(self) -> None:
        """generate_events should return the exact requested count."""
        events = generate_events(500, base_time=BASE_TIME)
        assert len(events) == 500

    def test_generate_events_sorted(self) -> None:
        """Generated events should be sorted by timestamp."""
        events = generate_events(1000, base_time=BASE_TIME)
        timestamps = [e.timestamp for e in events]
        assert timestamps == sorted(timestamps)

    def test_generate_events_within_time_span(self) -> None:
        """All event timestamps should be within [base_time, base_time + span]."""
        span = 300.0
        events = generate_events(1000, base_time=BASE_TIME, time_span_seconds=span)
        for e in events:
            assert BASE_TIME <= e.timestamp <= BASE_TIME + span

    def test_generate_events_for_ad(self) -> None:
        """generate_events_for_ad should produce events with the given ad_id."""
        events = generate_events_for_ad("ad_test", 50, base_time=BASE_TIME)
        assert all(e.ad_id == "ad_test" for e in events)
        assert len(events) == 50


# ---------------------------------------------------------------------------
# Map partition
# ---------------------------------------------------------------------------


class TestMapPartition:
    """Tests for the Map stage of the pipeline."""

    def test_partition_by_ad_id(self) -> None:
        """Events should be partitioned into separate lists by ad_id."""
        events = [
            _make_event(ad_id="a1"),
            _make_event(ad_id="a2"),
            _make_event(ad_id="a1"),
        ]
        parts = map_partition(events)
        assert set(parts.keys()) == {"a1", "a2"}
        assert len(parts["a1"]) == 2
        assert len(parts["a2"]) == 1

    def test_partition_empty(self) -> None:
        """Partitioning an empty sequence should return an empty dict."""
        assert map_partition([]) == {}

    def test_partition_preserves_all_events(self) -> None:
        """Total events across all partitions should equal input count."""
        events = generate_events(200, num_ads=10, base_time=BASE_TIME)
        parts = map_partition(events)
        total = sum(len(v) for v in parts.values())
        assert total == 200


# ---------------------------------------------------------------------------
# Aggregate counting
# ---------------------------------------------------------------------------


class TestAggregateCounts:
    """Tests for the Aggregate stage."""

    def test_single_ad_single_minute(self) -> None:
        """All events in one minute should produce a single MinuteCount."""
        events = [_make_event(ts=BASE_TIME + i) for i in range(10)]
        parts = map_partition(events)
        agg = aggregate_counts(parts)
        assert "ad_0001" in agg
        buckets = agg["ad_0001"]
        assert len(buckets) == 1
        mc = list(buckets.values())[0]
        assert mc.count == 10

    def test_single_ad_multiple_minutes(self) -> None:
        """Events spanning two minutes should produce two MinuteCounts."""
        events = [
            _make_event(ts=BASE_TIME + 10),  # minute 0
            _make_event(ts=BASE_TIME + 20),  # minute 0
            _make_event(ts=BASE_TIME + 70),  # minute 1
        ]
        parts = map_partition(events)
        agg = aggregate_counts(parts)
        buckets = agg["ad_0001"]
        assert len(buckets) == 2
        counts = sorted(mc.count for mc in buckets.values())
        assert counts == [1, 2]

    def test_multiple_ads(self) -> None:
        """Each ad should have its own set of minute buckets."""
        events = [
            _make_event(ad_id="a1", ts=BASE_TIME),
            _make_event(ad_id="a2", ts=BASE_TIME),
            _make_event(ad_id="a1", ts=BASE_TIME + 1),
        ]
        parts = map_partition(events)
        agg = aggregate_counts(parts)
        assert agg["a1"][_make_event().minute_key()].count == 2
        assert agg["a2"][_make_event().minute_key()].count == 1


# ---------------------------------------------------------------------------
# Reduce top-N
# ---------------------------------------------------------------------------


class TestReduceTopN:
    """Tests for the Reduce stage."""

    def test_top_n_ordering(self) -> None:
        """Results should be sorted descending by total click count."""
        events = (
            [_make_event(ad_id="a1", ts=BASE_TIME + i) for i in range(50)]
            + [_make_event(ad_id="a2", ts=BASE_TIME + i) for i in range(30)]
            + [_make_event(ad_id="a3", ts=BASE_TIME + i) for i in range(10)]
        )
        parts = map_partition(events)
        agg = aggregate_counts(parts)
        top = reduce_top_n(agg, n=3)
        assert [t.ad_id for t in top] == ["a1", "a2", "a3"]
        assert [t.total_clicks for t in top] == [50, 30, 10]

    def test_top_n_limits_results(self) -> None:
        """reduce_top_n should return at most N results."""
        events = generate_events(500, num_ads=50, base_time=BASE_TIME)
        parts = map_partition(events)
        agg = aggregate_counts(parts)
        top = reduce_top_n(agg, n=5)
        assert len(top) <= 5

    def test_top_n_empty(self) -> None:
        """Reducing an empty aggregation should return an empty list."""
        assert reduce_top_n({}, n=10) == []


# ---------------------------------------------------------------------------
# Tumbling window
# ---------------------------------------------------------------------------


class TestTumblingWindow:
    """Tests for fixed, non-overlapping tumbling windows."""

    def test_single_minute_window(self) -> None:
        """Events within one minute should fall into one window."""
        events = [_make_event(ts=BASE_TIME + i) for i in range(30)]
        tw = tumbling_window(events, window_seconds=60.0)
        assert len(tw["ad_0001"]) == 1
        assert tw["ad_0001"][0].count == 30

    def test_two_minute_windows(self) -> None:
        """Events spanning two minutes should produce two windows."""
        events = [
            _make_event(ts=BASE_TIME + 10),
            _make_event(ts=BASE_TIME + 70),
        ]
        tw = tumbling_window(events, window_seconds=60.0)
        assert len(tw["ad_0001"]) == 2

    def test_window_boundaries(self) -> None:
        """Window start/end should be aligned to window_seconds."""
        ts = BASE_TIME + 45
        events = [_make_event(ts=ts)]
        tw = tumbling_window(events, window_seconds=60.0)
        bucket = tw["ad_0001"][0]
        expected_start = ts - (ts % 60)
        assert bucket.window_start == expected_start
        assert bucket.window_end == expected_start + 60.0

    def test_no_overlap(self) -> None:
        """Each event should appear in exactly one tumbling window."""
        events = generate_events(500, num_ads=5, base_time=BASE_TIME, time_span_seconds=300)
        tw = tumbling_window(events, window_seconds=60.0)
        total = sum(b.count for buckets in tw.values() for b in buckets)
        assert total == 500


# ---------------------------------------------------------------------------
# Sliding window
# ---------------------------------------------------------------------------


class TestSlidingWindow:
    """Tests for overlapping sliding windows."""

    def test_sliding_wider_than_tumbling(self) -> None:
        """A sliding window should produce more buckets than tumbling."""
        events = generate_events(200, num_ads=3, base_time=BASE_TIME, time_span_seconds=300)
        tw = tumbling_window(events, window_seconds=60.0)
        sw = sliding_window(events, window_seconds=300.0, step_seconds=60.0)
        tw_total_buckets = sum(len(b) for b in tw.values())
        sw_total_buckets = sum(len(b) for b in sw.values())
        assert sw_total_buckets >= tw_total_buckets

    def test_sliding_window_overlap(self) -> None:
        """An event near a window boundary should appear in multiple windows."""
        # Single event at BASE_TIME + 120
        events = [_make_event(ts=BASE_TIME + 120)]
        sw = sliding_window(events, window_seconds=300.0, step_seconds=60.0)
        buckets = sw.get("ad_0001", [])
        # The event should appear in every window whose [start, start+300) contains BASE_TIME+120
        assert len(buckets) >= 2  # Should appear in multiple overlapping windows

    def test_sliding_empty(self) -> None:
        """Sliding window on empty input should return empty dict."""
        assert sliding_window([], window_seconds=300.0, step_seconds=60.0) == {}

    def test_sliding_window_sorted(self) -> None:
        """Buckets for each ad should be sorted by window_start."""
        events = generate_events(300, num_ads=5, base_time=BASE_TIME, time_span_seconds=600)
        sw = sliding_window(events, window_seconds=300.0, step_seconds=60.0)
        for ad_id, buckets in sw.items():
            starts = [b.window_start for b in buckets]
            assert starts == sorted(starts), f"Not sorted for {ad_id}"


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------


class TestFiltering:
    """Tests for filter predicates applied at the Map stage."""

    def test_filter_by_country(self) -> None:
        """Only events from the specified country should pass."""
        events = [
            _make_event(country="US"),
            _make_event(country="KR"),
            _make_event(country="US"),
        ]
        parts = map_partition(events, filters=[filter_by_country("US")])
        total = sum(len(v) for v in parts.values())
        assert total == 2

    def test_filter_by_ip(self) -> None:
        """Only events from the specified IP should pass."""
        events = [
            _make_event(ip="10.0.0.1"),
            _make_event(ip="10.0.0.2"),
            _make_event(ip="10.0.0.1"),
        ]
        parts = map_partition(events, filters=[filter_by_ip("10.0.0.1")])
        total = sum(len(v) for v in parts.values())
        assert total == 2

    def test_filter_by_user(self) -> None:
        """Only events from the specified user should pass."""
        events = [
            _make_event(user_id="u1"),
            _make_event(user_id="u2"),
        ]
        parts = map_partition(events, filters=[filter_by_user("u1")])
        total = sum(len(v) for v in parts.values())
        assert total == 1

    def test_compose_filters(self) -> None:
        """Composed filters should AND multiple predicates."""
        events = [
            _make_event(country="US", ip="10.0.0.1"),
            _make_event(country="US", ip="10.0.0.2"),
            _make_event(country="KR", ip="10.0.0.1"),
        ]
        parts = map_partition(
            events,
            filters=[filter_by_country("US"), filter_by_ip("10.0.0.1")],
        )
        total = sum(len(v) for v in parts.values())
        assert total == 1

    def test_pipeline_with_country_filter(self) -> None:
        """run_pipeline with country filter should only count matching events."""
        events = generate_events_for_ad("ad_us", 100, base_time=BASE_TIME, country="US")
        events += generate_events_for_ad("ad_kr", 50, base_time=BASE_TIME, country="KR")
        result = run_pipeline(events, top_n=5, filters=[filter_by_country("US")])
        assert len(result) == 1
        assert result[0].ad_id == "ad_us"
        assert result[0].total_clicks == 100


# ---------------------------------------------------------------------------
# Late event handling
# ---------------------------------------------------------------------------


class TestLateEvents:
    """Tests for watermark-based late event detection."""

    def test_no_late_events_without_watermark(self) -> None:
        """Without a watermark, no events should be counted as late."""
        store = AggregatedStore()
        events = [_make_event(ts=BASE_TIME + i) for i in range(10)]
        late = store.ingest(events)
        assert late == 0

    def test_late_events_below_watermark(self) -> None:
        """Events with minute_ts < watermark should be counted as late."""
        store = AggregatedStore()
        # Watermark at minute boundary so comparison is clean
        watermark = BASE_TIME - (BASE_TIME % 60) + 120  # a clean minute boundary + 2 min
        store.set_watermark(watermark)
        late_ts = watermark - 70   # minute_key well below watermark
        ontime_ts = watermark + 10  # minute_key at or above watermark
        events = [
            _make_event(ts=late_ts),    # late
            _make_event(ts=ontime_ts),  # on-time
        ]
        late = store.ingest(events)
        assert late == 1

    def test_late_events_still_stored(self) -> None:
        """Late events should still be stored (at-least-once semantics)."""
        store = AggregatedStore()
        store.set_watermark(BASE_TIME + 120)
        ts = BASE_TIME + 10
        events = [_make_event(ts=ts)]
        store.ingest(events)
        mk = ts - (ts % 60)  # the actual minute_key the event lands in
        count = store.count_by_ad("ad_0001", mk, mk + 60)
        assert count == 1


# ---------------------------------------------------------------------------
# Storage time range queries
# ---------------------------------------------------------------------------


class TestStorageQueries:
    """Tests for AggregatedStore query functionality."""

    def test_count_by_ad_time_range(self) -> None:
        """count_by_ad should return clicks within [start, end)."""
        store = AggregatedStore()
        events = (
            [_make_event(ts=BASE_TIME + i) for i in range(20)]       # minute 0
            + [_make_event(ts=BASE_TIME + 60 + i) for i in range(30)]  # minute 1
        )
        store.ingest(events)
        mk0 = BASE_TIME - (BASE_TIME % 60)
        # Only minute 0
        count = store.count_by_ad("ad_0001", mk0, mk0 + 60)
        assert count == 20

    def test_top_n_from_store(self) -> None:
        """top_n should return ads ranked by count."""
        store = AggregatedStore()
        events = (
            [_make_event(ad_id="a1", ts=BASE_TIME + i) for i in range(50)]
            + [_make_event(ad_id="a2", ts=BASE_TIME + i) for i in range(30)]
        )
        store.ingest(events)
        top = store.top_n(2, start_ts=BASE_TIME - 60, end_ts=BASE_TIME + 120)
        assert top[0][0] == "a1"
        assert top[0][1] == 50
        assert top[1][0] == "a2"
        assert top[1][1] == 30

    def test_time_series_for_ad(self) -> None:
        """time_series_for_ad should return minute-level counts sorted by time."""
        store = AggregatedStore()
        events = (
            [_make_event(ts=BASE_TIME + i) for i in range(5)]
            + [_make_event(ts=BASE_TIME + 60 + i) for i in range(3)]
        )
        store.ingest(events)
        ts = store.time_series_for_ad("ad_0001", BASE_TIME - 60, BASE_TIME + 180)
        assert len(ts) == 2
        assert ts[0][1] == 5
        assert ts[1][1] == 3

    def test_all_ad_ids(self) -> None:
        """all_ad_ids should list every ad_id with stored data."""
        store = AggregatedStore()
        events = [
            _make_event(ad_id="a1"),
            _make_event(ad_id="a2"),
            _make_event(ad_id="a3"),
        ]
        store.ingest(events)
        assert set(store.all_ad_ids()) == {"a1", "a2", "a3"}

    def test_count_all_in_range(self) -> None:
        """count_all_in_range should return counts for all ads."""
        store = AggregatedStore()
        events = [
            _make_event(ad_id="a1", ts=BASE_TIME),
            _make_event(ad_id="a1", ts=BASE_TIME + 1),
            _make_event(ad_id="a2", ts=BASE_TIME),
        ]
        store.ingest(events)
        counts = store.count_all_in_range(BASE_TIME - 60, BASE_TIME + 120)
        assert counts["a1"] == 2
        assert counts["a2"] == 1
