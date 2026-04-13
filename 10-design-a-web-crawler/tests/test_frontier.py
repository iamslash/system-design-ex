"""Tests for URL Frontier."""

from __future__ import annotations

import time

from src.frontier import URLFrontier


class TestFrontierPriority:
    """Priority ordering tests."""

    def test_higher_priority_first(self) -> None:
        """Lower priority values should be returned first."""
        frontier = URLFrontier(politeness_delay=0.0)
        frontier.add("http://low.com", priority=9, depth=0)
        frontier.add("http://high.com", priority=1, depth=0)
        frontier.add("http://mid.com", priority=5, depth=0)

        entry = frontier.get_next()
        assert entry is not None
        assert entry.url == "http://high.com"

        entry = frontier.get_next()
        assert entry is not None
        assert entry.url == "http://mid.com"

        entry = frontier.get_next()
        assert entry is not None
        assert entry.url == "http://low.com"

    def test_fifo_within_same_priority(self) -> None:
        """FIFO order should be maintained within the same priority level."""
        frontier = URLFrontier(politeness_delay=0.0)
        frontier.add("http://first.com", priority=5, depth=0)
        frontier.add("http://second.com", priority=5, depth=0)
        frontier.add("http://third.com", priority=5, depth=0)

        urls = []
        while not frontier.is_empty:
            entry = frontier.get_next()
            assert entry is not None
            urls.append(entry.url)

        assert urls == [
            "http://first.com",
            "http://second.com",
            "http://third.com",
        ]

    def test_empty_frontier_returns_none(self) -> None:
        """get_next should return None for an empty frontier."""
        frontier = URLFrontier()
        assert frontier.get_next() is None
        assert frontier.is_empty


class TestFrontierPoliteness:
    """Per-host politeness delay tests."""

    def test_wait_time_initially_zero(self) -> None:
        """Wait time should be 0 for a host that has never been visited."""
        frontier = URLFrontier(politeness_delay=1.0)
        wait = frontier.get_wait_time("http://example.com/page1")
        assert wait == 0.0

    def test_wait_time_after_access(self) -> None:
        """Wait time should be positive immediately after visiting the same host."""
        frontier = URLFrontier(politeness_delay=1.0)
        frontier.record_access("http://example.com/page1")
        wait = frontier.get_wait_time("http://example.com/page2")
        assert wait > 0.0

    def test_different_hosts_independent(self) -> None:
        """Wait times for different hosts should be independent."""
        frontier = URLFrontier(politeness_delay=1.0)
        frontier.record_access("http://a.com/page")

        # a.com requires waiting
        wait_a = frontier.get_wait_time("http://a.com/other")
        assert wait_a > 0.0

        # b.com requires no waiting
        wait_b = frontier.get_wait_time("http://b.com/page")
        assert wait_b == 0.0

    def test_wait_time_decreases_over_time(self) -> None:
        """Wait time should decrease as time passes."""
        frontier = URLFrontier(politeness_delay=0.1)
        frontier.record_access("http://example.com/page")
        wait1 = frontier.get_wait_time("http://example.com/other")

        time.sleep(0.05)
        wait2 = frontier.get_wait_time("http://example.com/other")
        assert wait2 < wait1


class TestFrontierDepth:
    """Max depth tests."""

    def test_within_max_depth_accepted(self) -> None:
        """URLs within max_depth should be accepted."""
        frontier = URLFrontier(max_depth=2)
        assert frontier.add("http://a.com", depth=0) is True
        assert frontier.add("http://b.com", depth=1) is True
        assert frontier.add("http://c.com", depth=2) is True
        assert frontier.size == 3

    def test_exceeding_max_depth_rejected(self) -> None:
        """URLs exceeding max_depth should be ignored."""
        frontier = URLFrontier(max_depth=2)
        assert frontier.add("http://deep.com", depth=3) is False
        assert frontier.size == 0

    def test_depth_stored_in_entry(self) -> None:
        """depth should be stored correctly in FrontierEntry."""
        frontier = URLFrontier(max_depth=5)
        frontier.add("http://example.com", depth=3)
        entry = frontier.get_next()
        assert entry is not None
        assert entry.depth == 3


class TestFrontierSize:
    """Size and empty state tests."""

    def test_size_increases_on_add(self) -> None:
        frontier = URLFrontier()
        assert frontier.size == 0
        frontier.add("http://a.com")
        assert frontier.size == 1
        frontier.add("http://b.com")
        assert frontier.size == 2

    def test_size_decreases_on_get(self) -> None:
        frontier = URLFrontier()
        frontier.add("http://a.com")
        frontier.add("http://b.com")
        frontier.get_next()
        assert frontier.size == 1
