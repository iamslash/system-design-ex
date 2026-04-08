"""Tests for URL Frontier."""

from __future__ import annotations

import time

from src.frontier import URLFrontier


class TestFrontierPriority:
    """Priority ordering tests."""

    def test_higher_priority_first(self) -> None:
        """낮은 priority 값이 먼저 반환되어야 한다."""
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
        """같은 priority 내에서는 FIFO 순서를 유지해야 한다."""
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
        """빈 frontier 에서 get_next 는 None 을 반환해야 한다."""
        frontier = URLFrontier()
        assert frontier.get_next() is None
        assert frontier.is_empty


class TestFrontierPoliteness:
    """Per-host politeness delay tests."""

    def test_wait_time_initially_zero(self) -> None:
        """첫 방문 호스트에 대한 대기 시간은 0 이어야 한다."""
        frontier = URLFrontier(politeness_delay=1.0)
        wait = frontier.get_wait_time("http://example.com/page1")
        assert wait == 0.0

    def test_wait_time_after_access(self) -> None:
        """방문 직후 같은 호스트에 대한 대기 시간은 양수여야 한다."""
        frontier = URLFrontier(politeness_delay=1.0)
        frontier.record_access("http://example.com/page1")
        wait = frontier.get_wait_time("http://example.com/page2")
        assert wait > 0.0

    def test_different_hosts_independent(self) -> None:
        """다른 호스트의 대기 시간은 서로 독립적이어야 한다."""
        frontier = URLFrontier(politeness_delay=1.0)
        frontier.record_access("http://a.com/page")

        # a.com 은 대기 필요
        wait_a = frontier.get_wait_time("http://a.com/other")
        assert wait_a > 0.0

        # b.com 은 대기 불필요
        wait_b = frontier.get_wait_time("http://b.com/page")
        assert wait_b == 0.0

    def test_wait_time_decreases_over_time(self) -> None:
        """시간이 지나면 대기 시간이 줄어야 한다."""
        frontier = URLFrontier(politeness_delay=0.1)
        frontier.record_access("http://example.com/page")
        wait1 = frontier.get_wait_time("http://example.com/other")

        time.sleep(0.05)
        wait2 = frontier.get_wait_time("http://example.com/other")
        assert wait2 < wait1


class TestFrontierDepth:
    """Max depth tests."""

    def test_within_max_depth_accepted(self) -> None:
        """max_depth 이내의 URL 은 추가되어야 한다."""
        frontier = URLFrontier(max_depth=2)
        assert frontier.add("http://a.com", depth=0) is True
        assert frontier.add("http://b.com", depth=1) is True
        assert frontier.add("http://c.com", depth=2) is True
        assert frontier.size == 3

    def test_exceeding_max_depth_rejected(self) -> None:
        """max_depth 를 초과하는 URL 은 무시되어야 한다."""
        frontier = URLFrontier(max_depth=2)
        assert frontier.add("http://deep.com", depth=3) is False
        assert frontier.size == 0

    def test_depth_stored_in_entry(self) -> None:
        """FrontierEntry 에 depth 가 올바르게 저장되어야 한다."""
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
