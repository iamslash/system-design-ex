"""URL Frontier: priority queue + per-host politeness.

A web crawler's URL Frontier serves two roles:
  1. Front queues (Priority): assign priority based on URL importance
  2. Back queues (Politeness): regulate the request interval for the same host

This module combines both functions into a single class for educational purposes.
"""

from __future__ import annotations

import heapq
import time
from dataclasses import dataclass, field
from urllib.parse import urlparse


@dataclass(order=True)
class FrontierEntry:
    """A frontier queue entry.

    Lower priority values are processed first (min-heap).
    """

    priority: int
    url: str = field(compare=False)
    depth: int = field(compare=False, default=0)
    _counter: int = field(compare=True, default=0, repr=False)


class URLFrontier:
    """URL Frontier with priority ordering and per-host politeness.

    - Priority queue: lower priority values are crawled first
    - Politeness: maintains a minimum delay between consecutive requests to the same host
    - Max depth: URLs exceeding the configured maximum depth are not added
    """

    def __init__(
        self,
        politeness_delay: float = 1.0,
        max_depth: int = 3,
    ) -> None:
        """Initialize frontier.

        Args:
            politeness_delay: Minimum request interval for the same host (seconds).
            max_depth: Maximum crawl depth. URLs exceeding this depth are ignored.
        """
        self._heap: list[FrontierEntry] = []
        self._politeness_delay = politeness_delay
        self._max_depth = max_depth
        self._last_access: dict[str, float] = {}  # host -> last access time
        self._counter = 0  # tie-breaker for heap (FIFO within same priority)

    def add(self, url: str, priority: int = 5, depth: int = 0) -> bool:
        """Add a URL to the frontier.

        Args:
            url: The URL to crawl.
            priority: Priority (0 = highest, 9 = lowest). Default 5.
            depth: The crawl depth of the current URL.

        Returns:
            True: successfully added, False: ignored because max_depth was exceeded.
        """
        if depth > self._max_depth:
            return False

        entry = FrontierEntry(
            priority=priority,
            url=url,
            depth=depth,
            _counter=self._counter,
        )
        self._counter += 1
        heapq.heappush(self._heap, entry)
        return True

    def get_next(self) -> FrontierEntry | None:
        """Return the next URL to crawl.

        For politeness delay, records the last access time for the host
        before returning.

        Returns:
            A FrontierEntry, or None if the frontier is empty.
        """
        if not self._heap:
            return None
        return heapq.heappop(self._heap)

    def get_wait_time(self, url: str) -> float:
        """Return how many seconds to wait before requesting the host of this URL.

        Returns:
            Wait time in seconds. 0 means the request can be made immediately.
        """
        host = self._get_host(url)
        last = self._last_access.get(host, 0.0)
        elapsed = time.monotonic() - last
        remaining = self._politeness_delay - elapsed
        return max(0.0, remaining)

    def record_access(self, url: str) -> None:
        """Record the access time for the host."""
        host = self._get_host(url)
        self._last_access[host] = time.monotonic()

    @property
    def size(self) -> int:
        """Number of URLs remaining in the frontier."""
        return len(self._heap)

    @property
    def is_empty(self) -> bool:
        return len(self._heap) == 0

    @property
    def politeness_delay(self) -> float:
        return self._politeness_delay

    @property
    def max_depth(self) -> int:
        return self._max_depth

    @staticmethod
    def _get_host(url: str) -> str:
        """Extract the host from a URL."""
        try:
            return urlparse(url).netloc.lower()
        except ValueError:
            return url
