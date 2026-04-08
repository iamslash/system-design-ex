"""URL Frontier: priority queue + per-host politeness.

Web crawler 의 URL Frontier 는 두 가지 역할을 한다:
  1. Front queues (Priority): URL 의 중요도에 따라 우선순위를 부여
  2. Back queues (Politeness): 같은 호스트에 대한 요청 간격을 조절

이 모듈은 교육용으로 두 기능을 하나의 클래스에 통합 구현한다.
"""

from __future__ import annotations

import heapq
import time
from dataclasses import dataclass, field
from urllib.parse import urlparse


@dataclass(order=True)
class FrontierEntry:
    """Frontier 큐 항목.

    priority 가 낮을수록 먼저 처리된다 (min-heap).
    """

    priority: int
    url: str = field(compare=False)
    depth: int = field(compare=False, default=0)
    _counter: int = field(compare=True, default=0, repr=False)


class URLFrontier:
    """URL Frontier with priority ordering and per-host politeness.

    - Priority queue: 낮은 priority 값이 먼저 크롤링됨
    - Politeness: 같은 호스트에 대한 연속 요청 사이에 최소 delay 유지
    - Max depth: 설정된 최대 깊이를 초과하는 URL 은 추가하지 않음
    """

    def __init__(
        self,
        politeness_delay: float = 1.0,
        max_depth: int = 3,
    ) -> None:
        """Initialize frontier.

        Args:
            politeness_delay: 같은 호스트에 대한 최소 요청 간격 (초).
            max_depth: 크롤링 최대 깊이. 이 깊이를 초과하는 URL 은 무시.
        """
        self._heap: list[FrontierEntry] = []
        self._politeness_delay = politeness_delay
        self._max_depth = max_depth
        self._last_access: dict[str, float] = {}  # host -> last access time
        self._counter = 0  # tie-breaker for heap (FIFO within same priority)

    def add(self, url: str, priority: int = 5, depth: int = 0) -> bool:
        """URL 을 Frontier 에 추가한다.

        Args:
            url: 크롤링할 URL.
            priority: 우선순위 (0 = 최고, 9 = 최저). 기본값 5.
            depth: 현재 URL 의 크롤링 깊이.

        Returns:
            True: 추가 성공, False: max_depth 초과로 무시됨.
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
        """다음에 크롤링할 URL 을 반환한다.

        Politeness delay 를 위해, 반환 전에 해당 호스트에 대한
        마지막 접근 시간을 기록한다.

        Returns:
            FrontierEntry 또는 frontier 가 비어 있으면 None.
        """
        if not self._heap:
            return None
        return heapq.heappop(self._heap)

    def get_wait_time(self, url: str) -> float:
        """해당 URL 의 호스트에 대해 대기해야 할 시간(초)을 반환한다.

        Returns:
            대기 시간. 0 이면 즉시 요청 가능.
        """
        host = self._get_host(url)
        last = self._last_access.get(host, 0.0)
        elapsed = time.monotonic() - last
        remaining = self._politeness_delay - elapsed
        return max(0.0, remaining)

    def record_access(self, url: str) -> None:
        """호스트에 대한 접근 시간을 기록한다."""
        host = self._get_host(url)
        self._last_access[host] = time.monotonic()

    @property
    def size(self) -> int:
        """Frontier 에 남아 있는 URL 수."""
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
        """URL 에서 호스트를 추출한다."""
        try:
            return urlparse(url).netloc.lower()
        except ValueError:
            return url
