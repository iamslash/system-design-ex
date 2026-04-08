"""BFS Web Crawler.

Seed URL 에서 시작하여 BFS (너비 우선 탐색) 방식으로 웹 페이지를 크롤링한다.
URL Frontier, HTML Parser, Dedup, Robots.txt 체커를 조합하여 동작한다.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from urllib.parse import urlparse

import requests

from src.dedup import ContentSeen, URLSeen
from src.frontier import URLFrontier
from src.parser import extract_links, extract_title
from src.robots_parser import RobotsChecker


@dataclass
class CrawlResult:
    """단일 페이지 크롤링 결과."""

    url: str
    status_code: int
    title: str = ""
    links_found: int = 0
    depth: int = 0
    content_duplicate: bool = False
    error: str | None = None


@dataclass
class CrawlStats:
    """크롤링 전체 통계."""

    pages_crawled: int = 0
    pages_failed: int = 0
    content_duplicates: int = 0
    robots_blocked: int = 0
    urls_discovered: int = 0
    elapsed_seconds: float = 0.0


class WebCrawler:
    """BFS web crawler.

    Seed URL 목록에서 시작하여 너비 우선으로 페이지를 탐색한다.
    각 컴포넌트의 역할:
      - URLFrontier: 크롤링 대기열 관리 (우선순위 + 예의)
      - URLSeen: 이미 본 URL 중복 방지
      - ContentSeen: 동일 컨텐츠 중복 탐지
      - RobotsChecker: robots.txt 준수
    """

    DEFAULT_USER_AGENT = "SystemDesignCrawler/1.0"

    def __init__(
        self,
        seed_urls: list[str],
        max_pages: int = 50,
        max_depth: int = 3,
        politeness_delay: float = 1.0,
        request_timeout: float = 10.0,
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        """Initialize crawler.

        Args:
            seed_urls: 크롤링 시작 URL 목록.
            max_pages: 최대 크롤링 페이지 수.
            max_depth: 최대 크롤링 깊이 (seed = depth 0).
            politeness_delay: 같은 호스트 요청 간 최소 대기 시간 (초).
            request_timeout: HTTP 요청 타임아웃 (초).
            user_agent: HTTP User-Agent 헤더.
        """
        self._max_pages = max_pages
        self._request_timeout = request_timeout
        self._user_agent = user_agent

        # 컴포넌트 초기화
        self._frontier = URLFrontier(
            politeness_delay=politeness_delay,
            max_depth=max_depth,
        )
        self._url_seen = URLSeen()
        self._content_seen = ContentSeen()
        self._robots = RobotsChecker(
            user_agent=user_agent,
            timeout=request_timeout,
        )

        # Seed URL 을 frontier 에 추가
        for url in seed_urls:
            if not self._url_seen.is_seen(url):
                self._url_seen.add(url)
                self._frontier.add(url, priority=0, depth=0)

    def crawl(self) -> tuple[list[CrawlResult], CrawlStats]:
        """BFS 크롤링을 실행한다.

        Returns:
            (결과 리스트, 통계) 튜플.
        """
        results: list[CrawlResult] = []
        stats = CrawlStats()
        start_time = time.monotonic()

        while not self._frontier.is_empty and stats.pages_crawled < self._max_pages:
            entry = self._frontier.get_next()
            if entry is None:
                break

            url = entry.url
            depth = entry.depth

            # 1. robots.txt 확인
            if not self._robots.is_allowed(url):
                stats.robots_blocked += 1
                continue

            # 2. Politeness delay 대기
            wait = self._frontier.get_wait_time(url)
            if wait > 0:
                time.sleep(wait)

            # 3. 페이지 다운로드
            result = self._download(url, depth)
            self._frontier.record_access(url)

            if result.error:
                stats.pages_failed += 1
                results.append(result)
                continue

            stats.pages_crawled += 1

            # 4. 컨텐츠 중복 확인
            if result.content_duplicate:
                stats.content_duplicates += 1
                results.append(result)
                continue

            results.append(result)

        stats.urls_discovered = self._url_seen.count
        stats.elapsed_seconds = time.monotonic() - start_time
        return results, stats

    def _download(self, url: str, depth: int) -> CrawlResult:
        """페이지를 다운로드하고 파싱한다."""
        try:
            resp = requests.get(
                url,
                timeout=self._request_timeout,
                headers={"User-Agent": self._user_agent},
                allow_redirects=True,
            )
        except requests.RequestException as e:
            return CrawlResult(
                url=url,
                status_code=0,
                depth=depth,
                error=str(e),
            )

        if resp.status_code != 200:
            return CrawlResult(
                url=url,
                status_code=resp.status_code,
                depth=depth,
                error=f"HTTP {resp.status_code}",
            )

        content = resp.text

        # 컨텐츠 중복 확인
        is_dup = self._content_seen.is_duplicate(content)

        # HTML 파싱 및 링크 추출
        title = extract_title(content)
        links = extract_links(content, url)

        # 새로운 URL 을 frontier 에 추가
        new_depth = depth + 1
        for link in links:
            if not self._url_seen.is_seen(link):
                self._url_seen.add(link)
                # 같은 도메인이면 높은 우선순위, 다른 도메인이면 낮은 우선순위
                priority = self._compute_priority(url, link)
                self._frontier.add(link, priority=priority, depth=new_depth)

        return CrawlResult(
            url=url,
            status_code=resp.status_code,
            title=title,
            links_found=len(links),
            depth=depth,
            content_duplicate=is_dup,
        )

    @staticmethod
    def _compute_priority(source_url: str, target_url: str) -> int:
        """링크의 크롤링 우선순위를 계산한다.

        같은 도메인 → 높은 우선순위 (3)
        다른 도메인 → 낮은 우선순위 (7)
        """
        try:
            source_host = urlparse(source_url).netloc.lower()
            target_host = urlparse(target_url).netloc.lower()
            return 3 if source_host == target_host else 7
        except ValueError:
            return 5
