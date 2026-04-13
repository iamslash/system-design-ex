"""BFS Web Crawler.

Starting from a seed URL, crawls web pages using BFS (breadth-first search).
Combines URL Frontier, HTML Parser, Dedup, and Robots.txt checker.
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
    """Result of crawling a single page."""

    url: str
    status_code: int
    title: str = ""
    links_found: int = 0
    depth: int = 0
    content_duplicate: bool = False
    error: str | None = None


@dataclass
class CrawlStats:
    """Overall crawl statistics."""

    pages_crawled: int = 0
    pages_failed: int = 0
    content_duplicates: int = 0
    robots_blocked: int = 0
    urls_discovered: int = 0
    elapsed_seconds: float = 0.0


class WebCrawler:
    """BFS web crawler.

    Explores pages breadth-first starting from a list of seed URLs.
    Each component's role:
      - URLFrontier: manages the crawl queue (priority + politeness)
      - URLSeen: prevents duplicate URL visits
      - ContentSeen: detects duplicate content
      - RobotsChecker: respects robots.txt
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
            seed_urls: List of seed URLs to start crawling from.
            max_pages: Maximum number of pages to crawl.
            max_depth: Maximum crawl depth (seed = depth 0).
            politeness_delay: Minimum wait time between requests to the same host (seconds).
            request_timeout: HTTP request timeout (seconds).
            user_agent: HTTP User-Agent header value.
        """
        self._max_pages = max_pages
        self._request_timeout = request_timeout
        self._user_agent = user_agent

        # Initialize components
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

        # Add seed URLs to frontier
        for url in seed_urls:
            if not self._url_seen.is_seen(url):
                self._url_seen.add(url)
                self._frontier.add(url, priority=0, depth=0)

    def crawl(self) -> tuple[list[CrawlResult], CrawlStats]:
        """Run BFS crawling.

        Returns:
            Tuple of (list of results, statistics).
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

            # 1. Check robots.txt
            if not self._robots.is_allowed(url):
                stats.robots_blocked += 1
                continue

            # 2. Wait for politeness delay
            wait = self._frontier.get_wait_time(url)
            if wait > 0:
                time.sleep(wait)

            # 3. Download page
            result = self._download(url, depth)
            self._frontier.record_access(url)

            if result.error:
                stats.pages_failed += 1
                results.append(result)
                continue

            stats.pages_crawled += 1

            # 4. Check content for duplicates
            if result.content_duplicate:
                stats.content_duplicates += 1
                results.append(result)
                continue

            results.append(result)

        stats.urls_discovered = self._url_seen.count
        stats.elapsed_seconds = time.monotonic() - start_time
        return results, stats

    def _download(self, url: str, depth: int) -> CrawlResult:
        """Download and parse a page."""
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

        # Check content for duplicates
        is_dup = self._content_seen.is_duplicate(content)

        # Parse HTML and extract links
        title = extract_title(content)
        links = extract_links(content, url)

        # Add new URLs to frontier
        new_depth = depth + 1
        for link in links:
            if not self._url_seen.is_seen(link):
                self._url_seen.add(link)
                # Same domain -> higher priority, different domain -> lower priority
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
        """Compute crawl priority for a link.

        Same domain -> higher priority (3)
        Different domain -> lower priority (7)
        """
        try:
            source_host = urlparse(source_url).netloc.lower()
            target_host = urlparse(target_url).netloc.lower()
            return 3 if source_host == target_host else 7
        except ValueError:
            return 5
