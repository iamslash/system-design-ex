"""robots.txt parser and access checker.

Uses urllib.robotparser to parse robots.txt and determine
whether crawling is allowed for a given URL.
Caches robots.txt per domain to avoid redundant requests.
"""

from __future__ import annotations

import urllib.robotparser
from urllib.parse import urlparse

import requests


class RobotsChecker:
    """robots.txt parser with per-domain caching.

    Checks whether the URL a crawler wants to visit is blocked by robots.txt.
    Once fetched, robots.txt is cached in memory for reuse.
    """

    DEFAULT_USER_AGENT = "SystemDesignCrawler"

    def __init__(
        self,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout: float = 5.0,
    ) -> None:
        """Initialize robots checker.

        Args:
            user_agent: The crawler's User-Agent name.
            timeout: Timeout in seconds for downloading robots.txt.
        """
        self._user_agent = user_agent
        self._timeout = timeout
        self._cache: dict[str, urllib.robotparser.RobotFileParser] = {}

    def is_allowed(self, url: str) -> bool:
        """Check whether crawling the URL is allowed by robots.txt.

        If robots.txt cannot be fetched (network error, etc.),
        conservatively returns True (allow).

        Args:
            url: The URL to check.

        Returns:
            True: crawling allowed, False: blocked.
        """
        parser = self._get_parser(url)
        if parser is None:
            return True  # Allow if robots.txt cannot be fetched
        return parser.can_fetch(self._user_agent, url)

    def _get_parser(self, url: str) -> urllib.robotparser.RobotFileParser | None:
        """Retrieve the robots.txt parser for the domain from cache, or create a new one."""
        robots_url = self._robots_url(url)
        if robots_url in self._cache:
            return self._cache[robots_url]

        parser = self._fetch_robots(robots_url)
        self._cache[robots_url] = parser
        return parser

    def _fetch_robots(
        self, robots_url: str
    ) -> urllib.robotparser.RobotFileParser | None:
        """Download and parse robots.txt.

        Returns None on network errors.
        """
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(robots_url)
        try:
            resp = requests.get(robots_url, timeout=self._timeout)
            if resp.status_code == 200:
                rp.parse(resp.text.splitlines())
                return rp
            elif resp.status_code in (401, 403):
                # Access forbidden: treat as blocking all URLs
                rp.parse(["User-agent: *", "Disallow: /"])
                return rp
            else:
                # 404 etc.: no robots.txt -> allow all
                return None
        except (requests.RequestException, OSError):
            return None

    @property
    def user_agent(self) -> str:
        return self._user_agent

    @property
    def cache_size(self) -> int:
        return len(self._cache)

    @staticmethod
    def _robots_url(url: str) -> str:
        """Build the robots.txt URL from a given URL."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/robots.txt"
