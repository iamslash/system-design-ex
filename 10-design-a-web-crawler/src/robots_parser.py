"""robots.txt parser and access checker.

urllib.robotparser 를 사용하여 robots.txt 를 파싱하고,
특정 URL 에 대한 크롤링 허용 여부를 판단한다.
도메인별로 robots.txt 를 캐싱하여 중복 요청을 방지한다.
"""

from __future__ import annotations

import urllib.robotparser
from urllib.parse import urlparse

import requests


class RobotsChecker:
    """robots.txt parser with per-domain caching.

    크롤러가 방문하려는 URL 이 robots.txt 에 의해 차단되는지 확인한다.
    한 번 가져온 robots.txt 는 메모리에 캐싱하여 재사용한다.
    """

    DEFAULT_USER_AGENT = "SystemDesignCrawler"

    def __init__(
        self,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout: float = 5.0,
    ) -> None:
        """Initialize robots checker.

        Args:
            user_agent: 크롤러의 User-Agent 이름.
            timeout: robots.txt 다운로드 타임아웃 (초).
        """
        self._user_agent = user_agent
        self._timeout = timeout
        self._cache: dict[str, urllib.robotparser.RobotFileParser] = {}

    def is_allowed(self, url: str) -> bool:
        """URL 에 대한 크롤링이 robots.txt 에 의해 허용되는지 확인한다.

        robots.txt 를 가져올 수 없는 경우 (네트워크 오류 등)
        보수적으로 허용(True)을 반환한다.

        Args:
            url: 확인할 URL.

        Returns:
            True: 크롤링 허용, False: 차단됨.
        """
        parser = self._get_parser(url)
        if parser is None:
            return True  # robots.txt 를 가져올 수 없으면 허용
        return parser.can_fetch(self._user_agent, url)

    def _get_parser(self, url: str) -> urllib.robotparser.RobotFileParser | None:
        """도메인의 robots.txt parser 를 캐시에서 가져오거나 새로 생성한다."""
        robots_url = self._robots_url(url)
        if robots_url in self._cache:
            return self._cache[robots_url]

        parser = self._fetch_robots(robots_url)
        self._cache[robots_url] = parser
        return parser

    def _fetch_robots(
        self, robots_url: str
    ) -> urllib.robotparser.RobotFileParser | None:
        """robots.txt 를 다운로드하고 파싱한다.

        네트워크 오류 발생 시 None 을 반환한다.
        """
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(robots_url)
        try:
            resp = requests.get(robots_url, timeout=self._timeout)
            if resp.status_code == 200:
                rp.parse(resp.text.splitlines())
                return rp
            elif resp.status_code in (401, 403):
                # 접근 금지: 모든 URL 차단으로 간주
                rp.parse(["User-agent: *", "Disallow: /"])
                return rp
            else:
                # 404 등: robots.txt 없음 → 모두 허용
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
        """URL 에서 robots.txt URL 을 생성한다."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/robots.txt"
