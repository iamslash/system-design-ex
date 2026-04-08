"""Redis-based URL storage.

Redis 에 단축 URL 매핑을 저장하고 조회한다.

키 구조:
  - url:short:{code}  — hash: {long_url, created_at, clicks}
  - url:long:{hash}   — dedup: long URL 해시 → 단축 코드
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass

from redis.asyncio import Redis


# Redis 키 접두사
SHORT_PREFIX = "url:short:"
LONG_PREFIX = "url:long:"


@dataclass
class URLEntry:
    """저장된 URL 항목."""

    short_code: str
    long_url: str
    created_at: float
    clicks: int


class RedisURLStore:
    """Redis 기반 URL 저장소."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def save(self, short_code: str, long_url: str) -> URLEntry:
        """단축 URL 매핑을 저장한다.

        SETNX 를 사용하여 역방향 매핑을 원자적으로 설정한다.
        동시 요청이 같은 long URL 을 단축하더라도 하나만 성공하고,
        나머지는 기존 코드를 반환한다.

        Args:
            short_code: 단축 코드.
            long_url: 원본 URL.

        Returns:
            저장된 URLEntry.
        """
        now = time.time()

        # 원자적 중복 방지: SETNX 로 역방향 매핑을 먼저 확보
        long_hash = self._hash_url(long_url)
        long_key = f"{LONG_PREFIX}{long_hash}"
        was_set = await self._redis.setnx(long_key, short_code)

        if not was_set:
            # 다른 요청이 먼저 저장함 → 기존 코드 사용
            existing_code = await self._redis.get(long_key)
            if existing_code and existing_code != short_code:
                existing = await self.get_by_code(existing_code)
                if existing:
                    return existing

        # 순방향 매핑 저장: short_code → long_url
        short_key = f"{SHORT_PREFIX}{short_code}"
        await self._redis.hset(
            short_key,
            mapping={
                "long_url": long_url,
                "created_at": str(now),
                "clicks": "0",
            },
        )

        return URLEntry(
            short_code=short_code,
            long_url=long_url,
            created_at=now,
            clicks=0,
        )

    async def get_by_code(self, short_code: str) -> URLEntry | None:
        """단축 코드로 URL 항목을 조회한다.

        Args:
            short_code: 단축 코드.

        Returns:
            URLEntry 또는 존재하지 않으면 None.
        """
        short_key = f"{SHORT_PREFIX}{short_code}"
        data = await self._redis.hgetall(short_key)

        if not data:
            return None

        return URLEntry(
            short_code=short_code,
            long_url=data["long_url"],
            created_at=float(data["created_at"]),
            clicks=int(data["clicks"]),
        )

    async def get_code_by_long_url(self, long_url: str) -> str | None:
        """원본 URL 로 기존 단축 코드를 조회한다 (중복 방지).

        Args:
            long_url: 원본 URL.

        Returns:
            기존 단축 코드 또는 존재하지 않으면 None.
        """
        long_hash = self._hash_url(long_url)
        code = await self._redis.get(f"{LONG_PREFIX}{long_hash}")
        return code

    async def increment_clicks(self, short_code: str) -> int:
        """클릭 카운트를 1 증가시킨다.

        Args:
            short_code: 단축 코드.

        Returns:
            증가된 클릭 수.
        """
        short_key = f"{SHORT_PREFIX}{short_code}"
        return await self._redis.hincrby(short_key, "clicks", 1)

    async def code_exists(self, short_code: str) -> bool:
        """단축 코드가 존재하는지 확인한다.

        Args:
            short_code: 단축 코드.

        Returns:
            존재하면 True.
        """
        return await self._redis.exists(f"{SHORT_PREFIX}{short_code}") > 0

    @staticmethod
    def _hash_url(url: str) -> str:
        """URL 의 SHA-256 해시를 반환한다 (중복 검사용)."""
        return hashlib.sha256(url.encode("utf-8")).hexdigest()
