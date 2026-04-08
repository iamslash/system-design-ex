"""URL deduplication and content fingerprinting.

Production crawlers use Bloom filters for memory-efficient URL dedup.
This module provides:
  - URLSeen: set-based URL dedup (+ conceptual Bloom filter)
  - ContentSeen: MD5 hash-based content dedup
"""

from __future__ import annotations

import hashlib


# ---------------------------------------------------------------------------
# Bloom Filter (conceptual / educational implementation)
# ---------------------------------------------------------------------------

class SimpleBloomFilter:
    """Educational Bloom filter implementation.

    실제 프로덕션에서는 pybloom_live 등 최적화된 라이브러리를 사용하지만,
    여기서는 개념 이해를 위해 직접 구현한다.

    Bloom filter 는 "아마도 존재함" 또는 "확실히 없음" 을 O(1) 에 판별한다.
    False positive 는 있지만 false negative 는 없다.
    """

    def __init__(self, size: int = 1_000_000, num_hashes: int = 5) -> None:
        """Initialize bloom filter.

        Args:
            size: 비트 배열 크기. 클수록 false positive 확률이 낮아진다.
            num_hashes: 해시 함수 개수. 보통 3-7 개를 사용한다.
        """
        self._size = size
        self._num_hashes = num_hashes
        self._bit_array = bytearray(size)  # 0 으로 초기화된 비트 배열
        self._count = 0

    def _hashes(self, item: str) -> list[int]:
        """여러 해시 함수 시뮬레이션 (double hashing 기법).

        h(i) = (h1 + i * h2) mod size
        """
        h1 = int(hashlib.md5(item.encode()).hexdigest(), 16)
        h2 = int(hashlib.sha1(item.encode()).hexdigest(), 16)
        return [(h1 + i * h2) % self._size for i in range(self._num_hashes)]

    def add(self, item: str) -> None:
        """항목을 Bloom filter 에 추가한다."""
        for idx in self._hashes(item):
            self._bit_array[idx] = 1
        self._count += 1

    def might_contain(self, item: str) -> bool:
        """항목이 존재할 *수도* 있는지 확인한다.

        Returns:
            True: 아마도 존재함 (false positive 가능)
            False: 확실히 없음
        """
        return all(self._bit_array[idx] == 1 for idx in self._hashes(item))

    @property
    def count(self) -> int:
        return self._count


# ---------------------------------------------------------------------------
# URL Seen (set-based, production 에서는 Bloom filter + Redis)
# ---------------------------------------------------------------------------

class URLSeen:
    """URL 중복 방문 방지.

    메모리 내 set 을 사용하여 이미 방문하거나 frontier 에 추가된
    URL 을 추적한다. 프로덕션에서는 Bloom filter 또는
    Redis set 으로 대체한다.
    """

    def __init__(self) -> None:
        self._seen: set[str] = set()

    def add(self, url: str) -> None:
        """URL 을 seen 집합에 추가한다."""
        self._seen.add(self._normalize(url))

    def is_seen(self, url: str) -> bool:
        """URL 이 이미 본 것인지 확인한다."""
        return self._normalize(url) in self._seen

    @property
    def count(self) -> int:
        return len(self._seen)

    @staticmethod
    def _normalize(url: str) -> str:
        """URL 정규화: trailing slash 제거, fragment 제거."""
        # fragment (#...) 제거
        if "#" in url:
            url = url[: url.index("#")]
        # trailing slash 제거 (root "/" 는 유지)
        if url.endswith("/") and len(url) > 1:
            url = url.rstrip("/")
        return url.lower()


# ---------------------------------------------------------------------------
# Content Seen (hash-based content dedup)
# ---------------------------------------------------------------------------

class ContentSeen:
    """컨텐츠 중복 탐지.

    같은 내용의 페이지가 다른 URL 에 존재할 수 있다 (미러, 복제 등).
    페이지 본문의 MD5 해시를 저장하여 중복 컨텐츠를 탐지한다.
    프로덕션에서는 SimHash 등 유사도 기반 방식도 사용한다.
    """

    def __init__(self) -> None:
        self._fingerprints: set[str] = set()

    def is_duplicate(self, content: str) -> bool:
        """컨텐츠가 이미 본 것인지 확인한다.

        Returns:
            True: 중복 컨텐츠 (이미 본 적 있음)
            False: 새로운 컨텐츠
        """
        fp = self._fingerprint(content)
        if fp in self._fingerprints:
            return True
        self._fingerprints.add(fp)
        return False

    @property
    def count(self) -> int:
        """저장된 고유 컨텐츠 수."""
        return len(self._fingerprints)

    @staticmethod
    def _fingerprint(content: str) -> str:
        """컨텐츠의 MD5 해시를 반환한다."""
        return hashlib.md5(content.encode("utf-8")).hexdigest()
