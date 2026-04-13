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

    In production, optimized libraries such as pybloom_live would be used,
    but here we implement it directly for conceptual understanding.

    A Bloom filter determines "probably exists" or "definitely absent" in O(1).
    False positives are possible, but false negatives are not.
    """

    def __init__(self, size: int = 1_000_000, num_hashes: int = 5) -> None:
        """Initialize bloom filter.

        Args:
            size: Bit array size. Larger values reduce false positive probability.
            num_hashes: Number of hash functions. Typically 3-7 are used.
        """
        self._size = size
        self._num_hashes = num_hashes
        self._bit_array = bytearray(size)  # bit array initialized to 0
        self._count = 0

    def _hashes(self, item: str) -> list[int]:
        """Simulate multiple hash functions (double hashing technique).

        h(i) = (h1 + i * h2) mod size
        """
        h1 = int(hashlib.md5(item.encode()).hexdigest(), 16)
        h2 = int(hashlib.sha1(item.encode()).hexdigest(), 16)
        return [(h1 + i * h2) % self._size for i in range(self._num_hashes)]

    def add(self, item: str) -> None:
        """Add an item to the Bloom filter."""
        for idx in self._hashes(item):
            self._bit_array[idx] = 1
        self._count += 1

    def might_contain(self, item: str) -> bool:
        """Check whether an item *might* exist.

        Returns:
            True: probably exists (false positives possible)
            False: definitely absent
        """
        return all(self._bit_array[idx] == 1 for idx in self._hashes(item))

    @property
    def count(self) -> int:
        return self._count


# ---------------------------------------------------------------------------
# URL Seen (set-based; in production use Bloom filter + Redis)
# ---------------------------------------------------------------------------

class URLSeen:
    """Prevent duplicate URL visits.

    Uses an in-memory set to track URLs already visited or added to the
    frontier. In production, replace with a Bloom filter or Redis set.
    """

    def __init__(self) -> None:
        self._seen: set[str] = set()

    def add(self, url: str) -> None:
        """Add a URL to the seen set."""
        self._seen.add(self._normalize(url))

    def is_seen(self, url: str) -> bool:
        """Check whether a URL has already been seen."""
        return self._normalize(url) in self._seen

    @property
    def count(self) -> int:
        return len(self._seen)

    @staticmethod
    def _normalize(url: str) -> str:
        """Normalize a URL: remove trailing slash and fragment."""
        # Remove fragment (#...)
        if "#" in url:
            url = url[: url.index("#")]
        # Remove trailing slash (but keep root "/")
        if url.endswith("/") and len(url) > 1:
            url = url.rstrip("/")
        return url.lower()


# ---------------------------------------------------------------------------
# Content Seen (hash-based content dedup)
# ---------------------------------------------------------------------------

class ContentSeen:
    """Content duplicate detection.

    The same content may exist at different URLs (mirrors, duplicates, etc.).
    Stores the MD5 hash of page body to detect duplicate content.
    In production, similarity-based approaches such as SimHash are also used.
    """

    def __init__(self) -> None:
        self._fingerprints: set[str] = set()

    def is_duplicate(self, content: str) -> bool:
        """Check whether content has already been seen.

        Returns:
            True: duplicate content (seen before)
            False: new content
        """
        fp = self._fingerprint(content)
        if fp in self._fingerprints:
            return True
        self._fingerprints.add(fp)
        return False

    @property
    def count(self) -> int:
        """Number of unique content entries stored."""
        return len(self._fingerprints)

    @staticmethod
    def _fingerprint(content: str) -> str:
        """Return the MD5 hash of the content."""
        return hashlib.md5(content.encode("utf-8")).hexdigest()
