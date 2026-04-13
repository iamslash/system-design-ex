"""Tests for URL dedup and content seen."""

from __future__ import annotations

from src.dedup import ContentSeen, SimpleBloomFilter, URLSeen


class TestURLSeen:
    """URL seen (set-based dedup) tests."""

    def test_unseen_url(self) -> None:
        """is_seen should be False for a URL that has not been seen before."""
        seen = URLSeen()
        assert seen.is_seen("http://example.com") is False

    def test_seen_after_add(self) -> None:
        """is_seen should be True for a URL that has been added."""
        seen = URLSeen()
        seen.add("http://example.com")
        assert seen.is_seen("http://example.com") is True

    def test_normalize_trailing_slash(self) -> None:
        """URLs with and without a trailing slash should be treated as the same URL."""
        seen = URLSeen()
        seen.add("http://example.com/page/")
        assert seen.is_seen("http://example.com/page") is True

    def test_normalize_fragment(self) -> None:
        """Fragments (#...) should be ignored."""
        seen = URLSeen()
        seen.add("http://example.com/page#section1")
        assert seen.is_seen("http://example.com/page#section2") is True

    def test_normalize_case_insensitive(self) -> None:
        """URLs should be compared case-insensitively."""
        seen = URLSeen()
        seen.add("http://Example.COM/Page")
        assert seen.is_seen("http://example.com/page") is True

    def test_count(self) -> None:
        """count should return the number of unique URLs."""
        seen = URLSeen()
        seen.add("http://a.com")
        seen.add("http://b.com")
        seen.add("http://a.com")  # duplicate
        assert seen.count == 2


class TestContentSeen:
    """Content fingerprint dedup tests."""

    def test_new_content_not_duplicate(self) -> None:
        """Content seen for the first time should not be a duplicate."""
        cs = ContentSeen()
        assert cs.is_duplicate("Hello, World!") is False

    def test_same_content_is_duplicate(self) -> None:
        """The same content should be detected as a duplicate."""
        cs = ContentSeen()
        cs.is_duplicate("Hello, World!")
        assert cs.is_duplicate("Hello, World!") is True

    def test_different_content_not_duplicate(self) -> None:
        """Different content should not be a duplicate."""
        cs = ContentSeen()
        cs.is_duplicate("Page A content")
        assert cs.is_duplicate("Page B content") is False

    def test_same_content_different_urls(self) -> None:
        """The same content from different URLs should be detected as a duplicate."""
        cs = ContentSeen()
        content = "<html><body>Mirror page</body></html>"
        assert cs.is_duplicate(content) is False  # first time
        assert cs.is_duplicate(content) is True   # second time (duplicate)

    def test_count(self) -> None:
        """count should return the number of unique content entries."""
        cs = ContentSeen()
        cs.is_duplicate("content A")
        cs.is_duplicate("content B")
        cs.is_duplicate("content A")  # duplicate -> not added
        assert cs.count == 2


class TestSimpleBloomFilter:
    """Educational bloom filter tests."""

    def test_added_item_might_contain(self) -> None:
        """might_contain should be True for an added item."""
        bf = SimpleBloomFilter(size=10000, num_hashes=3)
        bf.add("http://example.com")
        assert bf.might_contain("http://example.com") is True

    def test_unseen_item_usually_not_contained(self) -> None:
        """might_contain should be False for an item that was not added."""
        bf = SimpleBloomFilter(size=100000, num_hashes=3)
        bf.add("http://example.com")
        # False positive probability is very low with a sufficiently large filter
        assert bf.might_contain("http://totally-different-url.org") is False

    def test_no_false_negatives(self) -> None:
        """A Bloom filter should have no false negatives."""
        bf = SimpleBloomFilter(size=100000, num_hashes=5)
        urls = [f"http://example.com/page{i}" for i in range(100)]
        for url in urls:
            bf.add(url)

        # All added items must have might_contain == True
        for url in urls:
            assert bf.might_contain(url) is True

    def test_count(self) -> None:
        """count should return the number of added items."""
        bf = SimpleBloomFilter()
        bf.add("a")
        bf.add("b")
        bf.add("c")
        assert bf.count == 3
