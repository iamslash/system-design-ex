"""Tests for URL dedup and content seen."""

from __future__ import annotations

from src.dedup import ContentSeen, SimpleBloomFilter, URLSeen


class TestURLSeen:
    """URL seen (set-based dedup) tests."""

    def test_unseen_url(self) -> None:
        """처음 보는 URL 은 is_seen 이 False 여야 한다."""
        seen = URLSeen()
        assert seen.is_seen("http://example.com") is False

    def test_seen_after_add(self) -> None:
        """add 한 URL 은 is_seen 이 True 여야 한다."""
        seen = URLSeen()
        seen.add("http://example.com")
        assert seen.is_seen("http://example.com") is True

    def test_normalize_trailing_slash(self) -> None:
        """trailing slash 가 있든 없든 같은 URL 로 인식해야 한다."""
        seen = URLSeen()
        seen.add("http://example.com/page/")
        assert seen.is_seen("http://example.com/page") is True

    def test_normalize_fragment(self) -> None:
        """fragment (#...) 은 무시해야 한다."""
        seen = URLSeen()
        seen.add("http://example.com/page#section1")
        assert seen.is_seen("http://example.com/page#section2") is True

    def test_normalize_case_insensitive(self) -> None:
        """URL 은 대소문자 구분 없이 비교해야 한다."""
        seen = URLSeen()
        seen.add("http://Example.COM/Page")
        assert seen.is_seen("http://example.com/page") is True

    def test_count(self) -> None:
        """count 는 고유 URL 수를 반환해야 한다."""
        seen = URLSeen()
        seen.add("http://a.com")
        seen.add("http://b.com")
        seen.add("http://a.com")  # 중복
        assert seen.count == 2


class TestContentSeen:
    """Content fingerprint dedup tests."""

    def test_new_content_not_duplicate(self) -> None:
        """처음 보는 컨텐츠는 중복이 아니어야 한다."""
        cs = ContentSeen()
        assert cs.is_duplicate("Hello, World!") is False

    def test_same_content_is_duplicate(self) -> None:
        """같은 컨텐츠는 중복으로 탐지되어야 한다."""
        cs = ContentSeen()
        cs.is_duplicate("Hello, World!")
        assert cs.is_duplicate("Hello, World!") is True

    def test_different_content_not_duplicate(self) -> None:
        """다른 컨텐츠는 중복이 아니어야 한다."""
        cs = ContentSeen()
        cs.is_duplicate("Page A content")
        assert cs.is_duplicate("Page B content") is False

    def test_same_content_different_urls(self) -> None:
        """다른 URL 에서 같은 컨텐츠가 오면 중복으로 탐지해야 한다."""
        cs = ContentSeen()
        content = "<html><body>Mirror page</body></html>"
        assert cs.is_duplicate(content) is False  # 첫 번째
        assert cs.is_duplicate(content) is True   # 두 번째 (중복)

    def test_count(self) -> None:
        """count 는 고유 컨텐츠 수를 반환해야 한다."""
        cs = ContentSeen()
        cs.is_duplicate("content A")
        cs.is_duplicate("content B")
        cs.is_duplicate("content A")  # 중복 → 추가되지 않음
        assert cs.count == 2


class TestSimpleBloomFilter:
    """Educational bloom filter tests."""

    def test_added_item_might_contain(self) -> None:
        """추가한 항목은 might_contain 이 True 여야 한다."""
        bf = SimpleBloomFilter(size=10000, num_hashes=3)
        bf.add("http://example.com")
        assert bf.might_contain("http://example.com") is True

    def test_unseen_item_usually_not_contained(self) -> None:
        """추가하지 않은 항목은 대부분 might_contain 이 False 여야 한다."""
        bf = SimpleBloomFilter(size=100000, num_hashes=3)
        bf.add("http://example.com")
        # 충분히 큰 filter 에서 false positive 확률은 매우 낮음
        assert bf.might_contain("http://totally-different-url.org") is False

    def test_no_false_negatives(self) -> None:
        """Bloom filter 는 false negative 가 없어야 한다."""
        bf = SimpleBloomFilter(size=100000, num_hashes=5)
        urls = [f"http://example.com/page{i}" for i in range(100)]
        for url in urls:
            bf.add(url)

        # 추가한 모든 항목은 반드시 might_contain == True
        for url in urls:
            assert bf.might_contain(url) is True

    def test_count(self) -> None:
        """count 는 추가한 항목 수를 반환해야 한다."""
        bf = SimpleBloomFilter()
        bf.add("a")
        bf.add("b")
        bf.add("c")
        assert bf.count == 3
