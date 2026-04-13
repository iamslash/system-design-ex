"""Tests for HTML parser and link extractor."""

from __future__ import annotations

from src.parser import extract_links, extract_title


SAMPLE_HTML = """
<!DOCTYPE html>
<html>
<head><title>Test Page</title></head>
<body>
    <a href="/about">About</a>
    <a href="/contact">Contact</a>
    <a href="https://external.com/page">External</a>
    <a href="relative/path">Relative</a>
    <a href="#section">Fragment Only</a>
    <a href="javascript:void(0)">JS Link</a>
    <a href="mailto:test@example.com">Email</a>
    <a href="https://example.com/page#frag">With Fragment</a>
</body>
</html>
"""


class TestExtractLinks:
    """Link extraction tests."""

    def test_absolute_links(self) -> None:
        """Absolute URLs should be extracted correctly."""
        links = extract_links(SAMPLE_HTML, "http://example.com/")
        assert "https://external.com/page" in links

    def test_relative_to_absolute(self) -> None:
        """Relative URLs should be converted to absolute URLs."""
        links = extract_links(SAMPLE_HTML, "http://example.com/")
        assert "http://example.com/about" in links
        assert "http://example.com/contact" in links

    def test_relative_path(self) -> None:
        """Relative paths should be resolved against the base URL."""
        links = extract_links(SAMPLE_HTML, "http://example.com/dir/")
        assert "http://example.com/dir/relative/path" in links

    def test_fragment_only_excluded(self) -> None:
        """Fragment-only links (#...) should be excluded."""
        links = extract_links(SAMPLE_HTML, "http://example.com/")
        fragment_links = [l for l in links if l == "http://example.com/#section"]
        # fragment-only anchor (href="#section") should be excluded
        assert len(fragment_links) == 0

    def test_javascript_excluded(self) -> None:
        """javascript: links should be excluded."""
        links = extract_links(SAMPLE_HTML, "http://example.com/")
        js_links = [l for l in links if "javascript" in l]
        assert len(js_links) == 0

    def test_mailto_excluded(self) -> None:
        """mailto: links should be excluded."""
        links = extract_links(SAMPLE_HTML, "http://example.com/")
        mail_links = [l for l in links if "mailto" in l]
        assert len(mail_links) == 0

    def test_no_duplicates(self) -> None:
        """Duplicate links should be removed."""
        html = """
        <a href="/page">Link 1</a>
        <a href="/page">Link 2</a>
        <a href="/page">Link 3</a>
        """
        links = extract_links(html, "http://example.com/")
        assert links.count("http://example.com/page") == 1

    def test_empty_html(self) -> None:
        """Should return an empty list for empty HTML."""
        links = extract_links("", "http://example.com/")
        assert links == []

    def test_no_links(self) -> None:
        """Should return an empty list for HTML with no links."""
        html = "<html><body><p>No links here.</p></body></html>"
        links = extract_links(html, "http://example.com/")
        assert links == []


class TestExtractTitle:
    """Title extraction tests."""

    def test_title_extracted(self) -> None:
        """The content of the <title> tag should be extracted correctly."""
        title = extract_title(SAMPLE_HTML)
        assert title == "Test Page"

    def test_no_title(self) -> None:
        """Should return an empty string when there is no <title> tag."""
        html = "<html><body>No title</body></html>"
        title = extract_title(html)
        assert title == ""

    def test_empty_title(self) -> None:
        """An empty <title> should return an empty string."""
        html = "<html><head><title></title></head></html>"
        title = extract_title(html)
        assert title == ""

    def test_whitespace_title(self) -> None:
        """A title containing only whitespace should be stripped."""
        html = "<html><head><title>  Spaced Title  </title></head></html>"
        title = extract_title(html)
        assert title == "Spaced Title"
