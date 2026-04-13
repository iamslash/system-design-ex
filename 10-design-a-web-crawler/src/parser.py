"""HTML parser and link extractor.

Uses BeautifulSoup to extract links from HTML and
convert relative URLs to absolute URLs.
"""

from __future__ import annotations

from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup


def extract_links(html: str, base_url: str) -> list[str]:
    """Extract all <a href="..."> links from HTML.

    Args:
        html: HTML string.
        base_url: Base URL used to resolve relative URLs to absolute URLs.

    Returns:
        A list of normalized absolute URLs (deduplicated, HTTP/HTTPS only).
    """
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []

    for anchor in soup.find_all("a", href=True):
        href: str = anchor["href"].strip()

        # Ignore fragment-only links (#section)
        if href.startswith("#"):
            continue

        # Ignore non-HTTP schemes: javascript:, mailto:, tel:, data:, etc.
        if href.startswith(("javascript:", "mailto:", "tel:", "data:")):
            continue

        # Convert relative URL to absolute URL
        absolute = urljoin(base_url, href)

        # Normalize URL
        normalized = _normalize_url(absolute)
        if normalized and _is_valid_http(normalized):
            urls.append(normalized)

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique.append(u)

    return unique


def extract_title(html: str) -> str:
    """Extract the content of the <title> tag from HTML.

    Returns:
        The page title string, or an empty string if absent.
    """
    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.find("title")
    if title_tag and title_tag.string:
        return title_tag.string.strip()
    return ""


def _normalize_url(url: str) -> str | None:
    """Normalize a URL: remove fragment, unify trailing slash.

    Returns:
        The normalized URL, or None if parsing fails.
    """
    try:
        parsed = urlparse(url)
    except ValueError:
        return None

    # Invalid if scheme or host is missing
    if not parsed.scheme or not parsed.netloc:
        return None

    # Remove fragment, normalize path
    path = parsed.path or "/"
    # Convert empty path to "/"
    normalized = parsed._replace(fragment="", path=path)
    return normalized.geturl()


def _is_valid_http(url: str) -> bool:
    """Check whether the URL uses the HTTP or HTTPS scheme."""
    try:
        parsed = urlparse(url)
        return parsed.scheme in ("http", "https")
    except ValueError:
        return False
