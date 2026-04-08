"""HTML parser and link extractor.

BeautifulSoup 을 사용하여 HTML 에서 링크를 추출하고,
상대 URL 을 절대 URL 로 변환한다.
"""

from __future__ import annotations

from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup


def extract_links(html: str, base_url: str) -> list[str]:
    """HTML 에서 모든 <a href="..."> 링크를 추출한다.

    Args:
        html: HTML 문자열.
        base_url: 상대 URL 을 절대 URL 로 변환하기 위한 기준 URL.

    Returns:
        정규화된 절대 URL 목록 (중복 제거, HTTP/HTTPS 만 포함).
    """
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []

    for anchor in soup.find_all("a", href=True):
        href: str = anchor["href"].strip()

        # fragment-only 링크 (#section) 무시
        if href.startswith("#"):
            continue

        # javascript:, mailto:, tel: 등 비-HTTP 스킴 무시
        if href.startswith(("javascript:", "mailto:", "tel:", "data:")):
            continue

        # 상대 URL → 절대 URL 변환
        absolute = urljoin(base_url, href)

        # URL 정규화
        normalized = _normalize_url(absolute)
        if normalized and _is_valid_http(normalized):
            urls.append(normalized)

    # 중복 제거 (순서 유지)
    seen: set[str] = set()
    unique: list[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique.append(u)

    return unique


def extract_title(html: str) -> str:
    """HTML 에서 <title> 태그 내용을 추출한다.

    Returns:
        페이지 제목 문자열, 없으면 빈 문자열.
    """
    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.find("title")
    if title_tag and title_tag.string:
        return title_tag.string.strip()
    return ""


def _normalize_url(url: str) -> str | None:
    """URL 정규화: fragment 제거, trailing slash 통일.

    Returns:
        정규화된 URL 또는 파싱 실패 시 None.
    """
    try:
        parsed = urlparse(url)
    except ValueError:
        return None

    # 스킴과 호스트가 없으면 무효
    if not parsed.scheme or not parsed.netloc:
        return None

    # fragment 제거, path 정규화
    path = parsed.path or "/"
    # 빈 path 를 "/" 로 변환
    normalized = parsed._replace(fragment="", path=path)
    return normalized.geturl()


def _is_valid_http(url: str) -> bool:
    """URL 이 HTTP 또는 HTTPS 스킴인지 확인한다."""
    try:
        parsed = urlparse(url)
        return parsed.scheme in ("http", "https")
    except ValueError:
        return False
