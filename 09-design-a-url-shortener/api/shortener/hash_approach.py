"""Hash + collision resolution approach for URL shortening.

CRC32 해시의 결과에서 앞 7자를 취하고, 충돌이 발생하면
미리 정의된 문자열을 원본 URL 에 덧붙여 재해싱한다.
"""

from __future__ import annotations

import hashlib
import zlib

# 단축 코드 길이
SHORT_CODE_LENGTH = 7

# 충돌 시 원본 URL 에 덧붙일 문자열 목록
COLLISION_SUFFIXES = [
    "!",
    "@",
    "#",
    "$",
    "%",
    "^",
    "&",
    "*",
]

# 최대 충돌 재시도 횟수
MAX_RETRIES = len(COLLISION_SUFFIXES)


def _crc32_hex(url: str) -> str:
    """URL 의 CRC32 해시를 16진수 문자열로 반환한다."""
    crc = zlib.crc32(url.encode("utf-8")) & 0xFFFFFFFF
    return f"{crc:08x}"


def _md5_hex(url: str) -> str:
    """URL 의 MD5 해시를 16진수 문자열로 반환한다."""
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def generate_short_code(url: str, use_md5: bool = False) -> str:
    """URL 에서 해시 기반 단축 코드를 생성한다.

    CRC32 또는 MD5 해시의 앞 7자를 반환한다.

    Args:
        url: 원본 URL.
        use_md5: True 이면 MD5, False 이면 CRC32 사용.

    Returns:
        7자 단축 코드.
    """
    hash_hex = _md5_hex(url) if use_md5 else _crc32_hex(url)
    return hash_hex[:SHORT_CODE_LENGTH]


def generate_with_collision_resolution(
    url: str,
    exists_fn: callable,
    use_md5: bool = False,
) -> str:
    """충돌 해결을 포함한 단축 코드 생성.

    1. URL 을 해싱하여 앞 7자를 취한다.
    2. DB 에 이미 존재하면 미리 정의된 문자열을 URL 에 덧붙여 재해싱한다.
    3. 최대 MAX_RETRIES 까지 반복한다.

    Args:
        url: 원본 URL.
        exists_fn: 단축 코드가 이미 존재하는지 확인하는 콜백. (code) -> bool.
        use_md5: True 이면 MD5, False 이면 CRC32 사용.

    Returns:
        충돌이 해결된 7자 단축 코드.

    Raises:
        RuntimeError: 최대 재시도 횟수를 초과했을 때.
    """
    candidate_url = url

    for i in range(MAX_RETRIES + 1):
        code = generate_short_code(candidate_url, use_md5=use_md5)

        if not exists_fn(code):
            return code

        # 충돌 발생: 미리 정의된 문자열을 덧붙여 재해싱
        if i < MAX_RETRIES:
            candidate_url = url + COLLISION_SUFFIXES[i]

    raise RuntimeError(
        f"Failed to resolve hash collision after {MAX_RETRIES} retries "
        f"for URL: {url}"
    )
