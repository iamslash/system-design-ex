"""Hash + collision resolution approach for URL shortening.

Takes the first 7 characters from a CRC32 hash result; if a collision
occurs, appends a predefined string to the original URL and rehashes.
"""

from __future__ import annotations

import hashlib
import zlib

# Short code length
SHORT_CODE_LENGTH = 7

# List of strings to append to the original URL on collision
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

# Maximum number of collision retry attempts
MAX_RETRIES = len(COLLISION_SUFFIXES)


def _crc32_hex(url: str) -> str:
    """Return the CRC32 hash of the URL as a hex string."""
    crc = zlib.crc32(url.encode("utf-8")) & 0xFFFFFFFF
    return f"{crc:08x}"


def _md5_hex(url: str) -> str:
    """Return the MD5 hash of the URL as a hex string."""
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def generate_short_code(url: str, use_md5: bool = False) -> str:
    """Generate a hash-based short code from a URL.

    Returns the first 7 characters of the CRC32 or MD5 hash.

    Args:
        url: The original URL.
        use_md5: If True, use MD5; otherwise use CRC32.

    Returns:
        A 7-character short code.
    """
    hash_hex = _md5_hex(url) if use_md5 else _crc32_hex(url)
    return hash_hex[:SHORT_CODE_LENGTH]


def generate_with_collision_resolution(
    url: str,
    exists_fn: callable,
    use_md5: bool = False,
) -> str:
    """Generate a short code with collision resolution.

    1. Hash the URL and take the first 7 characters.
    2. If it already exists in the DB, append a predefined string to the URL and rehash.
    3. Repeat up to MAX_RETRIES times.

    Args:
        url: The original URL.
        exists_fn: Callback to check whether a short code already exists. (code) -> bool.
        use_md5: If True, use MD5; otherwise use CRC32.

    Returns:
        A collision-resolved 7-character short code.

    Raises:
        RuntimeError: When the maximum number of retries is exceeded.
    """
    candidate_url = url

    for i in range(MAX_RETRIES + 1):
        code = generate_short_code(candidate_url, use_md5=use_md5)

        if not exists_fn(code):
            return code

        # Collision detected: append a predefined string and rehash
        if i < MAX_RETRIES:
            candidate_url = url + COLLISION_SUFFIXES[i]

    raise RuntimeError(
        f"Failed to resolve hash collision after {MAX_RETRIES} retries "
        f"for URL: {url}"
    )
