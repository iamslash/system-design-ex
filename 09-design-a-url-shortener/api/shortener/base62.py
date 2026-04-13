"""Base62 encode/decode for URL shortening.

Converts an integer ID to a short string using Base62 characters (0-9, a-z, A-Z).
62^7 = 3,521,614,606,208 (about 3.5 trillion) unique codes can be generated.
"""

from __future__ import annotations

# 62 characters: digits (10) + lowercase (26) + uppercase (26)
CHARSET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
BASE = len(CHARSET)  # 62


def encode(num: int) -> str:
    """Encode an integer as a Base62 string.

    Args:
        num: A non-negative integer.

    Returns:
        Base62-encoded string.

    Raises:
        ValueError: When num is negative.

    Examples:
        >>> encode(0)
        '0'
        >>> encode(61)
        'Z'
        >>> encode(62)
        '10'
    """
    if num < 0:
        raise ValueError(f"num must be non-negative, got {num}")

    if num == 0:
        return CHARSET[0]

    # Collect remainders in reverse order to build the string
    chars: list[str] = []
    while num > 0:
        num, remainder = divmod(num, BASE)
        chars.append(CHARSET[remainder])

    return "".join(reversed(chars))


def decode(s: str) -> int:
    """Decode a Base62 string to an integer.

    Args:
        s: A Base62-encoded string.

    Returns:
        The decoded integer.

    Raises:
        ValueError: When the string contains an invalid character.

    Examples:
        >>> decode('0')
        0
        >>> decode('Z')
        61
        >>> decode('10')
        62
    """
    num = 0
    for char in s:
        idx = CHARSET.find(char)
        if idx == -1:
            raise ValueError(f"Invalid Base62 character: {char!r}")
        num = num * BASE + idx
    return num
