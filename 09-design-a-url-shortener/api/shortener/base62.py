"""Base62 encode/decode for URL shortening.

62진법 문자(0-9, a-z, A-Z)를 사용하여 정수 ID 를 짧은 문자열로 변환한다.
62^7 = 3,521,614,606,208 (약 3.5조) 개의 고유 코드를 생성할 수 있다.
"""

from __future__ import annotations

# 62개 문자: 숫자(10) + 소문자(26) + 대문자(26)
CHARSET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
BASE = len(CHARSET)  # 62


def encode(num: int) -> str:
    """정수를 Base62 문자열로 인코딩한다.

    Args:
        num: 0 이상의 정수.

    Returns:
        Base62 인코딩된 문자열.

    Raises:
        ValueError: num 이 음수일 때.

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

    # 나머지를 역순으로 모아서 문자열 생성
    chars: list[str] = []
    while num > 0:
        num, remainder = divmod(num, BASE)
        chars.append(CHARSET[remainder])

    return "".join(reversed(chars))


def decode(s: str) -> int:
    """Base62 문자열을 정수로 디코딩한다.

    Args:
        s: Base62 인코딩된 문자열.

    Returns:
        디코딩된 정수.

    Raises:
        ValueError: 유효하지 않은 문자가 포함되었을 때.

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
