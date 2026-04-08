"""Rate limiting algorithm implementations."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RateLimitResult:
    """Result of a rate limit check."""

    allowed: bool
    limit: int
    remaining: int
    retry_after: int  # seconds; 0 when allowed
