"""Ad click event model and generator.

Provides the AdClickEvent dataclass and utilities for generating
synthetic click streams for testing and demonstration.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Sequence


@dataclass(frozen=True, slots=True)
class AdClickEvent:
    """Immutable representation of a single ad click."""

    ad_id: str
    timestamp: float  # Unix epoch seconds
    user_id: str
    ip: str
    country: str

    def minute_key(self) -> float:
        """Return the floored minute boundary for this event's timestamp."""
        return self.timestamp - (self.timestamp % 60)


# -- Synthetic data generation ------------------------------------------------

_COUNTRIES: list[str] = ["US", "KR", "JP", "DE", "BR", "IN", "GB", "FR", "CA", "AU"]

_IP_POOL: list[str] = [f"192.168.{i}.{j}" for i in range(10) for j in range(1, 26)]


def _random_ad_id(num_ads: int) -> str:
    return f"ad_{random.randint(1, num_ads):04d}"


def _random_user_id(num_users: int) -> str:
    return f"user_{random.randint(1, num_users):06d}"


def generate_events(
    count: int,
    *,
    num_ads: int = 100,
    num_users: int = 10_000,
    time_span_seconds: float = 600.0,
    base_time: float | None = None,
) -> list[AdClickEvent]:
    """Generate *count* synthetic AdClickEvent instances.

    Args:
        count: Number of events to create.
        num_ads: Size of the ad-id pool (controls cardinality).
        num_users: Size of the user-id pool.
        time_span_seconds: Events are distributed over this window.
        base_time: Start of the window (defaults to *now*).

    Returns:
        A list of events sorted by timestamp.
    """
    if base_time is None:
        base_time = time.time() - time_span_seconds

    events: list[AdClickEvent] = []
    for _ in range(count):
        ts = base_time + random.random() * time_span_seconds
        events.append(
            AdClickEvent(
                ad_id=_random_ad_id(num_ads),
                timestamp=ts,
                user_id=_random_user_id(num_users),
                ip=random.choice(_IP_POOL),
                country=random.choice(_COUNTRIES),
            )
        )
    events.sort(key=lambda e: e.timestamp)
    return events


def generate_events_for_ad(
    ad_id: str,
    count: int,
    *,
    base_time: float,
    time_span_seconds: float = 60.0,
    country: str = "US",
) -> list[AdClickEvent]:
    """Generate events for a specific ad_id (useful in tests)."""
    events: list[AdClickEvent] = []
    for i in range(count):
        ts = base_time + (time_span_seconds / count) * i
        events.append(
            AdClickEvent(
                ad_id=ad_id,
                timestamp=ts,
                user_id=f"user_{i:06d}",
                ip=random.choice(_IP_POOL),
                country=country,
            )
        )
    return events
