"""Tests for rate limiting algorithms using fakeredis."""

from __future__ import annotations

import asyncio
import sys
import os
import time
from unittest.mock import patch

import pytest

# Add the api directory to the path so we can import the algorithm modules.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

from algorithms.token_bucket import TokenBucket
from algorithms.sliding_window_counter import SlidingWindowCounter


# ---------------------------------------------------------------------------
# Token Bucket
# ---------------------------------------------------------------------------


class TestTokenBucket:
    """Tests for the Token Bucket algorithm."""

    @pytest.mark.asyncio
    async def test_allows_requests_within_limit(self, redis_client) -> None:
        """Requests should succeed while tokens are available."""
        bucket = TokenBucket(redis_client, bucket_size=5, refill_rate=1.0)

        for i in range(5):
            result = await bucket.is_allowed("client-1")
            assert result.allowed, f"Request {i + 1} should be allowed"
            assert result.remaining == 5 - i - 1
            assert result.limit == 5

    @pytest.mark.asyncio
    async def test_rejects_when_empty(self, redis_client) -> None:
        """Once all tokens are consumed, requests should be rejected."""
        bucket = TokenBucket(redis_client, bucket_size=3, refill_rate=1.0)

        # Exhaust all tokens
        for _ in range(3):
            result = await bucket.is_allowed("client-1")
            assert result.allowed

        # Next request should be rejected
        result = await bucket.is_allowed("client-1")
        assert not result.allowed
        assert result.remaining == 0
        assert result.retry_after > 0

    @pytest.mark.asyncio
    async def test_refills_over_time(self, redis_client) -> None:
        """Tokens should refill after time passes."""
        bucket = TokenBucket(redis_client, bucket_size=2, refill_rate=10.0)

        # Exhaust all tokens
        for _ in range(2):
            await bucket.is_allowed("client-1")

        result = await bucket.is_allowed("client-1")
        assert not result.allowed

        # Wait for refill (10 tokens/sec means ~0.1s per token)
        await asyncio.sleep(0.3)

        result = await bucket.is_allowed("client-1")
        assert result.allowed

    @pytest.mark.asyncio
    async def test_independent_clients(self, redis_client) -> None:
        """Different clients should have independent buckets."""
        bucket = TokenBucket(redis_client, bucket_size=2, refill_rate=1.0)

        # Exhaust client-1
        for _ in range(2):
            await bucket.is_allowed("client-1")
        result = await bucket.is_allowed("client-1")
        assert not result.allowed

        # client-2 should still have tokens
        result = await bucket.is_allowed("client-2")
        assert result.allowed


# ---------------------------------------------------------------------------
# Sliding Window Counter
# ---------------------------------------------------------------------------


class TestSlidingWindowCounter:
    """Tests for the Sliding Window Counter algorithm."""

    @pytest.mark.asyncio
    async def test_allows_requests_within_window(self, redis_client) -> None:
        """Requests within the limit should be allowed."""
        counter = SlidingWindowCounter(redis_client, max_requests=5, window_size=60)

        for i in range(5):
            result = await counter.is_allowed("client-1")
            assert result.allowed, f"Request {i + 1} should be allowed"
            assert result.remaining == 5 - i - 1
            assert result.limit == 5

    @pytest.mark.asyncio
    async def test_rejects_excess_requests(self, redis_client) -> None:
        """Requests beyond the limit should be rejected."""
        counter = SlidingWindowCounter(redis_client, max_requests=3, window_size=60)

        # Use up the limit
        for _ in range(3):
            result = await counter.is_allowed("client-1")
            assert result.allowed

        # Next request should be rejected
        result = await counter.is_allowed("client-1")
        assert not result.allowed
        assert result.remaining == 0
        assert result.retry_after > 0

    @pytest.mark.asyncio
    async def test_resets_after_window(self, redis_client) -> None:
        """Requests should be allowed again after the window expires."""
        counter = SlidingWindowCounter(redis_client, max_requests=2, window_size=1)

        # Use up the limit
        for _ in range(2):
            await counter.is_allowed("client-1")

        result = await counter.is_allowed("client-1")
        assert not result.allowed

        # Wait for the window to expire
        await asyncio.sleep(1.1)

        result = await counter.is_allowed("client-1")
        assert result.allowed

    @pytest.mark.asyncio
    async def test_independent_clients(self, redis_client) -> None:
        """Different clients should have independent counters."""
        counter = SlidingWindowCounter(redis_client, max_requests=2, window_size=60)

        # Exhaust client-1
        for _ in range(2):
            await counter.is_allowed("client-1")
        result = await counter.is_allowed("client-1")
        assert not result.allowed

        # client-2 should still be allowed
        result = await counter.is_allowed("client-2")
        assert result.allowed
