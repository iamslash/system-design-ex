"""Shared fixtures for rate limiter tests."""

from __future__ import annotations

import pytest_asyncio
import fakeredis.aioredis


@pytest_asyncio.fixture
async def redis_client():
    """Create a fake async Redis client for testing."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.flushall()
    await client.aclose()
