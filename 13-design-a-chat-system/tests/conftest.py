"""Shared fixtures for chat system tests."""

from __future__ import annotations

import sys
import os

import pytest_asyncio
import fakeredis.aioredis

# Add the server directory to the path so we can import modules.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "server"))


@pytest_asyncio.fixture
async def redis_client():
    """Create a fake async Redis client for testing."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.flushall()
    await client.aclose()
