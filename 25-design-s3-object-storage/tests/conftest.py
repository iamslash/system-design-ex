"""Shared fixtures for S3 object storage tests."""

from __future__ import annotations

import os
import sys
import tempfile

import pytest
import pytest_asyncio
import fakeredis.aioredis

# Add the api directory to the path so we can import modules.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))


@pytest_asyncio.fixture
async def redis_client():
    """Create a fake async Redis client for testing."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.flushall()
    await client.aclose()


@pytest.fixture
def tmp_data_dir():
    """Create a temporary directory for data store tests."""
    with tempfile.TemporaryDirectory() as d:
        yield d
