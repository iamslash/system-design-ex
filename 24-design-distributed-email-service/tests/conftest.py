"""Shared fixtures for distributed email service tests."""

from __future__ import annotations

import sys
from pathlib import Path

import fakeredis
import pytest

# Ensure api/ is on sys.path so imports resolve correctly.
API_DIR = str(Path(__file__).resolve().parent.parent / "api")
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)


@pytest.fixture
def r() -> fakeredis.FakeRedis:
    """Provide a fresh fakeredis instance per test."""
    return fakeredis.FakeRedis(decode_responses=True)
