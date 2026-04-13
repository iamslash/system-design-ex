"""Tests for the nearby friends system components."""

from __future__ import annotations

import asyncio
import json
import sys
import os
import time
from unittest.mock import AsyncMock, patch

import fakeredis.aioredis
import pytest

# Add the server directory to the path so we can import modules.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "server"))

from location.tracker import LocationTracker
from location.history import LocationHistory
from friends.nearby import NearbyFinder, haversine_distance, EARTH_RADIUS_MILES
from pubsub.channel import LocationPubSub, _channel_name


# ---------------------------------------------------------------------------
# Haversine Distance Calculation
# ---------------------------------------------------------------------------


class TestHaversineDistance:
    """Tests for the Haversine great-circle distance formula."""

    def test_same_point_is_zero(self) -> None:
        """Distance between the same point should be zero."""
        d = haversine_distance(40.7128, -74.0060, 40.7128, -74.0060)
        assert d == 0.0

    def test_known_distance_nyc_to_la(self) -> None:
        """NYC to LA should be approximately 2,451 miles."""
        d = haversine_distance(40.7128, -74.0060, 34.0522, -118.2437)
        assert 2440 < d < 2460

    def test_short_distance(self) -> None:
        """Two points ~1 mile apart should return approximately 1 mile."""
        # 1 degree latitude ~ 69 miles, so 1/69 degree ~ 1 mile
        lat1 = 40.0
        lon1 = -74.0
        lat2 = lat1 + (1 / 69)
        lon2 = lon1
        d = haversine_distance(lat1, lon1, lat2, lon2)
        assert 0.9 < d < 1.1

    def test_symmetry(self) -> None:
        """Distance from A to B should equal distance from B to A."""
        d1 = haversine_distance(40.7128, -74.0060, 34.0522, -118.2437)
        d2 = haversine_distance(34.0522, -118.2437, 40.7128, -74.0060)
        assert d1 == d2

    def test_antipodal_points(self) -> None:
        """Opposite points on Earth should be ~half circumference apart."""
        d = haversine_distance(0.0, 0.0, 0.0, 180.0)
        half_circumference = EARTH_RADIUS_MILES * 3.14159265
        assert abs(d - half_circumference) < 1


# ---------------------------------------------------------------------------
# Location Tracker (Redis cache with TTL)
# ---------------------------------------------------------------------------


class TestLocationTracker:
    """Tests for location update and caching with TTL."""

    @pytest.mark.asyncio
    async def test_update_and_get(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Location should be retrievable after update."""
        tracker = LocationTracker(redis_client, ttl=60)
        await tracker.update("alice", 40.7128, -74.0060)
        loc = await tracker.get("alice")
        assert loc is not None
        assert loc["user_id"] == "alice"
        assert loc["latitude"] == 40.7128
        assert loc["longitude"] == -74.0060
        assert loc["timestamp"] > 0

    @pytest.mark.asyncio
    async def test_get_nonexistent_user(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Getting location of unknown user should return None."""
        tracker = LocationTracker(redis_client, ttl=60)
        loc = await tracker.get("nobody")
        assert loc is None

    @pytest.mark.asyncio
    async def test_update_overwrites_previous(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """A new update should overwrite the previous location."""
        tracker = LocationTracker(redis_client, ttl=60)
        await tracker.update("alice", 40.0, -74.0)
        await tracker.update("alice", 41.0, -75.0)
        loc = await tracker.get("alice")
        assert loc is not None
        assert loc["latitude"] == 41.0
        assert loc["longitude"] == -75.0

    @pytest.mark.asyncio
    async def test_ttl_is_set(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """TTL should be set on the location key after update."""
        tracker = LocationTracker(redis_client, ttl=120)
        await tracker.update("alice", 40.0, -74.0)
        ttl = await tracker.get_ttl("alice")
        assert 0 < ttl <= 120

    @pytest.mark.asyncio
    async def test_ttl_expiry_removes_location(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """After TTL expires, location should be gone (simulated with 1s TTL)."""
        tracker = LocationTracker(redis_client, ttl=1)
        await tracker.update("alice", 40.0, -74.0)
        loc = await tracker.get("alice")
        assert loc is not None

        await asyncio.sleep(1.1)
        loc = await tracker.get("alice")
        assert loc is None

    @pytest.mark.asyncio
    async def test_remove_explicit(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Explicit remove should delete the location immediately."""
        tracker = LocationTracker(redis_client, ttl=60)
        await tracker.update("alice", 40.0, -74.0)
        await tracker.remove("alice")
        loc = await tracker.get("alice")
        assert loc is None


# ---------------------------------------------------------------------------
# Location History
# ---------------------------------------------------------------------------


class TestLocationHistory:
    """Tests for append-only location history storage."""

    @pytest.mark.asyncio
    async def test_append_and_retrieve(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Appended entry should be retrievable."""
        hist = LocationHistory(redis_client)
        await hist.append("alice", 40.7128, -74.0060, 1000.0)
        entries = await hist.get_range("alice")
        assert len(entries) == 1
        assert entries[0]["latitude"] == 40.7128

    @pytest.mark.asyncio
    async def test_chronological_order(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Entries should be returned in chronological order."""
        hist = LocationHistory(redis_client)
        for i in range(5):
            await hist.append("alice", 40.0 + i * 0.01, -74.0, 1000.0 + i)
        entries = await hist.get_range("alice")
        assert len(entries) == 5
        assert entries[0]["timestamp"] == 1000.0
        assert entries[4]["timestamp"] == 1004.0

    @pytest.mark.asyncio
    async def test_count(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Count should reflect total entries."""
        hist = LocationHistory(redis_client)
        for i in range(3):
            await hist.append("alice", 40.0, -74.0, 1000.0 + i)
        assert await hist.count("alice") == 3

    @pytest.mark.asyncio
    async def test_time_range_filter(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """get_range should filter entries by time range."""
        hist = LocationHistory(redis_client)
        for i in range(10):
            await hist.append("alice", 40.0, -74.0, 1000.0 + i)
        entries = await hist.get_range("alice", start=1003, end=1007)
        assert len(entries) == 5  # 1003, 1004, 1005, 1006, 1007


# ---------------------------------------------------------------------------
# Nearby Finder (friendship + distance filtering)
# ---------------------------------------------------------------------------


class TestNearbyFinder:
    """Tests for nearby friend discovery."""

    @pytest.mark.asyncio
    async def test_add_and_get_friends(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Friendship should be bidirectional."""
        tracker = LocationTracker(redis_client, ttl=60)
        finder = NearbyFinder(redis_client, tracker)
        await finder.add_friendship("alice", "bob")
        assert "bob" in await finder.get_friends("alice")
        assert "alice" in await finder.get_friends("bob")

    @pytest.mark.asyncio
    async def test_remove_friendship(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Removing friendship should be bidirectional."""
        tracker = LocationTracker(redis_client, ttl=60)
        finder = NearbyFinder(redis_client, tracker)
        await finder.add_friendship("alice", "bob")
        await finder.remove_friendship("alice", "bob")
        assert "bob" not in await finder.get_friends("alice")
        assert "alice" not in await finder.get_friends("bob")

    @pytest.mark.asyncio
    async def test_find_nearby_within_radius(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Friends within radius should be returned."""
        tracker = LocationTracker(redis_client, ttl=60)
        finder = NearbyFinder(redis_client, tracker)

        # Alice and Bob are very close (~0 miles)
        await tracker.update("alice", 40.7128, -74.0060)
        await tracker.update("bob", 40.7130, -74.0062)
        await finder.add_friendship("alice", "bob")

        nearby = await finder.find_nearby("alice", radius_miles=5)
        assert len(nearby) == 1
        assert nearby[0]["user_id"] == "bob"
        assert nearby[0]["distance_miles"] < 1

    @pytest.mark.asyncio
    async def test_find_nearby_excludes_distant(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Friends outside the radius should not be returned."""
        tracker = LocationTracker(redis_client, ttl=60)
        finder = NearbyFinder(redis_client, tracker)

        # Alice in NYC, Charlie in LA (~2450 miles)
        await tracker.update("alice", 40.7128, -74.0060)
        await tracker.update("charlie", 34.0522, -118.2437)
        await finder.add_friendship("alice", "charlie")

        nearby = await finder.find_nearby("alice", radius_miles=5)
        assert len(nearby) == 0

    @pytest.mark.asyncio
    async def test_find_nearby_excludes_expired_location(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Friends whose location TTL expired should not appear."""
        tracker = LocationTracker(redis_client, ttl=1)
        finder = NearbyFinder(redis_client, tracker)

        await tracker.update("alice", 40.7128, -74.0060)
        await tracker.update("bob", 40.7130, -74.0062)
        await finder.add_friendship("alice", "bob")

        await asyncio.sleep(1.1)
        # Bob's location has expired; only refresh Alice
        await tracker.update("alice", 40.7128, -74.0060)

        nearby = await finder.find_nearby("alice", radius_miles=5)
        assert len(nearby) == 0

    @pytest.mark.asyncio
    async def test_find_nearby_no_friends(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """User with no friends should get empty list."""
        tracker = LocationTracker(redis_client, ttl=60)
        finder = NearbyFinder(redis_client, tracker)
        await tracker.update("alice", 40.7128, -74.0060)

        nearby = await finder.find_nearby("alice")
        assert nearby == []

    @pytest.mark.asyncio
    async def test_find_nearby_no_location(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """User without a location should get empty list."""
        tracker = LocationTracker(redis_client, ttl=60)
        finder = NearbyFinder(redis_client, tracker)
        await finder.add_friendship("alice", "bob")

        nearby = await finder.find_nearby("alice")
        assert nearby == []

    @pytest.mark.asyncio
    async def test_find_nearby_sorted_by_distance(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Results should be sorted by distance (nearest first)."""
        tracker = LocationTracker(redis_client, ttl=60)
        finder = NearbyFinder(redis_client, tracker)

        await tracker.update("alice", 40.7128, -74.0060)
        # Bob ~0.02 miles away
        await tracker.update("bob", 40.7130, -74.0062)
        # Charlie ~1 mile away
        await tracker.update("charlie", 40.7128 + (1 / 69), -74.0060)
        await finder.add_friendship("alice", "bob")
        await finder.add_friendship("alice", "charlie")

        nearby = await finder.find_nearby("alice", radius_miles=5)
        assert len(nearby) == 2
        assert nearby[0]["user_id"] == "bob"
        assert nearby[1]["user_id"] == "charlie"
        assert nearby[0]["distance_miles"] < nearby[1]["distance_miles"]


# ---------------------------------------------------------------------------
# Pub/Sub Channel
# ---------------------------------------------------------------------------


class TestPubSubChannel:
    """Tests for Redis Pub/Sub location broadcasting."""

    def test_channel_name(self) -> None:
        """Channel name should follow location:{user_id} pattern."""
        assert _channel_name("alice") == "location:alice"

    @pytest.mark.asyncio
    async def test_publish_returns_subscriber_count(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Publish should return the number of subscribers (0 if none)."""
        ps = LocationPubSub(redis_client)
        count = await ps.publish("alice", 40.0, -74.0)
        # No subscribers, so count should be 0
        assert count == 0

    @pytest.mark.asyncio
    async def test_subscribe_and_receive(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Subscriber should receive published location updates within radius."""
        ps = LocationPubSub(redis_client)
        received: list[dict] = []

        async def on_update(data: dict) -> None:
            received.append(data)

        # Bob's location getter returns position very close to alice's publish
        async def bob_location() -> dict | None:
            return {"latitude": 40.7130, "longitude": -74.0062}

        await ps.subscribe("bob", ["alice"], on_update, bob_location)

        # Give subscriber time to start listening
        await asyncio.sleep(0.1)

        # Alice publishes her location (very close to Bob)
        await ps.publish("alice", 40.7128, -74.0060)

        # Wait for delivery
        await asyncio.sleep(0.3)

        assert len(received) == 1
        assert received[0]["user_id"] == "alice"
        assert "distance_miles" in received[0]

        await ps.unsubscribe("bob")

    @pytest.mark.asyncio
    async def test_subscribe_filters_distant(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Updates from friends outside the radius should be filtered out."""
        ps = LocationPubSub(redis_client)
        received: list[dict] = []

        async def on_update(data: dict) -> None:
            received.append(data)

        # Bob is in NYC
        async def bob_location() -> dict | None:
            return {"latitude": 40.7128, "longitude": -74.0060}

        await ps.subscribe("bob", ["charlie"], on_update, bob_location)
        await asyncio.sleep(0.1)

        # Charlie publishes from LA (2450+ miles away)
        await ps.publish("charlie", 34.0522, -118.2437)
        await asyncio.sleep(0.3)

        # Should NOT have received anything (outside 5 mile radius)
        assert len(received) == 0

        await ps.unsubscribe("bob")

    @pytest.mark.asyncio
    async def test_unsubscribe_cleanup(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """After unsubscribe, no more updates should be received."""
        ps = LocationPubSub(redis_client)
        received: list[dict] = []

        async def on_update(data: dict) -> None:
            received.append(data)

        async def bob_location() -> dict | None:
            return {"latitude": 40.7128, "longitude": -74.0060}

        await ps.subscribe("bob", ["alice"], on_update, bob_location)
        await asyncio.sleep(0.1)
        await ps.unsubscribe("bob")
        await asyncio.sleep(0.1)

        await ps.publish("alice", 40.7130, -74.0062)
        await asyncio.sleep(0.3)

        assert len(received) == 0

    @pytest.mark.asyncio
    async def test_close_all(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """close_all should clean up all subscriptions."""
        ps = LocationPubSub(redis_client)

        async def noop(data: dict) -> None:
            pass

        async def loc() -> dict | None:
            return {"latitude": 0, "longitude": 0}

        await ps.subscribe("bob", ["alice"], noop, loc)
        await ps.subscribe("charlie", ["alice"], noop, loc)
        await asyncio.sleep(0.1)

        await ps.close_all()
        assert len(ps._subscriptions) == 0
        assert len(ps._listener_tasks) == 0
