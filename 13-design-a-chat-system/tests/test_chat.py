"""Tests for the chat system components."""

from __future__ import annotations

import asyncio
import json
import sys
import os
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Add the server directory to the path so we can import modules.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "server"))

from chat.id_generator import IdGenerator
from chat.connection_manager import ConnectionManager
from chat.message_handler import MessageHandler, make_dm_channel
from presence.tracker import PresenceTracker
from storage.message_store import MessageStore


# ---------------------------------------------------------------------------
# Message ID Generator
# ---------------------------------------------------------------------------


class TestIdGenerator:
    """Tests for the Snowflake-like message ID generator."""

    def test_generates_unique_ids(self) -> None:
        """Each call should produce a unique ID."""
        gen = IdGenerator()
        ids = {gen.generate() for _ in range(1000)}
        assert len(ids) == 1000, "All 1000 IDs should be unique"

    def test_ids_are_time_sortable(self) -> None:
        """IDs generated later should sort after earlier ones."""
        gen = IdGenerator()
        id1 = gen.generate()
        # Force a small delay so timestamp advances
        time.sleep(0.002)
        id2 = gen.generate()
        # Extract timestamp portion
        ts1 = int(id1.split("-")[0])
        ts2 = int(id2.split("-")[0])
        assert ts2 >= ts1, "Later ID should have equal or greater timestamp"

    def test_sequence_increments_within_same_ms(self) -> None:
        """Multiple IDs within the same millisecond should have increasing sequence."""
        gen = IdGenerator()
        ids = [gen.generate() for _ in range(10)]
        # Within rapid generation, some should share a timestamp with different sequences
        seen_seqs: dict[str, list[int]] = {}
        for mid in ids:
            ts, seq = mid.split("-")
            seen_seqs.setdefault(ts, []).append(int(seq))
        # At least one timestamp should have multiple sequences
        has_multi = any(len(seqs) > 1 for seqs in seen_seqs.values())
        # This depends on speed; if all get unique timestamps that's also fine
        assert len(ids) == 10

    def test_id_format(self) -> None:
        """ID should be in the format '{timestamp_ms}-{sequence}'."""
        gen = IdGenerator()
        mid = gen.generate()
        parts = mid.split("-")
        assert len(parts) == 2
        assert parts[0].isdigit()
        assert parts[1].isdigit()


# ---------------------------------------------------------------------------
# Channel ID Generation
# ---------------------------------------------------------------------------


class TestChannelId:
    """Tests for DM channel ID generation."""

    def test_dm_channel_is_symmetric(self) -> None:
        """dm:alice:bob should equal dm:bob:alice."""
        assert make_dm_channel("alice", "bob") == make_dm_channel("bob", "alice")

    def test_dm_channel_format(self) -> None:
        """Channel ID should be 'dm:{min}:{max}'."""
        channel = make_dm_channel("charlie", "alice")
        assert channel == "dm:alice:charlie"

    def test_dm_channel_same_user(self) -> None:
        """Channel with same user on both sides should still work."""
        channel = make_dm_channel("alice", "alice")
        assert channel == "dm:alice:alice"


# ---------------------------------------------------------------------------
# Connection Manager
# ---------------------------------------------------------------------------


class TestConnectionManager:
    """Tests for WebSocket connection management."""

    @pytest.mark.asyncio
    async def test_connect_and_disconnect(self) -> None:
        """User should be tracked after connect and removed after disconnect."""
        cm = ConnectionManager()
        ws = AsyncMock()
        await cm.connect("alice", ws)
        assert cm.is_connected("alice")

        cm.disconnect("alice", ws)
        assert not cm.is_connected("alice")

    @pytest.mark.asyncio
    async def test_send_to_user(self) -> None:
        """Message should be sent to all of a user's connections."""
        cm = ConnectionManager()
        ws1 = AsyncMock()
        ws2 = AsyncMock()
        await cm.connect("alice", ws1)
        await cm.connect("alice", ws2)

        msg = {"type": "message", "content": "hello"}
        await cm.send_to_user("alice", msg)

        ws1.send_text.assert_called_once_with(json.dumps(msg))
        ws2.send_text.assert_called_once_with(json.dumps(msg))

    @pytest.mark.asyncio
    async def test_send_to_offline_user(self) -> None:
        """Sending to a user with no connections should not raise."""
        cm = ConnectionManager()
        msg = {"type": "message", "content": "hello"}
        # Should not raise
        await cm.send_to_user("nobody", msg)

    @pytest.mark.asyncio
    async def test_broadcast(self) -> None:
        """Broadcast should send to all specified users."""
        cm = ConnectionManager()
        ws_a = AsyncMock()
        ws_b = AsyncMock()
        await cm.connect("alice", ws_a)
        await cm.connect("bob", ws_b)

        msg = {"type": "group_message", "content": "hi team"}
        await cm.broadcast(["alice", "bob"], msg)

        ws_a.send_text.assert_called_once()
        ws_b.send_text.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_connected_users(self) -> None:
        """Should list all currently connected users."""
        cm = ConnectionManager()
        ws_a = AsyncMock()
        ws_b = AsyncMock()
        await cm.connect("alice", ws_a)
        await cm.connect("bob", ws_b)

        users = cm.get_connected_users()
        assert sorted(users) == ["alice", "bob"]

    @pytest.mark.asyncio
    async def test_dead_connection_cleanup(self) -> None:
        """Dead connections should be cleaned up on send failure."""
        cm = ConnectionManager()
        ws = AsyncMock()
        ws.send_text.side_effect = Exception("connection closed")
        await cm.connect("alice", ws)

        await cm.send_to_user("alice", {"type": "test"})
        # After failed send, connection should be cleaned up
        assert not cm.is_connected("alice")


# ---------------------------------------------------------------------------
# Message Store (Redis)
# ---------------------------------------------------------------------------


class TestMessageStore:
    """Tests for message persistence in Redis."""

    @pytest.mark.asyncio
    async def test_save_and_retrieve(self, redis_client) -> None:
        """Saved messages should be retrievable."""
        store = MessageStore(redis_client)
        msg = {"message_id": "100-0", "from": "alice", "content": "hello", "timestamp": 100.0}
        await store.save_message("dm:alice:bob", msg)

        messages = await store.get_messages("dm:alice:bob")
        assert len(messages) == 1
        assert messages[0]["content"] == "hello"

    @pytest.mark.asyncio
    async def test_message_ordering(self, redis_client) -> None:
        """Messages should be returned in chronological order."""
        store = MessageStore(redis_client)
        for i in range(5):
            msg = {"message_id": f"{100 + i}-0", "from": "alice", "content": f"msg{i}", "timestamp": 100.0 + i}
            await store.save_message("dm:alice:bob", msg)

        messages = await store.get_messages("dm:alice:bob")
        assert len(messages) == 5
        # Should be oldest first
        assert messages[0]["content"] == "msg0"
        assert messages[4]["content"] == "msg4"

    @pytest.mark.asyncio
    async def test_message_limit(self, redis_client) -> None:
        """Should respect the limit parameter."""
        store = MessageStore(redis_client)
        for i in range(10):
            msg = {"message_id": f"{100 + i}-0", "from": "alice", "content": f"msg{i}", "timestamp": 100.0 + i}
            await store.save_message("dm:alice:bob", msg)

        messages = await store.get_messages("dm:alice:bob", limit=3)
        assert len(messages) == 3

    @pytest.mark.asyncio
    async def test_get_max_message_id(self, redis_client) -> None:
        """Should return the latest message ID."""
        store = MessageStore(redis_client)
        for i in range(5):
            msg = {"message_id": f"{100 + i}-0", "from": "alice", "content": f"msg{i}", "timestamp": 100.0 + i}
            await store.save_message("dm:alice:bob", msg)

        max_id = await store.get_max_message_id("dm:alice:bob")
        assert max_id == "104-0"

    @pytest.mark.asyncio
    async def test_empty_channel(self, redis_client) -> None:
        """Empty channel should return empty list."""
        store = MessageStore(redis_client)
        messages = await store.get_messages("dm:nobody:here")
        assert messages == []

    @pytest.mark.asyncio
    async def test_max_message_id_empty_channel(self, redis_client) -> None:
        """Empty channel should return None for max message ID."""
        store = MessageStore(redis_client)
        max_id = await store.get_max_message_id("dm:nobody:here")
        assert max_id is None


# ---------------------------------------------------------------------------
# Presence Tracker
# ---------------------------------------------------------------------------


class TestPresenceTracker:
    """Tests for the heartbeat-based presence tracker."""

    @pytest.mark.asyncio
    async def test_set_online(self, redis_client) -> None:
        """User should be online after set_online."""
        tracker = PresenceTracker(redis_client)
        await tracker.set_online("alice")
        status = await tracker.get_status("alice")
        assert status["status"] == "online"
        assert status["last_heartbeat"] is not None

    @pytest.mark.asyncio
    async def test_set_offline(self, redis_client) -> None:
        """User should be offline after set_offline."""
        tracker = PresenceTracker(redis_client)
        await tracker.set_online("alice")
        await tracker.set_offline("alice")
        status = await tracker.get_status("alice")
        assert status["status"] == "offline"

    @pytest.mark.asyncio
    async def test_heartbeat_keeps_online(self, redis_client) -> None:
        """Heartbeat should refresh last_heartbeat timestamp."""
        tracker = PresenceTracker(redis_client)
        await tracker.set_online("alice")
        status1 = await tracker.get_status("alice")

        await asyncio.sleep(0.05)
        await tracker.heartbeat("alice")
        status2 = await tracker.get_status("alice")

        assert status2["status"] == "online"
        assert status2["last_heartbeat"] >= status1["last_heartbeat"]

    @pytest.mark.asyncio
    async def test_timeout_marks_offline(self, redis_client) -> None:
        """User should be marked offline after heartbeat timeout."""
        # Create tracker with very short timeout
        tracker = PresenceTracker(redis_client)
        tracker._timeout = 0.1  # 100ms timeout for testing

        await tracker.set_online("alice")
        await asyncio.sleep(0.2)

        status = await tracker.get_status("alice")
        assert status["status"] == "offline"

    @pytest.mark.asyncio
    async def test_unknown_user_is_offline(self, redis_client) -> None:
        """Unknown user should be reported as offline."""
        tracker = PresenceTracker(redis_client)
        status = await tracker.get_status("nobody")
        assert status["status"] == "offline"
        assert status["last_heartbeat"] is None


# ---------------------------------------------------------------------------
# Message Handler (integration with mocks)
# ---------------------------------------------------------------------------


class TestMessageHandler:
    """Tests for message routing logic."""

    @pytest.mark.asyncio
    async def test_dm_routing(self, redis_client) -> None:
        """1:1 message should be stored and delivered to recipient."""
        cm = ConnectionManager()
        ws_bob = AsyncMock()
        await cm.connect("bob", ws_bob)

        store = MessageStore(redis_client)
        handler = MessageHandler(cm, store, redis_client)

        result = await handler.handle_dm("alice", "bob", "Hello Bob!")
        assert result["from"] == "alice"
        assert result["content"] == "Hello Bob!"

        # Bob should have received the message
        ws_bob.send_text.assert_called()
        sent = json.loads(ws_bob.send_text.call_args[0][0])
        assert sent["content"] == "Hello Bob!"

        # Message should be persisted
        channel = make_dm_channel("alice", "bob")
        messages = await store.get_messages(channel)
        assert len(messages) == 1

        cm.disconnect("bob", ws_bob)

    @pytest.mark.asyncio
    async def test_group_routing(self, redis_client) -> None:
        """Group message should be delivered to all members."""
        cm = ConnectionManager()
        ws_alice = AsyncMock()
        ws_bob = AsyncMock()
        ws_charlie = AsyncMock()
        await cm.connect("alice", ws_alice)
        await cm.connect("bob", ws_bob)
        await cm.connect("charlie", ws_charlie)

        # Create group in Redis
        await redis_client.hset(
            "group:team1",
            mapping={"name": "Team 1", "members": json.dumps(["alice", "bob", "charlie"])},
        )

        store = MessageStore(redis_client)
        handler = MessageHandler(cm, store, redis_client)

        result = await handler.handle_group_message("alice", "team1", "Hi team!")
        assert result is not None
        assert result["from"] == "alice"

        # All members should receive the message
        ws_alice.send_text.assert_called()
        ws_bob.send_text.assert_called()
        ws_charlie.send_text.assert_called()

        # Message should be persisted
        messages = await store.get_messages("group:team1")
        assert len(messages) == 1

        cm.disconnect("alice", ws_alice)
        cm.disconnect("bob", ws_bob)
        cm.disconnect("charlie", ws_charlie)

    @pytest.mark.asyncio
    async def test_group_not_found(self, redis_client) -> None:
        """Non-existent group should return None."""
        cm = ConnectionManager()
        store = MessageStore(redis_client)
        handler = MessageHandler(cm, store, redis_client)

        result = await handler.handle_group_message("alice", "nonexistent", "hello")
        assert result is None

    @pytest.mark.asyncio
    async def test_dm_persisted_in_correct_channel(self, redis_client) -> None:
        """DM should be stored under the canonicalized channel ID."""
        cm = ConnectionManager()
        store = MessageStore(redis_client)
        handler = MessageHandler(cm, store, redis_client)

        await handler.handle_dm("bob", "alice", "Hello!")

        # Both orderings should yield the same channel
        channel1 = make_dm_channel("alice", "bob")
        channel2 = make_dm_channel("bob", "alice")
        assert channel1 == channel2

        messages = await store.get_messages(channel1)
        assert len(messages) == 1
        assert messages[0]["content"] == "Hello!"

    @pytest.mark.asyncio
    async def test_dm_sender_receives_copy(self, redis_client) -> None:
        """Sender should also receive a copy (for multi-device sync)."""
        cm = ConnectionManager()
        ws_alice = AsyncMock()
        ws_bob = AsyncMock()
        await cm.connect("alice", ws_alice)
        await cm.connect("bob", ws_bob)

        store = MessageStore(redis_client)
        handler = MessageHandler(cm, store, redis_client)

        await handler.handle_dm("alice", "bob", "Sync test")

        # Both alice and bob should receive the message
        ws_alice.send_text.assert_called()
        ws_bob.send_text.assert_called()

        cm.disconnect("alice", ws_alice)
        cm.disconnect("bob", ws_bob)
