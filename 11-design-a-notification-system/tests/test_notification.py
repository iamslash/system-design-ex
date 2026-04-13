"""Tests for the notification system.

Uses fakeredis to run unit tests without a real Redis dependency.
"""

from __future__ import annotations

import asyncio
import json

import fakeredis.aioredis
import pytest

from models import Channel, NotificationRequest, Priority, UserPreferences
from notification.dispatcher import (
    dispatch_notification,
    get_user_preferences,
    save_user_preferences,
)
from notification.rate_limiter import check_rate_limit
from notification.template import render_template
from worker.consumer import is_duplicate, process_message, update_notification_status


# ---------------------------------------------------------------------------
# Template Rendering
# ---------------------------------------------------------------------------


class TestTemplateRendering:
    """Tests for template rendering and variable substitution."""

    def test_welcome_template(self) -> None:
        """The welcome template substitutes the name variable."""
        result = render_template("welcome", {"name": "Alice"})
        assert result["title"] == "Welcome, Alice!"
        assert "Alice" in result["body"]

    def test_payment_template(self) -> None:
        """The payment template substitutes the name and amount variables."""
        result = render_template("payment", {"name": "Bob", "amount": "$99.99"})
        assert result["title"] == "Payment Received"
        assert "Bob" in result["body"]
        assert "$99.99" in result["body"]

    def test_shipping_template(self) -> None:
        """The shipping template substitutes all variables."""
        result = render_template(
            "shipping",
            {"name": "Carol", "order_id": "12345", "tracking": "TRK-001"},
        )
        assert "Carol" in result["body"]
        assert "12345" in result["body"]
        assert "TRK-001" in result["body"]

    def test_missing_params_preserved(self) -> None:
        """Missing parameters are preserved as {key} placeholders."""
        result = render_template("welcome", {})
        assert "{name}" in result["title"]

    def test_unknown_template_uses_default(self) -> None:
        """An unknown template name falls back to the default template."""
        result = render_template("nonexistent", {})
        assert result["title"] == "Notification"


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


class TestDispatcher:
    """Tests that the dispatcher routes notifications to the correct queue."""

    @pytest.mark.asyncio
    async def test_dispatch_to_push_queue(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """A push channel notification is placed in queue:push."""
        request = NotificationRequest(
            user_id="user1",
            channel=Channel.PUSH,
            template="welcome",
            params={"name": "Alice"},
        )
        result = await dispatch_notification(redis_client, request)
        assert result["status"] == "pending"
        assert result["notification_id"] is not None

        # Verify that a message is in queue:push
        queue_len = await redis_client.llen("queue:push")
        assert queue_len == 1

    @pytest.mark.asyncio
    async def test_dispatch_to_sms_queue(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """An SMS channel notification is placed in queue:sms."""
        request = NotificationRequest(
            user_id="user1",
            channel=Channel.SMS,
            template="payment",
            params={"name": "Bob", "amount": "$50"},
        )
        result = await dispatch_notification(redis_client, request)
        assert result["status"] == "pending"

        queue_len = await redis_client.llen("queue:sms")
        assert queue_len == 1

    @pytest.mark.asyncio
    async def test_dispatch_to_email_queue(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """An email channel notification is placed in queue:email."""
        request = NotificationRequest(
            user_id="user1",
            channel=Channel.EMAIL,
            template="shipping",
            params={"name": "Carol", "order_id": "123", "tracking": "T-1"},
        )
        result = await dispatch_notification(redis_client, request)
        assert result["status"] == "pending"

        queue_len = await redis_client.llen("queue:email")
        assert queue_len == 1


# ---------------------------------------------------------------------------
# Rate Limiting
# ---------------------------------------------------------------------------


class TestRateLimiting:
    """Tests for per-user, per-channel rate limiting."""

    @pytest.mark.asyncio
    async def test_allows_within_limit(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Requests within the limit are allowed."""
        limits = {"push": 3, "sms": 3, "email": 3}
        for _ in range(3):
            allowed = await check_rate_limit(
                redis_client, "user1", "push", window_size=3600, limits=limits,
            )
            assert allowed is True

    @pytest.mark.asyncio
    async def test_blocks_excess_requests(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Requests exceeding the limit are blocked."""
        limits = {"push": 2, "sms": 2, "email": 2}
        for _ in range(2):
            await check_rate_limit(
                redis_client, "user1", "push", window_size=3600, limits=limits,
            )

        # The 3rd request should be blocked
        allowed = await check_rate_limit(
            redis_client, "user1", "push", window_size=3600, limits=limits,
        )
        assert allowed is False

    @pytest.mark.asyncio
    async def test_independent_channels(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Rate limits are independent per channel."""
        limits = {"push": 1, "sms": 1, "email": 1}
        # Reach the push limit
        await check_rate_limit(
            redis_client, "user1", "push", window_size=3600, limits=limits,
        )
        allowed_push = await check_rate_limit(
            redis_client, "user1", "push", window_size=3600, limits=limits,
        )
        assert allowed_push is False

        # SMS is still allowed
        allowed_sms = await check_rate_limit(
            redis_client, "user1", "sms", window_size=3600, limits=limits,
        )
        assert allowed_sms is True

    @pytest.mark.asyncio
    async def test_independent_users(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Rate limits are independent per user."""
        limits = {"push": 1, "sms": 1, "email": 1}
        await check_rate_limit(
            redis_client, "user1", "push", window_size=3600, limits=limits,
        )
        blocked = await check_rate_limit(
            redis_client, "user1", "push", window_size=3600, limits=limits,
        )
        assert blocked is False

        # user2 is still allowed
        allowed = await check_rate_limit(
            redis_client, "user2", "push", window_size=3600, limits=limits,
        )
        assert allowed is True


# ---------------------------------------------------------------------------
# User Preferences (opt-out)
# ---------------------------------------------------------------------------


class TestUserPreferences:
    """Tests for notification opt-out via user preferences."""

    @pytest.mark.asyncio
    async def test_opt_out_blocks_notification(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Notifications are not sent to opted-out channels."""
        # Configure SMS opt-out
        prefs = UserPreferences(push=True, sms=False, email=True)
        await save_user_preferences(redis_client, "user1", prefs)

        request = NotificationRequest(
            user_id="user1",
            channel=Channel.SMS,
            template="welcome",
            params={"name": "Test"},
        )
        result = await dispatch_notification(redis_client, request)
        assert result["status"] == "skipped"
        assert "opted out" in result["message"]

    @pytest.mark.asyncio
    async def test_opt_in_allows_notification(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Notifications are delivered normally to opted-in channels."""
        prefs = UserPreferences(push=True, sms=False, email=True)
        await save_user_preferences(redis_client, "user1", prefs)

        request = NotificationRequest(
            user_id="user1",
            channel=Channel.PUSH,
            template="welcome",
            params={"name": "Test"},
        )
        result = await dispatch_notification(redis_client, request)
        assert result["status"] == "pending"

    @pytest.mark.asyncio
    async def test_default_preferences_all_enabled(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Users without saved preferences have all channels enabled by default."""
        prefs = await get_user_preferences(redis_client, "new_user")
        assert prefs.push is True
        assert prefs.sms is True
        assert prefs.email is True


# ---------------------------------------------------------------------------
# Retry Mechanism
# ---------------------------------------------------------------------------


class TestRetryMechanism:
    """Tests for the retry mechanism on send failure."""

    @pytest.mark.asyncio
    async def test_failure_triggers_requeue(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """A failed send causes the message to be requeued."""
        notification_id = "test-retry-001"
        # Create a notification record
        await redis_client.hset(
            f"notification:{notification_id}",
            mapping={
                "notification_id": notification_id,
                "status": "pending",
                "retry_count": "0",
            },
        )

        # Process message with 100% failure rate
        message = json.dumps({
            "notification_id": notification_id,
            "user_id": "user1",
            "channel": "push",
            "title": "Test",
            "body": "Test body",
            "priority": "normal",
            "retry_count": 0,
        })

        from unittest.mock import AsyncMock, patch

        with patch("worker.consumer.CHANNEL_HANDLERS", {"push": AsyncMock(return_value=False)}):
            await process_message(redis_client, message)

        # Verify the message was requeued
        queue_len = await redis_client.llen("queue:push")
        assert queue_len == 1

        # Verify that retry_count was incremented
        requeued = await redis_client.rpop("queue:push")
        requeued_data = json.loads(requeued)
        assert requeued_data["retry_count"] == 1

    @pytest.mark.asyncio
    async def test_success_after_retry(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """A successful send after retry updates the status to sent."""
        notification_id = "test-retry-success-001"
        await redis_client.hset(
            f"notification:{notification_id}",
            mapping={
                "notification_id": notification_id,
                "status": "pending",
                "retry_count": "1",
            },
        )

        message = json.dumps({
            "notification_id": notification_id,
            "user_id": "user1",
            "channel": "push",
            "title": "Test",
            "body": "Test body",
            "priority": "normal",
            "retry_count": 1,
        })

        from unittest.mock import AsyncMock, patch

        with patch("worker.consumer.CHANNEL_HANDLERS", {"push": AsyncMock(return_value=True)}):
            await process_message(redis_client, message)

        status = await redis_client.hget(f"notification:{notification_id}", "status")
        assert status == "sent"

    @pytest.mark.asyncio
    async def test_max_retries_marks_failed(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Exceeding the maximum retries updates the status to failed."""
        notification_id = "test-max-retry-001"
        await redis_client.hset(
            f"notification:{notification_id}",
            mapping={
                "notification_id": notification_id,
                "status": "pending",
                "retry_count": "3",
            },
        )

        # Message already at MAX_RETRIES (3)
        message = json.dumps({
            "notification_id": notification_id,
            "user_id": "user1",
            "channel": "push",
            "title": "Test",
            "body": "Test body",
            "priority": "normal",
            "retry_count": 3,
        })

        from unittest.mock import AsyncMock, patch

        with patch("worker.consumer.CHANNEL_HANDLERS", {"push": AsyncMock(return_value=False)}):
            await process_message(redis_client, message)

        status = await redis_client.hget(f"notification:{notification_id}", "status")
        assert status == "failed"

        # Verify the message was NOT requeued
        queue_len = await redis_client.llen("queue:push")
        assert queue_len == 0


# ---------------------------------------------------------------------------
# Dedup (duplicate processing prevention)
# ---------------------------------------------------------------------------


class TestDedup:
    """Tests for duplicate notification processing prevention."""

    @pytest.mark.asyncio
    async def test_duplicate_not_processed(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """A notification already in sent status is not processed again."""
        notification_id = "test-dedup-001"
        # Store as already sent
        await redis_client.hset(
            f"notification:{notification_id}",
            mapping={
                "notification_id": notification_id,
                "status": "sent",
            },
        )

        assert await is_duplicate(redis_client, notification_id) is True

    @pytest.mark.asyncio
    async def test_pending_is_not_duplicate(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """A notification in pending status is not considered a duplicate."""
        notification_id = "test-dedup-002"
        await redis_client.hset(
            f"notification:{notification_id}",
            mapping={
                "notification_id": notification_id,
                "status": "pending",
            },
        )

        assert await is_duplicate(redis_client, notification_id) is False

    @pytest.mark.asyncio
    async def test_duplicate_skips_processing(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Duplicate messages do not invoke the channel handler."""
        notification_id = "test-dedup-003"
        await redis_client.hset(
            f"notification:{notification_id}",
            mapping={
                "notification_id": notification_id,
                "status": "sent",
            },
        )

        message = json.dumps({
            "notification_id": notification_id,
            "user_id": "user1",
            "channel": "push",
            "title": "Test",
            "body": "Test body",
            "priority": "normal",
            "retry_count": 0,
        })

        from unittest.mock import AsyncMock, patch

        mock_handler = AsyncMock(return_value=True)
        with patch("worker.consumer.CHANNEL_HANDLERS", {"push": mock_handler}):
            await process_message(redis_client, message)

        # The handler should not have been called
        mock_handler.assert_not_called()


# ---------------------------------------------------------------------------
# Notification Status Tracking
# ---------------------------------------------------------------------------


class TestStatusTracking:
    """Tests for notification status tracking (pending -> sent)."""

    @pytest.mark.asyncio
    async def test_pending_to_sent(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """A successfully delivered notification transitions from pending to sent."""
        notification_id = "test-status-001"
        await redis_client.hset(
            f"notification:{notification_id}",
            mapping={
                "notification_id": notification_id,
                "status": "pending",
                "retry_count": "0",
            },
        )

        message = json.dumps({
            "notification_id": notification_id,
            "user_id": "user1",
            "channel": "email",
            "title": "Test",
            "body": "Test body",
            "priority": "normal",
            "retry_count": 0,
        })

        from unittest.mock import AsyncMock, patch

        with patch("worker.consumer.CHANNEL_HANDLERS", {"email": AsyncMock(return_value=True)}):
            await process_message(redis_client, message)

        status = await redis_client.hget(f"notification:{notification_id}", "status")
        assert status == "sent"

        # Verify that the sent_at timestamp was recorded
        sent_at = await redis_client.hget(f"notification:{notification_id}", "sent_at")
        assert sent_at is not None

    @pytest.mark.asyncio
    async def test_update_notification_status(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """The status update utility works correctly."""
        notification_id = "test-update-001"
        await redis_client.hset(
            f"notification:{notification_id}",
            mapping={"notification_id": notification_id, "status": "pending"},
        )

        await update_notification_status(redis_client, notification_id, "delivered", retry_count=2)

        status = await redis_client.hget(f"notification:{notification_id}", "status")
        assert status == "delivered"

        retry = await redis_client.hget(f"notification:{notification_id}", "retry_count")
        assert retry == "2"
