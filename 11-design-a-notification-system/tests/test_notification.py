"""Tests for the notification system.

fakeredis 를 사용하여 Redis 의존성 없이 단위 테스트를 수행한다.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

import fakeredis.aioredis
import pytest
import pytest_asyncio

# api 디렉토리를 import 경로에 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

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
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def redis_client():
    """Create a fake async Redis client for testing."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.flushall()
    await client.aclose()


# ---------------------------------------------------------------------------
# Template Rendering
# ---------------------------------------------------------------------------


class TestTemplateRendering:
    """템플릿 렌더링 및 변수 치환 테스트."""

    def test_welcome_template(self) -> None:
        """welcome 템플릿이 name 변수를 치환한다."""
        result = render_template("welcome", {"name": "Alice"})
        assert result["title"] == "Welcome, Alice!"
        assert "Alice" in result["body"]

    def test_payment_template(self) -> None:
        """payment 템플릿이 name, amount 변수를 치환한다."""
        result = render_template("payment", {"name": "Bob", "amount": "$99.99"})
        assert result["title"] == "Payment Received"
        assert "Bob" in result["body"]
        assert "$99.99" in result["body"]

    def test_shipping_template(self) -> None:
        """shipping 템플릿이 모든 변수를 치환한다."""
        result = render_template(
            "shipping",
            {"name": "Carol", "order_id": "12345", "tracking": "TRK-001"},
        )
        assert "Carol" in result["body"]
        assert "12345" in result["body"]
        assert "TRK-001" in result["body"]

    def test_missing_params_preserved(self) -> None:
        """누락된 파라미터는 {key} 형태로 남는다."""
        result = render_template("welcome", {})
        assert "{name}" in result["title"]

    def test_unknown_template_uses_default(self) -> None:
        """존재하지 않는 템플릿 이름은 default 를 사용한다."""
        result = render_template("nonexistent", {})
        assert result["title"] == "Notification"


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


class TestDispatcher:
    """디스패처가 올바른 큐로 라우팅하는지 테스트."""

    @pytest.mark.asyncio
    async def test_dispatch_to_push_queue(self, redis_client) -> None:
        """push 채널 알림이 queue:push 에 들어간다."""
        request = NotificationRequest(
            user_id="user1",
            channel=Channel.PUSH,
            template="welcome",
            params={"name": "Alice"},
        )
        result = await dispatch_notification(redis_client, request)
        assert result["status"] == "pending"
        assert result["notification_id"] is not None

        # queue:push 에 메시지가 있는지 확인
        queue_len = await redis_client.llen("queue:push")
        assert queue_len == 1

    @pytest.mark.asyncio
    async def test_dispatch_to_sms_queue(self, redis_client) -> None:
        """sms 채널 알림이 queue:sms 에 들어간다."""
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
    async def test_dispatch_to_email_queue(self, redis_client) -> None:
        """email 채널 알림이 queue:email 에 들어간다."""
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
    """Per-user, per-channel rate limiting 테스트."""

    @pytest.mark.asyncio
    async def test_allows_within_limit(self, redis_client) -> None:
        """제한 이내 요청은 허용된다."""
        limits = {"push": 3, "sms": 3, "email": 3}
        for _ in range(3):
            allowed = await check_rate_limit(
                redis_client, "user1", "push", window_size=3600, limits=limits,
            )
            assert allowed is True

    @pytest.mark.asyncio
    async def test_blocks_excess_requests(self, redis_client) -> None:
        """제한 초과 요청은 차단된다."""
        limits = {"push": 2, "sms": 2, "email": 2}
        for _ in range(2):
            await check_rate_limit(
                redis_client, "user1", "push", window_size=3600, limits=limits,
            )

        # 3번째 요청은 차단
        allowed = await check_rate_limit(
            redis_client, "user1", "push", window_size=3600, limits=limits,
        )
        assert allowed is False

    @pytest.mark.asyncio
    async def test_independent_channels(self, redis_client) -> None:
        """채널별 rate limit 은 독립적이다."""
        limits = {"push": 1, "sms": 1, "email": 1}
        # push 제한 도달
        await check_rate_limit(
            redis_client, "user1", "push", window_size=3600, limits=limits,
        )
        allowed_push = await check_rate_limit(
            redis_client, "user1", "push", window_size=3600, limits=limits,
        )
        assert allowed_push is False

        # sms 는 여전히 허용
        allowed_sms = await check_rate_limit(
            redis_client, "user1", "sms", window_size=3600, limits=limits,
        )
        assert allowed_sms is True

    @pytest.mark.asyncio
    async def test_independent_users(self, redis_client) -> None:
        """사용자별 rate limit 은 독립적이다."""
        limits = {"push": 1, "sms": 1, "email": 1}
        await check_rate_limit(
            redis_client, "user1", "push", window_size=3600, limits=limits,
        )
        blocked = await check_rate_limit(
            redis_client, "user1", "push", window_size=3600, limits=limits,
        )
        assert blocked is False

        # user2 는 여전히 허용
        allowed = await check_rate_limit(
            redis_client, "user2", "push", window_size=3600, limits=limits,
        )
        assert allowed is True


# ---------------------------------------------------------------------------
# User Preferences (opt-out)
# ---------------------------------------------------------------------------


class TestUserPreferences:
    """사용자 알림 설정에 의한 opt-out 테스트."""

    @pytest.mark.asyncio
    async def test_opt_out_blocks_notification(self, redis_client) -> None:
        """opt-out 된 채널로는 알림이 전송되지 않는다."""
        # sms opt-out 설정
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
    async def test_opt_in_allows_notification(self, redis_client) -> None:
        """opt-in 된 채널로는 알림이 정상 전송된다."""
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
    async def test_default_preferences_all_enabled(self, redis_client) -> None:
        """설정이 없는 사용자는 모든 채널이 활성화된다."""
        prefs = await get_user_preferences(redis_client, "new_user")
        assert prefs.push is True
        assert prefs.sms is True
        assert prefs.email is True


# ---------------------------------------------------------------------------
# Retry Mechanism
# ---------------------------------------------------------------------------


class TestRetryMechanism:
    """실패 시 retry 메커니즘 테스트."""

    @pytest.mark.asyncio
    async def test_failure_triggers_requeue(self, redis_client) -> None:
        """전송 실패 시 메시지가 다시 큐에 들어간다."""
        notification_id = "test-retry-001"
        # 알림 레코드 생성
        await redis_client.hset(
            f"notification:{notification_id}",
            mapping={
                "notification_id": notification_id,
                "status": "pending",
                "retry_count": "0",
            },
        )

        # 100% 실패율로 메시지 처리
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

        # 큐에 재삽입되었는지 확인
        queue_len = await redis_client.llen("queue:push")
        assert queue_len == 1

        # retry_count 가 증가했는지 확인
        requeued = await redis_client.rpop("queue:push")
        requeued_data = json.loads(requeued)
        assert requeued_data["retry_count"] == 1

    @pytest.mark.asyncio
    async def test_success_after_retry(self, redis_client) -> None:
        """재시도 후 성공하면 상태가 sent 로 갱신된다."""
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
    async def test_max_retries_marks_failed(self, redis_client) -> None:
        """최대 재시도 초과 시 상태가 failed 로 갱신된다."""
        notification_id = "test-max-retry-001"
        await redis_client.hset(
            f"notification:{notification_id}",
            mapping={
                "notification_id": notification_id,
                "status": "pending",
                "retry_count": "3",
            },
        )

        # retry_count 가 이미 MAX_RETRIES(3) 인 메시지
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

        # 큐에 재삽입되지 않았는지 확인
        queue_len = await redis_client.llen("queue:push")
        assert queue_len == 0


# ---------------------------------------------------------------------------
# Dedup (중복 처리 방지)
# ---------------------------------------------------------------------------


class TestDedup:
    """중복 알림 처리 방지 테스트."""

    @pytest.mark.asyncio
    async def test_duplicate_not_processed(self, redis_client) -> None:
        """이미 sent 상태인 알림은 다시 처리되지 않는다."""
        notification_id = "test-dedup-001"
        # 이미 sent 상태로 저장
        await redis_client.hset(
            f"notification:{notification_id}",
            mapping={
                "notification_id": notification_id,
                "status": "sent",
            },
        )

        assert await is_duplicate(redis_client, notification_id) is True

    @pytest.mark.asyncio
    async def test_pending_is_not_duplicate(self, redis_client) -> None:
        """pending 상태의 알림은 중복이 아니다."""
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
    async def test_duplicate_skips_processing(self, redis_client) -> None:
        """중복 메시지는 채널 핸들러를 호출하지 않는다."""
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

        # 핸들러가 호출되지 않았어야 함
        mock_handler.assert_not_called()


# ---------------------------------------------------------------------------
# Notification Status Tracking
# ---------------------------------------------------------------------------


class TestStatusTracking:
    """알림 상태 추적 (pending -> sent) 테스트."""

    @pytest.mark.asyncio
    async def test_pending_to_sent(self, redis_client) -> None:
        """성공적으로 전송되면 pending -> sent 로 변경된다."""
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

        # sent_at 타임스탬프가 기록되었는지 확인
        sent_at = await redis_client.hget(f"notification:{notification_id}", "sent_at")
        assert sent_at is not None

    @pytest.mark.asyncio
    async def test_update_notification_status(self, redis_client) -> None:
        """상태 갱신 유틸리티가 올바르게 동작한다."""
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
