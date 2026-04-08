"""Tests for the news feed system.

fakeredis 를 사용하여 Redis 의존성 없이 단위 테스트를 수행한다.
"""

from __future__ import annotations

import os
import sys
import time

import fakeredis.aioredis
import pytest
import pytest_asyncio

# api 디렉토리를 import 경로에 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

from feed.fanout import fanout_to_followers
from feed.publisher import create_post, get_post
from feed.retrieval import get_feed
from social.graph import follow, get_followers, get_following, unfollow


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


async def _create_user(redis_client, user_id: str, name: str | None = None) -> None:
    """테스트용 사용자를 생성한다."""
    await redis_client.hset(
        f"user:{user_id}",
        mapping={
            "user_id": user_id,
            "name": name or user_id,
            "created_at": str(time.time()),
        },
    )


# ---------------------------------------------------------------------------
# Post Creation
# ---------------------------------------------------------------------------


class TestPostCreation:
    """포스트 생성 테스트."""

    @pytest.mark.asyncio
    async def test_create_post_returns_post_data(self, redis_client) -> None:
        """포스트 생성 시 올바른 데이터를 반환한다."""
        await _create_user(redis_client, "alice")
        result = await create_post(redis_client, "alice", "Hello world!")
        assert result["user_id"] == "alice"
        assert result["content"] == "Hello world!"
        assert "post_id" in result
        assert "created_at" in result

    @pytest.mark.asyncio
    async def test_create_post_stored_in_redis(self, redis_client) -> None:
        """생성된 포스트가 Redis 에 저장된다."""
        await _create_user(redis_client, "alice")
        result = await create_post(redis_client, "alice", "Test post")
        post_id = result["post_id"]

        post = await get_post(redis_client, post_id)
        assert post is not None
        assert post["user_id"] == "alice"
        assert post["content"] == "Test post"
        assert post["likes"] == "0"

    @pytest.mark.asyncio
    async def test_create_post_added_to_author_feed(self, redis_client) -> None:
        """작성자 본인의 피드에도 포스트가 추가된다."""
        await _create_user(redis_client, "alice")
        result = await create_post(redis_client, "alice", "My post")
        post_id = result["post_id"]

        feed_ids = await redis_client.zrevrange("feed:alice", 0, -1)
        assert post_id in feed_ids

    @pytest.mark.asyncio
    async def test_get_nonexistent_post_returns_none(self, redis_client) -> None:
        """존재하지 않는 포스트 조회 시 None 을 반환한다."""
        post = await get_post(redis_client, "nonexistent")
        assert post is None


# ---------------------------------------------------------------------------
# Fanout
# ---------------------------------------------------------------------------


class TestFanout:
    """Fanout on write 테스트."""

    @pytest.mark.asyncio
    async def test_fanout_pushes_to_follower_feed(self, redis_client) -> None:
        """포스트가 팔로워의 피드에 push 된다."""
        await _create_user(redis_client, "alice")
        await _create_user(redis_client, "bob")
        await follow(redis_client, "alice", "bob")

        result = await create_post(redis_client, "bob", "Bob's post")
        post_id = result["post_id"]

        # alice 의 피드에 bob 의 포스트가 있는지 확인
        feed_ids = await redis_client.zrevrange("feed:alice", 0, -1)
        assert post_id in feed_ids

    @pytest.mark.asyncio
    async def test_fanout_count(self, redis_client) -> None:
        """fanout_count 가 팔로워 수와 일치한다."""
        await _create_user(redis_client, "alice")
        await _create_user(redis_client, "bob")
        await _create_user(redis_client, "carol")
        await follow(redis_client, "alice", "bob")
        await follow(redis_client, "carol", "bob")

        result = await create_post(redis_client, "bob", "Bob's post")
        assert result["fanout_count"] == 2

    @pytest.mark.asyncio
    async def test_fanout_multiple_followers(self, redis_client) -> None:
        """여러 팔로워 모두에게 포스트가 push 된다."""
        await _create_user(redis_client, "bob")
        followers = ["alice", "carol", "dave"]
        for f in followers:
            await _create_user(redis_client, f)
            await follow(redis_client, f, "bob")

        result = await create_post(redis_client, "bob", "Hello everyone!")
        post_id = result["post_id"]

        for f in followers:
            feed_ids = await redis_client.zrevrange(f"feed:{f}", 0, -1)
            assert post_id in feed_ids

    @pytest.mark.asyncio
    async def test_no_fanout_without_followers(self, redis_client) -> None:
        """팔로워가 없으면 fanout 이 발생하지 않는다."""
        await _create_user(redis_client, "alice")
        result = await create_post(redis_client, "alice", "Lonely post")
        assert result["fanout_count"] == 0


# ---------------------------------------------------------------------------
# Feed Retrieval
# ---------------------------------------------------------------------------


class TestFeedRetrieval:
    """뉴스 피드 조회 테스트."""

    @pytest.mark.asyncio
    async def test_feed_returns_hydrated_posts(self, redis_client) -> None:
        """피드 조회 시 포스트 내용과 작성자 정보가 포함된다."""
        await _create_user(redis_client, "alice", "Alice Kim")
        await _create_user(redis_client, "bob", "Bob Lee")
        await follow(redis_client, "alice", "bob")

        await create_post(redis_client, "bob", "Hello from Bob!")

        feed = await get_feed(redis_client, "alice")
        assert len(feed) >= 1
        item = feed[0]
        assert item["content"] == "Hello from Bob!"
        assert item["author_name"] == "Bob Lee"
        assert item["user_id"] == "bob"

    @pytest.mark.asyncio
    async def test_feed_reverse_chronological_order(self, redis_client) -> None:
        """피드가 역시간순(최신 먼저)으로 정렬된다."""
        await _create_user(redis_client, "alice")
        await _create_user(redis_client, "bob")
        await follow(redis_client, "alice", "bob")

        # 시간 간격을 두고 3개의 포스트 생성
        post_ids = []
        for i in range(3):
            result = await create_post(redis_client, "bob", f"Post {i}")
            post_ids.append(result["post_id"])

        feed = await get_feed(redis_client, "alice")
        feed_post_ids = [item["post_id"] for item in feed]

        # 역순으로 정렬되어야 함 (최신 먼저)
        assert feed_post_ids == list(reversed(post_ids))

    @pytest.mark.asyncio
    async def test_empty_feed(self, redis_client) -> None:
        """피드가 비어 있으면 빈 리스트를 반환한다."""
        feed = await get_feed(redis_client, "nobody")
        assert feed == []

    @pytest.mark.asyncio
    async def test_feed_pagination(self, redis_client) -> None:
        """offset/limit 으로 피드를 페이지네이션할 수 있다."""
        await _create_user(redis_client, "alice")
        await _create_user(redis_client, "bob")
        await follow(redis_client, "alice", "bob")

        for i in range(5):
            await create_post(redis_client, "bob", f"Post {i}")

        page1 = await get_feed(redis_client, "alice", offset=0, limit=2)
        page2 = await get_feed(redis_client, "alice", offset=2, limit=2)

        assert len(page1) == 2
        assert len(page2) == 2
        # 페이지 간 겹침 없음
        p1_ids = {item["post_id"] for item in page1}
        p2_ids = {item["post_id"] for item in page2}
        assert p1_ids.isdisjoint(p2_ids)


# ---------------------------------------------------------------------------
# Feed Size Limit
# ---------------------------------------------------------------------------


class TestFeedSizeLimit:
    """피드 크기 제한 테스트."""

    @pytest.mark.asyncio
    async def test_feed_trimmed_to_max_size(self, redis_client) -> None:
        """피드가 FEED_MAX_SIZE 를 초과하면 오래된 항목이 제거된다."""
        import config
        original = config.settings.FEED_MAX_SIZE
        config.settings.FEED_MAX_SIZE = 5  # 테스트용으로 작은 값

        try:
            await _create_user(redis_client, "alice")
            await _create_user(redis_client, "bob")
            await follow(redis_client, "alice", "bob")

            for i in range(8):
                await create_post(redis_client, "bob", f"Post {i}")

            # alice 의 피드는 최대 5개여야 함
            feed_size = await redis_client.zcard("feed:alice")
            assert feed_size <= 5
        finally:
            config.settings.FEED_MAX_SIZE = original


# ---------------------------------------------------------------------------
# Social Graph — Follow/Unfollow
# ---------------------------------------------------------------------------


class TestSocialGraph:
    """소셜 그래프 (팔로우/언팔로우) 테스트."""

    @pytest.mark.asyncio
    async def test_follow(self, redis_client) -> None:
        """팔로우 시 양방향 관계가 생성된다."""
        result = await follow(redis_client, "alice", "bob")
        assert result["status"] == "ok"

        following = await get_following(redis_client, "alice")
        assert "bob" in following

        followers = await get_followers(redis_client, "bob")
        assert "alice" in followers

    @pytest.mark.asyncio
    async def test_unfollow(self, redis_client) -> None:
        """언팔로우 시 양방향 관계가 제거된다."""
        await follow(redis_client, "alice", "bob")
        result = await unfollow(redis_client, "alice", "bob")
        assert result["status"] == "ok"

        following = await get_following(redis_client, "alice")
        assert "bob" not in following

        followers = await get_followers(redis_client, "bob")
        assert "alice" not in followers

    @pytest.mark.asyncio
    async def test_cannot_follow_self(self, redis_client) -> None:
        """자기 자신을 팔로우할 수 없다."""
        result = await follow(redis_client, "alice", "alice")
        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_duplicate_follow(self, redis_client) -> None:
        """이미 팔로우 중인 사용자를 다시 팔로우하면 already_following 을 반환한다."""
        await follow(redis_client, "alice", "bob")
        result = await follow(redis_client, "alice", "bob")
        assert result["status"] == "already_following"

    @pytest.mark.asyncio
    async def test_unfollow_not_following(self, redis_client) -> None:
        """팔로우하지 않는 사용자를 언팔로우하면 not_following 을 반환한다."""
        result = await unfollow(redis_client, "alice", "bob")
        assert result["status"] == "not_following"

    @pytest.mark.asyncio
    async def test_follow_affects_feed(self, redis_client) -> None:
        """팔로우 후 팔로이의 새 포스트가 피드에 나타난다."""
        await _create_user(redis_client, "alice")
        await _create_user(redis_client, "bob")

        # bob 이 포스트 작성 (팔로우 전)
        pre_result = await create_post(redis_client, "bob", "Before follow")

        # alice 가 bob 을 팔로우
        await follow(redis_client, "alice", "bob")

        # bob 이 포스트 작성 (팔로우 후)
        post_result = await create_post(redis_client, "bob", "After follow")

        feed = await get_feed(redis_client, "alice")
        feed_post_ids = [item["post_id"] for item in feed]

        # 팔로우 후 포스트만 피드에 있어야 함
        assert post_result["post_id"] in feed_post_ids
        assert pre_result["post_id"] not in feed_post_ids

    @pytest.mark.asyncio
    async def test_unfollow_does_not_remove_existing_feed(self, redis_client) -> None:
        """언팔로우해도 이미 피드에 있는 포스트는 즉시 제거되지 않는다."""
        await _create_user(redis_client, "alice")
        await _create_user(redis_client, "bob")
        await follow(redis_client, "alice", "bob")

        result = await create_post(redis_client, "bob", "Bob's post")
        post_id = result["post_id"]

        await unfollow(redis_client, "alice", "bob")

        # 피드에 여전히 존재 (lazy cleanup)
        feed_ids = await redis_client.zrevrange("feed:alice", 0, -1)
        assert post_id in feed_ids


# ---------------------------------------------------------------------------
# Feed Hydration
# ---------------------------------------------------------------------------


class TestFeedHydration:
    """피드 hydration (포스트 + 작성자 정보) 테스트."""

    @pytest.mark.asyncio
    async def test_hydration_includes_author_name(self, redis_client) -> None:
        """hydrate 된 피드 항목에 작성자 이름이 포함된다."""
        await _create_user(redis_client, "bob", "Bob Lee")
        await _create_user(redis_client, "alice")
        await follow(redis_client, "alice", "bob")

        await create_post(redis_client, "bob", "Test content")

        feed = await get_feed(redis_client, "alice")
        assert len(feed) >= 1
        assert feed[0]["author_name"] == "Bob Lee"

    @pytest.mark.asyncio
    async def test_hydration_includes_likes(self, redis_client) -> None:
        """hydrate 된 피드 항목에 likes 수가 포함된다."""
        await _create_user(redis_client, "alice")
        result = await create_post(redis_client, "alice", "My post")
        post_id = result["post_id"]

        # likes 를 수동으로 증가
        await redis_client.hset(f"post:{post_id}", "likes", "42")

        feed = await get_feed(redis_client, "alice")
        assert len(feed) >= 1
        assert feed[0]["likes"] == 42

    @pytest.mark.asyncio
    async def test_hydration_missing_user_falls_back(self, redis_client) -> None:
        """작성자 정보가 없으면 user_id 를 이름으로 사용한다."""
        # user 해시 없이 포스트만 생성
        post_id = "999"
        await redis_client.hset(
            f"post:{post_id}",
            mapping={
                "post_id": post_id,
                "user_id": "ghost",
                "content": "Ghost post",
                "created_at": str(time.time()),
                "likes": "0",
            },
        )
        await redis_client.zadd("feed:viewer", {post_id: time.time()})

        feed = await get_feed(redis_client, "viewer")
        assert len(feed) == 1
        assert feed[0]["author_name"] == "ghost"
