"""Tests for the news feed system.

Uses fakeredis to run unit tests without a real Redis dependency.
"""

from __future__ import annotations

import time

import fakeredis.aioredis
import pytest

from feed.fanout import fanout_to_followers
from feed.publisher import create_post, get_post
from feed.retrieval import get_feed
from social.graph import follow, get_followers, get_following, unfollow


async def _create_user(redis_client: fakeredis.aioredis.FakeRedis, user_id: str, name: str | None = None) -> None:
    """Create a test user."""
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
    """Tests for post creation."""

    @pytest.mark.asyncio
    async def test_create_post_returns_post_data(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Post creation returns the correct data."""
        await _create_user(redis_client, "alice")
        result = await create_post(redis_client, "alice", "Hello world!")
        assert result["user_id"] == "alice"
        assert result["content"] == "Hello world!"
        assert "post_id" in result
        assert "created_at" in result

    @pytest.mark.asyncio
    async def test_create_post_stored_in_redis(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """A created post is stored in Redis."""
        await _create_user(redis_client, "alice")
        result = await create_post(redis_client, "alice", "Test post")
        post_id = result["post_id"]

        post = await get_post(redis_client, post_id)
        assert post is not None
        assert post["user_id"] == "alice"
        assert post["content"] == "Test post"
        assert post["likes"] == "0"

    @pytest.mark.asyncio
    async def test_create_post_added_to_author_feed(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """A created post is also added to the author's own feed."""
        await _create_user(redis_client, "alice")
        result = await create_post(redis_client, "alice", "My post")
        post_id = result["post_id"]

        feed_ids = await redis_client.zrevrange("feed:alice", 0, -1)
        assert post_id in feed_ids

    @pytest.mark.asyncio
    async def test_get_nonexistent_post_returns_none(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Fetching a non-existent post returns None."""
        post = await get_post(redis_client, "nonexistent")
        assert post is None


# ---------------------------------------------------------------------------
# Fanout
# ---------------------------------------------------------------------------


class TestFanout:
    """Tests for fanout on write."""

    @pytest.mark.asyncio
    async def test_fanout_pushes_to_follower_feed(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """A post is pushed to the follower's feed."""
        await _create_user(redis_client, "alice")
        await _create_user(redis_client, "bob")
        await follow(redis_client, "alice", "bob")

        result = await create_post(redis_client, "bob", "Bob's post")
        post_id = result["post_id"]

        # Verify that bob's post is in alice's feed
        feed_ids = await redis_client.zrevrange("feed:alice", 0, -1)
        assert post_id in feed_ids

    @pytest.mark.asyncio
    async def test_fanout_count(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """fanout_count matches the number of followers."""
        await _create_user(redis_client, "alice")
        await _create_user(redis_client, "bob")
        await _create_user(redis_client, "carol")
        await follow(redis_client, "alice", "bob")
        await follow(redis_client, "carol", "bob")

        result = await create_post(redis_client, "bob", "Bob's post")
        assert result["fanout_count"] == 2

    @pytest.mark.asyncio
    async def test_fanout_multiple_followers(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """A post is pushed to all multiple followers."""
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
    async def test_no_fanout_without_followers(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """No fanout occurs when the author has no followers."""
        await _create_user(redis_client, "alice")
        result = await create_post(redis_client, "alice", "Lonely post")
        assert result["fanout_count"] == 0


# ---------------------------------------------------------------------------
# Feed Retrieval
# ---------------------------------------------------------------------------


class TestFeedRetrieval:
    """Tests for news feed retrieval."""

    @pytest.mark.asyncio
    async def test_feed_returns_hydrated_posts(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Feed retrieval includes post content and author information."""
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
    async def test_feed_reverse_chronological_order(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """The feed is sorted in reverse chronological order (newest first)."""
        await _create_user(redis_client, "alice")
        await _create_user(redis_client, "bob")
        await follow(redis_client, "alice", "bob")

        # Create 3 posts with time intervals
        post_ids = []
        for i in range(3):
            result = await create_post(redis_client, "bob", f"Post {i}")
            post_ids.append(result["post_id"])

        feed = await get_feed(redis_client, "alice")
        feed_post_ids = [item["post_id"] for item in feed]

        # Should be sorted in reverse order (newest first)
        assert feed_post_ids == list(reversed(post_ids))

    @pytest.mark.asyncio
    async def test_empty_feed(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """An empty feed returns an empty list."""
        feed = await get_feed(redis_client, "nobody")
        assert feed == []

    @pytest.mark.asyncio
    async def test_feed_pagination(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """The feed can be paginated using offset/limit."""
        await _create_user(redis_client, "alice")
        await _create_user(redis_client, "bob")
        await follow(redis_client, "alice", "bob")

        for i in range(5):
            await create_post(redis_client, "bob", f"Post {i}")

        page1 = await get_feed(redis_client, "alice", offset=0, limit=2)
        page2 = await get_feed(redis_client, "alice", offset=2, limit=2)

        assert len(page1) == 2
        assert len(page2) == 2
        # No overlap between pages
        p1_ids = {item["post_id"] for item in page1}
        p2_ids = {item["post_id"] for item in page2}
        assert p1_ids.isdisjoint(p2_ids)


# ---------------------------------------------------------------------------
# Feed Size Limit
# ---------------------------------------------------------------------------


class TestFeedSizeLimit:
    """Tests for feed size limits."""

    @pytest.mark.asyncio
    async def test_feed_trimmed_to_max_size(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """When the feed exceeds FEED_MAX_SIZE, old entries are removed."""
        import config
        original = config.settings.FEED_MAX_SIZE
        config.settings.FEED_MAX_SIZE = 5  # Small value for testing

        try:
            await _create_user(redis_client, "alice")
            await _create_user(redis_client, "bob")
            await follow(redis_client, "alice", "bob")

            for i in range(8):
                await create_post(redis_client, "bob", f"Post {i}")

            # alice's feed should have at most 5 entries
            feed_size = await redis_client.zcard("feed:alice")
            assert feed_size <= 5
        finally:
            config.settings.FEED_MAX_SIZE = original


# ---------------------------------------------------------------------------
# Social Graph — Follow/Unfollow
# ---------------------------------------------------------------------------


class TestSocialGraph:
    """Tests for the social graph (follow/unfollow)."""

    @pytest.mark.asyncio
    async def test_follow(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Following creates a bidirectional relationship."""
        result = await follow(redis_client, "alice", "bob")
        assert result["status"] == "ok"

        following = await get_following(redis_client, "alice")
        assert "bob" in following

        followers = await get_followers(redis_client, "bob")
        assert "alice" in followers

    @pytest.mark.asyncio
    async def test_unfollow(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Unfollowing removes the bidirectional relationship."""
        await follow(redis_client, "alice", "bob")
        result = await unfollow(redis_client, "alice", "bob")
        assert result["status"] == "ok"

        following = await get_following(redis_client, "alice")
        assert "bob" not in following

        followers = await get_followers(redis_client, "bob")
        assert "alice" not in followers

    @pytest.mark.asyncio
    async def test_cannot_follow_self(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """A user cannot follow themselves."""
        result = await follow(redis_client, "alice", "alice")
        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_duplicate_follow(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Following a user already being followed returns already_following."""
        await follow(redis_client, "alice", "bob")
        result = await follow(redis_client, "alice", "bob")
        assert result["status"] == "already_following"

    @pytest.mark.asyncio
    async def test_unfollow_not_following(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Unfollowing a user not being followed returns not_following."""
        result = await unfollow(redis_client, "alice", "bob")
        assert result["status"] == "not_following"

    @pytest.mark.asyncio
    async def test_follow_affects_feed(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """New posts from a followed user appear in the follower's feed."""
        await _create_user(redis_client, "alice")
        await _create_user(redis_client, "bob")

        # bob posts before the follow
        pre_result = await create_post(redis_client, "bob", "Before follow")

        # alice follows bob
        await follow(redis_client, "alice", "bob")

        # bob posts after the follow
        post_result = await create_post(redis_client, "bob", "After follow")

        feed = await get_feed(redis_client, "alice")
        feed_post_ids = [item["post_id"] for item in feed]

        # Only the post created after the follow should be in the feed
        assert post_result["post_id"] in feed_post_ids
        assert pre_result["post_id"] not in feed_post_ids

    @pytest.mark.asyncio
    async def test_unfollow_does_not_remove_existing_feed(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Unfollowing does not immediately remove posts already in the feed."""
        await _create_user(redis_client, "alice")
        await _create_user(redis_client, "bob")
        await follow(redis_client, "alice", "bob")

        result = await create_post(redis_client, "bob", "Bob's post")
        post_id = result["post_id"]

        await unfollow(redis_client, "alice", "bob")

        # Still present in feed (lazy cleanup)
        feed_ids = await redis_client.zrevrange("feed:alice", 0, -1)
        assert post_id in feed_ids


# ---------------------------------------------------------------------------
# Feed Hydration
# ---------------------------------------------------------------------------


class TestFeedHydration:
    """Tests for feed hydration (post + author information)."""

    @pytest.mark.asyncio
    async def test_hydration_includes_author_name(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Hydrated feed items include the author's name."""
        await _create_user(redis_client, "bob", "Bob Lee")
        await _create_user(redis_client, "alice")
        await follow(redis_client, "alice", "bob")

        await create_post(redis_client, "bob", "Test content")

        feed = await get_feed(redis_client, "alice")
        assert len(feed) >= 1
        assert feed[0]["author_name"] == "Bob Lee"

    @pytest.mark.asyncio
    async def test_hydration_includes_likes(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Hydrated feed items include the likes count."""
        await _create_user(redis_client, "alice")
        result = await create_post(redis_client, "alice", "My post")
        post_id = result["post_id"]

        # Manually increment likes
        await redis_client.hset(f"post:{post_id}", "likes", "42")

        feed = await get_feed(redis_client, "alice")
        assert len(feed) >= 1
        assert feed[0]["likes"] == 42

    @pytest.mark.asyncio
    async def test_hydration_missing_user_falls_back(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """When author information is missing, user_id is used as the name."""
        # Create a post without a user hash
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
