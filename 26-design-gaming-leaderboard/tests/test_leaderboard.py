"""Tests for the gaming leaderboard using fakeredis."""

from __future__ import annotations

import os
import sys

import pytest

# Add the api directory to the path so we can import the service modules.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

from leaderboard.service import LeaderboardService
from leaderboard.user_store import UserStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

LB_KEY = "leaderboard:2026-04"


async def _populate(svc: LeaderboardService, users: dict[str, int], key: str = LB_KEY) -> None:
    """Helper to populate a leaderboard with {user_id: total_score} pairs."""
    for user_id, score in users.items():
        await svc.score_point(user_id, score, leaderboard_key=key)


# ---------------------------------------------------------------------------
# Score Point
# ---------------------------------------------------------------------------


class TestScorePoint:
    """Tests for scoring points."""

    @pytest.mark.asyncio
    async def test_score_single_point(self, redis_client) -> None:
        """A single score should register correctly."""
        svc = LeaderboardService(redis_client)
        new_score = await svc.score_point("alice", 1, leaderboard_key=LB_KEY)
        assert new_score == 1.0

    @pytest.mark.asyncio
    async def test_score_multiple_points(self, redis_client) -> None:
        """Scoring multiple points at once should work."""
        svc = LeaderboardService(redis_client)
        new_score = await svc.score_point("alice", 5, leaderboard_key=LB_KEY)
        assert new_score == 5.0

    @pytest.mark.asyncio
    async def test_score_accumulates(self, redis_client) -> None:
        """Repeated scoring should accumulate."""
        svc = LeaderboardService(redis_client)
        await svc.score_point("alice", 3, leaderboard_key=LB_KEY)
        new_score = await svc.score_point("alice", 7, leaderboard_key=LB_KEY)
        assert new_score == 10.0

    @pytest.mark.asyncio
    async def test_score_creates_user_profile(self, redis_client) -> None:
        """Scoring should auto-create a user profile."""
        svc = LeaderboardService(redis_client)
        await svc.score_point("new_user", 1, leaderboard_key=LB_KEY)
        store = UserStore(redis_client)
        profile = await store.get("new_user")
        assert profile is not None
        assert profile["user_id"] == "new_user"
        assert profile["display_name"] == "new_user"

    @pytest.mark.asyncio
    async def test_score_independent_users(self, redis_client) -> None:
        """Different users should have independent scores."""
        svc = LeaderboardService(redis_client)
        await svc.score_point("alice", 10, leaderboard_key=LB_KEY)
        await svc.score_point("bob", 5, leaderboard_key=LB_KEY)
        data_a = await svc.user_rank("alice", leaderboard_key=LB_KEY)
        data_b = await svc.user_rank("bob", leaderboard_key=LB_KEY)
        assert data_a["score"] == 10.0
        assert data_b["score"] == 5.0


# ---------------------------------------------------------------------------
# Top 10 Ordering
# ---------------------------------------------------------------------------


class TestTop10:
    """Tests for top-N leaderboard queries."""

    @pytest.mark.asyncio
    async def test_top_ordering_descending(self, redis_client) -> None:
        """Top entries should be ordered by score descending."""
        svc = LeaderboardService(redis_client)
        await _populate(svc, {"alice": 50, "bob": 100, "carol": 75})
        top = await svc.top(10, leaderboard_key=LB_KEY)
        assert [e["user_id"] for e in top] == ["bob", "carol", "alice"]
        assert [e["score"] for e in top] == [100.0, 75.0, 50.0]

    @pytest.mark.asyncio
    async def test_top_respects_limit(self, redis_client) -> None:
        """Should return at most N entries."""
        svc = LeaderboardService(redis_client)
        users = {f"user_{i}": i * 10 for i in range(1, 16)}
        await _populate(svc, users)
        top = await svc.top(10, leaderboard_key=LB_KEY)
        assert len(top) == 10

    @pytest.mark.asyncio
    async def test_top_rank_numbers(self, redis_client) -> None:
        """Ranks should be 1-based and sequential."""
        svc = LeaderboardService(redis_client)
        await _populate(svc, {"a": 30, "b": 20, "c": 10})
        top = await svc.top(10, leaderboard_key=LB_KEY)
        assert [e["rank"] for e in top] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_top_includes_display_name(self, redis_client) -> None:
        """Top entries should include display names from user profiles."""
        svc = LeaderboardService(redis_client)
        store = UserStore(redis_client)
        await store.upsert("alice", display_name="Alice W.")
        await svc.score_point("alice", 10, leaderboard_key=LB_KEY)
        top = await svc.top(10, leaderboard_key=LB_KEY)
        assert top[0]["display_name"] == "Alice W."

    @pytest.mark.asyncio
    async def test_top_fewer_than_n(self, redis_client) -> None:
        """When fewer than N users exist, return all of them."""
        svc = LeaderboardService(redis_client)
        await _populate(svc, {"alice": 10, "bob": 5})
        top = await svc.top(10, leaderboard_key=LB_KEY)
        assert len(top) == 2


# ---------------------------------------------------------------------------
# User Rank
# ---------------------------------------------------------------------------


class TestUserRank:
    """Tests for querying a user's rank."""

    @pytest.mark.asyncio
    async def test_rank_returns_correct_position(self, redis_client) -> None:
        """Rank should reflect descending score order."""
        svc = LeaderboardService(redis_client)
        await _populate(svc, {"alice": 100, "bob": 200, "carol": 150})
        data = await svc.user_rank("carol", leaderboard_key=LB_KEY)
        assert data["rank"] == 2
        assert data["score"] == 150.0

    @pytest.mark.asyncio
    async def test_rank_unknown_user(self, redis_client) -> None:
        """Unknown user should return rank=None and score=0."""
        svc = LeaderboardService(redis_client)
        data = await svc.user_rank("ghost", leaderboard_key=LB_KEY)
        assert data["rank"] is None
        assert data["score"] == 0.0

    @pytest.mark.asyncio
    async def test_rank_first_place(self, redis_client) -> None:
        """The highest scorer should have rank 1."""
        svc = LeaderboardService(redis_client)
        await _populate(svc, {"alice": 999, "bob": 1})
        data = await svc.user_rank("alice", leaderboard_key=LB_KEY)
        assert data["rank"] == 1

    @pytest.mark.asyncio
    async def test_rank_last_place(self, redis_client) -> None:
        """The lowest scorer should have the highest rank number."""
        svc = LeaderboardService(redis_client)
        await _populate(svc, {"alice": 999, "bob": 1, "carol": 500})
        data = await svc.user_rank("bob", leaderboard_key=LB_KEY)
        assert data["rank"] == 3


# ---------------------------------------------------------------------------
# Relative Position (around user)
# ---------------------------------------------------------------------------


class TestAroundUser:
    """Tests for the relative-position query (4 above + 4 below)."""

    @pytest.mark.asyncio
    async def test_around_includes_target_user(self, redis_client) -> None:
        """The target user should appear in the result."""
        svc = LeaderboardService(redis_client)
        users = {f"u{i}": (10 - i) * 10 for i in range(10)}
        await _populate(svc, users)
        entries = await svc.around_user("u5", span=4, leaderboard_key=LB_KEY)
        user_ids = [e["user_id"] for e in entries]
        assert "u5" in user_ids

    @pytest.mark.asyncio
    async def test_around_span_size(self, redis_client) -> None:
        """With enough neighbours, result should have 2*span+1 entries."""
        svc = LeaderboardService(redis_client)
        users = {f"u{i:02d}": (20 - i) * 10 for i in range(20)}
        await _populate(svc, users)
        entries = await svc.around_user("u10", span=4, leaderboard_key=LB_KEY)
        assert len(entries) == 9  # 4 + 1 + 4

    @pytest.mark.asyncio
    async def test_around_top_user_clamps(self, redis_client) -> None:
        """For the top user, the result should start at rank 1."""
        svc = LeaderboardService(redis_client)
        users = {f"u{i}": (10 - i) * 10 for i in range(10)}
        await _populate(svc, users)
        entries = await svc.around_user("u0", span=4, leaderboard_key=LB_KEY)
        assert entries[0]["rank"] == 1

    @pytest.mark.asyncio
    async def test_around_bottom_user(self, redis_client) -> None:
        """For the bottom user, the result should extend to the last rank."""
        svc = LeaderboardService(redis_client)
        users = {f"u{i}": (10 - i) * 10 for i in range(10)}
        await _populate(svc, users)
        entries = await svc.around_user("u9", span=4, leaderboard_key=LB_KEY)
        assert entries[-1]["user_id"] == "u9"

    @pytest.mark.asyncio
    async def test_around_unknown_user(self, redis_client) -> None:
        """Unknown user should return an empty list."""
        svc = LeaderboardService(redis_client)
        entries = await svc.around_user("ghost", span=4, leaderboard_key=LB_KEY)
        assert entries == []


# ---------------------------------------------------------------------------
# Monthly Leaderboard Rotation
# ---------------------------------------------------------------------------


class TestMonthlyRotation:
    """Tests for monthly leaderboard key separation."""

    @pytest.mark.asyncio
    async def test_different_months_are_independent(self, redis_client) -> None:
        """Scores in January should not appear in February."""
        svc = LeaderboardService(redis_client)
        jan_key = LeaderboardService.key_for(2026, 1)
        feb_key = LeaderboardService.key_for(2026, 2)
        await svc.score_point("alice", 100, leaderboard_key=jan_key)
        await svc.score_point("bob", 50, leaderboard_key=feb_key)

        jan_top = await svc.top(10, leaderboard_key=jan_key)
        feb_top = await svc.top(10, leaderboard_key=feb_key)

        assert len(jan_top) == 1
        assert jan_top[0]["user_id"] == "alice"
        assert len(feb_top) == 1
        assert feb_top[0]["user_id"] == "bob"

    @pytest.mark.asyncio
    async def test_key_format(self) -> None:
        """Key should follow leaderboard:YYYY-MM format."""
        key = LeaderboardService.key_for(2026, 4)
        assert key == "leaderboard:2026-04"

    @pytest.mark.asyncio
    async def test_current_key_format(self) -> None:
        """current_key should return a validly formatted key."""
        key = LeaderboardService.current_key()
        assert key.startswith("leaderboard:")
        parts = key.split(":")
        assert len(parts) == 2
        year_month = parts[1].split("-")
        assert len(year_month) == 2
        assert len(year_month[0]) == 4  # YYYY
        assert len(year_month[1]) == 2  # MM


# ---------------------------------------------------------------------------
# Tie Handling
# ---------------------------------------------------------------------------


class TestTieHandling:
    """Tests for how tied scores are handled."""

    @pytest.mark.asyncio
    async def test_tied_scores_both_appear(self, redis_client) -> None:
        """Users with the same score should both appear in the leaderboard."""
        svc = LeaderboardService(redis_client)
        await _populate(svc, {"alice": 100, "bob": 100})
        top = await svc.top(10, leaderboard_key=LB_KEY)
        assert len(top) == 2
        scores = {e["score"] for e in top}
        assert scores == {100.0}

    @pytest.mark.asyncio
    async def test_tied_scores_have_distinct_ranks(self, redis_client) -> None:
        """Redis ZREVRANK assigns distinct ranks even for tied scores."""
        svc = LeaderboardService(redis_client)
        await _populate(svc, {"alice": 100, "bob": 100, "carol": 100})
        top = await svc.top(10, leaderboard_key=LB_KEY)
        ranks = [e["rank"] for e in top]
        assert ranks == [1, 2, 3]


# ---------------------------------------------------------------------------
# Empty Leaderboard
# ---------------------------------------------------------------------------


class TestEmptyLeaderboard:
    """Tests for edge cases on an empty leaderboard."""

    @pytest.mark.asyncio
    async def test_top_empty(self, redis_client) -> None:
        """Top on an empty leaderboard should return an empty list."""
        svc = LeaderboardService(redis_client)
        top = await svc.top(10, leaderboard_key=LB_KEY)
        assert top == []

    @pytest.mark.asyncio
    async def test_rank_empty(self, redis_client) -> None:
        """Rank query on empty leaderboard should return rank=None."""
        svc = LeaderboardService(redis_client)
        data = await svc.user_rank("alice", leaderboard_key=LB_KEY)
        assert data["rank"] is None
        assert data["score"] == 0.0

    @pytest.mark.asyncio
    async def test_around_empty(self, redis_client) -> None:
        """Around query on empty leaderboard should return empty list."""
        svc = LeaderboardService(redis_client)
        entries = await svc.around_user("alice", span=4, leaderboard_key=LB_KEY)
        assert entries == []


# ---------------------------------------------------------------------------
# User Store
# ---------------------------------------------------------------------------


class TestUserStore:
    """Tests for user profile storage."""

    @pytest.mark.asyncio
    async def test_upsert_creates_profile(self, redis_client) -> None:
        """Upserting a new user should create a profile."""
        store = UserStore(redis_client)
        profile = await store.upsert("alice", display_name="Alice")
        assert profile["user_id"] == "alice"
        assert profile["display_name"] == "Alice"
        assert "created_at" in profile

    @pytest.mark.asyncio
    async def test_upsert_updates_display_name(self, redis_client) -> None:
        """Upserting with a new display name should update it."""
        store = UserStore(redis_client)
        await store.upsert("alice", display_name="Alice")
        await store.upsert("alice", display_name="Alice W.")
        profile = await store.get("alice")
        assert profile is not None
        assert profile["display_name"] == "Alice W."

    @pytest.mark.asyncio
    async def test_upsert_preserves_display_name(self, redis_client) -> None:
        """Upserting without display_name should keep the existing one."""
        store = UserStore(redis_client)
        await store.upsert("alice", display_name="Alice")
        await store.upsert("alice")
        profile = await store.get("alice")
        assert profile is not None
        assert profile["display_name"] == "Alice"

    @pytest.mark.asyncio
    async def test_get_unknown_user(self, redis_client) -> None:
        """Getting an unknown user should return None."""
        store = UserStore(redis_client)
        profile = await store.get("ghost")
        assert profile is None
