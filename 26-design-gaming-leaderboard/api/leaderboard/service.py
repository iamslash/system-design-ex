"""Leaderboard operations using Redis sorted sets.

Redis sorted sets provide O(log n) complexity for all core leaderboard
operations:

- ``ZADD`` / ``ZINCRBY`` -- add or increment a member's score
- ``ZREVRANGE``          -- retrieve top-N members (descending score)
- ``ZREVRANK``           -- rank of a member (0-based, highest score first)
- ``ZSCORE``             -- score of a specific member

Monthly leaderboards are keyed as ``leaderboard:{YYYY}-{MM}``.
"""

from __future__ import annotations

from datetime import datetime, timezone

from redis.asyncio import Redis

from leaderboard.user_store import UserStore


class LeaderboardService:
    """High-level leaderboard API backed by Redis sorted sets."""

    _KEY_PREFIX = "leaderboard"

    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._user_store = UserStore(redis)

    # ------------------------------------------------------------------
    # Key helpers
    # ------------------------------------------------------------------

    @staticmethod
    def current_key() -> str:
        """Return the leaderboard key for the current month."""
        now = datetime.now(timezone.utc)
        return f"leaderboard:{now.strftime('%Y-%m')}"

    @staticmethod
    def key_for(year: int, month: int) -> str:
        """Return the leaderboard key for a specific month."""
        return f"leaderboard:{year:04d}-{month:02d}"

    # ------------------------------------------------------------------
    # Score operations
    # ------------------------------------------------------------------

    async def score_point(
        self,
        user_id: str,
        points: int = 1,
        *,
        leaderboard_key: str | None = None,
    ) -> float:
        """Increment *user_id*'s score by *points* and return the new score.

        Automatically creates the user profile if it does not exist.
        """
        key = leaderboard_key or self.current_key()
        new_score = await self._redis.zincrby(key, points, user_id)
        # Ensure the user profile exists.
        await self._user_store.upsert(user_id)
        return float(new_score)

    # ------------------------------------------------------------------
    # Query operations
    # ------------------------------------------------------------------

    async def top(
        self,
        n: int = 10,
        *,
        leaderboard_key: str | None = None,
    ) -> list[dict]:
        """Return the top *n* entries with rank, user_id, score, and display_name."""
        key = leaderboard_key or self.current_key()
        # ZREVRANGE returns [(member, score), ...] with withscores=True
        results = await self._redis.zrevrange(key, 0, n - 1, withscores=True)
        entries: list[dict] = []
        for rank_idx, (member, score) in enumerate(results):
            profile = await self._user_store.get(member)
            entries.append({
                "rank": rank_idx + 1,
                "user_id": member,
                "score": score,
                "display_name": profile.get("display_name") if profile else None,
            })
        return entries

    async def user_rank(
        self,
        user_id: str,
        *,
        leaderboard_key: str | None = None,
    ) -> dict:
        """Return *user_id*'s rank (1-based) and score.

        Returns rank ``None`` if the user has no score in the leaderboard.
        """
        key = leaderboard_key or self.current_key()
        score = await self._redis.zscore(key, user_id)
        if score is None:
            return {
                "user_id": user_id,
                "rank": None,
                "score": 0.0,
                "display_name": None,
            }
        # ZREVRANK is 0-based; convert to 1-based.
        zero_rank = await self._redis.zrevrank(key, user_id)
        profile = await self._user_store.get(user_id)
        return {
            "user_id": user_id,
            "rank": zero_rank + 1 if zero_rank is not None else None,
            "score": float(score),
            "display_name": profile.get("display_name") if profile else None,
        }

    async def around_user(
        self,
        user_id: str,
        span: int = 4,
        *,
        leaderboard_key: str | None = None,
    ) -> list[dict]:
        """Return *span* entries above and below *user_id* (inclusive).

        The result includes up to ``2 * span + 1`` entries centered on the
        target user.
        """
        key = leaderboard_key or self.current_key()
        zero_rank = await self._redis.zrevrank(key, user_id)
        if zero_rank is None:
            return []

        start = max(0, zero_rank - span)
        end = zero_rank + span

        results = await self._redis.zrevrange(key, start, end, withscores=True)
        entries: list[dict] = []
        for idx, (member, score) in enumerate(results):
            profile = await self._user_store.get(member)
            entries.append({
                "rank": start + idx + 1,
                "user_id": member,
                "score": score,
                "display_name": profile.get("display_name") if profile else None,
            })
        return entries
