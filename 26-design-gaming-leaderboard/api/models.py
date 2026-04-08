"""Pydantic models for API request/response schemas."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ScoreRequest(BaseModel):
    """Request body for scoring a point."""

    user_id: str = Field(..., min_length=1, description="Unique user identifier")
    points: int = Field(default=1, ge=1, description="Points to add")


class LeaderboardEntry(BaseModel):
    """A single entry in the leaderboard."""

    rank: int
    user_id: str
    score: float
    display_name: str | None = None


class UserRankResponse(BaseModel):
    """Response for a user's rank and score."""

    user_id: str
    rank: int | None = None
    score: float
    display_name: str | None = None


class ScoreResponse(BaseModel):
    """Response after scoring a point."""

    user_id: str
    new_score: float
    leaderboard_key: str
