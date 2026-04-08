"""Pydantic models for request/response validation."""

from __future__ import annotations

from pydantic import BaseModel


class UserCreate(BaseModel):
    """Request body for user registration."""
    user_id: str
    name: str


class GroupCreate(BaseModel):
    """Request body for group creation."""
    group_id: str
    name: str
    members: list[str]


class UserResponse(BaseModel):
    """Response body for user info."""
    user_id: str
    name: str
    created_at: float


class GroupResponse(BaseModel):
    """Response body for group info."""
    group_id: str
    name: str
    members: list[str]


class MessageResponse(BaseModel):
    """Response body for a chat message."""
    message_id: str
    from_user: str
    content: str
    timestamp: float
    channel_id: str


class PresenceResponse(BaseModel):
    """Response body for user presence status."""
    user_id: str
    status: str
    last_heartbeat: float | None = None
