"""Pydantic models for the news feed system."""

from __future__ import annotations

from pydantic import BaseModel


class CreatePostRequest(BaseModel):
    """Request to create a post."""
    user_id: str
    content: str


class FollowRequest(BaseModel):
    """Request to follow a user."""
    follower_id: str
    followee_id: str


class UnfollowRequest(BaseModel):
    """Request to unfollow a user."""
    follower_id: str
    followee_id: str


class CreateUserRequest(BaseModel):
    """Request to create a user."""
    user_id: str
    name: str
