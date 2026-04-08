"""Pydantic models for the news feed system."""

from __future__ import annotations

from pydantic import BaseModel


class CreatePostRequest(BaseModel):
    """포스트 작성 요청."""
    user_id: str
    content: str


class FollowRequest(BaseModel):
    """팔로우 요청."""
    follower_id: str
    followee_id: str


class UnfollowRequest(BaseModel):
    """언팔로우 요청."""
    follower_id: str
    followee_id: str


class CreateUserRequest(BaseModel):
    """사용자 생성 요청."""
    user_id: str
    name: str
