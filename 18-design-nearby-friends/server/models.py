"""Pydantic models for request/response validation."""

from __future__ import annotations

from pydantic import BaseModel


class LocationUpdate(BaseModel):
    """Request body for location update via REST."""
    user_id: str
    latitude: float
    longitude: float


class FriendshipCreate(BaseModel):
    """Request body for creating a friendship (bidirectional)."""
    user_a: str
    user_b: str


class NearbyRequest(BaseModel):
    """Request body to query nearby friends."""
    user_id: str
    radius_miles: float | None = None


class LocationResponse(BaseModel):
    """Response body for a user's location."""
    user_id: str
    latitude: float
    longitude: float
    timestamp: float


class NearbyFriendResponse(BaseModel):
    """Response body for a nearby friend."""
    user_id: str
    latitude: float
    longitude: float
    distance_miles: float
    timestamp: float
