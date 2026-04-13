"""Pydantic models for the notification system."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class Channel(str, Enum):
    """Types of notification channels."""
    PUSH = "push"
    SMS = "sms"
    EMAIL = "email"


class Priority(str, Enum):
    """Notification priority levels."""
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


class NotificationStatus(str, Enum):
    """Notification processing status."""
    PENDING = "pending"
    SENT = "sent"
    DELIVERED = "delivered"
    FAILED = "failed"


class NotificationRequest(BaseModel):
    """Request model for sending a notification."""
    user_id: str
    channel: Channel
    template: str = "default"
    params: dict[str, Any] = Field(default_factory=dict)
    priority: Priority = Priority.NORMAL


class BatchNotificationRequest(BaseModel):
    """Request model for sending batch notifications."""
    user_ids: list[str]
    channel: Channel
    template: str = "default"
    params: dict[str, Any] = Field(default_factory=dict)
    priority: Priority = Priority.NORMAL


class NotificationRecord(BaseModel):
    """Notification log record."""
    notification_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    channel: Channel
    template: str
    params: dict[str, Any] = Field(default_factory=dict)
    priority: Priority = Priority.NORMAL
    status: NotificationStatus = NotificationStatus.PENDING
    retry_count: int = 0
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class UserPreferences(BaseModel):
    """User notification preferences (per-channel opt-in/out)."""
    push: bool = True
    sms: bool = True
    email: bool = True
