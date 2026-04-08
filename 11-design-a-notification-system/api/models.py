"""Pydantic models for the notification system."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class Channel(str, Enum):
    """알림 채널 종류."""
    PUSH = "push"
    SMS = "sms"
    EMAIL = "email"


class Priority(str, Enum):
    """알림 우선순위."""
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


class NotificationStatus(str, Enum):
    """알림 처리 상태."""
    PENDING = "pending"
    SENT = "sent"
    DELIVERED = "delivered"
    FAILED = "failed"


class NotificationRequest(BaseModel):
    """알림 전송 요청 모델."""
    user_id: str
    channel: Channel
    template: str = "default"
    params: dict[str, Any] = Field(default_factory=dict)
    priority: Priority = Priority.NORMAL


class BatchNotificationRequest(BaseModel):
    """배치 알림 전송 요청 모델."""
    user_ids: list[str]
    channel: Channel
    template: str = "default"
    params: dict[str, Any] = Field(default_factory=dict)
    priority: Priority = Priority.NORMAL


class NotificationRecord(BaseModel):
    """알림 로그 레코드."""
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
    """사용자 알림 설정 (채널별 opt-in/out)."""
    push: bool = True
    sms: bool = True
    email: bool = True
