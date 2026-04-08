"""Pydantic models for email service API."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field


class FolderType(str, Enum):
    INBOX = "inbox"
    SENT = "sent"
    DRAFTS = "drafts"
    TRASH = "trash"
    CUSTOM = "custom"


class Attachment(BaseModel):
    attachment_id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    filename: str
    content_type: str = "application/octet-stream"
    size: int = 0


class Email(BaseModel):
    email_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    from_addr: str
    to_addrs: list[str]
    cc_addrs: list[str] = Field(default_factory=list)
    bcc_addrs: list[str] = Field(default_factory=list)
    subject: str = ""
    body: str = ""
    attachments: list[Attachment] = Field(default_factory=list)
    thread_id: str | None = None
    in_reply_to: str | None = None
    is_read: bool = False
    folder: FolderType = FolderType.INBOX
    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class SendEmailRequest(BaseModel):
    from_addr: str
    to_addrs: list[str]
    cc_addrs: list[str] = Field(default_factory=list)
    bcc_addrs: list[str] = Field(default_factory=list)
    subject: str = ""
    body: str = ""
    attachments: list[Attachment] = Field(default_factory=list)
    in_reply_to: str | None = None


class MoveEmailRequest(BaseModel):
    email_id: str
    target_folder: FolderType


class SearchRequest(BaseModel):
    query: str
    user: str


class FolderCreateRequest(BaseModel):
    user: str
    folder_name: str
