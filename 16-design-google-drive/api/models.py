"""Pydantic models for the Google Drive file sync service."""

from __future__ import annotations

from pydantic import BaseModel, Field


class FileMetadata(BaseModel):
    """Response model for file metadata."""

    file_id: str
    filename: str
    user_id: str
    latest_version: int
    size: int
    created_at: str
    updated_at: str


class UploadResponse(BaseModel):
    """Response model for file upload."""

    file_id: str
    filename: str
    version: int
    size: int
    total_blocks: int
    new_blocks: int
    reused_blocks: int
    message: str


class VersionInfo(BaseModel):
    """Model for file version information."""

    version: int
    size: int
    block_count: int
    created_at: str


class SyncEvent(BaseModel):
    """Model for file change events."""

    event_type: str
    file_id: str
    filename: str
    user_id: str
    version: int = 0
    timestamp: str


class ConflictResponse(BaseModel):
    """Response model for sync conflicts."""

    conflict: bool = True
    file_id: str
    message: str
    your_version: int
    server_version: int
    server_updated_at: str
    server_updated_by: str
