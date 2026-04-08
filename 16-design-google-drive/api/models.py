"""Pydantic models for the Google Drive file sync service."""

from __future__ import annotations

from pydantic import BaseModel, Field


class FileMetadata(BaseModel):
    """파일 메타데이터 응답 모델."""

    file_id: str
    filename: str
    user_id: str
    latest_version: int
    size: int
    created_at: str
    updated_at: str


class UploadResponse(BaseModel):
    """파일 업로드 응답 모델."""

    file_id: str
    filename: str
    version: int
    size: int
    total_blocks: int
    new_blocks: int
    reused_blocks: int
    message: str


class VersionInfo(BaseModel):
    """파일 버전 정보 모델."""

    version: int
    size: int
    block_count: int
    created_at: str


class SyncEvent(BaseModel):
    """파일 변경 이벤트 모델."""

    event_type: str
    file_id: str
    filename: str
    user_id: str
    version: int = 0
    timestamp: str


class ConflictResponse(BaseModel):
    """동기화 충돌 응답 모델."""

    conflict: bool = True
    file_id: str
    message: str
    your_version: int
    server_version: int
    server_updated_at: str
    server_updated_by: str
