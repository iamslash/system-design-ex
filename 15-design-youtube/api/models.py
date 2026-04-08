"""Pydantic models for the video streaming system."""

from __future__ import annotations

from pydantic import BaseModel


class UploadInitRequest(BaseModel):
    """비디오 업로드 시작 요청."""
    title: str
    description: str = ""
    total_chunks: int = 1


class UploadCompleteRequest(BaseModel):
    """비디오 업로드 완료 요청."""
    pass
