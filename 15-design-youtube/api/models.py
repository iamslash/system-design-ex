"""Pydantic models for the video streaming system."""

from __future__ import annotations

from pydantic import BaseModel


class UploadInitRequest(BaseModel):
    """Video upload initiation request."""
    title: str
    description: str = ""
    total_chunks: int = 1


class UploadCompleteRequest(BaseModel):
    """Video upload completion request."""
    pass
