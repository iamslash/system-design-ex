"""Pydantic models for S3-like object storage API."""

from __future__ import annotations

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Bucket models
# ---------------------------------------------------------------------------


class BucketCreate(BaseModel):
    """Request body for creating a bucket."""

    bucket_name: str


class BucketInfo(BaseModel):
    """Bucket metadata returned in API responses."""

    bucket_name: str
    versioning_enabled: bool
    created_at: str
    object_count: int


class BucketListResponse(BaseModel):
    """Response for listing all buckets."""

    buckets: list[BucketInfo]


# ---------------------------------------------------------------------------
# Object models
# ---------------------------------------------------------------------------


class ObjectInfo(BaseModel):
    """Object metadata returned in API responses."""

    bucket_name: str
    object_name: str
    object_id: str
    version_id: str
    size: int
    created_at: str
    is_delete_marker: bool = False


class ObjectListResponse(BaseModel):
    """Response for listing objects in a bucket."""

    bucket_name: str
    prefix: str
    objects: list[ObjectInfo]


class ObjectVersionListResponse(BaseModel):
    """Response for listing object versions."""

    bucket_name: str
    object_name: str
    versions: list[ObjectInfo]


class VersioningRequest(BaseModel):
    """Request body for enabling/disabling versioning."""

    enabled: bool
