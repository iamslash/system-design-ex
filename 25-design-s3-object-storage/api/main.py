"""FastAPI application for S3-like object storage service."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, UploadFile, File, Query
from fastapi.responses import JSONResponse, Response
from redis.asyncio import Redis

from config import settings
from models import (
    BucketCreate,
    BucketInfo,
    BucketListResponse,
    ObjectInfo,
    ObjectListResponse,
    ObjectVersionListResponse,
    VersioningRequest,
)
from storage.data_store import DataStore
from storage.metadata import MetadataStore
from bucket.service import BucketService
from object.service import ObjectService
from object.versioning import VersioningService


# ---------------------------------------------------------------------------
# Global state & lifespan
# ---------------------------------------------------------------------------

redis_client: Redis | None = None
data_store: DataStore | None = None
bucket_svc: BucketService | None = None
object_svc: ObjectService | None = None
versioning_svc: VersioningService | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage Redis connection and data store lifecycle."""
    global redis_client, data_store, bucket_svc, object_svc, versioning_svc

    redis_client = Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        decode_responses=True,
    )
    data_store = DataStore(
        data_dir=settings.DATA_DIR,
        max_file_size=settings.MAX_FILE_SIZE,
    )
    meta = MetadataStore(redis_client)
    bucket_svc = BucketService(meta)
    object_svc = ObjectService(meta, data_store)
    versioning_svc = VersioningService(meta)

    yield

    await redis_client.aclose()


app = FastAPI(
    title="S3-like Object Storage",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Bucket endpoints
# ---------------------------------------------------------------------------


@app.post("/api/v1/buckets", response_model=BucketInfo, status_code=201)
async def create_bucket(body: BucketCreate) -> BucketInfo:
    """Create a new bucket."""
    try:
        info = await bucket_svc.create_bucket(body.bucket_name)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))

    created_dt = datetime.fromtimestamp(float(info["created_at"]), tz=timezone.utc)
    return BucketInfo(
        bucket_name=info["bucket_name"],
        versioning_enabled=info["versioning_enabled"],
        created_at=created_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
        object_count=0,
    )


@app.get("/api/v1/buckets", response_model=BucketListResponse)
async def list_buckets() -> BucketListResponse:
    """List all buckets."""
    buckets = await bucket_svc.list_buckets()
    items = []
    for b in buckets:
        created_dt = datetime.fromtimestamp(
            float(b["created_at"]), tz=timezone.utc
        )
        items.append(BucketInfo(
            bucket_name=b["bucket_name"],
            versioning_enabled=b["versioning_enabled"],
            created_at=created_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
            object_count=b.get("object_count", 0),
        ))
    return BucketListResponse(buckets=items)


@app.delete("/api/v1/buckets/{bucket_name}", status_code=204)
async def delete_bucket(bucket_name: str) -> Response:
    """Delete a bucket (must be empty)."""
    try:
        deleted = await bucket_svc.delete_bucket(bucket_name)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))

    if not deleted:
        raise HTTPException(status_code=404, detail="Bucket not found")
    return Response(status_code=204)


@app.put("/api/v1/buckets/{bucket_name}/versioning")
async def set_versioning(
    bucket_name: str,
    body: VersioningRequest,
) -> dict[str, str]:
    """Enable or disable versioning on a bucket."""
    try:
        await bucket_svc.set_versioning(bucket_name, body.enabled)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    status = "Enabled" if body.enabled else "Suspended"
    return {"status": status}


# ---------------------------------------------------------------------------
# Object endpoints
# ---------------------------------------------------------------------------


@app.put("/api/v1/buckets/{bucket_name}/objects/{object_name:path}")
async def put_object(
    bucket_name: str,
    object_name: str,
    file: UploadFile = File(...),
) -> ObjectInfo:
    """Upload an object to a bucket."""
    data = await file.read()
    try:
        meta = await object_svc.put_object(bucket_name, object_name, data)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    created_dt = datetime.fromtimestamp(
        float(meta["created_at"]), tz=timezone.utc
    )
    return ObjectInfo(
        bucket_name=meta["bucket_name"],
        object_name=meta["object_name"],
        object_id=meta["object_id"],
        version_id=meta["version_id"],
        size=int(meta["size"]),
        created_at=created_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
        is_delete_marker=meta["is_delete_marker"] == "1"
        if isinstance(meta["is_delete_marker"], str)
        else meta["is_delete_marker"],
    )


@app.get("/api/v1/buckets/{bucket_name}/objects/{object_name:path}")
async def get_object(
    bucket_name: str,
    object_name: str,
    version_id: str | None = Query(default=None),
) -> Response:
    """Download an object from a bucket."""
    result = await object_svc.get_object(bucket_name, object_name, version_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Object not found")

    meta, data = result
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={
            "X-Object-Version-Id": meta["version_id"],
            "X-Object-Size": str(meta["size"]),
        },
    )


@app.delete("/api/v1/buckets/{bucket_name}/objects/{object_name:path}")
async def delete_object(bucket_name: str, object_name: str) -> dict:
    """Delete an object. With versioning enabled, creates a delete marker."""
    try:
        meta = await object_svc.delete_object(bucket_name, object_name)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if meta is None:
        raise HTTPException(status_code=404, detail="Object not found")
    return {"deleted": True, "version_id": meta.get("version_id", "")}


@app.get(
    "/api/v1/buckets/{bucket_name}/objects",
    response_model=ObjectListResponse,
)
async def list_objects(
    bucket_name: str,
    prefix: str = Query(default=""),
) -> ObjectListResponse:
    """List objects in a bucket with optional prefix filter."""
    objects = await object_svc.list_objects(bucket_name, prefix)
    items = []
    for o in objects:
        created_dt = datetime.fromtimestamp(
            float(o["created_at"]), tz=timezone.utc
        )
        items.append(ObjectInfo(
            bucket_name=o["bucket_name"],
            object_name=o["object_name"],
            object_id=o["object_id"],
            version_id=o["version_id"],
            size=o["size"],
            created_at=created_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
            is_delete_marker=o.get("is_delete_marker", False),
        ))
    return ObjectListResponse(
        bucket_name=bucket_name,
        prefix=prefix,
        objects=items,
    )


# ---------------------------------------------------------------------------
# Versioning endpoints
# ---------------------------------------------------------------------------


@app.get(
    "/api/v1/buckets/{bucket_name}/objects/{object_name:path}/versions",
    response_model=ObjectVersionListResponse,
)
async def list_versions(
    bucket_name: str,
    object_name: str,
) -> ObjectVersionListResponse:
    """List all versions of an object."""
    versions = await versioning_svc.list_versions(bucket_name, object_name)
    items = []
    for v in versions:
        created_dt = datetime.fromtimestamp(
            float(v["created_at"]), tz=timezone.utc
        )
        items.append(ObjectInfo(
            bucket_name=v["bucket_name"],
            object_name=v["object_name"],
            object_id=v["object_id"],
            version_id=v["version_id"],
            size=v["size"],
            created_at=created_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
            is_delete_marker=v.get("is_delete_marker", False),
        ))
    return ObjectVersionListResponse(
        bucket_name=bucket_name,
        object_name=object_name,
        versions=items,
    )
