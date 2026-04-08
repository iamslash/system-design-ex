"""Object upload/download/delete service layer."""

from __future__ import annotations

from storage.data_store import DataStore
from storage.metadata import MetadataStore


class ObjectService:
    """Handles object-level operations: upload, download, delete, list."""

    def __init__(self, metadata: MetadataStore, data_store: DataStore) -> None:
        self._meta = metadata
        self._data = data_store

    async def put_object(
        self,
        bucket_name: str,
        object_name: str,
        data: bytes,
    ) -> dict:
        """Upload an object: store data in append-only file, metadata in Redis."""
        # Write data to append-only store
        object_id = self._data.put(data)

        # Record metadata
        meta = await self._meta.put_object_meta(
            bucket_name=bucket_name,
            object_name=object_name,
            object_id=object_id,
            size=len(data),
        )
        return meta

    async def get_object(
        self,
        bucket_name: str,
        object_name: str,
        version_id: str | None = None,
    ) -> tuple[dict, bytes] | None:
        """Download an object. Returns (metadata, data) or None."""
        meta = await self._meta.get_object_meta(
            bucket_name, object_name, version_id
        )
        if meta is None:
            return None

        if meta["is_delete_marker"]:
            return None

        data = self._data.get(meta["object_id"])
        if data is None:
            return None

        return meta, data

    async def delete_object(
        self,
        bucket_name: str,
        object_name: str,
    ) -> dict | None:
        """Delete an object. With versioning, creates a delete marker."""
        meta = await self._meta.delete_object_meta(bucket_name, object_name)
        return meta

    async def list_objects(
        self,
        bucket_name: str,
        prefix: str = "",
    ) -> list[dict]:
        """List objects in a bucket with optional prefix filter."""
        return await self._meta.list_objects(bucket_name, prefix)
