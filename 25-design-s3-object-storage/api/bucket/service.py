"""Bucket CRUD service layer."""

from __future__ import annotations

from storage.metadata import MetadataStore


class BucketService:
    """Handles bucket-level operations backed by Redis metadata."""

    def __init__(self, metadata: MetadataStore) -> None:
        self._meta = metadata

    async def create_bucket(self, bucket_name: str) -> dict:
        """Create a new bucket."""
        return await self._meta.create_bucket(bucket_name)

    async def get_bucket(self, bucket_name: str) -> dict | None:
        """Get bucket info."""
        info = await self._meta.get_bucket(bucket_name)
        if info is None:
            return None
        count = await self._meta.bucket_object_count(bucket_name)
        info["object_count"] = count
        return info

    async def delete_bucket(self, bucket_name: str) -> bool:
        """Delete a bucket (must be empty)."""
        return await self._meta.delete_bucket(bucket_name)

    async def list_buckets(self) -> list[dict]:
        """List all buckets with their metadata."""
        names = await self._meta.list_buckets()
        results = []
        for name in names:
            info = await self.get_bucket(name)
            if info:
                results.append(info)
        return results

    async def set_versioning(self, bucket_name: str, enabled: bool) -> None:
        """Enable or disable versioning on a bucket."""
        await self._meta.set_versioning(bucket_name, enabled)
