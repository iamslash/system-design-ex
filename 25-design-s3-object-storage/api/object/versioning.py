"""Object versioning service layer."""

from __future__ import annotations

from storage.metadata import MetadataStore


class VersioningService:
    """Handles object versioning: list versions, get specific version."""

    def __init__(self, metadata: MetadataStore) -> None:
        self._meta = metadata

    async def list_versions(
        self,
        bucket_name: str,
        object_name: str,
    ) -> list[dict]:
        """List all versions of an object (newest first)."""
        return await self._meta.list_object_versions(bucket_name, object_name)

    async def get_version_meta(
        self,
        bucket_name: str,
        object_name: str,
        version_id: str,
    ) -> dict | None:
        """Get metadata for a specific version."""
        return await self._meta.get_object_meta(
            bucket_name, object_name, version_id
        )
