"""Redis-based metadata store for buckets and objects.

Key schema:
  bucket:{name}              -> hash {versioning_enabled, created_at}
  bucket:list                -> set of bucket names
  obj:{bucket}:{key}:current -> latest version_id
  obj:{bucket}:{key}:versions -> list of version_ids (newest first)
  objmeta:{version_id}      -> hash {bucket_name, object_name, object_id,
                                      version_id, size, created_at, is_delete_marker}
  objkeys:{bucket}           -> set of object keys in the bucket
"""

from __future__ import annotations

import time
import uuid

from redis.asyncio import Redis


class MetadataStore:
    """Async Redis metadata store for S3-like object storage."""

    def __init__(self, redis: Redis) -> None:
        self._r = redis

    # ------------------------------------------------------------------
    # Bucket operations
    # ------------------------------------------------------------------

    async def create_bucket(self, name: str) -> dict:
        """Create a new bucket. Returns bucket info dict."""
        key = f"bucket:{name}"
        if await self._r.exists(key):
            raise ValueError(f"Bucket '{name}' already exists")

        now = str(time.time())
        await self._r.hset(key, mapping={
            "versioning_enabled": "0",
            "created_at": now,
        })
        await self._r.sadd("bucket:list", name)
        return {
            "bucket_name": name,
            "versioning_enabled": False,
            "created_at": now,
        }

    async def get_bucket(self, name: str) -> dict | None:
        """Get bucket info or None if not found."""
        key = f"bucket:{name}"
        data = await self._r.hgetall(key)
        if not data:
            return None
        return {
            "bucket_name": name,
            "versioning_enabled": data["versioning_enabled"] == "1",
            "created_at": data["created_at"],
        }

    async def delete_bucket(self, name: str) -> bool:
        """Delete a bucket. Returns False if not found."""
        key = f"bucket:{name}"
        if not await self._r.exists(key):
            return False

        # Check the bucket is empty
        obj_keys = await self._r.smembers(f"objkeys:{name}")
        if obj_keys:
            raise ValueError(f"Bucket '{name}' is not empty")

        await self._r.delete(key)
        await self._r.srem("bucket:list", name)
        return True

    async def list_buckets(self) -> list[str]:
        """Return all bucket names."""
        return sorted(await self._r.smembers("bucket:list"))

    async def set_versioning(self, bucket_name: str, enabled: bool) -> None:
        """Enable or disable versioning on a bucket."""
        key = f"bucket:{bucket_name}"
        if not await self._r.exists(key):
            raise ValueError(f"Bucket '{bucket_name}' not found")
        await self._r.hset(key, "versioning_enabled", "1" if enabled else "0")

    async def bucket_object_count(self, bucket_name: str) -> int:
        """Return the number of object keys in the bucket."""
        return await self._r.scard(f"objkeys:{bucket_name}")

    # ------------------------------------------------------------------
    # Object metadata operations
    # ------------------------------------------------------------------

    async def put_object_meta(
        self,
        bucket_name: str,
        object_name: str,
        object_id: str,
        size: int,
    ) -> dict:
        """Record metadata for a new object version.

        If versioning is enabled, the previous version is preserved.
        If versioning is disabled, the previous version metadata is removed.
        """
        bucket = await self.get_bucket(bucket_name)
        if bucket is None:
            raise ValueError(f"Bucket '{bucket_name}' not found")

        version_id = uuid.uuid4().hex
        now = str(time.time())

        meta = {
            "bucket_name": bucket_name,
            "object_name": object_name,
            "object_id": object_id,
            "version_id": version_id,
            "size": str(size),
            "created_at": now,
            "is_delete_marker": "0",
        }

        versioning = bucket["versioning_enabled"]

        if not versioning:
            # Remove previous version metadata if it exists
            prev_version = await self._r.get(
                f"obj:{bucket_name}:{object_name}:current"
            )
            if prev_version:
                await self._r.delete(f"objmeta:{prev_version}")
                await self._r.delete(f"obj:{bucket_name}:{object_name}:versions")

        # Store the version metadata
        await self._r.hset(f"objmeta:{version_id}", mapping=meta)

        # Update current pointer
        await self._r.set(
            f"obj:{bucket_name}:{object_name}:current", version_id
        )

        # Push to version list (newest first)
        await self._r.lpush(
            f"obj:{bucket_name}:{object_name}:versions", version_id
        )

        # Track the object key in the bucket
        await self._r.sadd(f"objkeys:{bucket_name}", object_name)

        return meta

    async def get_object_meta(
        self,
        bucket_name: str,
        object_name: str,
        version_id: str | None = None,
    ) -> dict | None:
        """Get metadata for an object (latest or specific version)."""
        if version_id is None:
            version_id = await self._r.get(
                f"obj:{bucket_name}:{object_name}:current"
            )
            if version_id is None:
                return None

        data = await self._r.hgetall(f"objmeta:{version_id}")
        if not data:
            return None

        return {
            "bucket_name": data["bucket_name"],
            "object_name": data["object_name"],
            "object_id": data["object_id"],
            "version_id": data["version_id"],
            "size": int(data["size"]),
            "created_at": data["created_at"],
            "is_delete_marker": data["is_delete_marker"] == "1",
        }

    async def delete_object_meta(
        self,
        bucket_name: str,
        object_name: str,
    ) -> dict | None:
        """Delete an object.

        With versioning: inserts a delete marker as the current version.
        Without versioning: removes all metadata for the object.

        Returns the delete marker meta or the removed meta.
        """
        bucket = await self.get_bucket(bucket_name)
        if bucket is None:
            raise ValueError(f"Bucket '{bucket_name}' not found")

        current_vid = await self._r.get(
            f"obj:{bucket_name}:{object_name}:current"
        )
        if current_vid is None:
            return None

        versioning = bucket["versioning_enabled"]

        if versioning:
            # Create a delete marker
            version_id = uuid.uuid4().hex
            now = str(time.time())
            meta = {
                "bucket_name": bucket_name,
                "object_name": object_name,
                "object_id": "",
                "version_id": version_id,
                "size": "0",
                "created_at": now,
                "is_delete_marker": "1",
            }
            await self._r.hset(f"objmeta:{version_id}", mapping=meta)
            await self._r.set(
                f"obj:{bucket_name}:{object_name}:current", version_id
            )
            await self._r.lpush(
                f"obj:{bucket_name}:{object_name}:versions", version_id
            )
            return {
                **meta,
                "size": 0,
                "is_delete_marker": True,
            }
        else:
            # Hard delete: remove all metadata
            meta = await self.get_object_meta(bucket_name, object_name)
            await self._r.delete(f"objmeta:{current_vid}")
            await self._r.delete(f"obj:{bucket_name}:{object_name}:current")
            await self._r.delete(f"obj:{bucket_name}:{object_name}:versions")
            await self._r.srem(f"objkeys:{bucket_name}", object_name)
            return meta

    async def list_objects(
        self,
        bucket_name: str,
        prefix: str = "",
    ) -> list[dict]:
        """List current (non-deleted) objects in a bucket with optional prefix."""
        all_keys = sorted(await self._r.smembers(f"objkeys:{bucket_name}"))
        results = []
        for obj_key in all_keys:
            if prefix and not obj_key.startswith(prefix):
                continue
            meta = await self.get_object_meta(bucket_name, obj_key)
            if meta and not meta["is_delete_marker"]:
                results.append(meta)
        return results

    async def list_object_versions(
        self,
        bucket_name: str,
        object_name: str,
    ) -> list[dict]:
        """List all versions of an object (newest first)."""
        version_ids = await self._r.lrange(
            f"obj:{bucket_name}:{object_name}:versions", 0, -1
        )
        results = []
        for vid in version_ids:
            data = await self._r.hgetall(f"objmeta:{vid}")
            if data:
                results.append({
                    "bucket_name": data["bucket_name"],
                    "object_name": data["object_name"],
                    "object_id": data["object_id"],
                    "version_id": data["version_id"],
                    "size": int(data["size"]),
                    "created_at": data["created_at"],
                    "is_delete_marker": data["is_delete_marker"] == "1",
                })
        return results
