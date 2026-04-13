"""Tests for S3-like object storage components."""

from __future__ import annotations

import os
import sys

import fakeredis.aioredis
import pytest

# Add the api directory to the path so we can import modules.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

from storage.data_store import DataStore, ObjectLocation
from storage.metadata import MetadataStore
from bucket.service import BucketService
from object.service import ObjectService
from object.versioning import VersioningService


# ===========================================================================
# DataStore (append-only file storage) tests
# ===========================================================================


class TestDataStorePut:
    """Tests for DataStore.put()."""

    def test_put_returns_object_id(self, tmp_data_dir: str) -> None:
        """put() should return a unique object_id string."""
        ds = DataStore(data_dir=tmp_data_dir)
        oid = ds.put(b"hello world")
        assert isinstance(oid, str)
        assert len(oid) == 32  # uuid hex

    def test_put_multiple_same_file(self, tmp_data_dir: str) -> None:
        """Multiple small puts should go into the same data file."""
        ds = DataStore(data_dir=tmp_data_dir)
        oid1 = ds.put(b"aaa")
        oid2 = ds.put(b"bbb")
        loc1 = ds.get_location(oid1)
        loc2 = ds.get_location(oid2)
        assert loc1.file_name == loc2.file_name
        assert loc2.offset == loc1.offset + loc1.size

    def test_put_increments_object_count(self, tmp_data_dir: str) -> None:
        """object_count should reflect the number of live objects."""
        ds = DataStore(data_dir=tmp_data_dir)
        assert ds.object_count == 0
        ds.put(b"a")
        ds.put(b"b")
        assert ds.object_count == 2


class TestDataStoreGet:
    """Tests for DataStore.get()."""

    def test_get_returns_correct_data(self, tmp_data_dir: str) -> None:
        """get() should return the exact data that was put()."""
        ds = DataStore(data_dir=tmp_data_dir)
        oid = ds.put(b"hello world")
        assert ds.get(oid) == b"hello world"

    def test_get_nonexistent_returns_none(self, tmp_data_dir: str) -> None:
        """get() with unknown object_id should return None."""
        ds = DataStore(data_dir=tmp_data_dir)
        assert ds.get("nonexistent_id") is None

    def test_get_multiple_objects_isolation(self, tmp_data_dir: str) -> None:
        """Each object should return its own data, not neighbors'."""
        ds = DataStore(data_dir=tmp_data_dir)
        oid1 = ds.put(b"first")
        oid2 = ds.put(b"second")
        oid3 = ds.put(b"third")
        assert ds.get(oid1) == b"first"
        assert ds.get(oid2) == b"second"
        assert ds.get(oid3) == b"third"


class TestDataStoreDelete:
    """Tests for DataStore.delete()."""

    def test_delete_existing(self, tmp_data_dir: str) -> None:
        """delete() should remove the object from the mapping."""
        ds = DataStore(data_dir=tmp_data_dir)
        oid = ds.put(b"data")
        assert ds.delete(oid) is True
        assert ds.get(oid) is None

    def test_delete_nonexistent(self, tmp_data_dir: str) -> None:
        """delete() on unknown object_id should return False."""
        ds = DataStore(data_dir=tmp_data_dir)
        assert ds.delete("unknown") is False

    def test_delete_decrements_count(self, tmp_data_dir: str) -> None:
        """Deleting should decrease the object count."""
        ds = DataStore(data_dir=tmp_data_dir)
        oid = ds.put(b"x")
        assert ds.object_count == 1
        ds.delete(oid)
        assert ds.object_count == 0


class TestDataStoreAppendOnly:
    """Tests verifying the append-only file behavior."""

    def test_data_file_grows_monotonically(self, tmp_data_dir: str) -> None:
        """The data file size should only grow with each put()."""
        ds = DataStore(data_dir=tmp_data_dir)
        oid1 = ds.put(b"aaaa")
        loc1 = ds.get_location(oid1)
        size_after_first = os.path.getsize(loc1.file_name)

        ds.put(b"bbbb")
        size_after_second = os.path.getsize(loc1.file_name)
        assert size_after_second > size_after_first

    def test_file_rotation_on_max_size(self, tmp_data_dir: str) -> None:
        """When file exceeds max size, a new file should be created."""
        ds = DataStore(data_dir=tmp_data_dir, max_file_size=10)
        oid1 = ds.put(b"12345")
        oid2 = ds.put(b"67890123456")  # exceeds 10 bytes, triggers rotation
        loc1 = ds.get_location(oid1)
        loc2 = ds.get_location(oid2)
        assert loc1.file_name != loc2.file_name

    def test_objects_readable_after_rotation(self, tmp_data_dir: str) -> None:
        """Objects in the old file should still be readable after rotation."""
        ds = DataStore(data_dir=tmp_data_dir, max_file_size=10)
        oid1 = ds.put(b"old-data")
        oid2 = ds.put(b"new-data-after-rotation")
        assert ds.get(oid1) == b"old-data"
        assert ds.get(oid2) == b"new-data-after-rotation"


class TestDataStoreObjectMapping:
    """Tests for the object_mapping lookup table."""

    def test_get_location(self, tmp_data_dir: str) -> None:
        """get_location should return an ObjectLocation with correct fields."""
        ds = DataStore(data_dir=tmp_data_dir)
        oid = ds.put(b"test-data")
        loc = ds.get_location(oid)
        assert isinstance(loc, ObjectLocation)
        assert loc.size == len(b"test-data")
        assert loc.offset == 0

    def test_object_mapping_property(self, tmp_data_dir: str) -> None:
        """object_mapping should return a dict of all live objects."""
        ds = DataStore(data_dir=tmp_data_dir)
        oid1 = ds.put(b"a")
        oid2 = ds.put(b"b")
        mapping = ds.object_mapping
        assert oid1 in mapping
        assert oid2 in mapping
        assert len(mapping) == 2

    def test_exists(self, tmp_data_dir: str) -> None:
        """exists() should return True for live objects."""
        ds = DataStore(data_dir=tmp_data_dir)
        oid = ds.put(b"data")
        assert ds.exists(oid) is True
        assert ds.exists("nonexistent") is False


class TestDataStoreCompaction:
    """Tests for data store compaction (GC)."""

    def test_compact_reclaims_space(self, tmp_data_dir: str) -> None:
        """Compaction should reclaim space from deleted objects."""
        ds = DataStore(data_dir=tmp_data_dir)
        oid1 = ds.put(b"keep-this")
        oid2 = ds.put(b"delete-this-data")
        ds.delete(oid2)

        reclaimed = ds.compact()
        assert reclaimed > 0
        assert ds.get(oid1) == b"keep-this"

    def test_compact_no_dead_data(self, tmp_data_dir: str) -> None:
        """Compaction with no dead data should reclaim 0 bytes."""
        ds = DataStore(data_dir=tmp_data_dir)
        ds.put(b"live-data")
        reclaimed = ds.compact()
        assert reclaimed == 0


# ===========================================================================
# MetadataStore (Redis) tests
# ===========================================================================


class TestMetadataStoreBucket:
    """Tests for bucket metadata operations."""

    @pytest.mark.asyncio
    async def test_create_bucket(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Creating a bucket should return its info."""
        meta = MetadataStore(redis_client)
        info = await meta.create_bucket("test-bucket")
        assert info["bucket_name"] == "test-bucket"
        assert info["versioning_enabled"] is False

    @pytest.mark.asyncio
    async def test_create_duplicate_bucket_raises(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Creating a bucket that already exists should raise ValueError."""
        meta = MetadataStore(redis_client)
        await meta.create_bucket("dup")
        with pytest.raises(ValueError, match="already exists"):
            await meta.create_bucket("dup")

    @pytest.mark.asyncio
    async def test_get_bucket(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """get_bucket should return the bucket info."""
        meta = MetadataStore(redis_client)
        await meta.create_bucket("mybucket")
        info = await meta.get_bucket("mybucket")
        assert info is not None
        assert info["bucket_name"] == "mybucket"

    @pytest.mark.asyncio
    async def test_get_nonexistent_bucket(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """get_bucket for unknown name should return None."""
        meta = MetadataStore(redis_client)
        assert await meta.get_bucket("nope") is None

    @pytest.mark.asyncio
    async def test_delete_bucket(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """delete_bucket should remove the bucket."""
        meta = MetadataStore(redis_client)
        await meta.create_bucket("del-me")
        assert await meta.delete_bucket("del-me") is True
        assert await meta.get_bucket("del-me") is None

    @pytest.mark.asyncio
    async def test_delete_nonempty_bucket_raises(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Deleting a non-empty bucket should raise ValueError."""
        meta = MetadataStore(redis_client)
        await meta.create_bucket("notempty")
        await meta.put_object_meta("notempty", "file.txt", "oid1", 100)
        with pytest.raises(ValueError, match="not empty"):
            await meta.delete_bucket("notempty")

    @pytest.mark.asyncio
    async def test_list_buckets(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """list_buckets should return all bucket names sorted."""
        meta = MetadataStore(redis_client)
        await meta.create_bucket("beta")
        await meta.create_bucket("alpha")
        names = await meta.list_buckets()
        assert names == ["alpha", "beta"]

    @pytest.mark.asyncio
    async def test_set_versioning(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """set_versioning should toggle the versioning flag."""
        meta = MetadataStore(redis_client)
        await meta.create_bucket("vbucket")
        info = await meta.get_bucket("vbucket")
        assert info["versioning_enabled"] is False

        await meta.set_versioning("vbucket", True)
        info = await meta.get_bucket("vbucket")
        assert info["versioning_enabled"] is True


# ===========================================================================
# Object upload / download / delete tests
# ===========================================================================


class TestObjectService:
    """Tests for ObjectService (upload, download, delete, list)."""

    @pytest.mark.asyncio
    async def test_put_and_get_object(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_data_dir: str) -> None:
        """Upload then download should return the same data."""
        meta = MetadataStore(redis_client)
        ds = DataStore(data_dir=tmp_data_dir)
        svc = ObjectService(meta, ds)
        await meta.create_bucket("b1")

        result = await svc.put_object("b1", "hello.txt", b"Hello World")
        assert result["object_name"] == "hello.txt"
        assert int(result["size"]) == 11

        got = await svc.get_object("b1", "hello.txt")
        assert got is not None
        obj_meta, data = got
        assert data == b"Hello World"

    @pytest.mark.asyncio
    async def test_get_nonexistent_object(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_data_dir: str) -> None:
        """Getting a non-existent object should return None."""
        meta = MetadataStore(redis_client)
        ds = DataStore(data_dir=tmp_data_dir)
        svc = ObjectService(meta, ds)
        await meta.create_bucket("b1")
        assert await svc.get_object("b1", "nope.txt") is None

    @pytest.mark.asyncio
    async def test_delete_object_without_versioning(
        self, redis_client: fakeredis.aioredis.FakeRedis, tmp_data_dir: str
    ) -> None:
        """Delete without versioning should hard-delete the object."""
        meta = MetadataStore(redis_client)
        ds = DataStore(data_dir=tmp_data_dir)
        svc = ObjectService(meta, ds)
        await meta.create_bucket("b1")
        await svc.put_object("b1", "file.txt", b"data")

        result = await svc.delete_object("b1", "file.txt")
        assert result is not None
        assert await svc.get_object("b1", "file.txt") is None

    @pytest.mark.asyncio
    async def test_overwrite_object(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_data_dir: str) -> None:
        """Uploading the same key should overwrite without versioning."""
        meta = MetadataStore(redis_client)
        ds = DataStore(data_dir=tmp_data_dir)
        svc = ObjectService(meta, ds)
        await meta.create_bucket("b1")

        await svc.put_object("b1", "f.txt", b"v1")
        await svc.put_object("b1", "f.txt", b"v2-updated")

        got = await svc.get_object("b1", "f.txt")
        assert got is not None
        _, data = got
        assert data == b"v2-updated"


# ===========================================================================
# Prefix listing tests
# ===========================================================================


class TestObjectListing:
    """Tests for listing objects with prefix filter."""

    @pytest.mark.asyncio
    async def test_list_objects_all(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_data_dir: str) -> None:
        """list_objects without prefix should return all objects."""
        meta = MetadataStore(redis_client)
        ds = DataStore(data_dir=tmp_data_dir)
        svc = ObjectService(meta, ds)
        await meta.create_bucket("b1")

        await svc.put_object("b1", "a.txt", b"a")
        await svc.put_object("b1", "b.txt", b"b")
        await svc.put_object("b1", "c.txt", b"c")

        objects = await svc.list_objects("b1")
        assert len(objects) == 3

    @pytest.mark.asyncio
    async def test_list_objects_with_prefix(
        self, redis_client: fakeredis.aioredis.FakeRedis, tmp_data_dir: str
    ) -> None:
        """list_objects with prefix should filter correctly."""
        meta = MetadataStore(redis_client)
        ds = DataStore(data_dir=tmp_data_dir)
        svc = ObjectService(meta, ds)
        await meta.create_bucket("b1")

        await svc.put_object("b1", "photos/cat.jpg", b"cat")
        await svc.put_object("b1", "photos/dog.jpg", b"dog")
        await svc.put_object("b1", "docs/readme.md", b"readme")

        photos = await svc.list_objects("b1", prefix="photos/")
        assert len(photos) == 2
        names = {o["object_name"] for o in photos}
        assert names == {"photos/cat.jpg", "photos/dog.jpg"}

    @pytest.mark.asyncio
    async def test_list_objects_empty_result(
        self, redis_client: fakeredis.aioredis.FakeRedis, tmp_data_dir: str
    ) -> None:
        """list_objects with non-matching prefix should return empty."""
        meta = MetadataStore(redis_client)
        ds = DataStore(data_dir=tmp_data_dir)
        svc = ObjectService(meta, ds)
        await meta.create_bucket("b1")
        await svc.put_object("b1", "file.txt", b"data")

        result = await svc.list_objects("b1", prefix="nope/")
        assert len(result) == 0


# ===========================================================================
# Versioning tests
# ===========================================================================


class TestVersioning:
    """Tests for object versioning with delete markers."""

    @pytest.mark.asyncio
    async def test_versioning_multiple_versions(
        self, redis_client: fakeredis.aioredis.FakeRedis, tmp_data_dir: str
    ) -> None:
        """With versioning enabled, multiple uploads create multiple versions."""
        meta = MetadataStore(redis_client)
        ds = DataStore(data_dir=tmp_data_dir)
        obj_svc = ObjectService(meta, ds)
        ver_svc = VersioningService(meta)

        await meta.create_bucket("vb")
        await meta.set_versioning("vb", True)

        await obj_svc.put_object("vb", "doc.txt", b"version-1")
        await obj_svc.put_object("vb", "doc.txt", b"version-2")
        await obj_svc.put_object("vb", "doc.txt", b"version-3")

        versions = await ver_svc.list_versions("vb", "doc.txt")
        assert len(versions) == 3
        # Newest first
        assert versions[0]["size"] == len(b"version-3")

    @pytest.mark.asyncio
    async def test_versioning_get_specific_version(
        self, redis_client: fakeredis.aioredis.FakeRedis, tmp_data_dir: str
    ) -> None:
        """Should be able to download a specific version by version_id."""
        meta = MetadataStore(redis_client)
        ds = DataStore(data_dir=tmp_data_dir)
        obj_svc = ObjectService(meta, ds)
        ver_svc = VersioningService(meta)

        await meta.create_bucket("vb")
        await meta.set_versioning("vb", True)

        m1 = await obj_svc.put_object("vb", "f.txt", b"v1-content")
        m2 = await obj_svc.put_object("vb", "f.txt", b"v2-content")

        # Get old version
        got = await obj_svc.get_object("vb", "f.txt", m1["version_id"])
        assert got is not None
        _, data = got
        assert data == b"v1-content"

        # Get latest version
        got = await obj_svc.get_object("vb", "f.txt", m2["version_id"])
        assert got is not None
        _, data = got
        assert data == b"v2-content"

    @pytest.mark.asyncio
    async def test_versioning_delete_marker(
        self, redis_client: fakeredis.aioredis.FakeRedis, tmp_data_dir: str
    ) -> None:
        """Deleting with versioning should create a delete marker."""
        meta = MetadataStore(redis_client)
        ds = DataStore(data_dir=tmp_data_dir)
        obj_svc = ObjectService(meta, ds)
        ver_svc = VersioningService(meta)

        await meta.create_bucket("vb")
        await meta.set_versioning("vb", True)

        m1 = await obj_svc.put_object("vb", "f.txt", b"content")
        result = await obj_svc.delete_object("vb", "f.txt")
        assert result["is_delete_marker"] is True

        # GET latest should return None (delete marker)
        assert await obj_svc.get_object("vb", "f.txt") is None

        # But the old version is still accessible
        got = await obj_svc.get_object("vb", "f.txt", m1["version_id"])
        assert got is not None
        _, data = got
        assert data == b"content"

    @pytest.mark.asyncio
    async def test_versioning_delete_marker_in_version_list(
        self, redis_client: fakeredis.aioredis.FakeRedis, tmp_data_dir: str
    ) -> None:
        """Delete markers should appear in version listings."""
        meta = MetadataStore(redis_client)
        ds = DataStore(data_dir=tmp_data_dir)
        obj_svc = ObjectService(meta, ds)
        ver_svc = VersioningService(meta)

        await meta.create_bucket("vb")
        await meta.set_versioning("vb", True)

        await obj_svc.put_object("vb", "f.txt", b"v1")
        await obj_svc.delete_object("vb", "f.txt")

        versions = await ver_svc.list_versions("vb", "f.txt")
        assert len(versions) == 2
        assert versions[0]["is_delete_marker"] is True
        assert versions[1]["is_delete_marker"] is False

    @pytest.mark.asyncio
    async def test_versioning_list_excludes_deleted(
        self, redis_client: fakeredis.aioredis.FakeRedis, tmp_data_dir: str
    ) -> None:
        """list_objects should not include objects with delete markers as current."""
        meta = MetadataStore(redis_client)
        ds = DataStore(data_dir=tmp_data_dir)
        obj_svc = ObjectService(meta, ds)

        await meta.create_bucket("vb")
        await meta.set_versioning("vb", True)

        await obj_svc.put_object("vb", "keep.txt", b"keep")
        await obj_svc.put_object("vb", "del.txt", b"delete-me")
        await obj_svc.delete_object("vb", "del.txt")

        objects = await obj_svc.list_objects("vb")
        names = [o["object_name"] for o in objects]
        assert "keep.txt" in names
        assert "del.txt" not in names


# ===========================================================================
# BucketService integration tests
# ===========================================================================


class TestBucketService:
    """Integration tests for BucketService."""

    @pytest.mark.asyncio
    async def test_create_and_list(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Creating buckets and listing should work end-to-end."""
        meta = MetadataStore(redis_client)
        svc = BucketService(meta)

        await svc.create_bucket("bucket-a")
        await svc.create_bucket("bucket-b")

        buckets = await svc.list_buckets()
        names = [b["bucket_name"] for b in buckets]
        assert "bucket-a" in names
        assert "bucket-b" in names

    @pytest.mark.asyncio
    async def test_delete_bucket(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Deleting a bucket should remove it from the list."""
        meta = MetadataStore(redis_client)
        svc = BucketService(meta)

        await svc.create_bucket("temp")
        assert await svc.delete_bucket("temp") is True
        buckets = await svc.list_buckets()
        assert len(buckets) == 0

    @pytest.mark.asyncio
    async def test_get_bucket_with_count(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """get_bucket should include the object count."""
        meta = MetadataStore(redis_client)
        svc = BucketService(meta)

        await svc.create_bucket("counted")
        await meta.put_object_meta("counted", "f1.txt", "oid1", 10)
        await meta.put_object_meta("counted", "f2.txt", "oid2", 20)

        info = await svc.get_bucket("counted")
        assert info["object_count"] == 2
