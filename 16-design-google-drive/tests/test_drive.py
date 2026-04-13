"""Tests for the Google Drive file sync service.

Uses fakeredis to run unit tests without a Redis dependency.
"""

from __future__ import annotations

import json
import os
import pathlib
import sys
import tempfile
from collections.abc import AsyncGenerator

import fakeredis.aioredis
import pytest
import pytest_asyncio

# Add the api directory to the import path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

from storage.block_server import (
    compress_block,
    compute_block_hash,
    decompress_block,
    load_block,
    split_into_blocks,
    store_block,
)
from storage.file_manager import (
    _find_file_id,
    delete_file,
    download_file,
    list_files,
    upload_file,
)
from storage.versioning import create_version, get_revisions, get_version, restore_version
from sync.conflict import check_conflict, resolve_conflict_first_writer_wins
from sync.notification import poll_sync_events, publish_sync_event


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def redis_client() -> AsyncGenerator[fakeredis.aioredis.FakeRedis, None]:
    """Create a fake async Redis client for testing."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.flushall()
    await client.aclose()


@pytest.fixture
def tmp_storage(tmp_path: pathlib.Path) -> str:
    """Create a temporary directory for block storage."""
    return str(tmp_path / "blocks")


# ---------------------------------------------------------------------------
# Block Server: Splitting
# ---------------------------------------------------------------------------


class TestBlockSplitting:
    """Tests for splitting files into blocks."""

    def test_split_exact_blocks(self) -> None:
        """File splits correctly when its size is an exact multiple of the block size."""
        data = b"A" * 8192
        blocks = split_into_blocks(data, block_size=4096)
        assert len(blocks) == 2
        assert all(len(b) == 4096 for b in blocks)

    def test_split_with_remainder(self) -> None:
        """The last block can be smaller than the block size."""
        data = b"B" * 5000
        blocks = split_into_blocks(data, block_size=4096)
        assert len(blocks) == 2
        assert len(blocks[0]) == 4096
        assert len(blocks[1]) == 904

    def test_split_small_file(self) -> None:
        """A file smaller than the block size becomes a single block."""
        data = b"C" * 100
        blocks = split_into_blocks(data, block_size=4096)
        assert len(blocks) == 1
        assert blocks[0] == data

    def test_split_empty_file(self) -> None:
        """An empty file returns an empty block list."""
        data = b""
        blocks = split_into_blocks(data, block_size=4096)
        assert len(blocks) == 0

    def test_reconstruction_from_blocks(self) -> None:
        """Joining split blocks produces data identical to the original."""
        data = os.urandom(10000)
        blocks = split_into_blocks(data, block_size=4096)
        reconstructed = b"".join(blocks)
        assert reconstructed == data


# ---------------------------------------------------------------------------
# Block Server: Hashing & Dedup
# ---------------------------------------------------------------------------


class TestBlockHashing:
    """Tests for block hashing and deduplication."""

    def test_same_content_same_hash(self) -> None:
        """Identical content always returns the same hash."""
        block = b"Hello, World!"
        hash1 = compute_block_hash(block)
        hash2 = compute_block_hash(block)
        assert hash1 == hash2

    def test_different_content_different_hash(self) -> None:
        """Different content returns different hashes."""
        hash1 = compute_block_hash(b"Hello")
        hash2 = compute_block_hash(b"World")
        assert hash1 != hash2

    def test_hash_is_sha256_hex(self) -> None:
        """Hash is a 64-character hexadecimal string (SHA-256)."""
        h = compute_block_hash(b"test")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)


# ---------------------------------------------------------------------------
# Block Server: Compression
# ---------------------------------------------------------------------------


class TestBlockCompression:
    """Tests for block compression."""

    def test_compress_decompress_roundtrip(self) -> None:
        """Compressing then decompressing produces data identical to the original."""
        original = b"Hello " * 1000
        compressed = compress_block(original)
        decompressed = decompress_block(compressed)
        assert decompressed == original

    def test_compression_reduces_size(self) -> None:
        """Compressing repetitive data reduces its size."""
        original = b"AAAA" * 4096
        compressed = compress_block(original)
        assert len(compressed) < len(original)


# ---------------------------------------------------------------------------
# Block Server: Store & Load
# ---------------------------------------------------------------------------


class TestBlockStorage:
    """Tests for block storage and loading."""

    @pytest.mark.asyncio
    async def test_store_new_block(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """A new block is stored and returns is_new=True."""
        block = b"new block data"
        block_hash, is_new = await store_block(redis_client, block, tmp_storage)
        assert is_new is True
        assert len(block_hash) == 64

    @pytest.mark.asyncio
    async def test_store_duplicate_block(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """Storing the same block twice returns is_new=False on the second call."""
        block = b"duplicate block data"
        _, is_new1 = await store_block(redis_client, block, tmp_storage)
        _, is_new2 = await store_block(redis_client, block, tmp_storage)
        assert is_new1 is True
        assert is_new2 is False

    @pytest.mark.asyncio
    async def test_load_stored_block(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """Loading a stored block returns data identical to the original."""
        block = b"load test data " * 100
        block_hash, _ = await store_block(redis_client, block, tmp_storage)
        loaded = await load_block(block_hash, tmp_storage)
        assert loaded == block

    @pytest.mark.asyncio
    async def test_dedup_stores_once(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """Duplicate blocks are stored only once on the filesystem."""
        block = b"dedup test data"
        hash1, _ = await store_block(redis_client, block, tmp_storage)
        hash2, _ = await store_block(redis_client, block, tmp_storage)

        assert hash1 == hash2
        # Only one file should exist on the filesystem
        block_files = os.listdir(tmp_storage)
        assert block_files.count(hash1) == 1


# ---------------------------------------------------------------------------
# File Manager: Upload & Download
# ---------------------------------------------------------------------------


class TestFileManager:
    """Tests for file upload and download."""

    @pytest.mark.asyncio
    async def test_upload_and_download_roundtrip(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """Downloading an uploaded file returns data identical to the original."""
        original = b"Hello, Google Drive!" * 500
        result = await upload_file(redis_client, "test.txt", original, "alice", tmp_storage)

        file_id = result["file_id"]
        filename, downloaded = await download_file(redis_client, file_id, storage_path=tmp_storage)

        assert filename == "test.txt"
        assert downloaded == original

    @pytest.mark.asyncio
    async def test_upload_returns_block_stats(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """Upload result includes block statistics."""
        data = b"X" * 10000
        result = await upload_file(redis_client, "stats.txt", data, "alice", tmp_storage)

        assert "total_blocks" in result
        assert "new_blocks" in result
        assert "reused_blocks" in result
        assert result["total_blocks"] == result["new_blocks"] + result["reused_blocks"]

    @pytest.mark.asyncio
    async def test_delta_sync_reuses_blocks(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """Re-uploading identical content reuses existing blocks (delta sync)."""
        # Construct data with two distinct blocks
        data = b"D" * 4096 + b"E" * 4096  # 2 distinct blocks
        result1 = await upload_file(redis_client, "delta.txt", data, "alice", tmp_storage)
        assert result1["new_blocks"] == 2
        assert result1["reused_blocks"] == 0

        # Re-upload same content — all blocks should be reused
        result2 = await upload_file(redis_client, "delta.txt", data, "alice", tmp_storage)
        assert result2["new_blocks"] == 0
        assert result2["reused_blocks"] == 2

    @pytest.mark.asyncio
    async def test_delta_sync_partial_reuse(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """Only changed blocks are stored anew when a file is partially modified."""
        block_size = 4096
        # First upload: 2 blocks
        data1 = b"A" * block_size + b"B" * block_size
        result1 = await upload_file(redis_client, "partial.txt", data1, "alice", tmp_storage)
        assert result1["new_blocks"] == 2

        # Second upload: first block unchanged, only second block modified
        data2 = b"A" * block_size + b"C" * block_size
        result2 = await upload_file(redis_client, "partial.txt", data2, "alice", tmp_storage)
        assert result2["new_blocks"] == 1
        assert result2["reused_blocks"] == 1

    @pytest.mark.asyncio
    async def test_empty_file_upload(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """An empty file can be uploaded."""
        result = await upload_file(redis_client, "empty.txt", b"", "alice", tmp_storage)
        assert result["size"] == 0
        assert result["total_blocks"] == 0

        # Download should also work
        _, downloaded = await download_file(
            redis_client, result["file_id"], storage_path=tmp_storage,
        )
        assert downloaded == b""


# ---------------------------------------------------------------------------
# File Manager: Listing & Deletion
# ---------------------------------------------------------------------------


class TestFileListing:
    """Tests for file listing and deletion."""

    @pytest.mark.asyncio
    async def test_list_user_files(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """A user's file list can be retrieved."""
        await upload_file(redis_client, "file1.txt", b"data1", "alice", tmp_storage)
        await upload_file(redis_client, "file2.txt", b"data2", "alice", tmp_storage)
        await upload_file(redis_client, "file3.txt", b"data3", "bob", tmp_storage)

        alice_files = await list_files(redis_client, "alice")
        assert len(alice_files) == 2

        bob_files = await list_files(redis_client, "bob")
        assert len(bob_files) == 1

    @pytest.mark.asyncio
    async def test_delete_file(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """Deleting a file removes it from the listing."""
        result = await upload_file(redis_client, "delete_me.txt", b"bye", "alice", tmp_storage)
        file_id = result["file_id"]

        # Verify before deletion
        files = await list_files(redis_client, "alice")
        assert len(files) == 1

        # Delete
        await delete_file(redis_client, file_id)

        # Verify after deletion
        files = await list_files(redis_client, "alice")
        assert len(files) == 0

    @pytest.mark.asyncio
    async def test_delete_nonexistent_file(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Deleting a non-existent file raises ValueError."""
        with pytest.raises(ValueError, match="File not found"):
            await delete_file(redis_client, "nonexistent-id")


# ---------------------------------------------------------------------------
# Versioning
# ---------------------------------------------------------------------------


class TestVersioning:
    """Tests for file version management."""

    @pytest.mark.asyncio
    async def test_multiple_versions(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """Uploading the same file multiple times increments the version."""
        await upload_file(redis_client, "versioned.txt", b"v1 content", "alice", tmp_storage)
        result2 = await upload_file(redis_client, "versioned.txt", b"v2 content", "alice", tmp_storage)
        result3 = await upload_file(redis_client, "versioned.txt", b"v3 content", "alice", tmp_storage)

        assert result2["version"] == 2
        assert result3["version"] == 3

    @pytest.mark.asyncio
    async def test_revision_history(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """The version history of a file can be retrieved."""
        r1 = await upload_file(redis_client, "hist.txt", b"first", "alice", tmp_storage)
        await upload_file(redis_client, "hist.txt", b"second version", "alice", tmp_storage)
        await upload_file(redis_client, "hist.txt", b"third version!!", "alice", tmp_storage)

        revisions = await get_revisions(redis_client, r1["file_id"])
        assert len(revisions) == 3
        assert revisions[0]["version"] == 1
        assert revisions[1]["version"] == 2
        assert revisions[2]["version"] == 3

    @pytest.mark.asyncio
    async def test_restore_previous_version(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """A file can be restored to a previous version."""
        r1 = await upload_file(redis_client, "restore.txt", b"original", "alice", tmp_storage)
        await upload_file(redis_client, "restore.txt", b"modified", "alice", tmp_storage)

        file_id = r1["file_id"]

        # Restore to v1
        restore_result = await restore_version(redis_client, file_id, 1)
        assert restore_result["restored_from"] == 1
        assert restore_result["new_version"] == 3

        # Download and verify v1 content
        _, data = await download_file(redis_client, file_id, storage_path=tmp_storage)
        assert data == b"original"

    @pytest.mark.asyncio
    async def test_download_specific_version(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """A specific version of a file can be downloaded."""
        r1 = await upload_file(redis_client, "ver.txt", b"version 1", "alice", tmp_storage)
        await upload_file(redis_client, "ver.txt", b"version 2", "alice", tmp_storage)

        file_id = r1["file_id"]

        # Download v1
        _, data_v1 = await download_file(redis_client, file_id, version=1, storage_path=tmp_storage)
        assert data_v1 == b"version 1"

        # Download v2
        _, data_v2 = await download_file(redis_client, file_id, version=2, storage_path=tmp_storage)
        assert data_v2 == b"version 2"


# ---------------------------------------------------------------------------
# Notification (Long Polling)
# ---------------------------------------------------------------------------


class TestNotification:
    """Tests for file change notifications."""

    @pytest.mark.asyncio
    async def test_publish_and_poll_events(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Published events can be retrieved via poll."""
        await publish_sync_event(redis_client, "alice", {
            "event_type": "upload",
            "file_id": "file-1",
            "filename": "test.txt",
            "user_id": "alice",
            "version": 1,
            "timestamp": "2024-01-01T00:00:00",
        })

        events = await poll_sync_events(redis_client, "alice", timeout=1)
        assert len(events) == 1
        assert events[0]["event_type"] == "upload"
        assert events[0]["filename"] == "test.txt"

    @pytest.mark.asyncio
    async def test_poll_returns_empty_on_timeout(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Returns an empty list after timeout when there are no events."""
        events = await poll_sync_events(redis_client, "nobody", timeout=1)
        assert events == []

    @pytest.mark.asyncio
    async def test_poll_drains_events(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Events are removed from the queue after being polled."""
        await publish_sync_event(redis_client, "alice", {
            "event_type": "upload",
            "file_id": "f1",
            "filename": "a.txt",
            "user_id": "alice",
            "version": 1,
            "timestamp": "2024-01-01T00:00:00",
        })

        # First poll — returns the event
        events1 = await poll_sync_events(redis_client, "alice", timeout=1)
        assert len(events1) == 1

        # Second poll — already drained
        events2 = await poll_sync_events(redis_client, "alice", timeout=1)
        assert len(events2) == 0

    @pytest.mark.asyncio
    async def test_upload_triggers_sync_event(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """A sync event is published when a file is uploaded."""
        await upload_file(redis_client, "sync.txt", b"data", "alice", tmp_storage)

        events = await poll_sync_events(redis_client, "alice", timeout=1)
        assert len(events) == 1
        assert events[0]["event_type"] == "upload"


# ---------------------------------------------------------------------------
# Conflict Detection
# ---------------------------------------------------------------------------


class TestConflictDetection:
    """Tests for sync conflict detection."""

    @pytest.mark.asyncio
    async def test_no_conflict_on_current_version(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """Modifying based on the latest version is not a conflict."""
        result = await upload_file(redis_client, "conf.txt", b"v1", "alice", tmp_storage)
        file_id = result["file_id"]

        conflict = await check_conflict(redis_client, file_id, expected_version=1)
        assert conflict is None

    @pytest.mark.asyncio
    async def test_conflict_on_stale_version(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """Modifying based on a stale version is a conflict."""
        result = await upload_file(redis_client, "conf2.txt", b"v1", "alice", tmp_storage)
        file_id = result["file_id"]
        await upload_file(redis_client, "conf2.txt", b"v2", "alice", tmp_storage)

        conflict = await check_conflict(redis_client, file_id, expected_version=1)
        assert conflict is not None
        assert conflict["conflict"] is True
        assert conflict["server_version"] == 2
        assert conflict["your_version"] == 1

    @pytest.mark.asyncio
    async def test_no_conflict_on_new_file(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """A non-existent file is not a conflict."""
        conflict = await check_conflict(redis_client, "new-file-id", expected_version=0)
        assert conflict is None

    @pytest.mark.asyncio
    async def test_resolve_first_writer_wins(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """The first-writer-wins strategy works correctly."""
        result = await upload_file(redis_client, "fww.txt", b"v1", "alice", tmp_storage)
        file_id = result["file_id"]

        # Bob tries to modify based on v1, but Alice has already updated to v2
        await upload_file(redis_client, "fww.txt", b"v2 by alice", "alice", tmp_storage)

        conflict = await resolve_conflict_first_writer_wins(
            redis_client, file_id, expected_version=1,
        )
        assert conflict is not None
        assert conflict["conflict"] is True


# ---------------------------------------------------------------------------
# File Metadata CRUD
# ---------------------------------------------------------------------------


class TestFileMetadata:
    """Tests for file metadata CRUD."""

    @pytest.mark.asyncio
    async def test_metadata_stored_on_upload(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """Metadata is stored in Redis when a file is uploaded."""
        result = await upload_file(redis_client, "meta.txt", b"content", "alice", tmp_storage)
        file_id = result["file_id"]

        meta = await redis_client.hgetall(f"file:{file_id}")
        assert meta["filename"] == "meta.txt"
        assert meta["user_id"] == "alice"
        assert meta["latest_version"] == "1"
        assert int(meta["size"]) == 7

    @pytest.mark.asyncio
    async def test_metadata_updated_on_reupload(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """Metadata is updated when a file is re-uploaded."""
        r1 = await upload_file(redis_client, "update.txt", b"short", "alice", tmp_storage)
        await upload_file(redis_client, "update.txt", b"longer content here", "alice", tmp_storage)

        meta = await redis_client.hgetall(f"file:{r1['file_id']}")
        assert meta["latest_version"] == "2"
        assert int(meta["size"]) == 19

    @pytest.mark.asyncio
    async def test_find_file_by_name(self, redis_client: fakeredis.aioredis.FakeRedis, tmp_storage: str) -> None:
        """A user's file can be found by name."""
        result = await upload_file(redis_client, "findme.txt", b"data", "alice", tmp_storage)

        found = await _find_file_id(redis_client, "findme.txt", "alice")
        assert found == result["file_id"]

        not_found = await _find_file_id(redis_client, "findme.txt", "bob")
        assert not_found is None
