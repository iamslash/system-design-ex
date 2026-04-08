"""Tests for the Google Drive file sync service.

fakeredis 를 사용하여 Redis 의존성 없이 단위 테스트를 수행한다.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile

import fakeredis.aioredis
import pytest
import pytest_asyncio

# api 디렉토리를 import 경로에 추가
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
async def redis_client():
    """Create a fake async Redis client for testing."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.flushall()
    await client.aclose()


@pytest.fixture
def tmp_storage(tmp_path):
    """Create a temporary directory for block storage."""
    return str(tmp_path / "blocks")


# ---------------------------------------------------------------------------
# Block Server: Splitting
# ---------------------------------------------------------------------------


class TestBlockSplitting:
    """파일을 블록으로 분할하는 테스트."""

    def test_split_exact_blocks(self) -> None:
        """파일 크기가 블록 크기의 정확한 배수일 때 올바르게 분할된다."""
        data = b"A" * 8192
        blocks = split_into_blocks(data, block_size=4096)
        assert len(blocks) == 2
        assert all(len(b) == 4096 for b in blocks)

    def test_split_with_remainder(self) -> None:
        """마지막 블록이 블록 크기보다 작을 수 있다."""
        data = b"B" * 5000
        blocks = split_into_blocks(data, block_size=4096)
        assert len(blocks) == 2
        assert len(blocks[0]) == 4096
        assert len(blocks[1]) == 904

    def test_split_small_file(self) -> None:
        """블록 크기보다 작은 파일은 하나의 블록이 된다."""
        data = b"C" * 100
        blocks = split_into_blocks(data, block_size=4096)
        assert len(blocks) == 1
        assert blocks[0] == data

    def test_split_empty_file(self) -> None:
        """빈 파일은 빈 블록 리스트를 반환한다."""
        data = b""
        blocks = split_into_blocks(data, block_size=4096)
        assert len(blocks) == 0

    def test_reconstruction_from_blocks(self) -> None:
        """분할된 블록을 합치면 원본 데이터와 동일하다."""
        data = os.urandom(10000)
        blocks = split_into_blocks(data, block_size=4096)
        reconstructed = b"".join(blocks)
        assert reconstructed == data


# ---------------------------------------------------------------------------
# Block Server: Hashing & Dedup
# ---------------------------------------------------------------------------


class TestBlockHashing:
    """블록 해싱과 중복 제거 테스트."""

    def test_same_content_same_hash(self) -> None:
        """동일한 내용은 항상 동일한 해시를 반환한다."""
        block = b"Hello, World!"
        hash1 = compute_block_hash(block)
        hash2 = compute_block_hash(block)
        assert hash1 == hash2

    def test_different_content_different_hash(self) -> None:
        """다른 내용은 다른 해시를 반환한다."""
        hash1 = compute_block_hash(b"Hello")
        hash2 = compute_block_hash(b"World")
        assert hash1 != hash2

    def test_hash_is_sha256_hex(self) -> None:
        """해시는 64자의 16진수 문자열이다 (SHA-256)."""
        h = compute_block_hash(b"test")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)


# ---------------------------------------------------------------------------
# Block Server: Compression
# ---------------------------------------------------------------------------


class TestBlockCompression:
    """블록 압축 테스트."""

    def test_compress_decompress_roundtrip(self) -> None:
        """압축 후 해제하면 원본과 동일하다."""
        original = b"Hello " * 1000
        compressed = compress_block(original)
        decompressed = decompress_block(compressed)
        assert decompressed == original

    def test_compression_reduces_size(self) -> None:
        """반복 데이터는 압축하면 크기가 줄어든다."""
        original = b"AAAA" * 4096
        compressed = compress_block(original)
        assert len(compressed) < len(original)


# ---------------------------------------------------------------------------
# Block Server: Store & Load
# ---------------------------------------------------------------------------


class TestBlockStorage:
    """블록 저장 및 로드 테스트."""

    @pytest.mark.asyncio
    async def test_store_new_block(self, redis_client, tmp_storage) -> None:
        """새 블록이 저장되고 is_new=True 를 반환한다."""
        block = b"new block data"
        block_hash, is_new = await store_block(redis_client, block, tmp_storage)
        assert is_new is True
        assert len(block_hash) == 64

    @pytest.mark.asyncio
    async def test_store_duplicate_block(self, redis_client, tmp_storage) -> None:
        """동일한 블록을 두 번 저장하면 두 번째는 is_new=False 를 반환한다."""
        block = b"duplicate block data"
        _, is_new1 = await store_block(redis_client, block, tmp_storage)
        _, is_new2 = await store_block(redis_client, block, tmp_storage)
        assert is_new1 is True
        assert is_new2 is False

    @pytest.mark.asyncio
    async def test_load_stored_block(self, redis_client, tmp_storage) -> None:
        """저장된 블록을 로드하면 원본과 동일하다."""
        block = b"load test data " * 100
        block_hash, _ = await store_block(redis_client, block, tmp_storage)
        loaded = await load_block(block_hash, tmp_storage)
        assert loaded == block

    @pytest.mark.asyncio
    async def test_dedup_stores_once(self, redis_client, tmp_storage) -> None:
        """중복 블록은 파일시스템에 한 번만 저장된다."""
        block = b"dedup test data"
        hash1, _ = await store_block(redis_client, block, tmp_storage)
        hash2, _ = await store_block(redis_client, block, tmp_storage)

        assert hash1 == hash2
        # 파일시스템에 하나의 파일만 존재
        block_files = os.listdir(tmp_storage)
        assert block_files.count(hash1) == 1


# ---------------------------------------------------------------------------
# File Manager: Upload & Download
# ---------------------------------------------------------------------------


class TestFileManager:
    """파일 업로드/다운로드 테스트."""

    @pytest.mark.asyncio
    async def test_upload_and_download_roundtrip(self, redis_client, tmp_storage) -> None:
        """업로드한 파일을 다운로드하면 원본과 동일하다."""
        original = b"Hello, Google Drive!" * 500
        result = await upload_file(redis_client, "test.txt", original, "alice", tmp_storage)

        file_id = result["file_id"]
        filename, downloaded = await download_file(redis_client, file_id, storage_path=tmp_storage)

        assert filename == "test.txt"
        assert downloaded == original

    @pytest.mark.asyncio
    async def test_upload_returns_block_stats(self, redis_client, tmp_storage) -> None:
        """업로드 결과에 블록 통계가 포함된다."""
        data = b"X" * 10000
        result = await upload_file(redis_client, "stats.txt", data, "alice", tmp_storage)

        assert "total_blocks" in result
        assert "new_blocks" in result
        assert "reused_blocks" in result
        assert result["total_blocks"] == result["new_blocks"] + result["reused_blocks"]

    @pytest.mark.asyncio
    async def test_delta_sync_reuses_blocks(self, redis_client, tmp_storage) -> None:
        """동일한 내용을 다시 업로드하면 블록이 재사용된다 (delta sync)."""
        # 블록 2개가 서로 다른 내용을 갖도록 구성
        data = b"D" * 4096 + b"E" * 4096  # 2 distinct blocks
        result1 = await upload_file(redis_client, "delta.txt", data, "alice", tmp_storage)
        assert result1["new_blocks"] == 2
        assert result1["reused_blocks"] == 0

        # 같은 내용 재업로드 — 모든 블록이 재사용되어야 한다
        result2 = await upload_file(redis_client, "delta.txt", data, "alice", tmp_storage)
        assert result2["new_blocks"] == 0
        assert result2["reused_blocks"] == 2

    @pytest.mark.asyncio
    async def test_delta_sync_partial_reuse(self, redis_client, tmp_storage) -> None:
        """부분적으로 변경된 파일은 변경된 블록만 새로 저장된다."""
        block_size = 4096
        # 첫 번째 업로드: 2 블록
        data1 = b"A" * block_size + b"B" * block_size
        result1 = await upload_file(redis_client, "partial.txt", data1, "alice", tmp_storage)
        assert result1["new_blocks"] == 2

        # 두 번째 업로드: 첫 번째 블록은 동일, 두 번째 블록만 변경
        data2 = b"A" * block_size + b"C" * block_size
        result2 = await upload_file(redis_client, "partial.txt", data2, "alice", tmp_storage)
        assert result2["new_blocks"] == 1
        assert result2["reused_blocks"] == 1

    @pytest.mark.asyncio
    async def test_empty_file_upload(self, redis_client, tmp_storage) -> None:
        """빈 파일도 업로드할 수 있다."""
        result = await upload_file(redis_client, "empty.txt", b"", "alice", tmp_storage)
        assert result["size"] == 0
        assert result["total_blocks"] == 0

        # 다운로드도 가능
        _, downloaded = await download_file(
            redis_client, result["file_id"], storage_path=tmp_storage,
        )
        assert downloaded == b""


# ---------------------------------------------------------------------------
# File Manager: Listing & Deletion
# ---------------------------------------------------------------------------


class TestFileListing:
    """파일 목록 조회 및 삭제 테스트."""

    @pytest.mark.asyncio
    async def test_list_user_files(self, redis_client, tmp_storage) -> None:
        """사용자의 파일 목록을 조회할 수 있다."""
        await upload_file(redis_client, "file1.txt", b"data1", "alice", tmp_storage)
        await upload_file(redis_client, "file2.txt", b"data2", "alice", tmp_storage)
        await upload_file(redis_client, "file3.txt", b"data3", "bob", tmp_storage)

        alice_files = await list_files(redis_client, "alice")
        assert len(alice_files) == 2

        bob_files = await list_files(redis_client, "bob")
        assert len(bob_files) == 1

    @pytest.mark.asyncio
    async def test_delete_file(self, redis_client, tmp_storage) -> None:
        """파일을 삭제하면 목록에서 사라진다."""
        result = await upload_file(redis_client, "delete_me.txt", b"bye", "alice", tmp_storage)
        file_id = result["file_id"]

        # 삭제 전 확인
        files = await list_files(redis_client, "alice")
        assert len(files) == 1

        # 삭제
        await delete_file(redis_client, file_id)

        # 삭제 후 확인
        files = await list_files(redis_client, "alice")
        assert len(files) == 0

    @pytest.mark.asyncio
    async def test_delete_nonexistent_file(self, redis_client) -> None:
        """존재하지 않는 파일 삭제 시 ValueError 가 발생한다."""
        with pytest.raises(ValueError, match="File not found"):
            await delete_file(redis_client, "nonexistent-id")


# ---------------------------------------------------------------------------
# Versioning
# ---------------------------------------------------------------------------


class TestVersioning:
    """파일 버전 관리 테스트."""

    @pytest.mark.asyncio
    async def test_multiple_versions(self, redis_client, tmp_storage) -> None:
        """같은 파일을 여러 번 업로드하면 버전이 증가한다."""
        await upload_file(redis_client, "versioned.txt", b"v1 content", "alice", tmp_storage)
        result2 = await upload_file(redis_client, "versioned.txt", b"v2 content", "alice", tmp_storage)
        result3 = await upload_file(redis_client, "versioned.txt", b"v3 content", "alice", tmp_storage)

        assert result2["version"] == 2
        assert result3["version"] == 3

    @pytest.mark.asyncio
    async def test_revision_history(self, redis_client, tmp_storage) -> None:
        """파일의 버전 히스토리를 조회할 수 있다."""
        r1 = await upload_file(redis_client, "hist.txt", b"first", "alice", tmp_storage)
        await upload_file(redis_client, "hist.txt", b"second version", "alice", tmp_storage)
        await upload_file(redis_client, "hist.txt", b"third version!!", "alice", tmp_storage)

        revisions = await get_revisions(redis_client, r1["file_id"])
        assert len(revisions) == 3
        assert revisions[0]["version"] == 1
        assert revisions[1]["version"] == 2
        assert revisions[2]["version"] == 3

    @pytest.mark.asyncio
    async def test_restore_previous_version(self, redis_client, tmp_storage) -> None:
        """이전 버전으로 복원할 수 있다."""
        r1 = await upload_file(redis_client, "restore.txt", b"original", "alice", tmp_storage)
        await upload_file(redis_client, "restore.txt", b"modified", "alice", tmp_storage)

        file_id = r1["file_id"]

        # v1 으로 복원
        restore_result = await restore_version(redis_client, file_id, 1)
        assert restore_result["restored_from"] == 1
        assert restore_result["new_version"] == 3

        # 다운로드해서 v1 내용 확인
        _, data = await download_file(redis_client, file_id, storage_path=tmp_storage)
        assert data == b"original"

    @pytest.mark.asyncio
    async def test_download_specific_version(self, redis_client, tmp_storage) -> None:
        """특정 버전을 다운로드할 수 있다."""
        r1 = await upload_file(redis_client, "ver.txt", b"version 1", "alice", tmp_storage)
        await upload_file(redis_client, "ver.txt", b"version 2", "alice", tmp_storage)

        file_id = r1["file_id"]

        # v1 다운로드
        _, data_v1 = await download_file(redis_client, file_id, version=1, storage_path=tmp_storage)
        assert data_v1 == b"version 1"

        # v2 다운로드
        _, data_v2 = await download_file(redis_client, file_id, version=2, storage_path=tmp_storage)
        assert data_v2 == b"version 2"


# ---------------------------------------------------------------------------
# Notification (Long Polling)
# ---------------------------------------------------------------------------


class TestNotification:
    """파일 변경 알림 테스트."""

    @pytest.mark.asyncio
    async def test_publish_and_poll_events(self, redis_client) -> None:
        """이벤트를 발행하면 poll 로 조회할 수 있다."""
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
    async def test_poll_returns_empty_on_timeout(self, redis_client) -> None:
        """이벤트가 없으면 타임아웃 후 빈 리스트를 반환한다."""
        events = await poll_sync_events(redis_client, "nobody", timeout=1)
        assert events == []

    @pytest.mark.asyncio
    async def test_poll_drains_events(self, redis_client) -> None:
        """poll 후 이벤트가 큐에서 제거된다."""
        await publish_sync_event(redis_client, "alice", {
            "event_type": "upload",
            "file_id": "f1",
            "filename": "a.txt",
            "user_id": "alice",
            "version": 1,
            "timestamp": "2024-01-01T00:00:00",
        })

        # 첫 번째 poll — 이벤트 반환
        events1 = await poll_sync_events(redis_client, "alice", timeout=1)
        assert len(events1) == 1

        # 두 번째 poll — 이미 drain 됨
        events2 = await poll_sync_events(redis_client, "alice", timeout=1)
        assert len(events2) == 0

    @pytest.mark.asyncio
    async def test_upload_triggers_sync_event(self, redis_client, tmp_storage) -> None:
        """파일 업로드 시 동기화 이벤트가 발행된다."""
        await upload_file(redis_client, "sync.txt", b"data", "alice", tmp_storage)

        events = await poll_sync_events(redis_client, "alice", timeout=1)
        assert len(events) == 1
        assert events[0]["event_type"] == "upload"


# ---------------------------------------------------------------------------
# Conflict Detection
# ---------------------------------------------------------------------------


class TestConflictDetection:
    """동기화 충돌 감지 테스트."""

    @pytest.mark.asyncio
    async def test_no_conflict_on_current_version(self, redis_client, tmp_storage) -> None:
        """최신 버전 기반 수정은 충돌이 아니다."""
        result = await upload_file(redis_client, "conf.txt", b"v1", "alice", tmp_storage)
        file_id = result["file_id"]

        conflict = await check_conflict(redis_client, file_id, expected_version=1)
        assert conflict is None

    @pytest.mark.asyncio
    async def test_conflict_on_stale_version(self, redis_client, tmp_storage) -> None:
        """오래된 버전 기반 수정은 충돌이다."""
        result = await upload_file(redis_client, "conf2.txt", b"v1", "alice", tmp_storage)
        file_id = result["file_id"]
        await upload_file(redis_client, "conf2.txt", b"v2", "alice", tmp_storage)

        conflict = await check_conflict(redis_client, file_id, expected_version=1)
        assert conflict is not None
        assert conflict["conflict"] is True
        assert conflict["server_version"] == 2
        assert conflict["your_version"] == 1

    @pytest.mark.asyncio
    async def test_no_conflict_on_new_file(self, redis_client) -> None:
        """존재하지 않는 파일은 충돌이 아니다."""
        conflict = await check_conflict(redis_client, "new-file-id", expected_version=0)
        assert conflict is None

    @pytest.mark.asyncio
    async def test_resolve_first_writer_wins(self, redis_client, tmp_storage) -> None:
        """first-writer-wins 전략이 올바르게 동작한다."""
        result = await upload_file(redis_client, "fww.txt", b"v1", "alice", tmp_storage)
        file_id = result["file_id"]

        # Bob 이 v1 기반으로 수정하려 하지만, Alice 가 이미 v2 로 업데이트
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
    """파일 메타데이터 CRUD 테스트."""

    @pytest.mark.asyncio
    async def test_metadata_stored_on_upload(self, redis_client, tmp_storage) -> None:
        """업로드 시 메타데이터가 Redis 에 저장된다."""
        result = await upload_file(redis_client, "meta.txt", b"content", "alice", tmp_storage)
        file_id = result["file_id"]

        meta = await redis_client.hgetall(f"file:{file_id}")
        assert meta["filename"] == "meta.txt"
        assert meta["user_id"] == "alice"
        assert meta["latest_version"] == "1"
        assert int(meta["size"]) == 7

    @pytest.mark.asyncio
    async def test_metadata_updated_on_reupload(self, redis_client, tmp_storage) -> None:
        """재업로드 시 메타데이터가 갱신된다."""
        r1 = await upload_file(redis_client, "update.txt", b"short", "alice", tmp_storage)
        await upload_file(redis_client, "update.txt", b"longer content here", "alice", tmp_storage)

        meta = await redis_client.hgetall(f"file:{r1['file_id']}")
        assert meta["latest_version"] == "2"
        assert int(meta["size"]) == 19

    @pytest.mark.asyncio
    async def test_find_file_by_name(self, redis_client, tmp_storage) -> None:
        """사용자의 파일을 이름으로 찾을 수 있다."""
        result = await upload_file(redis_client, "findme.txt", b"data", "alice", tmp_storage)

        found = await _find_file_id(redis_client, "findme.txt", "alice")
        assert found == result["file_id"]

        not_found = await _find_file_id(redis_client, "findme.txt", "bob")
        assert not_found is None
