"""Tests for the video streaming system.

Uses fakeredis for unit tests without a Redis dependency.
Uses temporary directories to isolate filesystem tests.
"""

from __future__ import annotations

import os
import pathlib
import sys
import tempfile
from collections.abc import AsyncGenerator

import fakeredis.aioredis
import pytest
import pytest_asyncio

# Add api directory to import path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

import config
from metadata.store import (
    create_video_metadata,
    delete_video_metadata,
    get_video_metadata,
    list_videos,
    update_video_status,
)
from video.streaming import (
    build_stream_response_info,
    get_video_path,
    parse_range_header,
    read_video_range,
)
from video.transcode import (
    _dag_assemble,
    _dag_encode,
    _dag_split,
    _dag_thumbnail,
    _dag_watermark,
    transcode_video,
)
from video.upload import (
    complete_upload,
    get_upload_status,
    initiate_upload,
    upload_chunk,
)


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
def temp_storage(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> str:
    """Set up a temporary video storage directory."""
    storage_path = str(tmp_path / "videos")
    os.makedirs(storage_path, exist_ok=True)
    monkeypatch.setattr(config.settings, "VIDEO_STORAGE_PATH", storage_path)
    return storage_path


def _create_sample_video(path: str, content: str = "sample video content") -> str:
    """Create a sample video file for testing."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(content)
    return path


# ---------------------------------------------------------------------------
# Upload Tests
# ---------------------------------------------------------------------------


class TestUploadInitiation:
    """Upload initiation tests."""

    @pytest.mark.asyncio
    async def test_initiate_upload_returns_upload_id(
        self, redis_client: fakeredis.aioredis.FakeRedis, temp_storage: str
    ) -> None:
        """Initiating an upload should return an upload_id."""
        result = await initiate_upload(redis_client, "Test Video", "desc", 3)
        assert "upload_id" in result
        assert "video_id" in result
        assert result["total_chunks"] == 3
        assert result["status"] == "uploading"

    @pytest.mark.asyncio
    async def test_initiate_upload_creates_presigned_url(
        self, redis_client: fakeredis.aioredis.FakeRedis, temp_storage: str
    ) -> None:
        """Initiating an upload should return a pre-signed URL (simulated)."""
        result = await initiate_upload(redis_client, "Test Video", "", 1)
        assert "presigned_url" in result
        assert result["upload_id"] in result["presigned_url"]

    @pytest.mark.asyncio
    async def test_upload_status_saved_in_redis(
        self, redis_client: fakeredis.aioredis.FakeRedis, temp_storage: str
    ) -> None:
        """Upload status should be saved in Redis."""
        result = await initiate_upload(redis_client, "My Video", "desc", 2)
        upload_id = result["upload_id"]

        status = await get_upload_status(redis_client, upload_id)
        assert status is not None
        assert status["title"] == "My Video"
        assert status["total_chunks"] == 2
        assert status["uploaded_chunks"] == 0
        assert status["status"] == "uploading"


class TestChunkUpload:
    """Chunk upload tests."""

    @pytest.mark.asyncio
    async def test_chunk_upload_tracking(
        self, redis_client: fakeredis.aioredis.FakeRedis, temp_storage: str
    ) -> None:
        """Upload progress should be tracked when uploading chunks."""
        result = await initiate_upload(redis_client, "Video", "", 3)
        upload_id = result["upload_id"]

        chunk_result = await upload_chunk(
            redis_client, upload_id, 0, b"chunk-0-data"
        )
        assert chunk_result["uploaded_chunks"] == 1
        assert chunk_result["total_chunks"] == 3

        chunk_result = await upload_chunk(
            redis_client, upload_id, 1, b"chunk-1-data"
        )
        assert chunk_result["uploaded_chunks"] == 2

    @pytest.mark.asyncio
    async def test_chunk_upload_invalid_index(
        self, redis_client: fakeredis.aioredis.FakeRedis, temp_storage: str
    ) -> None:
        """An invalid chunk index should return an error."""
        result = await initiate_upload(redis_client, "Video", "", 2)
        upload_id = result["upload_id"]

        chunk_result = await upload_chunk(
            redis_client, upload_id, 5, b"bad-chunk"
        )
        assert "error" in chunk_result

    @pytest.mark.asyncio
    async def test_chunk_upload_nonexistent_upload(
        self, redis_client: fakeredis.aioredis.FakeRedis, temp_storage: str
    ) -> None:
        """Uploading a chunk with a non-existent upload_id should return an error."""
        result = await upload_chunk(
            redis_client, "nonexistent", 0, b"data"
        )
        assert "error" in result

    @pytest.mark.asyncio
    async def test_resumable_chunk_overwrites(
        self, redis_client: fakeredis.aioredis.FakeRedis, temp_storage: str
    ) -> None:
        """Re-uploading the same chunk index should overwrite it (resumable)."""
        result = await initiate_upload(redis_client, "Video", "", 2)
        upload_id = result["upload_id"]

        await upload_chunk(redis_client, upload_id, 0, b"first-attempt")
        r1 = await upload_chunk(redis_client, upload_id, 0, b"second-attempt")

        # Even duplicate chunks count as 1 (using Set)
        assert r1["uploaded_chunks"] == 1


class TestCompleteUpload:
    """Upload completion tests."""

    @pytest.mark.asyncio
    async def test_complete_upload_merges_chunks(
        self, redis_client: fakeredis.aioredis.FakeRedis, temp_storage: str
    ) -> None:
        """Completing an upload should merge chunks into a single file."""
        result = await initiate_upload(redis_client, "Video", "", 2)
        upload_id = result["upload_id"]
        video_id = result["video_id"]

        await upload_chunk(redis_client, upload_id, 0, b"AAAA")
        await upload_chunk(redis_client, upload_id, 1, b"BBBB")

        complete = await complete_upload(redis_client, upload_id)
        assert "error" not in complete
        assert complete["video_id"] == video_id

        # Verify merged file
        merged_path = os.path.join(temp_storage, "originals", f"{video_id}.mp4")
        assert os.path.exists(merged_path)
        with open(merged_path, "rb") as f:
            assert f.read() == b"AAAABBBB"

    @pytest.mark.asyncio
    async def test_complete_upload_fails_if_chunks_missing(
        self, redis_client: fakeredis.aioredis.FakeRedis, temp_storage: str
    ) -> None:
        """Completion should fail if not all chunks have been uploaded."""
        result = await initiate_upload(redis_client, "Video", "", 3)
        upload_id = result["upload_id"]

        await upload_chunk(redis_client, upload_id, 0, b"data")
        # chunks 1 and 2 are not uploaded

        complete = await complete_upload(redis_client, upload_id)
        assert "error" in complete
        assert "Not all chunks" in complete["error"]

    @pytest.mark.asyncio
    async def test_complete_upload_triggers_status_change(
        self, redis_client: fakeredis.aioredis.FakeRedis, temp_storage: str
    ) -> None:
        """Upload status should change to 'completed' when upload is complete."""
        result = await initiate_upload(redis_client, "Video", "", 1)
        upload_id = result["upload_id"]

        await upload_chunk(redis_client, upload_id, 0, b"data")
        await complete_upload(redis_client, upload_id)

        status = await get_upload_status(redis_client, upload_id)
        assert status["status"] == "completed"


# ---------------------------------------------------------------------------
# Transcode Tests
# ---------------------------------------------------------------------------


class TestTranscode:
    """Transcoding pipeline tests."""

    @pytest.mark.asyncio
    async def test_transcode_produces_multiple_resolutions(
        self, redis_client: fakeredis.aioredis.FakeRedis, temp_storage: str
    ) -> None:
        """Transcoding should produce files at multiple resolutions."""
        video_id = "test-video-001"
        # Create original video file
        original_path = os.path.join(temp_storage, "originals", f"{video_id}.mp4")
        _create_sample_video(original_path, "original video content here")

        # Create video metadata
        await create_video_metadata(redis_client, video_id, "Test")

        result = await transcode_video(redis_client, video_id, original_path)
        assert result["status"] == "ready"
        assert set(result["resolutions"]) == {"360p", "720p", "1080p"}

        # Verify each resolution file exists
        for res in ["360p", "720p", "1080p"]:
            res_path = os.path.join(
                temp_storage, "transcoded", video_id, f"{res}.mp4"
            )
            assert os.path.exists(res_path), f"{res} file not found"

    @pytest.mark.asyncio
    async def test_transcode_dag_stages(
        self, redis_client: fakeredis.aioredis.FakeRedis, temp_storage: str
    ) -> None:
        """All stages of the DAG pipeline should be executed."""
        video_id = "test-video-002"
        original_path = os.path.join(temp_storage, "originals", f"{video_id}.mp4")
        _create_sample_video(original_path)

        await create_video_metadata(redis_client, video_id, "Test")
        result = await transcode_video(redis_client, video_id, original_path)

        expected_stages = ["split", "encode", "encode", "encode",
                          "thumbnail", "watermark", "assemble"]
        assert result["dag_stages"] == expected_stages

    @pytest.mark.asyncio
    async def test_transcode_updates_video_status(
        self, redis_client: fakeredis.aioredis.FakeRedis, temp_storage: str
    ) -> None:
        """Video status should be updated to 'ready' after transcoding completes."""
        video_id = "test-video-003"
        original_path = os.path.join(temp_storage, "originals", f"{video_id}.mp4")
        _create_sample_video(original_path)

        await create_video_metadata(redis_client, video_id, "Test")
        await transcode_video(redis_client, video_id, original_path)

        data = await redis_client.hgetall(f"video:{video_id}")
        assert data["status"] == "ready"
        assert "360p" in data["resolutions"]

    def test_dag_split(self, temp_storage: str) -> None:
        """The Split stage should create segments."""
        source = os.path.join(temp_storage, "test_split.mp4")
        _create_sample_video(source, "split test content")

        work_dir = os.path.join(temp_storage, "work_split")
        result = _dag_split(source, work_dir)

        assert result["stage"] == "split"
        assert result["segment_count"] == 1
        assert os.path.exists(result["segments"][0])

    def test_dag_encode(self, temp_storage: str) -> None:
        """The Encode stage should encode to the specified resolution."""
        segment_path = os.path.join(temp_storage, "segment.dat")
        _create_sample_video(segment_path, "segment data")

        work_dir = os.path.join(temp_storage, "work_encode")
        result = _dag_encode([segment_path], work_dir, "720p")

        assert result["stage"] == "encode"
        assert result["resolution"] == "720p"
        assert os.path.exists(result["output_path"])

    def test_dag_thumbnail(self, temp_storage: str) -> None:
        """The Thumbnail stage should generate a thumbnail."""
        source = os.path.join(temp_storage, "test_thumb.mp4")
        _create_sample_video(source)

        work_dir = os.path.join(temp_storage, "work_thumb")
        result = _dag_thumbnail(source, work_dir)

        assert result["stage"] == "thumbnail"
        assert os.path.exists(result["output_path"])


# ---------------------------------------------------------------------------
# Metadata Tests
# ---------------------------------------------------------------------------


class TestMetadata:
    """Video metadata tests."""

    @pytest.mark.asyncio
    async def test_create_metadata(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Should be able to create video metadata."""
        result = await create_video_metadata(
            redis_client, "vid-001", "My Video", "A description"
        )
        assert result["video_id"] == "vid-001"
        assert result["title"] == "My Video"
        assert result["status"] == "uploading"

    @pytest.mark.asyncio
    async def test_get_metadata(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Should be able to retrieve video metadata."""
        await create_video_metadata(redis_client, "vid-002", "Test Video")
        result = await get_video_metadata(redis_client, "vid-002")

        assert result is not None
        assert result["video_id"] == "vid-002"
        assert result["title"] == "Test Video"

    @pytest.mark.asyncio
    async def test_get_nonexistent_metadata(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Retrieving non-existent video metadata should return None."""
        result = await get_video_metadata(redis_client, "nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_views_increment(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """View count should increment when metadata is retrieved."""
        await create_video_metadata(redis_client, "vid-003", "Popular")

        r1 = await get_video_metadata(redis_client, "vid-003")
        assert r1["views"] == 1

        r2 = await get_video_metadata(redis_client, "vid-003")
        assert r2["views"] == 2

    @pytest.mark.asyncio
    async def test_update_video_status(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Should be able to update video status."""
        await create_video_metadata(redis_client, "vid-004", "Test")
        success = await update_video_status(
            redis_client, "vid-004", "ready", resolutions="360p,720p"
        )
        assert success is True

        data = await redis_client.hgetall("video:vid-004")
        assert data["status"] == "ready"
        assert data["resolutions"] == "360p,720p"

    @pytest.mark.asyncio
    async def test_update_nonexistent_video(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Updating status of a non-existent video should fail."""
        success = await update_video_status(redis_client, "ghost", "ready")
        assert success is False

    @pytest.mark.asyncio
    async def test_list_videos(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Should be able to retrieve video list in reverse chronological order."""
        await create_video_metadata(redis_client, "vid-a", "First")
        await create_video_metadata(redis_client, "vid-b", "Second")
        await create_video_metadata(redis_client, "vid-c", "Third")

        videos = await list_videos(redis_client)
        assert len(videos) == 3
        # Most recent first
        titles = [v["title"] for v in videos]
        assert titles[0] == "Third"

    @pytest.mark.asyncio
    async def test_delete_metadata(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Should be able to delete video metadata."""
        await create_video_metadata(redis_client, "vid-del", "Delete Me")
        deleted = await delete_video_metadata(redis_client, "vid-del")
        assert deleted is True

        result = await get_video_metadata(redis_client, "vid-del")
        assert result is None

    @pytest.mark.asyncio
    async def test_video_status_lifecycle(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Video status should transition uploading → transcoding → ready."""
        await create_video_metadata(redis_client, "vid-lc", "Lifecycle")

        # uploading
        data = await redis_client.hgetall("video:vid-lc")
        assert data["status"] == "uploading"

        # transcoding
        await update_video_status(redis_client, "vid-lc", "transcoding")
        data = await redis_client.hgetall("video:vid-lc")
        assert data["status"] == "transcoding"

        # ready
        await update_video_status(redis_client, "vid-lc", "ready")
        data = await redis_client.hgetall("video:vid-lc")
        assert data["status"] == "ready"


# ---------------------------------------------------------------------------
# Streaming Tests
# ---------------------------------------------------------------------------


class TestStreaming:
    """Video streaming tests."""

    def test_parse_range_full(self) -> None:
        """Should return the full range when no Range header is present."""
        start, end = parse_range_header(None, 1000)
        assert start == 0
        assert end == 999

    def test_parse_range_bytes(self) -> None:
        """Should parse bytes=0-499 format."""
        start, end = parse_range_header("bytes=0-499", 1000)
        assert start == 0
        assert end == 499

    def test_parse_range_open_end(self) -> None:
        """Should parse bytes=500- format (to end of file)."""
        start, end = parse_range_header("bytes=500-", 1000)
        assert start == 500
        assert end == 999

    def test_parse_range_suffix(self) -> None:
        """Should parse bytes=-200 format (last 200 bytes)."""
        start, end = parse_range_header("bytes=-200", 1000)
        assert start == 800
        assert end == 999

    def test_read_video_range(self, temp_storage: str) -> None:
        """Should read data for the specified byte range."""
        video_path = os.path.join(temp_storage, "range_test.mp4")
        with open(video_path, "wb") as f:
            f.write(b"0123456789ABCDEF")

        data = read_video_range(video_path, 4, 7)
        assert data == b"4567"

    def test_stream_with_range_header(self, temp_storage: str) -> None:
        """Should return 206 Partial Content when a Range header is present."""
        video_path = os.path.join(temp_storage, "stream_test.mp4")
        with open(video_path, "wb") as f:
            f.write(b"A" * 1000)

        info = build_stream_response_info(video_path, "bytes=0-499")
        assert info["status_code"] == 206
        assert len(info["data"]) == 500
        assert "Content-Range" in info["headers"]

    def test_stream_without_range_header(self, temp_storage: str) -> None:
        """Should return 200 OK with the full file when no Range header is present."""
        video_path = os.path.join(temp_storage, "stream_full.mp4")
        with open(video_path, "wb") as f:
            f.write(b"B" * 500)

        info = build_stream_response_info(video_path, None)
        assert info["status_code"] == 200
        assert len(info["data"]) == 500

    def test_get_video_path_transcoded(self, temp_storage: str) -> None:
        """Should return the transcoded file first."""
        video_id = "path-test"
        transcoded = os.path.join(
            temp_storage, "transcoded", video_id, "720p.mp4"
        )
        os.makedirs(os.path.dirname(transcoded), exist_ok=True)
        with open(transcoded, "w") as f:
            f.write("transcoded")

        path = get_video_path(video_id, "720p")
        assert path == transcoded

    def test_get_video_path_original_fallback(self, temp_storage: str) -> None:
        """Should fall back to the original file when no transcoded file exists."""
        video_id = "fallback-test"
        original = os.path.join(temp_storage, "originals", f"{video_id}.mp4")
        os.makedirs(os.path.dirname(original), exist_ok=True)
        with open(original, "w") as f:
            f.write("original")

        path = get_video_path(video_id, "720p")
        assert path == original

    def test_get_video_path_not_found(self, temp_storage: str) -> None:
        """Should return None when the video file does not exist."""
        path = get_video_path("nonexistent", "720p")
        assert path is None


# ---------------------------------------------------------------------------
# Integration: Upload → Transcode → Stream
# ---------------------------------------------------------------------------


class TestIntegration:
    """Integration tests from upload through streaming."""

    @pytest.mark.asyncio
    async def test_full_lifecycle(
        self, redis_client: fakeredis.aioredis.FakeRedis, temp_storage: str
    ) -> None:
        """Test the full flow: upload → transcoding → streaming."""
        # 1. Initiate upload
        upload = await initiate_upload(redis_client, "Full Test", "desc", 2)
        upload_id = upload["upload_id"]
        video_id = upload["video_id"]

        # Create metadata
        await create_video_metadata(redis_client, video_id, "Full Test", "desc")

        # 2. Upload chunks
        await upload_chunk(redis_client, upload_id, 0, b"chunk-0-content-")
        await upload_chunk(redis_client, upload_id, 1, b"chunk-1-content")

        # 3. Complete upload
        complete = await complete_upload(redis_client, upload_id)
        assert "error" not in complete

        # 4. Transcoding
        transcode = await transcode_video(
            redis_client, video_id, complete["file_path"]
        )
        assert transcode["status"] == "ready"

        # 5. Verify metadata
        meta = await get_video_metadata(redis_client, video_id)
        assert meta is not None
        assert meta["status"] == "ready"
        assert "720p" in meta["resolutions"]

        # 6. Verify streaming file
        video_path = get_video_path(video_id, "720p")
        assert video_path is not None

        # 7. Range streaming
        stream = build_stream_response_info(video_path, "bytes=0-9")
        assert stream["status_code"] == 206
        assert len(stream["data"]) == 10
