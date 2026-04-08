"""Tests for the video streaming system.

fakeredis 를 사용하여 Redis 의존성 없이 단위 테스트를 수행한다.
임시 디렉토리를 사용하여 파일시스템 테스트를 격리한다.
"""

from __future__ import annotations

import os
import sys
import tempfile

import fakeredis.aioredis
import pytest
import pytest_asyncio

# api 디렉토리를 import 경로에 추가
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
async def redis_client():
    """Create a fake async Redis client for testing."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.flushall()
    await client.aclose()


@pytest.fixture
def temp_storage(tmp_path, monkeypatch):
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
    """업로드 시작 테스트."""

    @pytest.mark.asyncio
    async def test_initiate_upload_returns_upload_id(
        self, redis_client, temp_storage
    ) -> None:
        """업로드 시작 시 upload_id 를 반환한다."""
        result = await initiate_upload(redis_client, "Test Video", "desc", 3)
        assert "upload_id" in result
        assert "video_id" in result
        assert result["total_chunks"] == 3
        assert result["status"] == "uploading"

    @pytest.mark.asyncio
    async def test_initiate_upload_creates_presigned_url(
        self, redis_client, temp_storage
    ) -> None:
        """업로드 시작 시 pre-signed URL (시뮬레이션)을 반환한다."""
        result = await initiate_upload(redis_client, "Test Video", "", 1)
        assert "presigned_url" in result
        assert result["upload_id"] in result["presigned_url"]

    @pytest.mark.asyncio
    async def test_upload_status_saved_in_redis(
        self, redis_client, temp_storage
    ) -> None:
        """업로드 상태가 Redis 에 저장된다."""
        result = await initiate_upload(redis_client, "My Video", "desc", 2)
        upload_id = result["upload_id"]

        status = await get_upload_status(redis_client, upload_id)
        assert status is not None
        assert status["title"] == "My Video"
        assert status["total_chunks"] == 2
        assert status["uploaded_chunks"] == 0
        assert status["status"] == "uploading"


class TestChunkUpload:
    """청크 업로드 테스트."""

    @pytest.mark.asyncio
    async def test_chunk_upload_tracking(
        self, redis_client, temp_storage
    ) -> None:
        """청크 업로드 시 진행률이 추적된다."""
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
        self, redis_client, temp_storage
    ) -> None:
        """유효하지 않은 청크 인덱스는 에러를 반환한다."""
        result = await initiate_upload(redis_client, "Video", "", 2)
        upload_id = result["upload_id"]

        chunk_result = await upload_chunk(
            redis_client, upload_id, 5, b"bad-chunk"
        )
        assert "error" in chunk_result

    @pytest.mark.asyncio
    async def test_chunk_upload_nonexistent_upload(
        self, redis_client, temp_storage
    ) -> None:
        """존재하지 않는 upload_id 로 청크 업로드 시 에러를 반환한다."""
        result = await upload_chunk(
            redis_client, "nonexistent", 0, b"data"
        )
        assert "error" in result

    @pytest.mark.asyncio
    async def test_resumable_chunk_overwrites(
        self, redis_client, temp_storage
    ) -> None:
        """동일한 청크 인덱스를 다시 업로드하면 덮어쓴다 (resumable)."""
        result = await initiate_upload(redis_client, "Video", "", 2)
        upload_id = result["upload_id"]

        await upload_chunk(redis_client, upload_id, 0, b"first-attempt")
        r1 = await upload_chunk(redis_client, upload_id, 0, b"second-attempt")

        # 중복 청크여도 카운트는 1 (Set 사용)
        assert r1["uploaded_chunks"] == 1


class TestCompleteUpload:
    """업로드 완료 테스트."""

    @pytest.mark.asyncio
    async def test_complete_upload_merges_chunks(
        self, redis_client, temp_storage
    ) -> None:
        """업로드 완료 시 청크가 하나의 파일로 병합된다."""
        result = await initiate_upload(redis_client, "Video", "", 2)
        upload_id = result["upload_id"]
        video_id = result["video_id"]

        await upload_chunk(redis_client, upload_id, 0, b"AAAA")
        await upload_chunk(redis_client, upload_id, 1, b"BBBB")

        complete = await complete_upload(redis_client, upload_id)
        assert "error" not in complete
        assert complete["video_id"] == video_id

        # 병합된 파일 확인
        merged_path = os.path.join(temp_storage, "originals", f"{video_id}.mp4")
        assert os.path.exists(merged_path)
        with open(merged_path, "rb") as f:
            assert f.read() == b"AAAABBBB"

    @pytest.mark.asyncio
    async def test_complete_upload_fails_if_chunks_missing(
        self, redis_client, temp_storage
    ) -> None:
        """모든 청크가 업로드되지 않으면 완료가 실패한다."""
        result = await initiate_upload(redis_client, "Video", "", 3)
        upload_id = result["upload_id"]

        await upload_chunk(redis_client, upload_id, 0, b"data")
        # chunk 1, 2 는 업로드하지 않음

        complete = await complete_upload(redis_client, upload_id)
        assert "error" in complete
        assert "Not all chunks" in complete["error"]

    @pytest.mark.asyncio
    async def test_complete_upload_triggers_status_change(
        self, redis_client, temp_storage
    ) -> None:
        """업로드 완료 시 상태가 'completed' 로 변경된다."""
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
    """트랜스코딩 파이프라인 테스트."""

    @pytest.mark.asyncio
    async def test_transcode_produces_multiple_resolutions(
        self, redis_client, temp_storage
    ) -> None:
        """트랜스코딩 후 여러 해상도 파일이 생성된다."""
        video_id = "test-video-001"
        # 원본 비디오 파일 생성
        original_path = os.path.join(temp_storage, "originals", f"{video_id}.mp4")
        _create_sample_video(original_path, "original video content here")

        # 비디오 메타데이터 생성
        await create_video_metadata(redis_client, video_id, "Test")

        result = await transcode_video(redis_client, video_id, original_path)
        assert result["status"] == "ready"
        assert set(result["resolutions"]) == {"360p", "720p", "1080p"}

        # 각 해상도 파일이 존재하는지 확인
        for res in ["360p", "720p", "1080p"]:
            res_path = os.path.join(
                temp_storage, "transcoded", video_id, f"{res}.mp4"
            )
            assert os.path.exists(res_path), f"{res} file not found"

    @pytest.mark.asyncio
    async def test_transcode_dag_stages(
        self, redis_client, temp_storage
    ) -> None:
        """DAG 파이프라인의 모든 단계가 실행된다."""
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
        self, redis_client, temp_storage
    ) -> None:
        """트랜스코딩 완료 후 비디오 상태가 'ready' 로 갱신된다."""
        video_id = "test-video-003"
        original_path = os.path.join(temp_storage, "originals", f"{video_id}.mp4")
        _create_sample_video(original_path)

        await create_video_metadata(redis_client, video_id, "Test")
        await transcode_video(redis_client, video_id, original_path)

        data = await redis_client.hgetall(f"video:{video_id}")
        assert data["status"] == "ready"
        assert "360p" in data["resolutions"]

    def test_dag_split(self, temp_storage) -> None:
        """Split 단계가 세그먼트를 생성한다."""
        source = os.path.join(temp_storage, "test_split.mp4")
        _create_sample_video(source, "split test content")

        work_dir = os.path.join(temp_storage, "work_split")
        result = _dag_split(source, work_dir)

        assert result["stage"] == "split"
        assert result["segment_count"] == 1
        assert os.path.exists(result["segments"][0])

    def test_dag_encode(self, temp_storage) -> None:
        """Encode 단계가 지정된 해상도로 인코딩한다."""
        segment_path = os.path.join(temp_storage, "segment.dat")
        _create_sample_video(segment_path, "segment data")

        work_dir = os.path.join(temp_storage, "work_encode")
        result = _dag_encode([segment_path], work_dir, "720p")

        assert result["stage"] == "encode"
        assert result["resolution"] == "720p"
        assert os.path.exists(result["output_path"])

    def test_dag_thumbnail(self, temp_storage) -> None:
        """Thumbnail 단계가 썸네일을 생성한다."""
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
    """비디오 메타데이터 테스트."""

    @pytest.mark.asyncio
    async def test_create_metadata(self, redis_client) -> None:
        """비디오 메타데이터를 생성할 수 있다."""
        result = await create_video_metadata(
            redis_client, "vid-001", "My Video", "A description"
        )
        assert result["video_id"] == "vid-001"
        assert result["title"] == "My Video"
        assert result["status"] == "uploading"

    @pytest.mark.asyncio
    async def test_get_metadata(self, redis_client) -> None:
        """비디오 메타데이터를 조회할 수 있다."""
        await create_video_metadata(redis_client, "vid-002", "Test Video")
        result = await get_video_metadata(redis_client, "vid-002")

        assert result is not None
        assert result["video_id"] == "vid-002"
        assert result["title"] == "Test Video"

    @pytest.mark.asyncio
    async def test_get_nonexistent_metadata(self, redis_client) -> None:
        """존재하지 않는 비디오 메타데이터 조회 시 None 을 반환한다."""
        result = await get_video_metadata(redis_client, "nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_views_increment(self, redis_client) -> None:
        """메타데이터 조회 시 조회수가 증가한다."""
        await create_video_metadata(redis_client, "vid-003", "Popular")

        r1 = await get_video_metadata(redis_client, "vid-003")
        assert r1["views"] == 1

        r2 = await get_video_metadata(redis_client, "vid-003")
        assert r2["views"] == 2

    @pytest.mark.asyncio
    async def test_update_video_status(self, redis_client) -> None:
        """비디오 상태를 갱신할 수 있다."""
        await create_video_metadata(redis_client, "vid-004", "Test")
        success = await update_video_status(
            redis_client, "vid-004", "ready", resolutions="360p,720p"
        )
        assert success is True

        data = await redis_client.hgetall("video:vid-004")
        assert data["status"] == "ready"
        assert data["resolutions"] == "360p,720p"

    @pytest.mark.asyncio
    async def test_update_nonexistent_video(self, redis_client) -> None:
        """존재하지 않는 비디오 상태 갱신은 실패한다."""
        success = await update_video_status(redis_client, "ghost", "ready")
        assert success is False

    @pytest.mark.asyncio
    async def test_list_videos(self, redis_client) -> None:
        """비디오 목록을 최신순으로 조회할 수 있다."""
        await create_video_metadata(redis_client, "vid-a", "First")
        await create_video_metadata(redis_client, "vid-b", "Second")
        await create_video_metadata(redis_client, "vid-c", "Third")

        videos = await list_videos(redis_client)
        assert len(videos) == 3
        # 최신 먼저
        titles = [v["title"] for v in videos]
        assert titles[0] == "Third"

    @pytest.mark.asyncio
    async def test_delete_metadata(self, redis_client) -> None:
        """비디오 메타데이터를 삭제할 수 있다."""
        await create_video_metadata(redis_client, "vid-del", "Delete Me")
        deleted = await delete_video_metadata(redis_client, "vid-del")
        assert deleted is True

        result = await get_video_metadata(redis_client, "vid-del")
        assert result is None

    @pytest.mark.asyncio
    async def test_video_status_lifecycle(self, redis_client) -> None:
        """비디오 상태가 uploading → transcoding → ready 로 전이된다."""
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
    """비디오 스트리밍 테스트."""

    def test_parse_range_full(self) -> None:
        """Range 헤더 없을 때 전체 범위를 반환한다."""
        start, end = parse_range_header(None, 1000)
        assert start == 0
        assert end == 999

    def test_parse_range_bytes(self) -> None:
        """bytes=0-499 형식을 파싱한다."""
        start, end = parse_range_header("bytes=0-499", 1000)
        assert start == 0
        assert end == 499

    def test_parse_range_open_end(self) -> None:
        """bytes=500- 형식을 파싱한다 (끝까지)."""
        start, end = parse_range_header("bytes=500-", 1000)
        assert start == 500
        assert end == 999

    def test_parse_range_suffix(self) -> None:
        """bytes=-200 형식을 파싱한다 (마지막 200바이트)."""
        start, end = parse_range_header("bytes=-200", 1000)
        assert start == 800
        assert end == 999

    def test_read_video_range(self, temp_storage) -> None:
        """지정된 바이트 범위의 데이터를 읽는다."""
        video_path = os.path.join(temp_storage, "range_test.mp4")
        with open(video_path, "wb") as f:
            f.write(b"0123456789ABCDEF")

        data = read_video_range(video_path, 4, 7)
        assert data == b"4567"

    def test_stream_with_range_header(self, temp_storage) -> None:
        """Range 헤더가 있으면 206 Partial Content 를 반환한다."""
        video_path = os.path.join(temp_storage, "stream_test.mp4")
        with open(video_path, "wb") as f:
            f.write(b"A" * 1000)

        info = build_stream_response_info(video_path, "bytes=0-499")
        assert info["status_code"] == 206
        assert len(info["data"]) == 500
        assert "Content-Range" in info["headers"]

    def test_stream_without_range_header(self, temp_storage) -> None:
        """Range 헤더가 없으면 200 OK 로 전체 파일을 반환한다."""
        video_path = os.path.join(temp_storage, "stream_full.mp4")
        with open(video_path, "wb") as f:
            f.write(b"B" * 500)

        info = build_stream_response_info(video_path, None)
        assert info["status_code"] == 200
        assert len(info["data"]) == 500

    def test_get_video_path_transcoded(self, temp_storage) -> None:
        """트랜스코딩된 파일을 우선 반환한다."""
        video_id = "path-test"
        transcoded = os.path.join(
            temp_storage, "transcoded", video_id, "720p.mp4"
        )
        os.makedirs(os.path.dirname(transcoded), exist_ok=True)
        with open(transcoded, "w") as f:
            f.write("transcoded")

        path = get_video_path(video_id, "720p")
        assert path == transcoded

    def test_get_video_path_original_fallback(self, temp_storage) -> None:
        """트랜스코딩 파일이 없으면 원본 파일로 폴백한다."""
        video_id = "fallback-test"
        original = os.path.join(temp_storage, "originals", f"{video_id}.mp4")
        os.makedirs(os.path.dirname(original), exist_ok=True)
        with open(original, "w") as f:
            f.write("original")

        path = get_video_path(video_id, "720p")
        assert path == original

    def test_get_video_path_not_found(self, temp_storage) -> None:
        """비디오 파일이 없으면 None 을 반환한다."""
        path = get_video_path("nonexistent", "720p")
        assert path is None


# ---------------------------------------------------------------------------
# Integration: Upload → Transcode → Stream
# ---------------------------------------------------------------------------


class TestIntegration:
    """업로드부터 스트리밍까지의 통합 테스트."""

    @pytest.mark.asyncio
    async def test_full_lifecycle(
        self, redis_client, temp_storage
    ) -> None:
        """업로드 → 트랜스코딩 → 스트리밍 전체 흐름을 테스트한다."""
        # 1. 업로드 시작
        upload = await initiate_upload(redis_client, "Full Test", "desc", 2)
        upload_id = upload["upload_id"]
        video_id = upload["video_id"]

        # 메타데이터 생성
        await create_video_metadata(redis_client, video_id, "Full Test", "desc")

        # 2. 청크 업로드
        await upload_chunk(redis_client, upload_id, 0, b"chunk-0-content-")
        await upload_chunk(redis_client, upload_id, 1, b"chunk-1-content")

        # 3. 업로드 완료
        complete = await complete_upload(redis_client, upload_id)
        assert "error" not in complete

        # 4. 트랜스코딩
        transcode = await transcode_video(
            redis_client, video_id, complete["file_path"]
        )
        assert transcode["status"] == "ready"

        # 5. 메타데이터 확인
        meta = await get_video_metadata(redis_client, video_id)
        assert meta is not None
        assert meta["status"] == "ready"
        assert "720p" in meta["resolutions"]

        # 6. 스트리밍 파일 확인
        video_path = get_video_path(video_id, "720p")
        assert video_path is not None

        # 7. Range 스트리밍
        stream = build_stream_response_info(video_path, "bytes=0-9")
        assert stream["status_code"] == 206
        assert len(stream["data"]) == 10
