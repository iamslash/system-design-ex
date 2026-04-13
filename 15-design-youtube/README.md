# Design YouTube

YouTube 와 유사한 비디오 스트리밍 시스템은 비디오 업로드, 트랜스코딩, 스트리밍의
세 가지 핵심 흐름으로 구성된다. 매일 수백만 사용자가 비디오를 시청하고 업로드하므로
대규모 스토리지, 효율적인 트랜스코딩 파이프라인, 낮은 지연시간의 스트리밍이 필수적이다.

## 아키텍처

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        Video Streaming System                                │
│                                                                              │
│  ┌──────────┐     ┌─────────┐     ┌──────────────┐     ┌────────────────┐   │
│  │          │     │         │     │              │     │                │   │
│  │  Client  │────▶│   LB    │────▶│  API Servers │────▶│Original Storage│   │
│  │          │     │         │     │  (FastAPI)   │     │   (S3/GCS)     │   │
│  └──────────┘     └─────────┘     └──────┬───────┘     └───────┬────────┘   │
│                                          │                     │            │
│                                          │                     ▼            │
│                                          │             ┌───────────────┐    │
│                                          │             │  Transcoding  │    │
│                                          │             │   Workers     │    │
│                                          │             │  (DAG-based)  │    │
│                                          │             └───────┬───────┘    │
│                                          │                     │            │
│                                          │                     ▼            │
│                                          │             ┌───────────────┐    │
│                                    ┌─────┴─────┐       │  Transcoded   │    │
│                                    │   Redis   │       │   Storage     │    │
│                                    │ (Metadata)│       └───────┬───────┘    │
│                                    └───────────┘               │            │
│                                                                ▼            │
│                                                        ┌───────────────┐    │
│                                                        │     CDN       │    │
│  ┌──────────┐                                          │ (CloudFront/  │    │
│  │  Client  │◀─────────────────────────────────────────│  Akamai)      │    │
│  │ (재생)   │                                          └───────────────┘    │
│  └──────────┘                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Back-of-the-Envelope 추정

| 항목 | 수치 |
|------|------|
| DAU | 5,000,000 (5백만) |
| 일일 비디오 업로드 | 사용자의 1% = 50,000건 |
| 평균 비디오 크기 | 300MB (원본) |
| 일일 스토리지 증가 | 50,000 x 300MB = **~15TB/일** (원본) |
| 트랜스코딩 후 | 원본 x 3해상도 x ~3배 = **~150TB/일** |
| CDN 비용 | 5M DAU x 5개/일 x 300MB x $0.02/GB = **~$150K/일** |
| 피크 업로드 QPS | 50,000 / 86,400 x 3(피크) = **~2 QPS** |
| 피크 스트리밍 QPS | 5M x 5 / 86,400 x 3 = **~870 QPS** |

## 두 가지 핵심 흐름

### 1. Video Upload Flow (비디오 업로드)

```
사용자가 비디오 업로드 요청
    │
    ▼
┌───────────────────┐
│  Upload Service   │──▶ upload_id 발급 (pre-signed URL 시뮬레이션)
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│  Chunk Upload     │──▶ 비디오를 청크 단위로 업로드 (resumable)
│  (병렬 가능)      │    각 청크를 임시 디렉토리에 저장
└───────┬───────────┘
        │ 모든 청크 업로드 완료
        ▼
┌───────────────────┐
│  Complete Upload  │──▶ 청크를 하나의 파일로 병합
└───────┬───────────┘    Original Storage 에 저장
        │
        ▼
┌───────────────────┐
│  Transcoding      │──▶ DAG 파이프라인 실행
│  Pipeline         │    360p, 720p, 1080p 생성
└───────┬───────────┘
        │
        ▼
    비디오 상태: ready
    CDN 에 배포 (실제 시스템)
```

### 2. Video Streaming Flow (비디오 스트리밍)

```
사용자가 비디오 재생 요청
    │
    ▼
┌───────────────────┐
│  CDN Edge Server  │──▶ 캐시에 있으면 바로 응답
└───────┬───────────┘
        │ 캐시 미스
        ▼
┌───────────────────┐
│  Origin Server    │──▶ Transcoded Storage 에서 파일 조회
└───────┬───────────┘
        │
        ▼
    HTTP Range 요청으로 바이트 범위 전송
    206 Partial Content 응답
        │
        ▼
    브라우저 <video> 태그가 시킹 시 Range 요청
    bytes=0-999999 → bytes=3000000-3999999
```

## 비디오 트랜스코딩

### 컨테이너 포맷과 코덱

| 구분 | 종류 | 설명 |
|------|------|------|
| **컨테이너** | MP4, HLS(.m3u8+.ts), DASH(.mpd) | 비디오/오디오/자막을 담는 그릇 |
| **비디오 코덱** | H.264, H.265(HEVC), VP9, AV1 | 프레임을 압축하는 알고리즘 |
| **오디오 코덱** | AAC, Opus | 오디오를 압축하는 알고리즘 |

### DAG 기반 트랜스코딩 파이프라인

실제 트랜스코딩은 DAG(Directed Acyclic Graph) 형태로 병렬 처리된다:

```
    원본 비디오
        │
        ▼
    ┌─────────┐
    │  Split   │  ── 비디오를 세그먼트로 분할 (GOP 단위)
    └────┬────┘
         │
    ┌────┴────────────────┬──────────────┐
    ▼                     ▼              ▼
┌─────────┐         ┌─────────┐    ┌──────────┐
│ Encode  │         │Thumbnail│    │Watermark │
│ 360p/   │         │ Extract │    │  Overlay │
│ 720p/   │         └────┬────┘    └────┬─────┘
│ 1080p   │              │              │
└────┬────┘              │              │
     │              ┌────┘              │
     ▼              ▼                   ▼
    ┌─────────────────────────────────────┐
    │            Assemble                  │
    │  다양한 해상도 + 썸네일 + 워터마크    │
    └──────────────────────────────────────┘
```

### 트랜스코딩 구현 (`video/transcode.py`)

```python
# DAG 노드 정의 — 각 단계는 독립적 태스크이다

def _dag_split(source_path: str, work_dir: str) -> dict[str, Any]:
    """Split 단계: 원본 비디오를 세그먼트로 분할한다.

    실제로는 GOP(Group of Pictures) 단위로 비디오를 분할한다.
    시뮬레이션에서는 원본 파일의 내용을 읽어 작업 디렉토리에 복사한다.
    """
    split_dir = os.path.join(work_dir, "segments")
    os.makedirs(split_dir, exist_ok=True)

    content = b""
    if os.path.exists(source_path):
        with open(source_path, "rb") as f:
            content = f.read()

    # 세그먼트로 분할 (시뮬레이션: 단일 세그먼트)
    segment_path = os.path.join(split_dir, "segment_000.dat")
    with open(segment_path, "wb") as f:
        f.write(content)

    return {"stage": "split", "segments": [segment_path], "segment_count": 1}


def _dag_encode(
    segments: list[str], work_dir: str, resolution: str,
) -> dict[str, Any]:
    """Encode 단계: 세그먼트를 특정 해상도로 인코딩한다.

    실제로는 H.264/H.265 코덱으로 인코딩한다:
      - 컨테이너 포맷: MP4, HLS(.m3u8 + .ts), DASH(.mpd)
      - 비디오 코덱: H.264 (호환성), H.265/HEVC (효율성), VP9, AV1
      - 오디오 코덱: AAC, Opus
    """
    # 해상도별 비트레이트 매핑
    bitrate_map = {"360p": "800kbps", "720p": "2500kbps", "1080p": "5000kbps"}

    encoded_path = os.path.join(work_dir, "encoded", f"video_{resolution}.mp4")
    header = (
        f"[Encoded Video]\n"
        f"Resolution: {resolution}\n"
        f"Bitrate: {bitrate_map.get(resolution, 'unknown')}\n"
        f"Codec: H.264 (simulated)\n"
    ).encode()

    with open(encoded_path, "wb") as f:
        f.write(header + original_content)

    return {"stage": "encode", "resolution": resolution, "output_path": encoded_path}


def _dag_thumbnail(source_path: str, work_dir: str) -> dict[str, Any]:
    """Thumbnail 단계: 비디오에서 썸네일 이미지를 추출한다."""
    thumb_path = os.path.join(work_dir, "thumbnails", "thumbnail.jpg")
    with open(thumb_path, "w") as f:
        f.write("[Thumbnail Image]\nExtracted from video frame at 00:00:01\n")
    return {"stage": "thumbnail", "output_path": thumb_path}


def _dag_watermark(encoded_paths: list[str], work_dir: str) -> dict[str, Any]:
    """Watermark 단계: 인코딩된 비디오에 워터마크를 오버레이한다.
    DRM/저작권 보호의 일환으로 비디오에 워터마크를 삽입한다.
    """
    watermark_info = os.path.join(work_dir, "watermarked", "watermark_info.txt")
    with open(watermark_info, "w") as f:
        f.write("[Watermark Applied]\n")
        for path in encoded_paths:
            f.write(f"Applied to: {os.path.basename(path)}\n")
    return {"stage": "watermark", "applied_to": [os.path.basename(p) for p in encoded_paths]}


def _dag_assemble(
    work_dir: str, output_dir: str, video_id: str, resolutions: list[str],
) -> dict[str, Any]:
    """Assemble 단계: 모든 산출물을 최종 디렉토리로 취합한다.
    실제로는 CDN 에 업로드하는 단계이기도 하다.
    """
    video_output_dir = os.path.join(output_dir, video_id)
    os.makedirs(video_output_dir, exist_ok=True)
    # 인코딩된 파일 + 썸네일 복사
    ...
    return {"stage": "assemble", "video_dir": video_output_dir, "files": assembled_files}
```

### DAG 실행기 — 전체 파이프라인 실행

```python
async def transcode_video(
    redis: Redis, video_id: str, source_path: str,
) -> dict[str, Any]:
    """DAG 기반 트랜스코딩 파이프라인을 실행한다.

    파이프라인 단계:
      1. Split   — 원본을 세그먼트로 분할
      2. Encode  — 각 해상도(360p, 720p, 1080p)로 병렬 인코딩
      3. Thumbnail — 썸네일 추출 (Encode 와 병렬 가능)
      4. Watermark — 워터마크 삽입
      5. Assemble  — 최종 산출물 취합
    """
    resolutions = settings.TRANSCODE_RESOLUTIONS
    work_dir = os.path.join(settings.VIDEO_STORAGE_PATH, "work", video_id)

    # 비디오 상태를 'transcoding' 으로 갱신
    await redis.hset(f"video:{video_id}", "status", "transcoding")

    # Step 1: Split
    split_result = _dag_split(source_path, work_dir)

    # Step 2: Encode (각 해상도별 — 실제로는 병렬)
    encoded_paths = []
    for res in resolutions:
        enc = _dag_encode(split_result["segments"], work_dir, res)
        encoded_paths.append(enc["output_path"])

    # Step 3: Thumbnail (Encode 와 독립적으로 실행 가능)
    _dag_thumbnail(source_path, work_dir)

    # Step 4: Watermark
    _dag_watermark(encoded_paths, work_dir)

    # Step 5: Assemble
    _dag_assemble(work_dir, output_dir, video_id, resolutions)

    # 비디오 상태를 'ready' 로 갱신
    await redis.hset(f"video:{video_id}", mapping={
        "status": "ready",
        "resolutions": ",".join(resolutions),
    })

    return {"video_id": video_id, "status": "ready", "resolutions": resolutions}
```

## 비디오 업로드 (`video/upload.py`)

### 청크 업로드 + Resumable Upload

```python
async def initiate_upload(
    redis: Redis, title: str, description: str, total_chunks: int,
) -> dict[str, Any]:
    """업로드를 시작하고 upload_id 를 발급한다.

    Pre-signed URL 시뮬레이션: 실제 시스템에서는 S3 pre-signed URL 을
    발급하여 클라이언트가 직접 스토리지에 업로드하도록 한다.
    """
    upload_id = str(uuid.uuid4())
    video_id = str(uuid.uuid4())

    # 업로드 상태를 Redis 에 저장
    await redis.hset(f"upload:{upload_id}", mapping={
        "upload_id": upload_id,
        "video_id": video_id,
        "title": title,
        "total_chunks": str(total_chunks),
        "uploaded_chunks": "0",
        "status": "uploading",
    })

    # 청크 저장 디렉토리 생성
    chunk_dir = os.path.join(settings.VIDEO_STORAGE_PATH, "chunks", upload_id)
    os.makedirs(chunk_dir, exist_ok=True)

    return {
        "upload_id": upload_id,
        "video_id": video_id,
        "presigned_url": f"/api/v1/videos/upload/{upload_id}/chunk/{{chunk_index}}",
    }


async def upload_chunk(
    redis: Redis, upload_id: str, chunk_index: int, chunk_data: bytes,
) -> dict[str, Any]:
    """청크 하나를 업로드한다.

    Resumable upload: 이미 업로드된 청크는 덮어쓴다.
    클라이언트는 실패한 청크만 다시 업로드하면 된다.
    """
    # 청크를 파일로 저장
    chunk_path = os.path.join(chunk_dir, f"chunk_{chunk_index:05d}")
    with open(chunk_path, "wb") as f:
        f.write(chunk_data)

    # 업로드된 청크 추적 (Redis Set 사용 — 중복 방지)
    await redis.sadd(f"upload_chunks:{upload_id}", str(chunk_index))
    uploaded_count = await redis.scard(f"upload_chunks:{upload_id}")

    return {"upload_id": upload_id, "uploaded_chunks": uploaded_count}


async def complete_upload(redis: Redis, upload_id: str) -> dict[str, Any]:
    """업로드를 완료하고 청크를 하나의 파일로 병합한다.

    모든 청크가 업로드되었는지 확인한 뒤:
      1. 청크 파일들을 순서대로 읽어 하나의 파일로 합침
      2. 비디오 상태를 갱신
      3. 임시 청크 디렉토리 정리
    """
    # 청크를 하나의 파일로 병합
    with open(output_path, "wb") as outfile:
        for i in range(total_chunks):
            chunk_path = os.path.join(chunk_dir, f"chunk_{i:05d}")
            with open(chunk_path, "rb") as chunk_file:
                outfile.write(chunk_file.read())

    return {"upload_id": upload_id, "video_id": video_id, "file_path": output_path}
```

## 비디오 스트리밍 (`video/streaming.py`)

### HTTP Byte-Range Serving

```python
def parse_range_header(range_header: str | None, file_size: int) -> tuple[int, int]:
    """Range 헤더를 파싱하여 (start, end) 바이트 범위를 반환한다.

    지원하는 형식:
      - bytes=0-999        → (0, 999)
      - bytes=500-         → (500, file_size-1)
      - bytes=-500         → (file_size-500, file_size-1)
      - None (헤더 없음)   → (0, file_size-1)
    """
    if not range_header or not range_header.startswith("bytes="):
        return 0, file_size - 1

    range_spec = range_header[6:]  # "bytes=" 제거
    parts = range_spec.split("-")
    start_str, end_str = parts

    if not start_str:
        # bytes=-500 → 마지막 500바이트
        suffix_length = int(end_str)
        start = max(0, file_size - suffix_length)
        end = file_size - 1
    elif not end_str:
        # bytes=500- → 500부터 끝까지
        start = int(start_str)
        end = file_size - 1
    else:
        start = int(start_str)
        end = min(int(end_str), file_size - 1)

    return start, end


def build_stream_response_info(
    file_path: str, range_header: str | None,
) -> dict[str, Any]:
    """스트리밍 응답에 필요한 정보를 구성한다."""
    file_size = os.path.getsize(file_path)
    start, end = parse_range_header(range_header, file_size)
    data = read_video_range(file_path, start, end)

    if range_header and range_header.startswith("bytes="):
        # 206 Partial Content — 클라이언트가 시킹 시 사용
        return {
            "status_code": 206,
            "data": data,
            "headers": {
                "Content-Range": f"bytes {start}-{end}/{file_size}",
                "Accept-Ranges": "bytes",
                "Content-Length": str(end - start + 1),
                "Content-Type": "video/mp4",
            },
        }
    else:
        # 200 OK — 전체 파일 전송
        return {
            "status_code": 200,
            "data": data,
            "headers": {
                "Accept-Ranges": "bytes",
                "Content-Length": str(file_size),
                "Content-Type": "video/mp4",
            },
        }
```

## 메타데이터 저장소 (`metadata/store.py`)

### Redis 기반 비디오 메타데이터 CRUD

```python
async def create_video_metadata(
    redis: Redis, video_id: str, title: str, description: str = "",
) -> dict[str, Any]:
    """비디오 메타데이터를 생성한다."""
    created_at = time.time()
    await redis.hset(f"video:{video_id}", mapping={
        "video_id": video_id,
        "title": title,
        "description": description,
        "status": "uploading",   # 초기 상태
        "resolutions": "",
        "created_at": str(created_at),
        "views": "0",
    })

    # 비디오 목록 Sorted Set 에 추가 (score = created_at)
    await redis.zadd("video_list", {video_id: created_at})

    return {"video_id": video_id, "title": title, "status": "uploading"}


async def get_video_metadata(redis: Redis, video_id: str) -> dict[str, Any] | None:
    """비디오 메타데이터를 조회한다. 조회 시 views 를 1 증가시킨다."""
    data = await redis.hgetall(f"video:{video_id}")
    if not data:
        return None

    await redis.hincrby(f"video:{video_id}", "views", 1)
    return {
        "video_id": data["video_id"],
        "title": data["title"],
        "status": data["status"],
        "resolutions": [r for r in data.get("resolutions", "").split(",") if r],
        "views": int(data.get("views", "0")) + 1,
    }


async def list_videos(redis: Redis, offset: int = 0, limit: int = 20) -> list[dict]:
    """비디오 목록을 최신순으로 조회한다.

    video_list Sorted Set 에서 역시간순으로 video_id 목록을 가져온 뒤,
    파이프라인으로 메타데이터를 일괄 조회한다.
    """
    video_ids = await redis.zrevrange("video_list", offset, offset + limit - 1)

    pipe = redis.pipeline()
    for vid in video_ids:
        pipe.hgetall(f"video:{vid}")
    results = await pipe.execute()

    return [_format_video(data) for data in results if data]
```

## Redis 데이터 구조

```
┌──────────────────────────────────────────────────────┐
│                    Redis                              │
│                                                      │
│  Video Metadata                                      │
│  ┌──────────────────────────────────────┐            │
│  │ video:{video_id}  (Hash)            │            │
│  │   title, description, status,        │            │
│  │   resolutions, views, created_at,    │            │
│  │   thumbnail, transcoded_at           │            │
│  └──────────────────────────────────────┘            │
│                                                      │
│  Video List (최신순 정렬)                             │
│  ┌──────────────────────────────────────┐            │
│  │ video_list  (Sorted Set)            │            │
│  │   score = created_at                 │            │
│  │   member = video_id                  │            │
│  └──────────────────────────────────────┘            │
│                                                      │
│  Upload State                                        │
│  ┌──────────────────────────────────────┐            │
│  │ upload:{upload_id}  (Hash)          │            │
│  │   video_id, title, total_chunks,     │            │
│  │   uploaded_chunks, status            │            │
│  └──────────────────────────────────────┘            │
│                                                      │
│  Upload Chunk Tracking                               │
│  ┌──────────────────────────────────────┐            │
│  │ upload_chunks:{upload_id}  (Set)    │            │
│  │   → 업로드된 청크 인덱스 집합        │            │
│  └──────────────────────────────────────┘            │
└──────────────────────────────────────────────────────┘
```

## 속도 최적화

### 1. 병렬 업로드 (Parallel Upload)

대용량 파일을 여러 청크로 분할하여 병렬로 업로드한다:

```
클라이언트
    │
    ├──▶ Chunk 0 ──▶ Upload Server A
    ├──▶ Chunk 1 ──▶ Upload Server B
    ├──▶ Chunk 2 ──▶ Upload Server C
    └──▶ Chunk 3 ──▶ Upload Server A
              │
              ▼
    Complete 요청 → 청크 병합
```

### 2. 가까운 업로드 센터 (Upload Centers)

사용자와 가까운 데이터센터로 업로드를 라우팅한다:

```
한국 사용자 ──▶ 서울 업로드 센터 ──▶ S3 Seoul Region
미국 사용자 ──▶ US-East 업로드 센터 ──▶ S3 US-East Region
```

### 3. 메시지 큐 기반 병렬 트랜스코딩

```
업로드 완료
    │
    ▼
┌────────────┐
│ Message    │──▶ Worker 1: Split
│ Queue      │──▶ Worker 2: Encode 360p
│ (Kafka/    │──▶ Worker 3: Encode 720p
│  SQS)      │──▶ Worker 4: Encode 1080p
└────────────┘──▶ Worker 5: Thumbnail
```

## 안전 최적화

### 1. Pre-signed URL

클라이언트가 API 서버를 거치지 않고 직접 스토리지에 업로드:

```
클라이언트 ──▶ API: "업로드 시작" ──▶ S3 Pre-signed URL 발급
     │
     └──▶ Pre-signed URL 로 S3 에 직접 업로드 (API 서버 부하 감소)
```

### 2. DRM + AES 암호화

```
원본 비디오 ──▶ AES-128 암호화 ──▶ 암호화된 비디오
                                       │
라이선스 서버 ◀── 키 요청 ◀── 클라이언트 재생기
     │
     └──▶ 복호화 키 전달 ──▶ 클라이언트에서 복호화 후 재생
```

### 3. 워터마크

비디오에 비가시적 워터마크를 삽입하여 불법 복제 추적.

## 비용 최적화

### CDN 전략

| 콘텐츠 유형 | 전략 | 이유 |
|-------------|------|------|
| 인기 콘텐츠 (상위 20%) | CDN 캐싱 | 전체 트래픽의 80% 차지, 캐시 적중률 높음 |
| 준인기 콘텐츠 | CDN + Origin 병행 | 적당한 트래픽, 비용 대비 효과적 |
| Long-tail 콘텐츠 | Origin 직접 서빙 | 접근 빈도 낮아 CDN 캐싱 비용 대비 효과 낮음 |

```
전체 비디오 중...

┌─────────────────────┐
│ 인기 (20%)          │ ──▶ CDN 에 캐싱      ($$$)
├─────────────────────┤
│ 준인기 (30%)        │ ──▶ CDN + Origin     ($$)
├─────────────────────┤
│ Long-tail (50%)     │ ──▶ Origin 직접 서빙  ($)
└─────────────────────┘
```

### 추가 비용 절감

- 특정 지역에서만 인기인 콘텐츠는 해당 리전의 CDN 에만 캐싱
- 인기 없는 비디오는 해상도를 줄여 저장 (360p만 유지)
- 짧은 비디오(<1분)는 실시간 트랜스코딩
- 오래된 비디오는 콜드 스토리지(S3 Glacier)로 이동

## Quick Start

```bash
# 서비스 시작
docker-compose up --build

# 헬스 체크
curl http://localhost:8015/health

# 비디오 업로드 시작
curl -X POST http://localhost:8015/api/v1/videos/upload \
  -H "Content-Type: application/json" \
  -d '{"title": "My Video", "description": "A test video", "total_chunks": 1}'

# 청크 업로드 (upload_id 사용)
curl -X PUT http://localhost:8015/api/v1/videos/upload/<upload_id>/chunk/0 \
  -F "file=@sample.mp4"

# 업로드 완료 + 트랜스코딩
curl -X POST http://localhost:8015/api/v1/videos/upload/<upload_id>/complete

# 비디오 메타데이터 조회
curl http://localhost:8015/api/v1/videos/<video_id>

# 비디오 스트리밍 (Range 요청)
curl -H "Range: bytes=0-1023" \
  http://localhost:8015/api/v1/videos/<video_id>/stream?resolution=720p

# 비디오 목록 조회
curl http://localhost:8015/api/v1/videos
```

## CLI 사용법

```bash
# 비디오 업로드
python scripts/cli.py upload sample.mp4 --title "My Video"

# 비디오 상태 조회
python scripts/cli.py status <video_id>

# 비디오 목록 조회
python scripts/cli.py list

# 비디오 스트리밍 (첫 1KB 다운로드)
python scripts/cli.py stream <video_id> --resolution 720p

# 헬스 체크
python scripts/cli.py --health
```

### CLI 출력 예시

```
$ python scripts/cli.py upload sample.mp4 --title "My Video"
Uploading: sample.mp4
  Title: My Video
  Size: 1234 bytes (1 chunks)
  Upload ID: a1b2c3d4-e5f6-7890-abcd-ef1234567890
  Video ID: f0e1d2c3-b4a5-6789-0fed-cba987654321
  Chunk 1/1 uploaded
Upload complete!
  Video ID: f0e1d2c3-b4a5-6789-0fed-cba987654321
  Status: ready
  Resolutions: 360p, 720p, 1080p

$ python scripts/cli.py status f0e1d2c3-b4a5-6789-0fed-cba987654321
Video: f0e1d2c3-b4a5-6789-0fed-cba987654321
  Title: My Video
  Description:
  Status: ready
  Views: 1
  Resolutions: 360p, 720p, 1080p

$ python scripts/cli.py list
Videos (1):
  [f0e1d2c3] My Video (ready) views=2 [360p, 720p, 1080p]

$ python scripts/cli.py stream f0e1d2c3-b4a5-6789-0fed-cba987654321 --resolution 720p
Stream: f0e1d2c3-b4a5-6789-0fed-cba987654321 (720p)
  HTTP Status: 206
  Received: 1024 bytes
  Preview: [Encoded Video]
  Resolution: 720p
  Bitrate: 2500kbps
  ...

$ python scripts/cli.py --health
Health: 200
  Status: ok
  Redis: 7.4.2
```

## API Endpoints

| Method | Path | 설명 |
|--------|------|------|
| `POST` | `/api/v1/videos/upload` | 업로드 시작, upload_id 발급 |
| `PUT` | `/api/v1/videos/upload/{upload_id}/chunk/{chunk_index}` | 청크 업로드 |
| `POST` | `/api/v1/videos/upload/{upload_id}/complete` | 업로드 완료 + 트랜스코딩 |
| `GET` | `/api/v1/videos/{video_id}` | 비디오 메타데이터 조회 |
| `GET` | `/api/v1/videos/{video_id}/stream` | 비디오 스트리밍 (Range 지원) |
| `GET` | `/api/v1/videos` | 비디오 목록 조회 |
| `GET` | `/health` | 헬스 체크 |

## 환경 변수

| 변수 | 기본값 | 설명 |
|---|---|---|
| `REDIS_HOST` | `redis` | Redis 호스트 |
| `REDIS_PORT` | `6379` | Redis 포트 |
| `VIDEO_STORAGE_PATH` | `/data/videos` | 비디오 파일 저장 경로 |
| `MAX_CHUNK_SIZE` | `10485760` | 청크 최대 크기 (10MB) |

## 테스트

```bash
# 의존성 설치
pip install -r api/requirements.txt

# 테스트 실행
python -m pytest tests/ -v
```

| 테스트 | 검증 내용 |
|--------|----------|
| Upload initiation | upload_id 반환, pre-signed URL, Redis 상태 저장 |
| Chunk upload | 진행률 추적, 유효하지 않은 인덱스, 존재하지 않는 업로드, resumable |
| Complete upload | 청크 병합, 미완료 청크 에러, 상태 변경 |
| Transcode DAG | 다중 해상도 생성, DAG 단계 실행, 상태 갱신, split/encode/thumbnail |
| Metadata CRUD | 생성, 조회, 조회수 증가, 상태 갱신, 목록 조회, 삭제, 상태 전이 |
| Streaming | Range 파싱, 바이트 범위 읽기, 206/200 응답, 파일 경로 탐색 |
| Integration | 업로드→트랜스코딩→스트리밍 전체 흐름 |

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide", Chapter 14
