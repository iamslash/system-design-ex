# Design Google Drive

Google Drive 는 파일을 클라우드에 저장하고, 여러 디바이스 간 동기화하며,
공유할 수 있는 서비스다. 블록 단위 저장, 중복 제거(dedup), 델타 동기화,
버전 관리가 핵심이다.

## 아키텍처

```
 ┌──────────┐  ┌──────────┐  ┌──────────┐
 │ Client A │  │ Client B │  │ Client C │
 └────┬─────┘  └────┬─────┘  └────┬─────┘
      │              │              │
      ▼              ▼              ▼
 ┌─────────────────────────────────────────┐
 │           Load Balancer                 │
 └──────────────────┬──────────────────────┘
                    │
                    ▼
 ┌─────────────────────────────────────────┐
 │         API Servers (FastAPI)           │
 │                                         │
 │  ┌──────────────┐  ┌────────────────┐   │
 │  │ File Manager  │  │ Block Server   │   │
 │  │ (Upload/     │  │ (Split/Hash/   │   │
 │  │  Download)   │  │  Compress/     │   │
 │  └──────────────┘  │  Dedup)        │   │
 │  ┌──────────────┐  └────────────────┘   │
 │  │ Versioning   │  ┌────────────────┐   │
 │  │ (History/    │  │ Notification   │   │
 │  │  Restore)    │  │ (Long Polling) │   │
 │  └──────────────┘  └────────────────┘   │
 └──────────┬──────────────┬───────────────┘
            │              │
            ▼              ▼
 ┌─────────────────┐  ┌───────────────────┐
 │  Block Storage   │  │  Metadata DB      │
 │  (Filesystem)   │  │  (Redis)          │
 │                  │  │                   │
 │  /data/blocks/   │  │  file:{id}        │
 │   {sha256_hash}  │  │  file_version:... │
 │                  │  │  block:{hash}     │
 │  (압축된 블록)    │  │  user_files:...   │
 └─────────────────┘  │  sync_events:...  │
                      └───────────────────┘
```

### 요청 흐름

1. 클라이언트가 파일을 업로드하면 API 서버가 수신한다.
2. Block Server 가 파일을 4KB 블록으로 분할하고 각 블록을 SHA-256 해시한다.
3. 이미 존재하는 해시(중복 블록)는 건너뛰고, 새 블록만 zlib 압축 후 저장한다.
4. 메타데이터(파일 정보, 버전, 블록 목록)를 Redis 에 기록한다.
5. 동기화 이벤트를 발행하여 다른 디바이스에 변경을 통보한다.

## Back-of-the-envelope Estimation

| 항목 | 수치 |
|------|------|
| DAU | 10M |
| 사용자당 평균 파일 수 | 200 |
| 평균 파일 크기 | 500KB |
| 총 저장 용량 | 10M x 200 x 500KB = **1PB** (dedup 전) |
| Dedup 후 예상 | ~500PB (누적, 중복 제거로 30-50% 절감) |
| 일일 업로드 | 10M x 2 files/day = 20M files |
| QPS (Upload) | 20M / 86400 ≈ **~230 QPS** |
| 피크 QPS | ~230 x 3 ≈ **~700 QPS** |

## 핵심 개념

### Block Storage (블록 저장)

파일을 통째로 저장하는 대신 고정 크기(4KB) 블록으로 분할하여 저장한다.

| 장점 | 설명 |
|------|------|
| **중복 제거 (Dedup)** | 동일 내용의 블록은 해시가 같으므로 한 번만 저장 |
| **델타 동기화** | 파일 수정 시 변경된 블록만 업로드 → 대역폭 절약 |
| **압축** | 블록 단위 zlib 압축으로 저장 공간 절약 |
| **버전 관리** | 버전별 블록 해시 목록만 저장하면 됨 |

### Delta Sync (델타 동기화)

파일을 수정하면 전체 파일이 아니라 변경된 블록만 업로드한다:

```
원본 파일:   [Block A] [Block B] [Block C] [Block D]
수정 파일:   [Block A] [Block B'] [Block C] [Block D']
                        ^^^^^^^^             ^^^^^^^^
                        변경됨               변경됨

업로드되는 블록: Block B', Block D' (2개만 전송)
재사용 블록:     Block A, Block C   (서버에 이미 존재)
```

### Sync Conflict Resolution (동기화 충돌 해결)

두 명의 사용자가 동시에 같은 파일을 수정하면 충돌이 발생한다.
**First Writer Wins** 전략을 사용한다:

```
Alice (v1 기반 수정)          Server (v1)          Bob (v1 기반 수정)
     │                         │                      │
     │── upload v2 ───────────▶│                      │
     │                         │ (v1 → v2 반영)       │
     │◀── OK (v2) ────────────│                      │
     │                         │                      │
     │                         │◀── upload v2 ────────│
     │                         │ 충돌! server=v2,     │
     │                         │ bob expects v1       │
     │                         │── 409 Conflict ─────▶│
     │                         │                      │
     │                         │  Bob 은 v2 를 받아   │
     │                         │  merge 후 v3 업로드  │
```

## 핵심 구현

### 1. Block Server (`storage/block_server.py`)

파일을 블록으로 분할하고, SHA-256 해시로 중복을 검사하며,
zlib 압축 후 저장한다. 동일 해시의 블록은 한 번만 저장된다.

```python
def split_into_blocks(data: bytes, block_size: int | None = None) -> list[bytes]:
    """파일 데이터를 고정 크기 블록으로 분할한다.

    Args:
        data: 원본 파일 바이트 데이터
        block_size: 블록 크기 (기본값: 4096 바이트)

    Returns:
        블록 리스트 (마지막 블록은 block_size 보다 작을 수 있다)
    """
    if block_size is None:
        block_size = settings.BLOCK_SIZE
    blocks: list[bytes] = []
    for i in range(0, len(data), block_size):
        blocks.append(data[i : i + block_size])
    return blocks


def compute_block_hash(block: bytes) -> str:
    """블록의 SHA-256 해시를 계산한다.

    동일한 내용의 블록은 항상 동일한 해시를 반환하므로
    중복 검사(dedup)에 사용된다.
    """
    return hashlib.sha256(block).hexdigest()


def compress_block(block: bytes) -> bytes:
    """블록을 zlib 으로 압축한다."""
    return zlib.compress(block)


async def store_block(
    redis: Redis,
    block: bytes,
    storage_path: str | None = None,
) -> tuple[str, bool]:
    """블록을 저장한다. 이미 존재하는 블록이면 건너뛴다 (dedup).

    Returns:
        (block_hash, is_new) — 해시값과 새로 저장되었는지 여부
    """
    block_hash = compute_block_hash(block)

    # 중복 검사: Redis 에 해시가 이미 있으면 저장하지 않는다
    exists = await redis.exists(f"block:{block_hash}")
    if exists:
        return block_hash, False

    # 압축 후 파일시스템에 저장
    compressed = compress_block(block)
    block_path = os.path.join(storage_path, block_hash)
    os.makedirs(storage_path, exist_ok=True)
    with open(block_path, "wb") as f:
        f.write(compressed)

    # Redis 에 블록 메타데이터 기록
    await redis.hset(
        f"block:{block_hash}",
        mapping={
            "original_size": str(len(block)),
            "compressed_size": str(len(compressed)),
        },
    )
    return block_hash, True
```

### 2. File Manager (`storage/file_manager.py`)

업로드: 파일 분할 → 블록 저장(dedup) → 버전 생성 → 메타데이터 갱신.
다운로드: 메타데이터 조회 → 블록 재조립 → 파일 반환.

```python
async def upload_file(
    redis: Redis,
    filename: str,
    data: bytes,
    user_id: str,
    storage_path: str | None = None,
) -> dict[str, Any]:
    """파일을 업로드한다.

    처리 흐름:
      1. 파일을 블록으로 분할
      2. 각 블록을 저장 (dedup — 동일 해시의 블록은 건너뜀)
      3. 새 버전 생성 (블록 해시 목록 저장)
      4. 파일 메타데이터 갱신
      5. 동기화 이벤트 발행
    """
    now = datetime.now(timezone.utc).isoformat()

    # 1. 파일을 블록으로 분할
    blocks = split_into_blocks(data)

    # 2. 각 블록 저장 (dedup 적용)
    block_hashes: list[str] = []
    new_blocks = 0
    reused_blocks = 0
    for block in blocks:
        block_hash, is_new = await store_block(redis, block, storage_path)
        block_hashes.append(block_hash)
        if is_new:
            new_blocks += 1
        else:
            reused_blocks += 1

    # 3. 파일 ID 결정 (기존 파일이면 기존 ID, 신규면 새 UUID)
    file_id = await _find_file_id(redis, filename, user_id)
    if file_id is None:
        file_id = str(uuid.uuid4())
        await redis.sadd(f"user_files:{user_id}", file_id)
        version = 1
    else:
        version = int(await redis.hget(f"file:{file_id}", "latest_version") or "0") + 1

    # 4. 메타데이터 및 버전 저장
    await redis.hset(f"file:{file_id}", mapping={...})
    await create_version(redis, file_id, version, block_hashes, len(data))

    return {
        "file_id": file_id,
        "version": version,
        "total_blocks": len(block_hashes),
        "new_blocks": new_blocks,       # 새로 저장된 블록 수
        "reused_blocks": reused_blocks,  # 재사용된 블록 수 (dedup)
    }


async def download_file(
    redis: Redis,
    file_id: str,
    version: int | None = None,
    storage_path: str | None = None,
) -> tuple[str, bytes]:
    """파일을 다운로드한다.

    메타데이터에서 블록 해시 목록을 조회하고, 각 블록을 읽어
    원본 파일로 재조립한다.
    """
    file_meta = await redis.hgetall(f"file:{file_id}")
    if version is None:
        version = int(file_meta["latest_version"])

    # 버전의 블록 해시 목록 조회
    version_data = await get_version(redis, file_id, version)
    block_hashes = json.loads(version_data["block_hashes"])

    # 블록을 순서대로 읽어 재조립
    chunks: list[bytes] = []
    for block_hash in block_hashes:
        chunk = await load_block(block_hash, storage_path)
        chunks.append(chunk)

    return filename, b"".join(chunks)
```

### 3. Versioning (`storage/versioning.py`)

각 업로드마다 새 버전을 생성한다. 버전은 블록 해시 목록만 저장하므로
공간 효율적이다. 이전 버전으로 복원 시 해당 버전의 블록 해시를 새 버전으로 복사한다.

```python
async def create_version(
    redis: Redis,
    file_id: str,
    version: int,
    block_hashes: list[str],
    size: int,
) -> None:
    """파일의 새 버전을 저장한다."""
    now = datetime.now(timezone.utc).isoformat()
    await redis.hset(
        f"file_version:{file_id}:{version}",
        mapping={
            "version": str(version),
            "block_hashes": json.dumps(block_hashes),
            "size": str(size),
            "block_count": str(len(block_hashes)),
            "created_at": now,
        },
    )


async def restore_version(
    redis: Redis,
    file_id: str,
    target_version: int,
) -> dict[str, Any]:
    """파일을 특정 버전으로 복원한다.

    복원은 대상 버전의 블록 해시 목록을 새 버전으로 복사하는 방식.
    기존 버전 히스토리는 유지되며, 복원 자체도 새 버전으로 기록된다.
    """
    file_meta = await redis.hgetall(f"file:{file_id}")
    target_data = await get_version(redis, file_id, target_version)

    # 대상 버전의 블록 해시를 새 버전으로 복사
    new_version = int(file_meta["latest_version"]) + 1
    block_hashes = json.loads(target_data["block_hashes"])
    await create_version(redis, file_id, new_version, block_hashes, int(target_data["size"]))

    # 파일 메타데이터 갱신
    await redis.hset(f"file:{file_id}", mapping={
        "latest_version": str(new_version),
        "size": target_data["size"],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })

    return {
        "restored_from": target_version,
        "new_version": new_version,
        "message": f"Restored from v{target_version} as v{new_version}",
    }
```

### 4. Notification Service (`sync/notification.py`)

Long-polling 방식으로 파일 변경 이벤트를 클라이언트에 전달한다.
클라이언트가 `/sync/poll` 에 요청하면 이벤트가 있을 때까지 최대 30초 대기한다.

```python
async def publish_sync_event(
    redis: Redis,
    user_id: str,
    event: dict[str, Any],
) -> None:
    """동기화 이벤트를 사용자의 이벤트 큐에 발행한다."""
    await redis.lpush(f"sync_events:{user_id}", json.dumps(event))
    # 최대 1000개 이벤트만 유지
    await redis.ltrim(f"sync_events:{user_id}", 0, 999)


async def poll_sync_events(
    redis: Redis,
    user_id: str,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """사용자의 동기화 이벤트를 long-polling 으로 조회한다.

    이미 이벤트가 있으면 즉시 반환한다.
    이벤트가 없으면 최대 timeout 초 동안 새 이벤트를 기다린다.
    """
    if timeout is None:
        timeout = settings.POLL_TIMEOUT

    key = f"sync_events:{user_id}"

    # 이미 이벤트가 있으면 즉시 반환
    events = await _drain_events(redis, key)
    if events:
        return events

    # 이벤트가 없으면 polling 으로 대기
    elapsed = 0
    while elapsed < timeout:
        await asyncio.sleep(1)
        elapsed += 1
        events = await _drain_events(redis, key)
        if events:
            return events

    return []
```

### 5. Conflict Resolution (`sync/conflict.py`)

동시에 같은 파일을 수정하면 충돌이 발생한다.
First Writer Wins 전략으로 먼저 업로드한 버전이 반영된다.

```python
async def check_conflict(
    redis: Redis,
    file_id: str,
    expected_version: int,
) -> dict[str, Any] | None:
    """파일의 현재 버전과 클라이언트가 기대하는 버전을 비교한다.

    클라이언트가 "나는 v2 기반으로 수정했다" 라고 보냈는데
    서버의 최신 버전이 v3 이면 충돌이 발생한 것이다.

    Returns:
        충돌 정보 딕셔너리 (충돌 없으면 None)
    """
    file_meta = await redis.hgetall(f"file:{file_id}")
    if not file_meta:
        return None  # 파일이 없으면 충돌 아님 (새 파일)

    server_version = int(file_meta.get("latest_version", "0"))

    if server_version > expected_version:
        return {
            "conflict": True,
            "file_id": file_id,
            "message": f"Conflict: server has v{server_version}, "
                       f"you expected v{expected_version}",
            "your_version": expected_version,
            "server_version": server_version,
            "server_updated_at": file_meta.get("updated_at", ""),
            "server_updated_by": file_meta.get("user_id", ""),
        }

    return None
```

## Upload 흐름

```
Client                          API Server                    Block Storage    Redis
  │                                │                              │              │
  │── POST /files/upload ─────────▶│                              │              │
  │   (file multipart)             │                              │              │
  │                                │  1. split_into_blocks(4KB)   │              │
  │                                │  2. for each block:          │              │
  │                                │     hash = SHA-256(block)    │              │
  │                                │     exists? ─────────────────┼─── GET ─────▶│
  │                                │                              │◀── yes/no ───│
  │                                │     if new:                  │              │
  │                                │       compress(zlib) ────────▶│ save         │
  │                                │       record hash ───────────┼─── SET ─────▶│
  │                                │                              │              │
  │                                │  3. create_version(hashes)   │              │
  │                                │     ────────────────────────────── HSET ───▶│
  │                                │  4. update file metadata     │              │
  │                                │     ────────────────────────────── HSET ───▶│
  │                                │  5. publish_sync_event       │              │
  │                                │     ────────────────────────────── LPUSH ──▶│
  │                                │                              │              │
  │◀── {file_id, version,          │                              │              │
  │     new_blocks, reused_blocks} │                              │              │
```

## Download 흐름

```
Client                          API Server                    Block Storage    Redis
  │                                │                              │              │
  │── GET /files/{id}/download ───▶│                              │              │
  │                                │  1. get file metadata ───────┼─── HGETALL ─▶│
  │                                │                              │◀── metadata ─│
  │                                │  2. get version block list   │              │
  │                                │     ────────────────────────────── HGETALL ─▶│
  │                                │                              │◀── hashes ───│
  │                                │  3. for each hash:           │              │
  │                                │     load_block(hash) ────────▶│ read         │
  │                                │◀── decompressed block ───────│              │
  │                                │                              │              │
  │                                │  4. join(blocks) = file      │              │
  │◀── file content ──────────────│                              │              │
```

## Notification Service

### Long Polling vs WebSocket

| 방식 | 동작 | 지연 | 서버 부하 | 구현 복잡도 |
|------|------|------|----------|------------|
| Polling | 주기적으로 서버에 요청 | 높음 | 높음 (불필요한 요청) | 낮음 |
| **Long Polling** | 서버가 이벤트 있을 때까지 응답 보류 | **중간** | **중간** | **중간** |
| WebSocket | 양방향 실시간 연결 | 낮음 | 낮음 | 높음 |

Google Drive 의 알림은 실시간 양방향 통신이 필요하지 않으므로
**Long Polling** 이 적합하다. 클라이언트가 `/sync/poll` 에 요청하면
새 이벤트가 있을 때까지 최대 30초 대기한 뒤 응답한다.

```
Client                            Server                          Redis
  │                                 │                               │
  │── GET /sync/poll ──────────────▶│                               │
  │   (user_id=alice)               │                               │
  │                                 │── RPOP sync_events:alice ────▶│
  │                                 │◀── (empty) ──────────────────│
  │                                 │                               │
  │         ... 대기 (최대 30초) ... │                               │
  │                                 │                               │
  │                                 │  (다른 요청이 이벤트 발행)       │
  │                                 │── RPOP sync_events:alice ────▶│
  │                                 │◀── event data ───────────────│
  │                                 │                               │
  │◀── {events: [...]} ────────────│                               │
```

## 저장 공간 절약 전략

### 1. Block-level Deduplication (블록 중복 제거)

```
파일 A: [Block 1] [Block 2] [Block 3]
파일 B: [Block 1] [Block 2] [Block 4]   ← Block 1, 2 는 A 와 동일
파일 C: [Block 1] [Block 5] [Block 3]   ← Block 1 은 A 와, Block 3 도 A 와 동일

저장되는 고유 블록: Block 1, 2, 3, 4, 5  (5개)
Dedup 없이 저장:   9개 블록 (파일별 3개씩)
절약률: 44%
```

### 2. 압축 (Compression)

블록 단위 zlib 압축으로 텍스트 파일은 50-90% 크기 감소:

```
원본 블록 4096 bytes (반복 텍스트)
  → 압축 후 ~200 bytes (95% 절감)

원본 블록 4096 bytes (바이너리)
  → 압축 후 ~3800 bytes (7% 절감)
```

### 3. Intelligent Backup Strategy

| 전략 | 설명 |
|------|------|
| 최신 N 개 버전 유지 | 오래된 버전 자동 삭제 |
| Cold Storage 이동 | 30일 미접근 파일을 저렴한 스토리지로 |
| 참조 카운팅 | 어떤 파일에서도 참조되지 않는 블록만 GC |

## Redis 데이터 구조

```
file:{file_id}                    (Hash)
  ├── file_id
  ├── filename
  ├── user_id
  ├── latest_version
  ├── size
  ├── created_at
  └── updated_at

file_version:{file_id}:{version}  (Hash)
  ├── version
  ├── block_hashes (JSON array)
  ├── size
  ├── block_count
  └── created_at

block:{sha256_hash}               (Hash)
  ├── original_size
  └── compressed_size

user_files:{user_id}              (Set)
  └── [file_id, file_id, ...]

sync_events:{user_id}             (List)
  └── [event_json, event_json, ...]
```

## API Endpoints

| Method | Path | 설명 |
|--------|------|------|
| `POST` | `/api/v1/files/upload` | 파일 업로드 (multipart) |
| `GET` | `/api/v1/files/{file_id}/download` | 파일 다운로드 |
| `GET` | `/api/v1/files/{file_id}` | 파일 메타데이터 조회 |
| `GET` | `/api/v1/files/{file_id}/revisions` | 버전 히스토리 조회 |
| `POST` | `/api/v1/files/{file_id}/restore/{version}` | 특정 버전으로 복원 |
| `DELETE` | `/api/v1/files/{file_id}` | 파일 삭제 |
| `GET` | `/api/v1/files` | 사용자 파일 목록 |
| `GET` | `/api/v1/sync/poll` | Long-polling 변경 알림 |
| `GET` | `/health` | 헬스 체크 |

## Quick Start

### Docker 실행

```bash
cd 16-design-google-drive
docker-compose up --build
```

서비스가 `http://localhost:8016` 에서 실행된다.

### CLI 사용법

```bash
# 헬스 체크
python scripts/cli.py --health

# 파일 업로드
python scripts/cli.py upload myfile.txt --user alice

# 같은 파일 재업로드 (delta sync — 변경된 블록만 저장)
python scripts/cli.py upload myfile.txt --user alice

# 파일 다운로드
python scripts/cli.py download <file_id> -o downloaded.txt

# 사용자 파일 목록
python scripts/cli.py list --user alice

# 버전 히스토리
python scripts/cli.py revisions <file_id>

# 특정 버전으로 복원
python scripts/cli.py restore <file_id> --version 1

# 파일 삭제
python scripts/cli.py delete <file_id>

# 동기화 이벤트 polling
python scripts/cli.py poll --user alice --timeout 5
```

### 샘플 출력

```
$ python scripts/cli.py upload README.md --user alice
[OK] Uploaded README.md v1: 3 new blocks, 0 reused blocks
  File ID: a1b2c3d4-e5f6-...
  Version: 1
  Size: 10240 bytes
  Total blocks: 3
  New blocks: 3
  Reused blocks: 0

$ python scripts/cli.py upload README.md --user alice
[OK] Uploaded README.md v2: 1 new blocks, 2 reused blocks
  File ID: a1b2c3d4-e5f6-...
  Version: 2
  Size: 10500 bytes
  Total blocks: 3
  New blocks: 1
  Reused blocks: 2

$ python scripts/cli.py list --user alice
User: alice (1 files)
  a1b2c3d4... README.md (v2, 10500 bytes)

$ python scripts/cli.py revisions a1b2c3d4-e5f6-...
File: a1b2c3d4... (2 versions)
  v1: 10240 bytes, 3 blocks, 2024-01-01T00:00:00
  v2: 10500 bytes, 3 blocks, 2024-01-01T00:01:00
```

### 테스트 실행

```bash
# 의존성 설치 (로컬)
pip install -r api/requirements.txt fakeredis pytest pytest-asyncio

# 테스트 실행 (Docker 불필요)
python -m pytest tests/ -v
```

```
tests/test_drive.py::TestBlockSplitting::test_split_exact_blocks PASSED
tests/test_drive.py::TestBlockSplitting::test_split_with_remainder PASSED
tests/test_drive.py::TestBlockSplitting::test_split_empty_file PASSED
tests/test_drive.py::TestBlockHashing::test_same_content_same_hash PASSED
tests/test_drive.py::TestBlockHashing::test_different_content_different_hash PASSED
tests/test_drive.py::TestBlockCompression::test_compression_reduces_size PASSED
tests/test_drive.py::TestBlockStorage::test_store_new_block PASSED
tests/test_drive.py::TestBlockStorage::test_dedup_stores_once PASSED
tests/test_drive.py::TestFileManager::test_upload_and_download_roundtrip PASSED
tests/test_drive.py::TestFileManager::test_delta_sync_reuses_blocks PASSED
tests/test_drive.py::TestFileManager::test_delta_sync_partial_reuse PASSED
tests/test_drive.py::TestFileManager::test_empty_file_upload PASSED
tests/test_drive.py::TestFileListing::test_list_user_files PASSED
tests/test_drive.py::TestFileListing::test_delete_file PASSED
tests/test_drive.py::TestVersioning::test_multiple_versions PASSED
tests/test_drive.py::TestVersioning::test_revision_history PASSED
tests/test_drive.py::TestVersioning::test_restore_previous_version PASSED
tests/test_drive.py::TestVersioning::test_download_specific_version PASSED
tests/test_drive.py::TestNotification::test_publish_and_poll_events PASSED
tests/test_drive.py::TestNotification::test_poll_returns_empty_on_timeout PASSED
tests/test_drive.py::TestNotification::test_upload_triggers_sync_event PASSED
tests/test_drive.py::TestConflictDetection::test_conflict_on_stale_version PASSED
tests/test_drive.py::TestConflictDetection::test_no_conflict_on_new_file PASSED
tests/test_drive.py::TestConflictDetection::test_resolve_first_writer_wins PASSED
tests/test_drive.py::TestFileMetadata::test_metadata_stored_on_upload PASSED
========================== 37 passed in 2.21s ==================================
```
