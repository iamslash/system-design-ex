# S3-like Object Storage

Amazon S3 와 유사한 오브젝트 스토리지를 간소화하여 구현한다. FastAPI + Redis (메타데이터) + 파일시스템 (데이터) 구성이다.

## 실행

```bash
docker-compose up --build
# API: http://localhost:8025
# Health: http://localhost:8025/health
```

## 테스트

```bash
pip install -r api/requirements.txt
pytest tests/ -v
```

---

## 아키텍처

```
Client ──► FastAPI (API Layer)
               │
       ┌───────┴───────┐
       ▼               ▼
   Redis           Filesystem
 (metadata)       (object data)
```

**핵심 설계: 메타데이터와 데이터의 분리**

Unix 의 inode 구조와 유사하게 **메타데이터**(파일 이름, 크기, 위치 정보)와 **실제 데이터**를
분리 저장한다. 이를 통해:

- 메타데이터 조회가 빠르다 (Redis O(1) lookup)
- 데이터 저장소를 독립적으로 확장할 수 있다
- 같은 데이터를 여러 메타데이터가 참조할 수 있다 (deduplication 가능)

```python
# 메타데이터 (Redis) - inode 역할
# key: objmeta:{version_id}
{
    "bucket_name": "my-bucket",
    "object_name": "photos/cat.jpg",   # 오브젝트 키
    "object_id": "a1b2c3d4...",         # 데이터 저장소 참조 ID
    "version_id": "v1e2f3...",          # 버전 고유 ID
    "size": 1048576,
    "created_at": "1712534400.0",
    "is_delete_marker": "0"
}

# 데이터 저장소 (Filesystem) - data block 역할
# object_mapping: object_id -> (file_name, offset, size)
```

---

## 스토리지 유형 비교

| 유형 | 단위 | 접근 방식 | 예시 |
|------|------|-----------|------|
| **Block Storage** | 고정 크기 블록 | 디바이스 레벨, OS 가 파일시스템 관리 | EBS, SAN |
| **File Storage** | 파일/디렉토리 | 계층적 경로 (`/dir/file`) | NFS, EFS |
| **Object Storage** | 오브젝트 (key-value) | 플랫 네임스페이스, HTTP API | S3, GCS |

오브젝트 스토리지의 장점:
- **무한 확장**: 플랫 구조로 디렉토리 제한 없음
- **풍부한 메타데이터**: 각 오브젝트에 사용자 정의 메타데이터 첨부 가능
- **HTTP API**: 별도 클라이언트 없이 REST 로 접근

---

## Append-Only 파일 저장소

여러 오브젝트를 하나의 큰 파일에 순차적으로 기록한다. 개별 파일 대비 장점:

- inode 소비를 줄인다 (수십억 개의 작은 파일 문제 해결)
- Sequential write 로 디스크 성능이 좋다
- 파일 열기/닫기 오버헤드가 적다

```python
@dataclass
class ObjectLocation:
    """데이터 파일 내 오브젝트의 위치 정보."""
    file_name: str   # 어떤 데이터 파일에 있는지
    offset: int      # 파일 내 시작 위치
    size: int        # 오브젝트 크기 (바이트)


class DataStore:
    """Append-only 파일 기반 데이터 저장소.

    object_mapping 딕셔너리가 object_id -> ObjectLocation 을
    매핑하여 O(1) 검색을 제공한다.
    """

    def put(self, data: bytes) -> str:
        # 현재 파일에 데이터를 append
        with open(self._current_file, "ab") as f:
            f.write(data)

        # 매핑 테이블에 위치 기록
        location = ObjectLocation(
            file_name=self._current_file,
            offset=self._current_offset,
            size=len(data),
        )
        self._object_mapping[object_id] = location
        self._current_offset += len(data)
        return object_id

    def get(self, object_id: str) -> bytes | None:
        # 매핑 테이블에서 위치를 찾아 정확한 범위만 읽는다
        location = self._object_mapping.get(object_id)
        if location is None:
            return None
        with open(location.file_name, "rb") as f:
            f.seek(location.offset)
            return f.read(location.size)
```

**파일 로테이션**: 데이터 파일이 `MAX_FILE_SIZE` (기본 64 MB)를 초과하면 새 파일을 생성한다.

---

## Compaction / GC (가비지 컬렉션)

삭제된 오브젝트의 데이터는 즉시 제거되지 않는다 (append-only 특성). Compaction 이
라이브 오브젝트만 새 파일로 복사하여 공간을 회수한다.

```python
def compact(self) -> int:
    """라이브 오브젝트만 재기록하여 공간 회수."""
    reclaimed = 0
    for file_name, objects in file_objects.items():
        original_size = os.path.getsize(file_name)
        live_size = sum(loc.size for _, loc in objects)

        if live_size >= original_size:
            continue  # 죽은 데이터가 없으면 건너뜀

        # 라이브 데이터만 새 파일에 기록
        new_file = file_name + ".compact"
        with open(new_file, "wb") as wf:
            for oid, loc in sorted(objects, key=lambda x: x[1].offset):
                data = read_from(file_name, loc.offset, loc.size)
                wf.write(data)
                # 매핑 테이블 갱신
                self._object_mapping[oid] = ObjectLocation(
                    file_name=file_name,
                    offset=new_offset,
                    size=loc.size,
                )
        os.replace(new_file, file_name)
        reclaimed += original_size - live_size
    return reclaimed
```

---

## 데이터 내구성 (Durability)

실제 S3 는 99.999999999% (11 nines) 내구성을 제공한다. 두 가지 주요 전략:

### Replication (복제)

```
Primary ──write──► Replica-1
       └──write──► Replica-2
       └──write──► Replica-3
```

- 간단하고 읽기 성능이 좋다
- 저장 비용: N 배 (보통 3 배)
- 소규모 시스템에 적합

### Erasure Coding (삭제 부호화)

```
원본 데이터 → k 개 데이터 청크 + m 개 패리티 청크
예: (k=8, m=4) → 12 개 청크 중 아무 8 개로 복원 가능
```

- 저장 효율: k/(k+m) = 8/12 = 67% (복제 대비 50% 절약)
- 계산 비용이 더 높다
- 대규모 시스템에서 비용 효율적 (S3, GCS 가 사용)

---

## Multipart Upload

대용량 파일을 여러 파트로 나누어 병렬 업로드한다.

```
1. InitiateMultipartUpload → upload_id 발급
2. UploadPart (1..N) → 각 파트를 병렬로 업로드
3. CompleteMultipartUpload → 파트를 하나의 오브젝트로 결합

파트 1 ──┐
파트 2 ──┼──► CompleteMultipart ──► 하나의 오브젝트
파트 3 ──┘
```

장점:
- 네트워크 대역폭을 최대한 활용 (병렬 전송)
- 실패한 파트만 재전송 (전체 재업로드 불필요)
- 일시 중지/재개 가능

> 이 구현에서는 간소화를 위해 단일 PUT 업로드만 지원한다.

---

## 버전 관리 (Versioning)

버킷 단위로 버전 관리를 활성화할 수 있다. 같은 키에 여러 버전을 유지하여 실수로 인한
덮어쓰기나 삭제를 방지한다.

```python
# 버전 관리 활성화 시 삭제 흐름
async def delete_object_meta(self, bucket_name, object_name):
    if versioning:
        # 실제 데이터는 보존, delete marker 를 현재 버전으로 설정
        meta = {
            "is_delete_marker": "1",
            "object_id": "",       # 데이터 없음
            "size": "0",
        }
        # version list 에 추가 (최신 순)
        await self._r.lpush(versions_key, version_id)
        # current 포인터를 delete marker 로 갱신
        await self._r.set(current_key, version_id)
    else:
        # 버전 관리 비활성화: 메타데이터 완전 삭제
        await self._r.delete(meta_key, current_key, versions_key)
```

### Delete Marker 동작

```
PUT v1 "hello"  → versions: [v1]           current → v1
PUT v2 "world"  → versions: [v2, v1]       current → v2
DELETE           → versions: [dm, v2, v1]  current → dm (delete marker)

GET latest  → 404 (delete marker)
GET v1      → "hello" (이전 버전 접근 가능)
GET v2      → "world" (이전 버전 접근 가능)
```

---

## Redis 키 스키마

```
bucket:{name}                    → hash {versioning_enabled, created_at}
bucket:list                      → set of bucket names
obj:{bucket}:{key}:current       → 최신 version_id
obj:{bucket}:{key}:versions      → list of version_ids (최신 순)
objmeta:{version_id}             → hash {bucket_name, object_name, object_id,
                                         version_id, size, created_at,
                                         is_delete_marker}
objkeys:{bucket}                 → set of object keys in the bucket
```

---

## API 엔드포인트

| Method | Path | 설명 |
|--------|------|------|
| `POST` | `/api/v1/buckets` | 버킷 생성 |
| `GET` | `/api/v1/buckets` | 버킷 목록 |
| `DELETE` | `/api/v1/buckets/{name}` | 버킷 삭제 |
| `PUT` | `/api/v1/buckets/{name}/versioning` | 버전 관리 설정 |
| `PUT` | `/api/v1/buckets/{bucket}/objects/{key}` | 오브젝트 업로드 |
| `GET` | `/api/v1/buckets/{bucket}/objects/{key}` | 오브젝트 다운로드 |
| `DELETE` | `/api/v1/buckets/{bucket}/objects/{key}` | 오브젝트 삭제 |
| `GET` | `/api/v1/buckets/{bucket}/objects` | 오브젝트 목록 (prefix 필터) |
| `GET` | `/api/v1/buckets/{bucket}/objects/{key}/versions` | 버전 목록 |

---

## CLI 사용법

```bash
# 버킷 생성
python scripts/cli.py create-bucket my-bucket

# 버전 관리 활성화
python scripts/cli.py set-versioning my-bucket enabled

# 오브젝트 업로드
python scripts/cli.py upload my-bucket photos/cat.jpg ./cat.jpg

# 오브젝트 다운로드
python scripts/cli.py download my-bucket photos/cat.jpg -o ./downloaded.jpg

# 오브젝트 목록 (prefix 필터)
python scripts/cli.py list-objects my-bucket --prefix photos/

# 버전 목록
python scripts/cli.py list-versions my-bucket photos/cat.jpg

# 오브젝트 삭제
python scripts/cli.py delete-object my-bucket photos/cat.jpg

# 버킷 삭제
python scripts/cli.py delete-bucket my-bucket
```

---

## 디렉토리 구조

```
25-design-s3-object-storage/
├── README.md
├── docker-compose.yml
├── .env.example
├── pytest.ini
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py              # FastAPI 엔드포인트
│   ├── config.py             # 환경 변수 설정
│   ├── models.py             # Pydantic 모델
│   ├── storage/
│   │   ├── data_store.py     # Append-only 파일 저장소
│   │   └── metadata.py       # Redis 메타데이터 저장소
│   ├── bucket/
│   │   └── service.py        # 버킷 CRUD 서비스
│   └── object/
│       ├── service.py        # 오브젝트 업로드/다운로드/삭제
│       └── versioning.py     # 버전 관리 서비스
├── scripts/
│   └── cli.py                # CLI 클라이언트
└── tests/
    └── test_s3.py            # 40 개 테스트
```
