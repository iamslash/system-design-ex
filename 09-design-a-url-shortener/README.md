# Design A URL Shortener

URL 단축기(URL Shortener)는 긴 URL 을 짧은 URL 로 변환하는 서비스다.
예를 들어 `https://www.example.com/very/long/path?query=value` 를
`http://short.ly/aBcDeFg` 로 변환한다. 사용자가 짧은 URL 을 방문하면
원래의 긴 URL 로 리다이렉트(redirect)된다.

## 아키텍처

```
┌──────────┐     ┌──────────────┐     ┌─────────────────┐     ┌──────────────┐
│  Client  │────▶│ Load Balancer│────▶│  API Server     │────▶│    Redis     │
│ (Browser │     │              │     │  (FastAPI)       │     │              │
│  / CLI)  │◀────│              │◀────│                  │◀────│  - URL 매핑  │
└──────────┘     └──────────────┘     │  ┌────────────┐  │     │  - 카운터    │
                                      │  │ Base62     │  │     │  - 클릭 수   │
                                      │  │    OR      │  │     └──────────────┘
                                      │  │ Hash+충돌  │  │
                                      │  └────────────┘  │
                                      └─────────────────┘
```

### 요청 흐름

1. 클라이언트가 `POST /api/v1/shorten` 에 긴 URL 을 보낸다.
2. API 서버가 중복 확인 후 단축 코드를 생성한다 (Base62 또는 Hash).
3. 단축 코드와 원본 URL 매핑을 Redis 에 저장한다.
4. 클라이언트가 `GET /{short_code}` 를 요청하면 301 리다이렉트로 원본 URL 을 반환한다.

## Quick Start

```bash
# 서비스 시작
docker-compose up --build

# 헬스 체크
curl http://localhost:8009/health

# URL 단축
curl -X POST http://localhost:8009/api/v1/shorten \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.example.com/very/long/url"}'

# 리다이렉트 (브라우저에서 열기)
curl -i http://localhost:8009/aBcDeFg

# 통계 확인
curl http://localhost:8009/api/v1/stats/aBcDeFg
```

## CLI 사용법

```bash
# URL 단축
python scripts/cli.py shorten "https://www.example.com/very/long/url"

# 원본 URL 조회
python scripts/cli.py redirect aBcDeFg

# 통계 확인
python scripts/cli.py stats aBcDeFg

# 헬스 체크
python scripts/cli.py --health
```

### CLI 출력 예시

```
$ python scripts/cli.py shorten "https://www.example.com/very/long/url"
Short URL : http://localhost:8009/6LAze
Short Code: 6LAze

$ python scripts/cli.py stats 6LAze
Short Code : 6LAze
Short URL  : http://localhost:8009/6LAze
Long URL   : https://www.example.com/very/long/url
Clicks     : 3
Created At : 2024-01-15 12:34:56 UTC
```

## 개략적 규모 추정 (Back-of-the-Envelope Estimation)

| 항목 | 수치 |
|------|------|
| 일일 URL 단축 요청 | 1억 (100M) |
| 읽기/쓰기 비율 | 10:1 |
| 일일 리다이렉트 요청 | 10억 (1B) |
| 초당 쓰기 (Write QPS) | 100M / 86400 ≈ 1,160 |
| 초당 읽기 (Read QPS) | 1,160 × 10 = 11,600 |
| URL 평균 길이 | 100 bytes |
| 10년간 저장 URL 수 | 100M × 365 × 10 = 3,650억 (365B) |
| 10년간 저장 용량 | 365B × 100 bytes = **36.5 TB** |

## 해시 값 길이 계산

단축 URL 에 사용할 문자: `[0-9, a-z, A-Z]` = 62개 문자 (Base62)

| 길이 (n) | 가능한 조합 (62^n) | 충분한가? |
|----------|--------------------|-----------|
| 6 | 56.8B (568억) | 10년 365B 에 부족 |
| **7** | **3.5T (3.5조)** | **365B 을 충분히 커버** |
| 8 | 218T | 과도하게 큼 |

**62^7 = 3,521,614,606,208 (약 3.5조)** 개의 고유 URL 을 표현할 수 있으므로
10년간 3,650억 개의 URL 을 충분히 수용한다.

## API 엔드포인트

| Method | Path | 설명 |
|--------|------|------|
| `POST` | `/api/v1/shorten` | 긴 URL 을 단축 |
| `GET` | `/{short_code}` | 301 리다이렉트 |
| `GET` | `/api/v1/stats/{short_code}` | 통계 (클릭 수, 생성일) |
| `GET` | `/health` | 헬스 체크 |

### POST /api/v1/shorten

```json
// Request
{"url": "https://www.example.com/very/long/url"}

// Response
{"short_url": "http://localhost:8009/aBcDeFg", "short_code": "aBcDeFg"}
```

### GET /api/v1/stats/{short_code}

```json
{
  "short_code": "aBcDeFg",
  "long_url": "https://www.example.com/very/long/url",
  "short_url": "http://localhost:8009/aBcDeFg",
  "clicks": 42,
  "created_at": "2024-01-15 12:34:56 UTC"
}
```

## 접근법 1: Base62 변환

자동 증가 ID 를 Base62 로 인코딩하여 단축 코드를 생성한다.

```
ID (정수) → Base62 인코딩 → 단축 코드
100000001 → "6LAze"
100000002 → "6LAzf"
```

### 핵심 구현 — Base62 Encode / Decode

```python
# 62개 문자: 숫자(10) + 소문자(26) + 대문자(26)
CHARSET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
BASE = len(CHARSET)  # 62


def encode(num: int) -> str:
    """정수를 Base62 문자열로 인코딩한다."""
    if num < 0:
        raise ValueError(f"num must be non-negative, got {num}")
    if num == 0:
        return CHARSET[0]

    # 나머지를 역순으로 모아서 문자열 생성
    chars: list[str] = []
    while num > 0:
        num, remainder = divmod(num, BASE)
        chars.append(CHARSET[remainder])

    return "".join(reversed(chars))


def decode(s: str) -> int:
    """Base62 문자열을 정수로 디코딩한다."""
    num = 0
    for char in s:
        idx = CHARSET.find(char)
        if idx == -1:
            raise ValueError(f"Invalid Base62 character: {char!r}")
        num = num * BASE + idx
    return num
```

### ID 생성 — Redis INCR

```python
COUNTER_KEY = "url:id_counter"
START_VALUE = 100_000_000  # 최소 5자 이상의 코드 보장

async def next_id(redis: Redis) -> int:
    """Redis INCR 로 원자적으로 카운터를 증가시킨다."""
    exists = await redis.exists(COUNTER_KEY)
    if not exists:
        await redis.set(COUNTER_KEY, START_VALUE)
    return await redis.incr(COUNTER_KEY)
```

**장점:**
- ID 가 순차적이므로 충돌이 발생하지 않음
- 단축 코드 길이가 예측 가능

**단점:**
- 다음 단축 URL 을 예측할 수 있음 (보안 고려)
- 분산 환경에서 ID 생성기 자체가 병목이 될 수 있음

## 접근법 2: 해시 + 충돌 해결

CRC32/MD5 해시의 앞 7자를 취하고, 충돌 시 미리 정의된 문자열을 덧붙여 재해싱한다.

```
원본 URL → CRC32 해시 → 앞 7자 → DB 확인
                                    ├─ 존재하지 않음 → 사용
                                    └─ 존재함 (충돌) → URL + "!" → 재해싱
```

### 핵심 구현 — Hash + Collision Resolution

```python
SHORT_CODE_LENGTH = 7

# 충돌 시 원본 URL 에 덧붙일 문자열 목록
COLLISION_SUFFIXES = ["!", "@", "#", "$", "%", "^", "&", "*"]


def generate_short_code(url: str, use_md5: bool = False) -> str:
    """URL 에서 해시 기반 단축 코드를 생성한다.
    CRC32 또는 MD5 해시의 앞 7자를 반환한다."""
    if use_md5:
        hash_hex = hashlib.md5(url.encode("utf-8")).hexdigest()
    else:
        crc = zlib.crc32(url.encode("utf-8")) & 0xFFFFFFFF
        hash_hex = f"{crc:08x}"
    return hash_hex[:SHORT_CODE_LENGTH]


def generate_with_collision_resolution(url, exists_fn, use_md5=False):
    """충돌 해결을 포함한 단축 코드 생성.
    1. URL 을 해싱하여 앞 7자를 취한다.
    2. DB 에 이미 존재하면 미리 정의된 문자열을 URL 에 덧붙여 재해싱한다.
    3. 최대 MAX_RETRIES 까지 반복한다."""
    candidate_url = url
    for i in range(MAX_RETRIES + 1):
        code = generate_short_code(candidate_url, use_md5=use_md5)
        if not exists_fn(code):
            return code
        if i < MAX_RETRIES:
            candidate_url = url + COLLISION_SUFFIXES[i]
    raise RuntimeError(f"Failed to resolve collision for: {url}")
```

**장점:**
- URL 을 미리 계산 가능 (분산 환경에 유리)
- ID 생성기가 필요 없음

**단점:**
- 충돌 가능성이 있어 DB 확인이 필수
- 충돌 해결 시 추가 DB 조회 발생

## 301 vs 302 리다이렉트

| 항목 | 301 Moved Permanently | 302 Found |
|------|----------------------|-----------|
| 캐싱 | 브라우저가 리다이렉트를 **캐시** | 캐시하지 않음 |
| 서버 부하 | 낮음 (캐시된 요청은 서버에 안 옴) | 높음 (매번 서버에 요청) |
| 클릭 추적 | 첫 번째 클릭만 서버에 기록 | **모든 클릭** 서버에 기록 |
| 분석 정확도 | 낮음 | 높음 |

**본 구현에서는 301 을 사용**한다. 트래픽 감소가 우선이며, 클릭 카운트는
리다이렉트 전에 Redis `HINCRBY` 로 증가시키므로 첫 요청에서 기록된다.

> 참고: 정밀한 클릭 분석이 필요하면 302 로 변경하면 된다.

## URL 단축 흐름

```
┌──────────┐       ┌───────────────┐       ┌─────────┐
│  Client  │       │   API Server  │       │  Redis  │
└────┬─────┘       └──────┬────────┘       └────┬────┘
     │  POST /api/v1/shorten                     │
     │  {"url": "https://..."}                   │
     │────────────────────▶│                     │
     │                     │  1. 중복 확인         │
     │                     │  GET url:long:{hash}│
     │                     │────────────────────▶│
     │                     │◀────────────────────│
     │                     │                     │
     │                     │  2. (없으면) ID 생성   │
     │                     │  INCR url:id_counter│
     │                     │────────────────────▶│
     │                     │◀────────────────────│
     │                     │                     │
     │                     │  3. Base62 인코딩     │
     │                     │  100000001 → "6LAze"│
     │                     │                     │
     │                     │  4. 매핑 저장          │
     │                     │  HSET url:short:... │
     │                     │  SET url:long:...   │
     │                     │────────────────────▶│
     │                     │◀────────────────────│
     │                     │                     │
     │  {"short_url": "http://localhost:8009/6LAze"}
     │◀────────────────────│                     │
```

## URL 리다이렉트 흐름

```
┌──────────┐       ┌───────────────┐       ┌─────────┐
│  Client  │       │   API Server  │       │  Redis  │
└────┬─────┘       └──────┬────────┘       └────┬────┘
     │  GET /6LAze                               │
     │────────────────────▶│                     │
     │                     │  1. 매핑 조회         │
     │                     │  HGETALL url:short:..│
     │                     │────────────────────▶│
     │                     │◀────────────────────│
     │                     │                     │
     │                     │  2. 클릭 수 증가      │
     │                     │  HINCRBY clicks 1   │
     │                     │────────────────────▶│
     │                     │◀────────────────────│
     │                     │                     │
     │  301 Moved Permanently                    │
     │  Location: https://www.example.com/...    │
     │◀────────────────────│                     │
```

## Redis 키 구조

| 키 패턴 | 타입 | 설명 |
|---------|------|------|
| `url:id_counter` | String | Base62 방식의 자동 증가 카운터 |
| `url:short:{code}` | Hash | `{long_url, created_at, clicks}` |
| `url:long:{sha256}` | String | 중복 방지: 원본 URL 해시 → 단축 코드 |

## 환경 변수

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `SHORTENER_APPROACH` | `base62` | 단축 방식 (`base62`, `hash`) |
| `BASE_URL` | `http://localhost:8009` | 생성되는 단축 URL 의 베이스 |
| `REDIS_HOST` | `redis` | Redis 호스트 |
| `REDIS_PORT` | `6379` | Redis 포트 |

## 테스트 실행

```bash
cd 09-design-a-url-shortener
pip install fakeredis pytest pytest-asyncio fastapi pydantic redis
pytest tests/ -v
```

### 테스트 항목

| 테스트 | 설명 |
|--------|------|
| Base62 encode/decode roundtrip | 인코딩 후 디코딩하면 원래 값 복원 |
| Hash approach: generate & collision | 해시 생성 및 충돌 해결 동작 확인 |
| ID generator: sequential & unique | 순차적이고 고유한 ID 생성 |
| Shorten + redirect flow | URL 단축 후 조회 통합 테스트 |
| Deduplication | 같은 URL 은 같은 단축 코드 반환 |
| Stats tracking | 클릭 수가 정확히 증가 |
| Invalid code returns None | 존재하지 않는 코드 조회 시 None |

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide", Chapter 8
