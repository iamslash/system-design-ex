# Design A Rate Limiter

Rate Limiter 는 클라이언트 또는 서비스가 보내는 트래픽의 처리율(rate)을 제어하는
장치다. HTTP 를 예로 들면, 특정 기간 내에 전송되는 클라이언트 요청 횟수를 제한한다.
임계치(threshold)를 넘어서면 추가로 도달한 모든 호출은 처리가 중단(throttle)된다.

## 아키텍처

```
                         ┌─────────────────────────┐
                         │       Client (CLI)       │
                         └────────────┬─────────────┘
                                      │  HTTP Request
                                      ▼
                         ┌─────────────────────────┐
                         │   FastAPI Application    │
                         │                          │
                         │  ┌───────────────────┐   │
                         │  │  Rate Limiter      │   │
                         │  │  Middleware         │   │
                         │  │                     │   │
                         │  │  ┌───────────────┐  │   │
                         │  │  │ Token Bucket  │  │   │
                         │  │  │      OR       │  │   │
                         │  │  │ Sliding Window│  │   │
                         │  │  └──────┬────────┘  │   │
                         │  └─────────┼───────────┘   │
                         │            │               │
                         └────────────┼───────────────┘
                                      │  GET / SET
                                      ▼
                         ┌─────────────────────────┐
                         │     Redis 7 (Alpine)     │
                         │                          │
                         │  - Token count & ts      │
                         │  - Sorted sets (window)  │
                         └─────────────────────────┘
```

### 요청 흐름

1. 클라이언트가 `GET /api/limited` 요청을 보낸다.
2. Rate Limiter Middleware 가 클라이언트 IP 를 식별한다 (`X-Forwarded-For` 또는 remote address).
3. 선택된 알고리즘(Token Bucket / Sliding Window Counter)에 따라 Redis 에서 현재 상태를 조회한다.
4. 요청이 허용되면 응답 헤더에 남은 횟수를 포함하여 `200 OK` 를 반환한다.
5. 한도를 초과하면 `429 Too Many Requests` 와 `X-Ratelimit-Retry-After` 헤더를 반환한다.

## Quick Start

```bash
# 서비스 시작
docker-compose up --build

# 헬스 체크
curl http://localhost:8005/health

# Rate limited 엔드포인트 호출
curl -i http://localhost:8005/api/limited

# Rate limit 설정 확인
curl http://localhost:8005/api/config
```

## CLI 사용법

```bash
# 헬스 체크
python scripts/cli.py --health

# 요청 20회 연속 전송 (burst test)
python scripts/cli.py --burst 20

# 현재 설정 확인
python scripts/cli.py --config
```

### CLI 출력 예시

```
[1/20] GET /api/limited -> 200  Remaining: 9  Limit: 10
[2/20] GET /api/limited -> 200  Remaining: 8  Limit: 10
...
[11/20] GET /api/limited -> 429  Retry-After: 52s
...
--- Summary ---
Passed (200): 10
Rejected (429): 10
```

## 알고리즘

### Token Bucket

버킷에 토큰이 채워져 있고, 요청마다 토큰 하나를 소비한다. 토큰이 없으면 요청은
거부된다. 토큰은 일정한 속도(`refill_rate`)로 보충되며 최대 `bucket_size` 개까지
채워진다.

- **장점**: 메모리 효율적, 버스트 트래픽 허용
- **매개변수**: `BUCKET_SIZE` (버킷 크기), `REFILL_RATE` (초당 토큰 보충 수)

```
시간 →
토큰: [■■■■■■■■■■]  bucket_size = 10
요청 →    ■ 소비        refill_rate = 1/s
         [■■■■■■■■■ ]
```

**핵심 구현** — Redis Lua 스크립트로 원자적(atomic) 실행:

```lua
-- Token Bucket Lua Script (Redis 에서 원자적으로 실행)
local key = KEYS[1]
local bucket_size = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])

if tokens == nil then
    tokens = bucket_size    -- 첫 요청: 버킷을 가득 채움
    last_refill = now
end

-- 경과 시간에 비례하여 토큰 보충
local elapsed = now - last_refill
local refill = elapsed * refill_rate
tokens = math.min(bucket_size, tokens + refill)
last_refill = now

-- 토큰 소비 시도
if tokens >= 1 then
    tokens = tokens - 1
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', last_refill)
    return {1, math.floor(tokens)}   -- {allowed=1, remaining}
else
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', last_refill)
    return {0, 0}                     -- {allowed=0, remaining=0}
end
```

Python 에서 호출:

```python
class TokenBucket:
    async def is_allowed(self, client_id: str) -> RateLimitResult:
        key = f"rate_limit:token_bucket:{client_id}"
        now = time.time()
        script = await self._get_script()
        allowed_int, remaining = await script(
            keys=[key],
            args=[self._bucket_size, self._refill_rate, now],
        )
        return RateLimitResult(
            allowed=bool(allowed_int),
            limit=self._bucket_size,
            remaining=int(remaining),
            retry_after=0 if allowed_int else math.ceil(1.0 / self._refill_rate),
        )
```

### Sliding Window Counter

Redis Sorted Set 을 사용하여 각 요청의 타임스탬프를 기록하고, 현재 시점 기준
`window_size` 초 이내의 요청만 카운트한다.

- **장점**: 윈도우 경계 문제 해소, 정확한 카운트
- **매개변수**: `RATE_LIMIT_WINDOW` (윈도우 크기, 초), `RATE_LIMIT_REQUESTS` (최대 요청 수)

```
|-------- window_size --------|
|  req  req  req  req  req    | → 5 requests (allowed)
|  req  req  req  req  req req req ...| → 7+ requests (rejected if max=5)
                              now
```

**핵심 구현** — Redis Sorted Set + Lua 스크립트:

```lua
-- Sliding Window Counter Lua Script
local key = KEYS[1]
local window_size = tonumber(ARGV[1])
local max_requests = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local member = ARGV[4]

-- 현재 윈도우 밖의 오래된 요청 제거
local window_start = now - window_size
redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

-- 윈도우 내 현재 요청 수 카운트
local current_count = redis.call('ZCARD', key)

if current_count < max_requests then
    -- 새 요청을 타임스탬프(score)와 함께 추가
    redis.call('ZADD', key, now, member)
    redis.call('EXPIRE', key, window_size + 1)
    return {1, max_requests - current_count - 1}  -- {allowed, remaining}
else
    redis.call('EXPIRE', key, window_size + 1)
    return {0, 0}                                   -- rejected
end
```

### Rate Limiter Middleware

미들웨어가 모든 요청을 가로채서 알고리즘을 적용하고 응답 헤더를 설정한다:

```python
class RateLimiterMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        # Rate limit 제외 경로 (health, config 등)
        if request.url.path in _UNLIMITED_PATHS:
            return await call_next(request)

        client_ip = self._get_client_ip(request)  # X-Forwarded-For 또는 remote addr
        result = await algorithm.is_allowed(client_ip)

        if not result.allowed:
            return JSONResponse(
                status_code=429,
                content={"detail": f"Rate limit exceeded. Try again in {result.retry_after} seconds."},
                headers={
                    "X-Ratelimit-Limit": str(result.limit),
                    "X-Ratelimit-Remaining": "0",
                    "X-Ratelimit-Retry-After": str(result.retry_after),
                },
            )

        response = await call_next(request)
        response.headers["X-Ratelimit-Limit"] = str(result.limit)
        response.headers["X-Ratelimit-Remaining"] = str(result.remaining)
        return response
```

## Rate Limit 응답 헤더

| 헤더 | 설명 |
|---|---|
| `X-Ratelimit-Limit` | 윈도우 내 허용된 최대 요청 수 |
| `X-Ratelimit-Remaining` | 윈도우 내 남은 요청 수 |
| `X-Ratelimit-Retry-After` | 요청 재시도까지 대기해야 할 초 (429 응답 시) |

## HTTP 429 Too Many Requests

한도를 초과하면 다음과 같은 응답이 반환된다:

```json
{
  "detail": "Rate limit exceeded. Try again in 52 seconds."
}
```

## 분산 환경에서의 Rate Limiting

### Race Condition 문제

두 요청이 동시에 카운터를 읽으면 둘 다 같은 값을 보고 증가시켜 한도를 초과할 수 있다.
**해결**: Redis Lua 스크립트로 읽기-확인-쓰기를 원자적으로 실행.

```
Request 1 ─┐         ┌─ read counter = 3
            ├─ Redis ─┤  (Lua 스크립트: 원자적 실행)
Request 2 ─┘         └─ read counter = 4 (정확)
```

### Synchronization 문제

여러 Rate Limiter 서버가 있으면 각 서버의 카운터가 동기화되지 않는다.
**해결**: 중앙 집중식 Redis 저장소를 사용하여 모든 Rate Limiter 가 같은 상태를 공유.

## 환경 변수

| 변수 | 기본값 | 설명 |
|---|---|---|
| `RATE_LIMIT_ALGORITHM` | `token_bucket` | 알고리즘 선택 (`token_bucket`, `sliding_window_counter`) |
| `RATE_LIMIT_REQUESTS` | `10` | 윈도우당 최대 요청 수 |
| `RATE_LIMIT_WINDOW` | `60` | 윈도우 크기 (초) |
| `BUCKET_SIZE` | `10` | Token Bucket 최대 토큰 수 |
| `REFILL_RATE` | `1` | Token Bucket 초당 보충 토큰 수 |
| `REDIS_HOST` | `redis` | Redis 호스트 |
| `REDIS_PORT` | `6379` | Redis 포트 |

## 알고리즘 비교

| 알고리즘 | 메모리 | 버스트 허용 | 정확도 | 구현 난이도 |
|----------|--------|-----------|--------|-----------|
| Token Bucket | 낮음 | O (토큰 잔여분) | 보통 | 쉬움 |
| Leaking Bucket | 낮음 | X (고정 처리율) | 보통 | 쉬움 |
| Fixed Window Counter | 낮음 | O (경계 문제) | 낮음 | 쉬움 |
| Sliding Window Log | 높음 | X | 높음 | 보통 |
| Sliding Window Counter | 낮음 | X | 높음 | 보통 |

## 테스트 실행

```bash
cd 05-design-a-rate-limiter
pip install -r api/requirements.txt
pytest tests/ -v
```

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide", Chapter 4
