# Design A Gaming Leaderboard

게임 리더보드는 수백만 명의 플레이어 중 실시간 순위를 제공해야 한다. 핵심 요구사항은
다음과 같다:

- 점수 기록 (score a point)
- 상위 10명 조회 (top 10)
- 특정 유저의 순위 조회 (user rank)
- 특정 유저 기준 상대적 위치 (4명 위 + 4명 아래)
- 월별 리더보드 (monthly rotation)

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
                         │  POST /v1/scores         │
                         │  GET  /v1/scores         │
                         │  GET  /v1/scores/{id}    │
                         │  GET  /v1/scores/{id}/   │
                         │       around             │
                         └────────────┬─────────────┘
                                      │  ZADD / ZINCRBY
                                      │  ZREVRANGE / ZREVRANK
                                      ▼
                         ┌─────────────────────────┐
                         │     Redis 7 (Alpine)     │
                         │                          │
                         │  Sorted Set: 리더보드     │
                         │  Hash: 유저 프로필        │
                         └─────────────────────────┘
```

## 왜 SQL 이 아닌가?

관계형 DB 에서 순위를 구하려면 다음과 같은 쿼리가 필요하다:

```sql
SELECT *, RANK() OVER (ORDER BY score DESC) AS rank
FROM leaderboard
WHERE month = '2026-04';
```

이 쿼리의 시간 복잡도는 **O(n)** 이다. 전체 테이블을 스캔하여 정렬해야 하기 때문이다.
인덱스를 사용하더라도 특정 유저의 순위를 구하려면 해당 유저보다 높은 점수를 가진
행의 수를 세야 한다:

```sql
SELECT COUNT(*) + 1 AS rank
FROM leaderboard
WHERE score > (SELECT score FROM leaderboard WHERE user_id = 'alice');
```

**25M DAU** 기준으로 매 요청마다 이 쿼리를 실행하면 DB 가 감당할 수 없다.

## Redis Sorted Set

Redis Sorted Set 은 내부적으로 **skip list + hash table** 로 구현되어 있다.
모든 핵심 연산이 **O(log n)** 에 수행된다.

```
Skip List 구조:

Level 3:  HEAD ────────────────────────────── 100
Level 2:  HEAD ──────── 30 ────────── 80 ──── 100
Level 1:  HEAD ── 10 ── 30 ── 50 ── 80 ── 100
Level 0:  HEAD ── 10 ── 30 ── 50 ── 80 ── 100  (linked list)
```

각 노드는 확률적으로 상위 레벨에 포함된다. 검색, 삽입, 삭제 모두 평균 **O(log n)**.

### 핵심 Redis 명령어

| 명령어 | 용도 | 시간 복잡도 |
|--------|------|-------------|
| `ZADD` | 멤버 추가 (점수 지정) | O(log n) |
| `ZINCRBY` | 멤버 점수 증가 | O(log n) |
| `ZREVRANGE` | 상위 N명 조회 (점수 내림차순) | O(log n + m) |
| `ZREVRANK` | 특정 멤버의 순위 (0-based) | O(log n) |
| `ZSCORE` | 특정 멤버의 점수 | O(1) |

### 월별 리더보드 키

```
leaderboard:{YYYY}-{MM}
```

예: `leaderboard:2026-04` (2026년 4월)

매월 새로운 키를 사용하므로 자연스럽게 리더보드가 리셋된다. 이전 달의 데이터는
그대로 보존되어 기록 조회가 가능하다.

## 핵심 구현

### 점수 기록 (ZINCRBY)

```python
# 유저의 점수를 증가시키고 새 점수를 반환
async def score_point(self, user_id: str, points: int = 1,
                      *, leaderboard_key: str | None = None) -> float:
    key = leaderboard_key or self.current_key()
    # ZINCRBY: O(log n) — 멤버가 없으면 자동 생성
    new_score = await self._redis.zincrby(key, points, user_id)
    await self._user_store.upsert(user_id)
    return float(new_score)
```

### 상위 10명 조회 (ZREVRANGE)

```python
# 점수 내림차순으로 상위 N명 반환
async def top(self, n: int = 10, *, leaderboard_key: str | None = None) -> list[dict]:
    key = leaderboard_key or self.current_key()
    # ZREVRANGE 0 9: 상위 10명, withscores=True 로 점수 포함
    results = await self._redis.zrevrange(key, 0, n - 1, withscores=True)
    entries = []
    for rank_idx, (member, score) in enumerate(results):
        profile = await self._user_store.get(member)
        entries.append({
            "rank": rank_idx + 1,      # 1-based rank
            "user_id": member,
            "score": score,
            "display_name": profile.get("display_name") if profile else None,
        })
    return entries
```

### 유저 순위 조회 (ZREVRANK)

```python
# 특정 유저의 순위와 점수 반환
async def user_rank(self, user_id: str, *, leaderboard_key: str | None = None) -> dict:
    key = leaderboard_key or self.current_key()
    score = await self._redis.zscore(key, user_id)
    if score is None:
        return {"user_id": user_id, "rank": None, "score": 0.0}
    # ZREVRANK: 0-based, 1-based 로 변환
    zero_rank = await self._redis.zrevrank(key, user_id)
    return {
        "user_id": user_id,
        "rank": zero_rank + 1,
        "score": float(score),
    }
```

### 상대적 위치 조회 (4 above + 4 below)

```python
# 유저 기준 위아래 span 명씩 반환
async def around_user(self, user_id: str, span: int = 4,
                      *, leaderboard_key: str | None = None) -> list[dict]:
    key = leaderboard_key or self.current_key()
    zero_rank = await self._redis.zrevrank(key, user_id)
    if zero_rank is None:
        return []
    start = max(0, zero_rank - span)  # 4명 위 (클램프)
    end = zero_rank + span            # 4명 아래
    # ZREVRANGE start end: 범위 내 멤버들을 점수 내림차순으로
    results = await self._redis.zrevrange(key, start, end, withscores=True)
    return [{"rank": start + idx + 1, "user_id": m, "score": s}
            for idx, (m, s) in enumerate(results)]
```

### 유저 프로필 저장 (Redis Hash)

리더보드 데이터와 유저 프로필은 분리하여 저장한다:

```python
# Redis Hash 에 유저 프로필 저장
# key: user:{user_id}
# fields: user_id, display_name, created_at
async def upsert(self, user_id: str, display_name: str | None = None) -> dict:
    key = f"user:{user_id}"
    exists = await self._redis.exists(key)
    if not exists:
        await self._redis.hset(key, mapping={
            "user_id": user_id,
            "display_name": display_name or user_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
    elif display_name is not None:
        await self._redis.hset(key, "display_name", display_name)
    return await self.get(user_id)
```

## API 엔드포인트

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `POST` | `/v1/scores` | 점수 기록 (`{"user_id": "alice", "points": 1}`) |
| `GET` | `/v1/scores` | 상위 10명 조회 |
| `GET` | `/v1/scores/{user_id}` | 유저 순위 및 점수 |
| `GET` | `/v1/scores/{user_id}/around` | 유저 기준 상대적 위치 (4 위 + 4 아래) |

## Quick Start

```bash
# 서비스 시작
docker-compose up --build

# 헬스 체크
curl http://localhost:8026/health

# 점수 기록
curl -X POST http://localhost:8026/v1/scores \
  -H "Content-Type: application/json" \
  -d '{"user_id": "alice", "points": 10}'

# 상위 10명 조회
curl http://localhost:8026/v1/scores

# 유저 순위 조회
curl http://localhost:8026/v1/scores/alice

# 상대적 위치 조회
curl http://localhost:8026/v1/scores/alice/around
```

## CLI 사용법

```bash
# 헬스 체크
python scripts/cli.py --health

# 점수 기록
python scripts/cli.py --score alice 10

# 상위 10명
python scripts/cli.py --top

# 유저 순위
python scripts/cli.py --rank alice

# 상대적 위치
python scripts/cli.py --around alice
```

## 테스트

```bash
pip install -r api/requirements.txt
pytest tests/ -v
```

## 스케일링 전략

### 문제: 단일 Redis 인스턴스의 한계

Redis Sorted Set 은 단일 키에 모든 유저를 저장한다. 25M DAU 에서 단일 노드의
메모리와 CPU 가 병목이 될 수 있다.

### 방법 1: Fixed Partition (고정 파티션)

점수 범위로 샤드를 나눈다:

```
Shard 1: score 0 ~ 999
Shard 2: score 1000 ~ 9999
Shard 3: score 10000+
```

- **장점**: 구현이 단순하다.
- **단점**: 점수 분포가 불균등하면 핫스팟이 발생한다. 유저 대부분이 낮은 점수대에
  몰리면 Shard 1 에 부하가 집중된다.

### 방법 2: Hash Partition (해시 파티션)

`user_id` 를 해시하여 샤드를 배정한다:

```
shard_id = hash(user_id) % num_shards
```

- **장점**: 부하가 균등하게 분산된다.
- **단점**: 전체 순위를 구하려면 **scatter-gather** 가 필요하다. 모든 샤드에
  쿼리를 보내고 결과를 병합해야 한다.

### Scatter-Gather 패턴

```
Client
  │
  ├─► Shard 1: Top 10 → [{alice: 100}, {bob: 90}, ...]
  ├─► Shard 2: Top 10 → [{carol: 95}, {dave: 85}, ...]
  └─► Shard 3: Top 10 → [{eve: 110}, {frank: 80}, ...]
      │
      ▼
  Application: 3개 결과를 병합 후 전체 Top 10 계산
```

Top 10 조회: 각 샤드에서 Top 10 을 가져온 뒤 애플리케이션에서 병합.
유저 순위 조회: 해당 유저의 샤드에서만 순위를 가져옴 (근사값).
정확한 순위가 필요하면 모든 샤드에서 해당 유저보다 높은 점수의 수를 합산.

## 스토리지 추정

25M DAU 기준 월별 리더보드:

| 항목 | 계산 | 크기 |
|------|------|------|
| Sorted Set 멤버 | 25M x (user_id 평균 16 bytes + score 8 bytes + overhead 32 bytes) | ~1.4 GB |
| User Hash | 25M x (3 fields x 평균 32 bytes + overhead) | ~3.2 GB |
| **총 메모리** | | **~4.6 GB** |

단일 Redis 인스턴스(64 GB)로 충분히 수용 가능하다. 월별로 키가 분리되므로
오래된 리더보드는 TTL 을 설정하거나 별도 스토리지로 아카이브할 수 있다.

## 환경 변수

| 변수 | 기본값 | 설명 |
|---|---|---|
| `REDIS_HOST` | `redis` | Redis 호스트 |
| `REDIS_PORT` | `6379` | Redis 포트 |

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide Vol. 2", Chapter 26
