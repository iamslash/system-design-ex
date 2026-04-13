# Design A News Feed System

뉴스 피드 시스템(News Feed System)은 사용자가 팔로우하는 사람들의 최신 게시물을
시간 역순으로 보여주는 시스템이다. Facebook, Twitter, Instagram 등 대부분의
소셜 네트워크의 핵심 기능이며, 높은 읽기 처리량과 낮은 지연시간이 요구된다.

## 아키텍처

```
                          ┌──────────────────────────────────────────────────────────┐
                          │                   News Feed System                       │
                          │                                                          │
┌─────────────┐           │  ┌──────────────┐    ┌──────────────────────────────┐     │
│   Client    │───┐       │  │              │    │         Services             │     │
└─────────────┘   │       │  │              │    │                              │     │
                  │       │  │     Web      │    │  ┌────────────────────┐      │     │
┌─────────────┐   ├──LB──▶│  │   Servers   │───▶│  │   Post Service    │      │     │
│   Client    │───┤       │  │  (FastAPI)   │    │  │  - 포스트 생성     │      │     │
└─────────────┘   │       │  │              │    │  │  - 포스트 저장     │      │     │
                  │       │  │              │    │  └────────┬───────────┘      │     │
┌─────────────┐   │       │  │              │    │           │ trigger          │     │
│   Client    │───┘       │  │              │    │  ┌────────▼───────────┐      │     │
└─────────────┘           │  │              │    │  │  Fanout Service    │      │     │
                          │  │              │    │  │  - 팔로워 조회     │      │     │
                          │  │              │    │  │  - 피드 캐시 push  │      │     │
                          │  │              │    │  └────────────────────┘      │     │
                          │  │              │    │                              │     │
                          │  │              │    │  ┌────────────────────┐      │     │
                          │  │              │───▶│  │ News Feed Service  │      │     │
                          │  │              │    │  │  - 피드 조회       │      │     │
                          │  │              │    │  │  - 포스트 hydrate  │      │     │
                          │  └──────────────┘    │  └────────────────────┘      │     │
                          │                      │                              │     │
                          │                      │         Redis Cache          │     │
                          │                      │  ┌──────────────────────┐    │     │
                          │                      │  │ feed:{uid}   (ZSET) │    │     │
                          │                      │  │ post:{pid}   (HASH) │    │     │
                          │                      │  │ user:{uid}   (HASH) │    │     │
                          │                      │  │ followers:{uid}(SET)│    │     │
                          │                      │  │ following:{uid}(SET)│    │     │
                          │                      │  └──────────────────────┘    │     │
                          │                      └──────────────────────────────┘     │
                          └──────────────────────────────────────────────────────────┘
```

## 두 가지 핵심 흐름

### 1. Feed Publishing (포스트 발행)

사용자가 포스트를 작성하면 다음 순서로 처리된다:

```
사용자가 포스트 작성
    │
    ▼
┌───────────────┐
│ Post Service  │──▶ post:{post_id} 해시에 저장
└───────┬───────┘
        │ trigger
        ▼
┌───────────────┐
│Fanout Service │──▶ followers:{author_id} 에서 팔로워 목록 조회
└───────┬───────┘          │
        │                  ▼
        │         각 팔로워의 feed:{follower_id} Sorted Set 에
        │         post_id 를 ZADD (score = timestamp)
        │                  │
        │                  ▼
        │         ZREMRANGEBYRANK 로 피드 크기 제한 (최대 200개)
        ▼
    발행 완료
```

### 2. News Feed Building (피드 조회)

사용자가 뉴스 피드를 요청하면 다음 순서로 처리된다:

```
사용자가 피드 요청
    │
    ▼
┌──────────────────┐
│News Feed Service │──▶ feed:{user_id} 에서 ZREVRANGE 로 post_id 목록 조회
└───────┬──────────┘
        │
        ▼
  post_id 목록으로 post:{post_id} 해시 일괄 조회 (Pipeline)
        │
        ▼
  각 포스트의 user_id 로 user:{user_id} 해시 일괄 조회 (Pipeline)
        │
        ▼
  포스트 데이터 + 작성자 정보를 합쳐 hydrated feed 반환
```

## Fanout on Write vs Fanout on Read

| | Fanout on Write (Push) | Fanout on Read (Pull) |
|---|---|---|
| **동작** | 포스트 작성 시 모든 팔로워 피드에 push | 피드 조회 시 팔로잉 목록의 최신 포스트를 pull |
| **쓰기 비용** | 높음 (팔로워 수 비례) | 낮음 (포스트 1건만 저장) |
| **읽기 비용** | 낮음 (미리 계산된 피드 조회) | 높음 (조회 시 N명의 포스트 병합) |
| **지연시간** | 읽기 시 매우 빠름 | 읽기 시 느릴 수 있음 |
| **적합 대상** | 팔로워 수가 적은 일반 사용자 | 팔로워가 수백만인 셀럽/인플루언서 |
| **핫키 문제** | 셀럽이 포스트하면 수백만 건 fanout | 없음 |

### 하이브리드 접근법 (Hybrid Approach)

실제 대규모 시스템에서는 두 방식을 결합한다:

- **일반 사용자**: Fanout on Write — 팔로워가 적어 쓰기 비용이 낮고, 읽기가 빠름
- **셀럽/인플루언서**: Fanout on Read — 팔로워가 수백만이므로 쓰기 시 fanout 하지 않고,
  피드 조회 시 셀럽의 최신 포스트를 별도로 가져와 일반 피드와 병합

```
피드 조회 시:
  1. feed:{user_id} 에서 미리 계산된 피드 가져오기 (일반 사용자 포스트)
  2. 팔로우 중인 셀럽 목록에서 최신 포스트 가져오기
  3. 두 결과를 시간순으로 병합하여 반환
```

이 프로젝트에서는 **Fanout on Write** 방식을 구현한다.

## 핵심 컴포넌트

### 1. Publisher — 포스트 생성 (`feed/publisher.py`)

포스트를 생성하고 Redis 에 저장한 뒤, fanout 을 트리거한다:

```python
# 동일 밀리초 내 고유성을 보장하기 위한 시퀀스 카운터
_counter_lock = threading.Lock()
_last_ts: int = 0
_sequence: int = 0


def _generate_post_id() -> str:
    """타임스탬프 기반 포스트 ID 를 생성한다 (Snowflake-like).

    밀리초 단위 타임스탬프 + 시퀀스 번호로 고유성을 보장한다.
    """
    global _last_ts, _sequence
    with _counter_lock:
        ts = int(time.time() * 1000)
        if ts == _last_ts:
            _sequence += 1
        else:
            _last_ts = ts
            _sequence = 0
        return f"{ts}{_sequence:04d}"


async def create_post(
    redis: Redis,
    user_id: str,
    content: str,
) -> dict[str, Any]:
    """포스트를 생성하고 팔로워 피드에 fanout 한다.

    처리 흐름:
      1. 타임스탬프 기반 post_id 생성
      2. post:{post_id} 해시에 포스트 데이터 저장
      3. fanout_to_followers 호출 → 팔로워 피드에 push
    """
    post_id = _generate_post_id()
    created_at = float(post_id)  # post_id 를 score 로 사용하여 정렬 보장

    # 포스트 데이터를 Redis Hash 에 저장
    await redis.hset(
        f"post:{post_id}",
        mapping={
            "post_id": post_id,
            "user_id": user_id,
            "content": content,
            "created_at": str(created_at),
            "likes": "0",
        },
    )

    # 작성자 본인의 피드에도 추가
    await redis.zadd(f"feed:{user_id}", {post_id: created_at})

    # 팔로워 피드에 fanout
    follower_count = await fanout_to_followers(redis, user_id, post_id, created_at)

    return {
        "post_id": post_id,
        "user_id": user_id,
        "content": content,
        "created_at": created_at,
        "fanout_count": follower_count,
    }
```

### 2. Fanout Service — 팔로워 피드에 Push (`feed/fanout.py`)

포스트가 생성되면 작성자의 팔로워 목록을 조회하고,
각 팔로워의 `feed:{user_id}` Sorted Set 에 `post_id` 를 추가한다:

```python
async def fanout_to_followers(
    redis: Redis,
    author_id: str,
    post_id: str,
    timestamp: float,
) -> int:
    """작성자의 모든 팔로워 피드에 post_id 를 push 한다.

    처리 흐름:
      1. followers:{author_id} 에서 팔로워 목록 조회
      2. 각 팔로워의 feed:{follower_id} Sorted Set 에 ZADD
      3. 피드 크기가 FEED_MAX_SIZE 를 초과하면 오래된 항목 제거
    """
    # 팔로워 목록 조회
    followers = await redis.smembers(f"followers:{author_id}")

    if not followers:
        return 0

    # 각 팔로워의 피드에 포스트 push (파이프라인으로 일괄 처리)
    pipe = redis.pipeline()
    for follower_id in followers:
        feed_key = f"feed:{follower_id}"
        pipe.zadd(feed_key, {post_id: timestamp})
        # 피드 크기 제한 — 가장 오래된 항목부터 제거
        pipe.zremrangebyrank(feed_key, 0, -(settings.FEED_MAX_SIZE + 1))
    await pipe.execute()

    return len(followers)
```

### 3. Feed Retrieval — 피드 조회 + Hydration (`feed/retrieval.py`)

사용자의 피드를 조회하고, 각 포스트와 작성자 정보를 hydrate 하여 반환한다:

```python
async def get_feed(
    redis: Redis,
    user_id: str,
    offset: int = 0,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """사용자의 뉴스 피드를 역시간순으로 조회한다.

    처리 흐름:
      1. feed:{user_id} Sorted Set 에서 ZREVRANGE 로 post_id 목록 조회
      2. 각 post_id 로 post:{post_id} 해시에서 포스트 데이터 조회
      3. 각 포스트의 user_id 로 user:{user_id} 해시에서 작성자 정보 조회
      4. 포스트 + 작성자 정보를 합쳐 반환
    """
    feed_key = f"feed:{user_id}"

    # 1. 피드에서 post_id 목록을 역시간순으로 조회
    post_ids = await redis.zrevrange(feed_key, offset, offset + limit - 1)

    if not post_ids:
        return []

    # 2. 각 포스트 데이터를 파이프라인으로 일괄 조회
    pipe = redis.pipeline()
    for post_id in post_ids:
        pipe.hgetall(f"post:{post_id}")
    post_results = await pipe.execute()

    # 3. 작성자 정보를 일괄 조회 (중복 제거)
    author_ids: set[str] = set()
    posts: list[dict[str, str]] = []
    for post_data in post_results:
        if post_data:
            posts.append(post_data)
            author_ids.add(post_data.get("user_id", ""))

    # 작성자 정보 파이프라인 조회
    author_map: dict[str, dict[str, str]] = {}
    if author_ids:
        pipe = redis.pipeline()
        ordered_ids = list(author_ids)
        for author_id in ordered_ids:
            pipe.hgetall(f"user:{author_id}")
        author_results = await pipe.execute()
        for aid, adata in zip(ordered_ids, author_results):
            if adata:
                author_map[aid] = adata

    # 4. 포스트 + 작성자 정보를 합쳐 반환
    feed_items: list[dict[str, Any]] = []
    for post in posts:
        author_id = post.get("user_id", "")
        author_info = author_map.get(author_id, {})
        feed_items.append({
            "post_id": post.get("post_id", ""),
            "user_id": author_id,
            "author_name": author_info.get("name", author_id),
            "content": post.get("content", ""),
            "created_at": post.get("created_at", ""),
            "likes": int(post.get("likes", "0")),
        })

    return feed_items
```

### 4. Social Graph — 팔로우/언팔로우 (`social/graph.py`)

Redis Set 으로 양방향 팔로우 관계를 관리한다:

```python
async def follow(redis: Redis, follower_id: str, followee_id: str) -> dict[str, Any]:
    """follower_id 가 followee_id 를 팔로우한다.

    양방향으로 Redis Set 을 갱신한다:
      - following:{follower_id} 에 followee_id 추가
      - followers:{followee_id} 에 follower_id 추가
    """
    if follower_id == followee_id:
        return {"status": "error", "message": "Cannot follow yourself"}

    already = await redis.sismember(f"following:{follower_id}", followee_id)
    if already:
        return {"status": "already_following", ...}

    pipe = redis.pipeline()
    pipe.sadd(f"following:{follower_id}", followee_id)
    pipe.sadd(f"followers:{followee_id}", follower_id)
    await pipe.execute()

    return {"status": "ok", "follower": follower_id, "followee": followee_id}


async def unfollow(redis: Redis, follower_id: str, followee_id: str) -> dict[str, Any]:
    """follower_id 가 followee_id 를 언팔로우한다."""
    removed = await redis.srem(f"following:{follower_id}", followee_id)
    if not removed:
        return {"status": "not_following", ...}

    await redis.srem(f"followers:{followee_id}", follower_id)
    return {"status": "ok", ...}


async def get_followers(redis: Redis, user_id: str) -> list[str]:
    """user_id 를 팔로우하는 사용자 목록을 반환한다."""
    return sorted(await redis.smembers(f"followers:{user_id}"))


async def get_following(redis: Redis, user_id: str) -> list[str]:
    """user_id 가 팔로우하는 사용자 목록을 반환한다."""
    return sorted(await redis.smembers(f"following:{user_id}"))
```

## 캐시 아키텍처

Redis 에 4종류의 데이터를 저장한다:

```
┌─────────────────────────────────────────────────────┐
│                    Redis Cache                       │
│                                                      │
│  News Feed Cache                                     │
│  ┌───────────────────────────────────┐               │
│  │ feed:{user_id}  (Sorted Set)     │               │
│  │   score = timestamp               │               │
│  │   member = post_id                │               │
│  │   최대 200개 유지 (ZREMRANGEBYRANK)│               │
│  └───────────────────────────────────┘               │
│                                                      │
│  Post Cache                                          │
│  ┌───────────────────────────────────┐               │
│  │ post:{post_id}  (Hash)           │               │
│  │   user_id, content, created_at,   │               │
│  │   likes                           │               │
│  └───────────────────────────────────┘               │
│                                                      │
│  User Cache                                          │
│  ┌───────────────────────────────────┐               │
│  │ user:{user_id}  (Hash)           │               │
│  │   name, created_at               │               │
│  └───────────────────────────────────┘               │
│                                                      │
│  Social Graph                                        │
│  ┌───────────────────────────────────┐               │
│  │ following:{user_id}  (Set)       │               │
│  │   → 이 사용자가 팔로우하는 ID 집합│               │
│  │ followers:{user_id}  (Set)       │               │
│  │   → 이 사용자를 팔로우하는 ID 집합│               │
│  └───────────────────────────────────┘               │
└─────────────────────────────────────────────────────┘
```

## Quick Start

```bash
# 서비스 시작
docker-compose up --build

# 헬스 체크
curl http://localhost:8012/health

# 사용자 생성
curl -X POST http://localhost:8012/api/v1/users \
  -H "Content-Type: application/json" \
  -d '{"user_id": "alice", "name": "Alice Kim"}'

curl -X POST http://localhost:8012/api/v1/users \
  -H "Content-Type: application/json" \
  -d '{"user_id": "bob", "name": "Bob Lee"}'

# 팔로우
curl -X POST http://localhost:8012/api/v1/follow \
  -H "Content-Type: application/json" \
  -d '{"follower_id": "alice", "followee_id": "bob"}'

# 포스트 작성
curl -X POST http://localhost:8012/api/v1/posts \
  -H "Content-Type: application/json" \
  -d '{"user_id": "bob", "content": "Hello world!"}'

# 뉴스 피드 조회 (alice 가 bob 의 포스트를 볼 수 있다)
curl http://localhost:8012/api/v1/feed/alice

# 친구 목록 조회
curl http://localhost:8012/api/v1/friends/alice
```

## CLI 사용법

```bash
# 사용자 생성
python scripts/cli.py create-user alice
python scripts/cli.py create-user bob --name "Bob Lee"

# 팔로우
python scripts/cli.py follow alice bob        # alice 가 bob 을 팔로우

# 포스트 작성
python scripts/cli.py post bob "Hello world!" # bob 이 포스트 작성

# 뉴스 피드 조회 (alice 가 bob 의 포스트를 본다)
python scripts/cli.py feed alice

# 친구 목록 조회
python scripts/cli.py friends alice

# 언팔로우
python scripts/cli.py unfollow alice bob

# 헬스 체크
python scripts/cli.py --health
```

### CLI 출력 예시

```
$ python scripts/cli.py create-user alice
User created: alice (alice)

$ python scripts/cli.py follow alice bob
alice now follows bob

$ python scripts/cli.py post bob "Hello world!"
Post created: 17756787456780000
  Author: bob
  Content: Hello world!
  Fanout: 1 followers

$ python scripts/cli.py feed alice
Feed for alice (1 posts):
  [17756787456780000] @Bob Lee: Hello world! (likes: 0)

$ python scripts/cli.py friends alice
Friends of alice:
  Following (1):
    - bob
  Followers (0):

$ python scripts/cli.py --health
Health: 200
  Status: ok
  Redis: 7.4.2
```

## API Endpoints

| Method | Path | 설명 |
|--------|------|------|
| `POST` | `/api/v1/posts` | 포스트 생성 + fanout |
| `GET` | `/api/v1/posts/{post_id}` | 포스트 조회 |
| `GET` | `/api/v1/feed/{user_id}` | 뉴스 피드 조회 (hydrated) |
| `POST` | `/api/v1/follow` | 팔로우 |
| `POST` | `/api/v1/unfollow` | 언팔로우 |
| `GET` | `/api/v1/friends/{user_id}` | 친구 목록 (팔로잉/팔로워) |
| `POST` | `/api/v1/users` | 사용자 생성 |
| `GET` | `/health` | 헬스 체크 |

### 요청/응답 예시

**POST /api/v1/posts**

```json
{
  "user_id": "bob",
  "content": "Hello world!"
}
```

**응답:**

```json
{
  "post_id": "17756787456780000",
  "user_id": "bob",
  "content": "Hello world!",
  "created_at": 1.775678745678e+15,
  "fanout_count": 2
}
```

**GET /api/v1/feed/alice**

```json
{
  "user_id": "alice",
  "count": 2,
  "feed": [
    {
      "post_id": "17756787456790001",
      "user_id": "bob",
      "author_name": "Bob Lee",
      "content": "Second post!",
      "created_at": "1.775678745679e+15",
      "likes": 0
    },
    {
      "post_id": "17756787456780000",
      "user_id": "bob",
      "author_name": "Bob Lee",
      "content": "Hello world!",
      "created_at": "1.775678745678e+15",
      "likes": 0
    }
  ]
}
```

**POST /api/v1/follow**

```json
{
  "follower_id": "alice",
  "followee_id": "bob"
}
```

## 환경 변수

| 변수 | 기본값 | 설명 |
|---|---|---|
| `REDIS_HOST` | `redis` | Redis 호스트 |
| `REDIS_PORT` | `6379` | Redis 포트 |
| `FEED_MAX_SIZE` | `200` | 사용자 피드 최대 포스트 수 |

## 테스트

```bash
# 의존성 설치
pip install -r api/requirements.txt

# 테스트 실행
python -m pytest tests/ -v
```

| 테스트 | 검증 내용 |
|--------|----------|
| Post creation | 포스트 생성, Redis 저장, 작성자 피드 추가, 존재하지 않는 포스트 |
| Fanout | 팔로워 피드에 push, fanout 카운트, 다중 팔로워, 팔로워 없음 |
| Feed retrieval | hydrated 피드, 역시간순 정렬, 빈 피드, 페이지네이션 |
| Feed size limit | FEED_MAX_SIZE 초과 시 오래된 항목 제거 |
| Social graph | 팔로우, 언팔로우, 자기 자신 팔로우 방지, 중복 팔로우, 피드 영향 |
| Feed hydration | 작성자 이름, likes, 작성자 정보 없을 때 fallback |

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide", Chapter 11
