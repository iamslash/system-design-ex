# Design Nearby Friends

주변 친구 서비스는 사용자의 실시간 위치를 기반으로 가까이 있는 친구를 찾아주는
시스템이다. WebSocket 으로 위치를 실시간 갱신하고, Redis Pub/Sub 으로 친구에게
위치 변경을 브로드캐스트하며, Haversine 공식으로 거리를 계산하여 반경 5마일
이내의 친구만 필터링한다.

## 아키텍처

```
 ┌──────────┐  ┌──────────┐  ┌──────────┐
 │ Client A │  │ Client B │  │ Client C │
 └────┬─────┘  └────┬─────┘  └────┬─────┘
      │ ws          │ ws          │ ws
      ▼             ▼             ▼
 ┌─────────────────────────────────────────┐
 │      Nearby Friends Server (FastAPI)    │
 │                                         │
 │  ┌──────────────┐  ┌────────────────┐   │
 │  │ Location     │  │ Nearby         │   │
 │  │ Tracker      │  │ Finder         │   │
 │  │ (Cache+TTL)  │  │ (Haversine)    │   │
 │  └──────────────┘  └────────────────┘   │
 │  ┌──────────────┐  ┌────────────────┐   │
 │  │ Location     │  │ Location       │   │
 │  │ History      │  │ Pub/Sub        │   │
 │  │ (Time-series)│  │ (Broadcast)    │   │
 │  └──────────────┘  └────────────────┘   │
 └──────────────────┬──────────────────────┘
                    │ GET / SET / PUB / SUB
                    ▼
 ┌─────────────────────────────────────────┐
 │          Redis 7 (Alpine)               │
 │                                         │
 │  - location:{user_id}         (Hash+TTL)│
 │  - location_history:{user_id} (Sorted Set)
 │  - friends:{user_id}          (Set)     │
 │  - location:{user_id}         (Pub/Sub) │
 └─────────────────────────────────────────┘
```

## Peer-to-Peer vs Shared Backend

| 접근 방식 | 장점 | 단점 |
|-----------|------|------|
| **P2P** (기기 간 직접 통신) | 서버 부하 없음, 지연 낮음 | NAT 문제, 오프라인 처리 불가, 친구 목록 동기화 어려움 |
| **Shared Backend** (서버 중개) | 중앙 집중 친구 관리, 히스토리 저장, 확장 용이 | 서버 비용, 추가 지연 |

본 구현은 **Shared Backend** 방식을 채택한다. 서버가 모든 사용자의 위치를
캐싱하고 친구 관계를 관리하므로 일관성 있는 서비스를 제공할 수 있다.

## WebSocket 흐름

```
Alice (Client)                    Server                     Bob (Client)
     │                              │                              │
     │──── ws connect ─────────────▶│                              │
     │                              │  1. friends:alice 조회       │
     │                              │  2. 친구 채널 구독            │
     │                              │     (SUBSCRIBE location:bob) │
     │                              │                              │
     │  {"type":"location_update",  │                              │
     │   "latitude":40.71,          │                              │
     │   "longitude":-74.00}        │                              │
     │─────────────────────────────▶│                              │
     │                              │  3. location:alice 캐시 갱신  │
     │                              │     (HSET + EXPIRE 60s)      │
     │                              │  4. location_history:alice 추가│
     │                              │  5. PUBLISH location:alice   │
     │                              │                              │
     │◀─── location_ack ───────────│                              │
     │                              │                              │
     │                              │  Bob 이 Alice 채널 구독 중    │
     │                              │  distance = haversine(...)   │
     │                              │  distance <= 5 miles?        │
     │                              │         YES → 전달           │
     │                              │                              │
     │                              │── friend_location ──────────▶│
     │                              │  {"user_id":"alice",         │
     │                              │   "latitude":40.71,          │
     │                              │   "distance_miles":0.5}      │
```

## Redis Pub/Sub 설계

각 사용자는 자신의 Redis Pub/Sub 채널 `location:{user_id}` 을 가진다.

```
                    ┌──────────────────────────┐
                    │     Redis Pub/Sub         │
                    │                          │
  Alice 위치 갱신 ──▶ PUBLISH location:alice ──┬──▶ Bob (구독 중, 5mi 이내 → 전달)
                    │                          ├──▶ Charlie (구독 중, 20mi → 필터)
                    │                          └──▶ Dave (미구독 → 미전달)
                    └──────────────────────────┘
```

**동작 방식:**
1. 사용자가 온라인 접속하면 모든 친구의 채널을 구독한다.
2. 친구가 위치를 갱신하면 해당 채널에 PUBLISH 된다.
3. 구독 리스너가 메시지를 수신하면 Haversine 거리를 계산한다.
4. 반경(기본 5마일) 이내인 경우에만 WebSocket 으로 전달한다.
5. 연결이 끊기면 모든 구독을 해제하고 정리한다.

```python
class LocationPubSub:
    """사용자별 위치 Pub/Sub 채널 관리.
    PUBLISH: 사용자가 위치를 보고하면 자신의 채널에 발행.
    SUBSCRIBE: 온라인 접속 시 모든 친구 채널을 구독.
    """

    async def publish(self, user_id: str, latitude: float, longitude: float) -> int:
        """사용자의 채널에 위치 업데이트를 발행한다."""
        channel = f"location:{user_id}"
        payload = json.dumps({
            "user_id": user_id,
            "latitude": latitude,
            "longitude": longitude,
        })
        return await self._redis.publish(channel, payload)

    async def subscribe(
        self,
        user_id: str,
        friend_ids: list[str],
        on_update: Callable[[dict], Awaitable[None]],
        my_location_getter: Callable[[], Awaitable[dict | None]],
    ) -> None:
        """친구들의 위치 채널을 구독한다.
        수신된 업데이트는 Haversine 거리 필터를 거쳐
        반경 이내인 경우에만 on_update 콜백으로 전달된다.
        """
        pubsub = self._redis.pubsub()
        channels = [f"location:{fid}" for fid in friend_ids]
        await pubsub.subscribe(*channels)

        async def _listen() -> None:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                data = json.loads(message["data"])
                my_loc = await my_location_getter()
                dist = haversine_distance(
                    my_loc["latitude"], my_loc["longitude"],
                    data["latitude"], data["longitude"],
                )
                if dist <= NEARBY_RADIUS_MILES:  # 5마일 이내만 전달
                    data["distance_miles"] = round(dist, 4)
                    await on_update(data)
```

## Haversine 공식

두 지점 간의 대원 거리(great-circle distance)를 계산하는 공식이다.

```
a = sin²(Δlat/2) + cos(lat₁) × cos(lat₂) × sin²(Δlon/2)
c = 2 × atan2(√a, √(1−a))
d = R × c
```

여기서 R 은 지구의 반지름(3,958.8 마일)이다.

```python
EARTH_RADIUS_MILES = 3958.8

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """두 지점 간의 대원 거리를 마일 단위로 계산한다.
    파라미터는 십진도(decimal degrees) 형식.
    """
    lat1_r, lon1_r = math.radians(lat1), math.radians(lon1)
    lat2_r, lon2_r = math.radians(lat2), math.radians(lon2)

    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return EARTH_RADIUS_MILES * c
```

**검증:**
- 같은 지점: 거리 = 0
- NYC → LA: ~2,451 마일
- 위도 1/69도 차이: ~1 마일

## 위치 캐시 (TTL=60s)

비활성 사용자의 위치를 자동으로 제거하기 위해 Redis Hash 에 TTL 을 설정한다.
사용자가 위치를 갱신할 때마다 TTL 이 리셋된다.

```python
class LocationTracker:
    """실시간 위치 캐시. Redis Hash + TTL 로 비활성 사용자 자동 만료."""

    async def update(self, user_id: str, latitude: float, longitude: float) -> dict:
        """위치를 저장하고 TTL 을 리셋한다."""
        key = f"location:{user_id}"
        mapping = {
            "latitude": str(latitude),
            "longitude": str(longitude),
            "timestamp": str(time.time()),
        }
        await self._redis.hset(key, mapping=mapping)
        await self._redis.expire(key, self._ttl)  # 60초 후 자동 만료
        return {...}

    async def get(self, user_id: str) -> dict | None:
        """캐시된 위치를 조회한다. TTL 만료 시 None 반환."""
        data = await self._redis.hgetall(f"location:{user_id}")
        if not data:
            return None  # 만료됨 또는 미등록
        return {...}
```

## Scaling 고려사항

### 1. Redis Cluster

사용자 수가 증가하면 단일 Redis 인스턴스로는 Pub/Sub 과 캐시를 감당하기
어렵다. Redis Cluster 를 사용하여 키를 샤딩하고 Pub/Sub 부하를 분산한다.

### 2. Geospatial Index

현재 구현은 모든 친구의 위치를 순회하며 거리를 계산한다. 친구 수가 매우
많은 경우 Redis 의 `GEOADD` / `GEORADIUS` 명령을 사용하여 O(N) 을
O(log N + M) 으로 개선할 수 있다.

```
GEOADD user_locations -74.0060 40.7128 alice
GEORADIUSBYMEMBER user_locations alice 5 mi
```

### 3. 위치 갱신 빈도 제한

모바일 기기가 매초 위치를 보고하면 Redis 와 네트워크에 큰 부하가 발생한다.
클라이언트 측에서 다음 조건일 때만 갱신하도록 최적화한다:
- 이전 위치에서 일정 거리 이상 이동한 경우
- 최소 갱신 간격(예: 10초) 경과

### 4. 수평 확장 (Multi-server)

```
 Client ──▶ Load Balancer ──┬──▶ Server A ──┐
                            ├──▶ Server B ──┤──▶ Redis Cluster
                            └──▶ Server C ──┘
```

WebSocket 연결은 특정 서버에 고정되므로 Redis Pub/Sub 이 서버 간 메시지
브릿지 역할을 한다. Server A 의 사용자가 위치를 갱신하면 Redis Pub/Sub 을
통해 Server B 에 접속한 친구에게도 전달된다.

### 5. 위치 히스토리 분리

실시간 캐시(Redis)와 히스토리 저장소를 분리한다. 히스토리는 Cassandra 나
TimescaleDB 같은 시계열 DB 에 저장하여 장기 분석과 궤적 조회를 지원한다.

## Quick Start

```bash
# 서비스 시작
docker-compose up --build

# 헬스 체크
curl http://localhost:8018/health

# 친구 관계 생성
curl -X POST http://localhost:8018/api/v1/friends \
  -H "Content-Type: application/json" \
  -d '{"user_a": "alice", "user_b": "bob"}'

# Alice 위치 갱신
curl -X POST http://localhost:8018/api/v1/location \
  -H "Content-Type: application/json" \
  -d '{"user_id": "alice", "latitude": 40.7128, "longitude": -74.0060}'

# Bob 위치 갱신
curl -X POST http://localhost:8018/api/v1/location \
  -H "Content-Type: application/json" \
  -d '{"user_id": "bob", "latitude": 40.7130, "longitude": -74.0062}'

# Alice 주변 친구 조회
curl http://localhost:8018/api/v1/nearby/alice

# 위치 히스토리 조회
curl http://localhost:8018/api/v1/location-history/alice
```

## CLI 사용법

```bash
# 인터랙티브 위치 추적 모드
python scripts/cli.py track alice

# 추적 모드에서:
# /update 40.7128 -74.0060    — 위치 갱신
# /nearby                      — 주변 친구 조회
# /nearby 10                   — 반경 10마일 내 친구 조회
# /quit                        — 연결 종료

# 비대화형 명령
python scripts/cli.py nearby alice                # 주변 친구 조회
python scripts/cli.py add-friend alice bob         # 친구 관계 생성
python scripts/cli.py history alice                # 위치 히스토리 조회
python scripts/cli.py location alice 40.7128 -74.0060  # REST 위치 갱신
python scripts/cli.py --health                     # 헬스 체크
```

## API Endpoints

### REST

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `POST` | `/api/v1/location` | 위치 갱신 `{"user_id": "alice", "latitude": 40.71, "longitude": -74.00}` |
| `POST` | `/api/v1/friends` | 친구 관계 생성 `{"user_a": "alice", "user_b": "bob"}` |
| `DELETE` | `/api/v1/friends` | 친구 관계 삭제 |
| `GET` | `/api/v1/friends/{user_id}` | 친구 목록 조회 |
| `GET` | `/api/v1/nearby/{user_id}` | 주변 친구 조회 (`?radius=10` 반경 지정 가능) |
| `GET` | `/api/v1/location/{user_id}` | 현재 캐시된 위치 조회 |
| `GET` | `/api/v1/location-history/{user_id}` | 위치 히스토리 조회 |
| `GET` | `/health` | 헬스 체크 |

### WebSocket

| 경로 | 설명 |
|------|------|
| `ws://localhost:8018/ws/{user_id}` | 실시간 위치 추적 연결 |

**클라이언트 -> 서버:**

| type | 필드 | 설명 |
|------|------|------|
| `location_update` | `latitude`, `longitude` | 위치 갱신 |
| `get_nearby` | `radius_miles` _(선택)_ | 주변 친구 조회 |

**서버 -> 클라이언트:**

| type | 필드 | 설명 |
|------|------|------|
| `location_ack` | `latitude`, `longitude`, `timestamp` | 위치 갱신 확인 |
| `friend_location` | `user_id`, `latitude`, `longitude`, `distance_miles` | 근처 친구 위치 업데이트 |
| `nearby_result` | `nearby_friends` | 주변 친구 목록 응답 |

## Redis 데이터 구조

| 키 패턴 | 타입 | 설명 |
|---------|------|------|
| `location:{user_id}` | Hash (TTL=60s) | `{latitude, longitude, timestamp}` |
| `location_history:{user_id}` | Sorted Set | 위치 히스토리 (score=timestamp) |
| `friends:{user_id}` | Set | 친구 목록 |
| `location:{user_id}` | Pub/Sub Channel | 위치 브로드캐스트 채널 |

## 환경 변수

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `REDIS_HOST` | `redis` | Redis 호스트 |
| `REDIS_PORT` | `6379` | Redis 포트 |
| `LOCATION_TTL` | `60` | 위치 캐시 TTL (초) |
| `NEARBY_RADIUS_MILES` | `5` | 주변 친구 기본 반경 (마일) |

## 테스트 실행

```bash
# 의존성 설치
pip install -r server/requirements.txt

# 테스트 실행
pytest tests/ -v
```

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide Vol.2", Chapter 18
