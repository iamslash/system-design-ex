# Design A Chat System

채팅 시스템은 사용자 간 실시간 메시지 교환을 지원하는 서비스다. 1:1 채팅과
그룹 채팅을 모두 지원하며, WebSocket 을 통한 양방향 실시간 통신이 핵심이다.

## 아키텍처

```
 ┌──────────┐  ┌──────────┐  ┌──────────┐
 │ Client A │  │ Client B │  │ Client C │
 └────┬─────┘  └────┬─────┘  └────┬─────┘
      │ ws          │ ws          │ ws
      ▼             ▼             ▼
 ┌─────────────────────────────────────────┐
 │            Chat Server (FastAPI)         │
 │                                         │
 │  ┌──────────────┐  ┌────────────────┐   │
 │  │ Connection   │  │ Message        │   │
 │  │ Manager      │  │ Handler        │   │
 │  │ (WebSocket)  │  │ (1:1 & Group)  │   │
 │  └──────────────┘  └────────────────┘   │
 │  ┌──────────────┐  ┌────────────────┐   │
 │  │ Presence     │  │ ID Generator   │   │
 │  │ Tracker      │  │ (Snowflake)    │   │
 │  └──────────────┘  └────────────────┘   │
 └──────────────────┬──────────────────────┘
                    │ GET / SET
                    ▼
 ┌─────────────────────────────────────────┐
 │          Redis 7 (Alpine)               │
 │                                         │
 │  - messages:{channel}  (Sorted Set)     │
 │  - presence:{user_id}  (Hash)           │
 │  - user:{user_id}      (Hash)           │
 │  - group:{group_id}    (Hash)           │
 └─────────────────────────────────────────┘
```

### 요청 흐름

1. 클라이언트가 `ws://localhost:8013/ws/{user_id}` 로 WebSocket 연결을 맺는다.
2. 서버는 Connection Manager 에 연결을 등록하고 Presence 를 online 으로 설정한다.
3. 클라이언트가 JSON 메시지를 전송하면 Message Handler 가 수신자에게 라우팅한다.
4. 모든 메시지는 Redis Sorted Set 에 타임스탬프 기준으로 저장된다.
5. 연결이 끊어지면 Presence 를 offline 으로 변경하고 다른 사용자에게 통보한다.

## Polling vs Long Polling vs WebSocket

| 방식 | 동작 | 지연 | 서버 부하 | 양방향 |
|------|------|------|----------|--------|
| Polling | 클라이언트가 주기적으로 서버에 요청 | 높음 (주기 간격) | 높음 (불필요한 요청) | X |
| Long Polling | 서버가 새 데이터 있을 때까지 응답 보류 | 중간 | 중간 | X |
| **WebSocket** | 한번 연결 후 양방향 실시간 통신 | **낮음** | **낮음** | **O** |

채팅 시스템에서는 **WebSocket** 이 최적이다. 한번 연결을 맺으면 서버와 클라이언트가
자유롭게 메시지를 주고받을 수 있어 실시간 채팅에 이상적이다.

## 상위 레벨 설계

| 계층 | 역할 | 구현 |
|------|------|------|
| Stateless (API) | 사용자 등록, 그룹 생성, 히스토리 조회 | FastAPI REST |
| **Stateful (Chat)** | 실시간 메시지 라우팅, 연결 관리 | FastAPI WebSocket |
| Storage | 메시지 저장, 사용자/그룹 정보, 프레즌스 | Redis |
| Third-party | 푸시 알림 (오프라인 사용자) | _(확장 가능)_ |

## 핵심 구현

### 1. WebSocket Connection Manager

활성 WebSocket 연결을 사용자별로 관리한다. 하나의 사용자가 여러 디바이스에서
접속할 수 있으므로 `user_id -> list[WebSocket]` 형태로 다중 연결을 지원한다.

```python
class ConnectionManager:
    """활성 WebSocket 연결을 사용자별로 관리한다."""

    def __init__(self) -> None:
        # user_id -> 활성 WebSocket 연결 목록
        self._connections: dict[str, list[WebSocket]] = {}

    async def connect(self, user_id: str, websocket: WebSocket) -> None:
        """새 WebSocket 연결을 수락하고 사용자 연결 목록에 추가한다."""
        await websocket.accept()
        if user_id not in self._connections:
            self._connections[user_id] = []
        self._connections[user_id].append(websocket)

    def disconnect(self, user_id: str, websocket: WebSocket) -> None:
        """사용자의 WebSocket 연결을 제거한다."""
        if user_id in self._connections:
            self._connections[user_id] = [
                ws for ws in self._connections[user_id] if ws is not websocket
            ]
            if not self._connections[user_id]:
                del self._connections[user_id]

    async def send_to_user(self, user_id: str, message: dict) -> None:
        """특정 사용자의 모든 연결에 메시지를 전송한다."""
        if user_id in self._connections:
            payload = json.dumps(message)
            for ws in self._connections[user_id]:
                await ws.send_text(payload)

    async def broadcast(self, user_ids: list[str], message: dict) -> None:
        """여러 사용자에게 메시지를 브로드캐스트한다."""
        for user_id in user_ids:
            await self.send_to_user(user_id, message)
```

### 2. Message Handler (1:1 + 그룹 메시지 라우팅)

1:1 메시지와 그룹 메시지를 각각 처리한다. 메시지 ID 생성 → Redis 저장 →
수신자에게 실시간 전달의 흐름으로 동작한다.

```python
def make_dm_channel(user_a: str, user_b: str) -> str:
    """1:1 채팅의 채널 ID 를 생성한다.
    두 사용자를 정렬하여 동일한 쌍은 항상 같은 채널 ID 를 갖도록 한다.
    예: dm:alice:bob == dm:bob:alice
    """
    a, b = sorted([user_a, user_b])
    return f"dm:{a}:{b}"


class MessageHandler:
    """1:1 메시지와 그룹 메시지를 라우팅한다."""

    async def handle_dm(self, from_user: str, to_user: str, content: str) -> dict:
        """1:1 메시지 처리: 채널 ID 생성 → 저장 → 수신자 전달."""
        channel_id = make_dm_channel(from_user, to_user)
        msg_id = id_generator.generate()
        message_data = {
            "type": "message", "message_id": msg_id,
            "from": from_user, "to": to_user,
            "content": content, "channel_id": channel_id,
            "timestamp": time.time(),
        }
        await self._store.save_message(channel_id, message_data)  # Redis 저장
        await self._conn.send_to_user(to_user, message_data)      # 수신자 전달
        await self._conn.send_to_user(from_user, message_data)    # 발신자 동기화
        return message_data

    async def handle_group_message(self, from_user: str, group_id: str, content: str) -> dict:
        """그룹 메시지 처리: 멤버 조회 → 저장 → 전체 멤버 전달."""
        members = json.loads(group_data.get("members", "[]"))
        channel_id = f"group:{group_id}"
        # ... 메시지 생성 후 저장
        await self._store.save_message(channel_id, message_data)
        await self._conn.broadcast(members, message_data)  # 모든 멤버에게 전달
        return message_data
```

### 3. Message ID Generator (Snowflake-like)

시간순 정렬이 가능한 고유 메시지 ID 를 생성한다. 밀리초 타임스탬프 + 시퀀스
카운터로 구성되어, 같은 밀리초 내에서도 고유성을 보장한다.

```python
class IdGenerator:
    """Snowflake 방식의 시간 기반 고유 ID 생성기.
    형식: "{timestamp_ms}-{sequence}"
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_timestamp_ms: int = 0
        self._sequence: int = 0

    def generate(self) -> str:
        """고유하고 시간순 정렬 가능한 메시지 ID 를 생성한다."""
        with self._lock:
            now_ms = int(time.time() * 1000)
            if now_ms == self._last_timestamp_ms:
                self._sequence += 1          # 같은 밀리초: 시퀀스 증가
            else:
                self._last_timestamp_ms = now_ms
                self._sequence = 0           # 새 밀리초: 시퀀스 초기화
            return f"{now_ms}-{self._sequence}"
```

### 4. Presence Tracker (하트비트 기반 온라인/오프라인)

사용자가 주기적으로(5초마다) 하트비트를 전송하고, 타임아웃(30초) 내에
하트비트가 없으면 오프라인으로 판정한다.

```python
class PresenceTracker:
    """하트비트 기반 사용자 온라인/오프라인 상태 추적기.
    Redis key: presence:{user_id} -> {status, last_heartbeat}
    """

    async def heartbeat(self, user_id: str) -> None:
        """하트비트를 수신하여 last_heartbeat 를 갱신한다."""
        now = time.time()
        await self._redis.hset(
            f"presence:{user_id}",
            mapping={"status": "online", "last_heartbeat": str(now)},
        )

    async def get_status(self, user_id: str) -> dict:
        """사용자의 현재 접속 상태를 조회한다.
        last_heartbeat 이후 HEARTBEAT_TIMEOUT 초가 지나면 offline 판정.
        """
        data = await self._redis.hgetall(f"presence:{user_id}")
        last_hb = float(data.get("last_heartbeat", "0"))
        status = data.get("status", "offline")

        # 타임아웃 확인
        if status == "online" and (time.time() - last_hb) > self._timeout:
            status = "offline"
            await self.set_offline(user_id)

        return {"user_id": user_id, "status": status, "last_heartbeat": last_hb}
```

### 5. Message Store (Redis Sorted Set)

메시지를 채널별 Redis Sorted Set 에 저장한다. 타임스탬프를 score 로 사용하여
효율적인 시간순 조회와 페이지네이션을 지원한다.

```python
class MessageStore:
    """Redis Sorted Set 기반 메시지 저장소.
    key: messages:{channel_id}, score: timestamp, member: message JSON
    """

    async def save_message(self, channel_id: str, message: dict) -> None:
        """메시지를 Redis Sorted Set 에 저장한다."""
        key = f"messages:{channel_id}"
        ts = message.get("timestamp", 0)
        await self._redis.zadd(key, {json.dumps(message): ts})

    async def get_messages(self, channel_id: str, limit: int = 100) -> list[dict]:
        """채널의 최신 메시지를 조회한다 (ZREVRANGE → 시간순 반환)."""
        key = f"messages:{channel_id}"
        raw = await self._redis.zrevrange(key, 0, limit - 1)
        return [json.loads(m) for m in reversed(raw)]  # 오래된 것 먼저

    async def get_max_message_id(self, channel_id: str) -> str | None:
        """채널의 최신 메시지 ID 반환 (디바이스 간 동기화용)."""
        latest = await self._redis.zrevrange(key, 0, 0)
        if latest:
            return json.loads(latest[0]).get("message_id")
        return None
```

## 1:1 채팅 흐름

```
Alice (Client)                    Server                     Bob (Client)
     │                              │                              │
     │──── ws connect ─────────────▶│                              │
     │                              │◀──── ws connect ─────────────│
     │                              │                              │
     │  {"type":"message",          │                              │
     │   "to":"bob",                │                              │
     │   "content":"Hi!"}           │                              │
     │─────────────────────────────▶│                              │
     │                              │  1. make_dm_channel(alice,bob)
     │                              │  2. id_generator.generate()
     │                              │  3. message_store.save()
     │                              │                              │
     │◀─── message (발신자 동기화) ──│── message (수신자 전달) ────▶│
     │                              │                              │
     │  {"type":"message",          │  {"type":"message",          │
     │   "from":"alice",            │   "from":"alice",            │
     │   "message_id":"...",        │   "message_id":"...",        │
     │   "content":"Hi!"}           │   "content":"Hi!"}           │
```

## 그룹 채팅 흐름

```
Alice                     Server                     Bob        Charlie
  │                         │                         │            │
  │  {"type":"group_message",│                         │            │
  │   "group_id":"team1",   │                         │            │
  │   "content":"Hello!"}   │                         │            │
  │────────────────────────▶│                         │            │
  │                         │  1. Redis: group:team1 조회
  │                         │  2. members = [alice,bob,charlie]
  │                         │  3. message_store.save()
  │                         │  4. broadcast(members)
  │                         │                         │            │
  │◀──── group_message ─────│──── group_message ─────▶│            │
  │                         │──── group_message ──────────────────▶│
```

## 디바이스 간 메시지 동기화

각 디바이스는 마지막으로 수신한 `cur_max_message_id` 를 로컬에 저장한다.
재접속 시 서버에 이 ID 를 전달하면 그 이후의 메시지만 받을 수 있다.

```
Device A (online)          Server              Device B (재접속)
     │                       │                       │
     │── message ───────────▶│                       │
     │                       │── save (Redis) ──▶    │
     │                       │                       │
     │                       │◀── "내 마지막 ID 는 104-0" ──│
     │                       │                       │
     │                       │── 105-0 이후 메시지 ─▶│
```

`get_max_message_id()` 로 채널의 최신 메시지 ID 를 조회할 수 있다.

## 온라인 프레즌스 (Heartbeat)

```
Client                              Server                    Redis
  │                                    │                        │
  │── ws connect ─────────────────────▶│                        │
  │                                    │── HSET presence:alice  │
  │                                    │   {status:online}  ───▶│
  │                                    │                        │
  │── {"type":"heartbeat"} (5초마다) ─▶│                        │
  │                                    │── HSET last_heartbeat ▶│
  │                                    │                        │
  │     ... 30초간 하트비트 없음 ...    │                        │
  │                                    │                        │
  │  (다른 사용자가 presence 조회)      │                        │
  │                                    │── HGET presence:alice ▶│
  │                                    │◀── last_hb = 30초 전 ──│
  │                                    │                        │
  │                                    │  timeout 초과 → offline │
```

## Quick Start

```bash
# 서비스 시작
docker-compose up --build

# 헬스 체크
curl http://localhost:8013/health

# 사용자 등록
curl -X POST http://localhost:8013/api/v1/users \
  -H "Content-Type: application/json" \
  -d '{"user_id": "alice", "name": "Alice"}'

# 그룹 생성
curl -X POST http://localhost:8013/api/v1/groups \
  -H "Content-Type: application/json" \
  -d '{"group_id": "team1", "name": "Team", "members": ["alice", "bob", "charlie"]}'

# 메시지 히스토리 조회
curl http://localhost:8013/api/v1/messages/dm:alice:bob

# 프레즌스 확인
curl http://localhost:8013/api/v1/presence/alice
```

## CLI 사용법

```bash
# 인터랙티브 채팅 모드
python scripts/cli.py chat alice

# 채팅 모드에서:
# /msg bob Hello!          — 1:1 메시지 전송
# /group team1 Hi team!    — 그룹 메시지 전송
# /history bob             — 1:1 채팅 히스토리 조회
# /online                  — 접속 중인 사용자 확인
# /quit                    — 연결 종료

# 비대화형 명령
python scripts/cli.py history alice bob         # DM 히스토리 조회
python scripts/cli.py create-group team1 alice bob charlie
python scripts/cli.py presence alice
python scripts/cli.py --health
```

## API Endpoints

### REST

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `POST` | `/api/v1/users` | 사용자 등록 `{"user_id": "alice", "name": "Alice"}` |
| `POST` | `/api/v1/groups` | 그룹 생성 `{"group_id": "team1", "name": "Team", "members": [...]}` |
| `GET` | `/api/v1/messages/{channel_id}` | 메시지 히스토리 (`dm:alice:bob` 또는 `group:team1`) |
| `GET` | `/api/v1/presence/{user_id}` | 사용자 온라인 상태 조회 |
| `GET` | `/health` | 헬스 체크 |

### WebSocket

| 경로 | 설명 |
|------|------|
| `ws://localhost:8013/ws/{user_id}` | WebSocket 채팅 연결 |

**클라이언트 → 서버:**

| type | 필드 | 설명 |
|------|------|------|
| `message` | `to`, `content` | 1:1 메시지 |
| `group_message` | `group_id`, `content` | 그룹 메시지 |
| `heartbeat` | _(없음)_ | 프레즌스 유지 |

**서버 → 클라이언트:**

| type | 필드 | 설명 |
|------|------|------|
| `message` | `from`, `content`, `message_id`, `timestamp` | 수신 메시지 |
| `group_message` | `from`, `group_id`, `content`, `message_id`, `timestamp` | 그룹 메시지 |
| `presence` | `user_id`, `status` | 사용자 상태 변경 알림 |

## Redis 데이터 구조

| 키 패턴 | 타입 | 설명 |
|---------|------|------|
| `messages:{channel_id}` | Sorted Set | 메시지 (score=timestamp) |
| `presence:{user_id}` | Hash | `{status, last_heartbeat}` |
| `user:{user_id}` | Hash | `{name, created_at}` |
| `group:{group_id}` | Hash | `{name, members (JSON array)}` |

## 환경 변수

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `REDIS_HOST` | `redis` | Redis 호스트 |
| `REDIS_PORT` | `6379` | Redis 포트 |
| `HEARTBEAT_INTERVAL` | `5` | 하트비트 전송 주기 (초) |
| `HEARTBEAT_TIMEOUT` | `30` | 오프라인 판정 타임아웃 (초) |

## 테스트 실행

```bash
# 의존성 설치
pip install -r server/requirements.txt

# 테스트 실행
pytest tests/ -v
```

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide", Chapter 12
