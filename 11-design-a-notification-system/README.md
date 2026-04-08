# Design A Notification System

알림 시스템(Notification System)은 모바일 푸시, SMS, 이메일 등 다양한 채널을 통해
사용자에게 중요한 정보를 전달하는 시스템이다. 높은 처리량, 확장성, 신뢰성이 요구되며
메시지 큐를 활용하여 서비스 간 결합도를 낮추고 개별 컴포넌트를 독립적으로 확장할 수 있다.

## 아키텍처

```
                          ┌──────────────────────────────────────────────────────────┐
                          │                   Notification System                    │
                          │                                                          │
┌─────────────┐           │  ┌──────────────┐    ┌──────────────────────────────┐     │
│  Service 1  │───┐       │  │              │    │     Message Queues (Redis)   │     │
└─────────────┘   │       │  │              │    │                              │     │
                  │       │  │  Notification │    │  ┌──────────┐               │     │    ┌──────────────┐
┌─────────────┐   ├──────▶│  │   Server     │───▶│  │queue:push│───▶ Worker ───┼────▶│  │ APNs / FCM   │───▶ iOS/Android
│  Service 2  │───┤       │  │  (FastAPI)   │    │  └──────────┘               │     │    └──────────────┘
└─────────────┘   │       │  │              │    │  ┌──────────┐               │     │    ┌──────────────┐
                  │       │  │  - Dispatcher│    │  │queue:sms │───▶ Worker ───┼────▶│  │ Twilio/Nexmo │───▶ Phone
┌─────────────┐   │       │  │  - Template  │    │  └──────────┘               │     │    └──────────────┘
│  Service N  │───┘       │  │  - RateLimit │    │  ┌───────────┐              │     │    ┌──────────────┐
└─────────────┘           │  │              │    │  │queue:email│───▶ Worker ───┼────▶│  │ SendGrid/SES │───▶ Inbox
                          │  └──────────────┘    │  └───────────┘              │     │    └──────────────┘
                          │                      └──────────────────────────────┘     │
                          └──────────────────────────────────────────────────────────┘
```

### 알림 유형

| 유형 | 채널 | Third-party Service | 대상 |
|------|------|---------------------|------|
| Push | `push` | APNs (iOS), FCM (Android) | 모바일 기기 |
| SMS | `sms` | Twilio, Nexmo | 전화번호 |
| Email | `email` | SendGrid, Amazon SES | 이메일 주소 |

### 연락처 정보 수집 흐름

사용자가 앱을 설치하거나 계정을 생성할 때 연락처 정보를 수집한다:

```
사용자 가입/로그인
    │
    ▼
┌─────────────────┐     ┌─────────────┐
│  API Server     │────▶│   User DB   │
│  - device token │     │  - email    │
│  - phone number │     │  - phone    │
│  - email        │     │  - tokens[] │
└─────────────────┘     └─────────────┘
```

- **iOS Push**: APNs device token
- **Android Push**: FCM registration token
- **SMS**: 전화번호
- **Email**: 이메일 주소

## High-level Design

### 초기 설계 (직접 전송)

```
Service ──▶ Notification Server ──▶ Third-party Service ──▶ Device
```

**문제점**: Third-party 서비스 장애 시 알림 유실, 하나의 컴포넌트가 병목

### 개선된 설계 (메시지 큐)

```
Service ──▶ Notification Server ──▶ Message Queue ──▶ Worker ──▶ Third-party Service
```

**개선점**:
- 메시지 큐가 **버퍼** 역할 → 일시적 장애에도 메시지 유실 없음
- Worker 를 독립적으로 **수평 확장** 가능
- 채널별 큐 분리 → 한 채널의 장애가 다른 채널에 영향 없음

## 핵심 컴포넌트

### 1. Dispatcher (`notification/dispatcher.py`)

알림 요청을 검증하고 올바른 채널 큐로 라우팅한다:

```python
async def dispatch_notification(
    redis: Redis,
    request: NotificationRequest,
) -> dict[str, Any]:
    """알림 요청을 검증하고 적절한 채널 큐에 넣는다.

    처리 흐름:
      1. 사용자 설정 확인 (opt-out 여부)
      2. Rate limit 확인
      3. 템플릿 렌더링
      4. 알림 레코드 생성 및 Redis 에 저장
      5. 채널 큐에 LPUSH
    """
    # 1. 사용자 설정 확인 — opt-out 채널이면 전송하지 않음
    prefs = await get_user_preferences(redis, request.user_id)
    channel_enabled = getattr(prefs, request.channel.value, True)
    if not channel_enabled:
        return {"status": "skipped", "message": f"User opted out of {request.channel.value}"}

    # 2. Rate limit 확인
    allowed = await check_rate_limit(redis, request.user_id, request.channel.value)
    if not allowed:
        return {"status": "rate_limited", "message": f"Rate limit exceeded"}

    # 3. 템플릿 렌더링
    rendered = render_template(request.template, request.params)

    # 4. 알림 레코드 생성 및 저장
    record = NotificationRecord(user_id=request.user_id, channel=request.channel, ...)
    await redis.hset(f"notification:{record.notification_id}", mapping=flat)

    # 5. 채널 큐에 메시지 삽입 (LPUSH)
    queue_name = f"queue:{request.channel.value}"  # queue:push, queue:sms, queue:email
    await redis.lpush(queue_name, message)
    return {"notification_id": record.notification_id, "status": "pending"}
```

### 2. Template Engine (`notification/template.py`)

템플릿에 정의된 제목/본문에서 `{변수명}` 을 실제 값으로 치환한다:

```python
# 채널별 알림 템플릿 정의
TEMPLATES: dict[str, dict[str, str]] = {
    "welcome": {
        "title": "Welcome, {name}!",
        "body": "Hi {name}, welcome to our service. We are glad to have you!",
    },
    "payment": {
        "title": "Payment Received",
        "body": "Hi {name}, your payment of {amount} has been processed successfully.",
    },
    "shipping": {
        "title": "Order Shipped",
        "body": "Hi {name}, your order #{order_id} has been shipped. Tracking: {tracking}",
    },
}


def render_template(template_name: str, params: dict[str, Any]) -> dict[str, str]:
    """템플릿 이름과 파라미터로 제목/본문을 렌더링한다.

    >>> render_template("welcome", {"name": "Alice"})
    {'title': 'Welcome, Alice!', 'body': 'Hi Alice, welcome to our service. ...'}
    """
    tmpl = TEMPLATES.get(template_name, TEMPLATES["default"])
    title = tmpl["title"].format_map(_SafeDict(params))
    body = tmpl["body"].format_map(_SafeDict(params))
    return {"title": title, "body": body}


class _SafeDict(dict):
    """누락된 키를 {key} 그대로 남기는 dict — format_map 에서 KeyError 방지."""
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"
```

### 3. Rate Limiter (`notification/rate_limiter.py`)

Redis Sorted Set 기반 슬라이딩 윈도우로 사용자별/채널별 전송 횟수를 제한한다:

```python
# 채널별 시간당 최대 전송 수
RATE_LIMITS: dict[str, int] = {
    "push": 10,   # 시간당 10회
    "sms": 5,     # 시간당 5회
    "email": 20,  # 시간당 20회
}

WINDOW_SIZE: int = 3600  # 1시간


async def check_rate_limit(redis: Redis, user_id: str, channel: str) -> bool:
    """사용자의 채널별 rate limit 을 확인한다.

    Returns:
        True 이면 전송 허용, False 이면 rate limit 초과.
    """
    max_count = RATE_LIMITS.get(channel, 10)
    key = f"rate_limit:{user_id}:{channel}"
    now = time.time()
    window_start = now - WINDOW_SIZE

    pipe = redis.pipeline()
    pipe.zremrangebyscore(key, "-inf", window_start)  # 윈도우 밖 레코드 제거
    pipe.zcard(key)                                    # 현재 윈도우 내 카운트
    results = await pipe.execute()
    current_count: int = results[1]

    if current_count >= max_count:
        return False

    # 허용: 현재 타임스탬프를 기록
    await redis.zadd(key, {str(now): now})
    await redis.expire(key, WINDOW_SIZE + 1)
    return True
```

### 4. Channel Handlers (`channels/push.py`, `sms.py`, `email.py`)

실제 환경에서는 APNs/FCM, Twilio, SendGrid 등의 API 를 호출하지만,
여기서는 로그 출력으로 시뮬레이션한다. 설정 가능한 실패율(`FAILURE_RATE`)을
통해 retry 메커니즘을 테스트할 수 있다:

```python
# channels/push.py — Push 알림 (APNs/FCM 시뮬레이션)
async def send_push(user_id: str, title: str, body: str, failure_rate: float | None = None) -> bool:
    rate = failure_rate if failure_rate is not None else settings.FAILURE_RATE

    # 설정된 확률로 실패를 시뮬레이션
    if random.random() < rate:
        logger.error("[PUSH FAILED] user=%s title='%s' (simulated failure)", user_id, title)
        return False

    logger.info("[PUSH SENT] user=%s title='%s' body='%s'", user_id, title, body)
    return True


# channels/sms.py — SMS (Twilio/Nexmo 시뮬레이션)
async def send_sms(user_id: str, title: str, body: str, failure_rate: float | None = None) -> bool:
    # ... 동일한 패턴 (로그로 시뮬레이션)


# channels/email.py — Email (SendGrid/SES 시뮬레이션)
async def send_email(user_id: str, title: str, body: str, failure_rate: float | None = None) -> bool:
    # ... 동일한 패턴 (로그로 시뮬레이션)
```

### 5. Worker/Consumer (`worker/consumer.py`)

Redis 큐에서 메시지를 꺼내 채널 핸들러로 전송하고, 실패 시 **exponential backoff** 으로
재시도한다:

```python
# 채널별 전송 함수 매핑
CHANNEL_HANDLERS = {
    "push": send_push,
    "sms": send_sms,
    "email": send_email,
}

QUEUES = ["queue:push", "queue:sms", "queue:email"]


async def process_message(redis: Redis, raw_message: str) -> None:
    """큐에서 꺼낸 메시지 하나를 처리한다."""
    message = json.loads(raw_message)
    notification_id = message["notification_id"]
    channel = message["channel"]
    retry_count = message.get("retry_count", 0)

    # 1. 중복 확인 — 이미 sent/delivered 이면 스킵
    if await is_duplicate(redis, notification_id):
        return

    # 2. 채널 핸들러 호출
    handler = CHANNEL_HANDLERS[channel]
    success = await handler(message["user_id"], message["title"], message["body"])

    if success:
        # 전송 성공 → 상태를 sent 로 갱신
        await update_notification_status(redis, notification_id, "sent", retry_count)
    else:
        # 전송 실패 → retry 또는 failed
        retry_count += 1
        if retry_count <= settings.MAX_RETRIES:
            # Exponential backoff: 2^(retry_count-1) 초 대기 후 재큐잉
            backoff = 2 ** (retry_count - 1)  # 1s, 2s, 4s
            logger.warning("Notification %s failed (attempt %d/%d), retrying in %ds...",
                           notification_id, retry_count, settings.MAX_RETRIES, backoff)
            await asyncio.sleep(backoff)
            message["retry_count"] = retry_count
            await redis.lpush(f"queue:{channel}", json.dumps(message))
        else:
            # 최대 재시도 초과 → failed
            await update_notification_status(redis, notification_id, "failed", retry_count)


async def consume_queues(redis: Redis) -> None:
    """모든 채널 큐에서 메시지를 지속적으로 소비하는 워커 루프.

    BRPOP 으로 블로킹 대기하며, 메시지가 도착하면 process_message 를 호출한다.
    """
    while True:
        result = await redis.brpop(QUEUES, timeout=settings.WORKER_POLL_INTERVAL)
        if result is None:
            continue  # 타임아웃 — 메시지 없음
        _queue_name, raw_message = result
        await process_message(redis, raw_message)
```

## 신뢰성 (Reliability)

### 알림 로그 (Notification Log)

모든 알림은 Redis Hash 에 기록되어 상태를 추적할 수 있다:

```
notification:{id}
├── notification_id: "uuid"
├── user_id: "user123"
├── channel: "push"
├── template: "welcome"
├── status: "pending" → "sent" → "delivered" / "failed"
├── retry_count: 0
├── created_at: "2024-01-01T00:00:00"
├── updated_at: "2024-01-01T00:00:01"
└── sent_at: "2024-01-01T00:00:01"
```

### Retry 메커니즘

전송 실패 시 **exponential backoff** 으로 최대 3회까지 재시도한다:

```
시도 1 실패 → 1초 대기 → 재큐잉
시도 2 실패 → 2초 대기 → 재큐잉
시도 3 실패 → 4초 대기 → 재큐잉
시도 4 실패 → 상태를 "failed" 로 마킹
```

| 시도 | 대기 시간 | 계산식 |
|------|----------|--------|
| 1 | 1s | 2^(1-1) = 1 |
| 2 | 2s | 2^(2-1) = 2 |
| 3 | 4s | 2^(3-1) = 4 |

### 중복 처리 방지 (Dedup)

Worker 가 메시지를 처리하기 전에 `notification_id` 로 이미 `sent` 또는 `delivered`
상태인지 확인한다. 네트워크 장애 등으로 같은 메시지가 큐에 중복 삽입되더라도
한 번만 전송된다.

```python
async def is_duplicate(redis: Redis, notification_id: str) -> bool:
    """이미 처리 완료(sent/delivered)된 알림인지 확인한다."""
    status = await redis.hget(f"notification:{notification_id}", "status")
    return status in ("sent", "delivered")
```

## 이벤트 추적 (Event Tracking)

알림의 라이프사이클을 추적한다:

```
                ┌─────────┐
                │ pending │  큐에 삽입됨
                └────┬────┘
                     │
                     ▼
                ┌─────────┐
                │  sent   │  채널 핸들러 전송 성공
                └────┬────┘
                     │
                     ▼
                ┌───────────┐
                │ delivered │  사용자 기기에 도달 (확인)
                └───────────┘

실패 경로:
                ┌─────────┐
                │ pending │ ──retry──▶ pending (retry_count 증가)
                └────┬────┘
                     │ (max retries 초과)
                     ▼
                ┌─────────┐
                │ failed  │
                └─────────┘
```

## Quick Start

```bash
# 서비스 시작
docker-compose up --build

# 헬스 체크
curl http://localhost:8011/health

# 알림 전송
curl -X POST http://localhost:8011/api/v1/notify \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user123", "channel": "push", "template": "welcome", "params": {"name": "Alice"}}'

# 알림 상태 확인
curl http://localhost:8011/api/v1/notifications/<notification_id>/status

# 사용자 알림 히스토리
curl http://localhost:8011/api/v1/notifications/user123

# 사용자 설정 조회
curl http://localhost:8011/api/v1/settings/user123

# 사용자 설정 변경 (SMS opt-out)
curl -X PUT http://localhost:8011/api/v1/settings/user123 \
  -H "Content-Type: application/json" \
  -d '{"push": true, "sms": false, "email": true}'
```

## CLI 사용법

```bash
# 알림 전송
python scripts/cli.py send --user user123 --channel push --template welcome --params '{"name": "Alice"}'

# 배치 전송
python scripts/cli.py batch --users user1,user2,user3 --channel email --template payment --params '{"amount": "$99"}'

# 알림 상태 확인
python scripts/cli.py status <notification_id>

# 사용자 알림 히스토리
python scripts/cli.py history user123

# 사용자 설정 변경
python scripts/cli.py settings user123 --push on --sms off --email on

# 헬스 체크 (큐 상태 포함)
python scripts/cli.py --health
```

### CLI 출력 예시

```
$ python scripts/cli.py send --user user123 --channel push --template welcome --params '{"name": "Alice"}'
[200] {
  "notification_id": "a1b2c3d4-...",
  "status": "pending",
  "message": "Notification queued to push"
}

$ python scripts/cli.py history user123
User: user123 (3 notifications)
  [    sent] a1b2c3d4... channel=push template=welcome
  [  failed] e5f6g7h8... channel=sms template=payment
  [ pending] i9j0k1l2... channel=email template=shipping

$ python scripts/cli.py --health
Health: 200
  Status: ok
  Queue push: 0 pending
  Queue sms: 2 pending
  Queue email: 0 pending
```

## API Endpoints

| Method | Path | 설명 |
|--------|------|------|
| `POST` | `/api/v1/notify` | 알림 전송 |
| `POST` | `/api/v1/notify/batch` | 배치 알림 전송 |
| `GET` | `/api/v1/notifications/{user_id}` | 사용자 알림 히스토리 |
| `GET` | `/api/v1/notifications/{notification_id}/status` | 알림 상태 조회 |
| `GET` | `/api/v1/settings/{user_id}` | 사용자 알림 설정 조회 |
| `PUT` | `/api/v1/settings/{user_id}` | 사용자 알림 설정 변경 |
| `GET` | `/health` | 헬스 체크 (큐 상태 포함) |

### 요청/응답 예시

**POST /api/v1/notify**

```json
{
  "user_id": "user123",
  "channel": "push",
  "template": "welcome",
  "params": {"name": "Alice", "amount": "$99.99"},
  "priority": "high"
}
```

**POST /api/v1/notify/batch**

```json
{
  "user_ids": ["user1", "user2", "user3"],
  "channel": "email",
  "template": "payment",
  "params": {"name": "User", "amount": "$99"},
  "priority": "normal"
}
```

## 환경 변수

| 변수 | 기본값 | 설명 |
|---|---|---|
| `REDIS_HOST` | `redis` | Redis 호스트 |
| `REDIS_PORT` | `6379` | Redis 포트 |
| `RATE_LIMIT_PUSH` | `10` | Push 시간당 최대 전송 수 |
| `RATE_LIMIT_SMS` | `5` | SMS 시간당 최대 전송 수 |
| `RATE_LIMIT_EMAIL` | `20` | Email 시간당 최대 전송 수 |
| `FAILURE_RATE` | `0.1` | 시뮬레이션 실패율 (0.0~1.0) |
| `WORKER_POLL_INTERVAL` | `1` | Worker 큐 폴링 간격 (초) |
| `MAX_RETRIES` | `3` | 최대 재시도 횟수 |

## 테스트

```bash
# 의존성 설치
pip install fakeredis pytest pytest-asyncio

# 테스트 실행
python -m pytest tests/ -v
```

| 테스트 | 검증 내용 |
|--------|----------|
| Template rendering | 변수 치환, 누락 파라미터 처리, 미정의 템플릿 |
| Dispatcher routing | push/sms/email 큐 라우팅 |
| Rate limiting | 제한 이내 허용, 초과 차단, 채널/사용자 독립성 |
| User preferences | opt-out 차단, opt-in 허용, 기본값 |
| Retry mechanism | 실패 → 재큐잉, 재시도 성공, 최대 초과 → failed |
| Dedup | 중복 메시지 스킵, pending 은 비중복 |
| Status tracking | pending → sent 전환, sent_at 기록 |

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide", Chapter 10
