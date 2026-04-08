# Design Hotel Reservation System

호텔 예약 시스템은 객실 재고를 관리하고 예약의 생성, 확인, 취소를 처리하는 시스템이다.
동시성 제어, 멱등성 보장, 오버부킹 정책이 핵심 설계 포인트다.

## 아키텍처

```
                    ┌──────────────────────────────────────────────────────┐
                    │                   Client (CLI)                       │
                    └──────────────────┬───────────────────────────────────┘
                                       │ HTTP (Hotel / Inventory / Reservation)
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                          FastAPI Application (:8023)                         │
│                                                                              │
│  ┌────────────────┐  ┌────────────────────┐  ┌──────────────────────────┐   │
│  │  Hotel Service  │  │ Inventory Service  │  │  Reservation Service     │   │
│  │  (Hotel/Room    │  │ (Room availability │  │  (Idempotent booking,    │   │
│  │   CRUD)         │  │  Optimistic lock)  │  │   State machine)         │   │
│  └───────┬─────── ┘  └────────┬───────────┘  └──────────┬───────────────┘   │
│          │                    │                          │                   │
│          └────────────────────┼──────────────────────────┘                   │
│                               │                                              │
└───────────────────────────────┼──────────────────────────────────────────────┘
                                │ GET / SET / WATCH+MULTI
                                ▼
                    ┌─────────────────────────┐
                    │    Redis 7 (Alpine)      │
                    │                          │
                    │  - Strings (inventory)   │
                    │  - Strings (reservation) │
                    │  - Sets (hotel index)    │
                    └─────────────────────────┘
```

### 주요 컴포넌트

| 컴포넌트 | 역할 | 파일 |
|----------|------|------|
| **Hotel Service** | 호텔/객실 타입 CRUD | `api/hotel/service.py` |
| **Inventory Service** | 날짜별 객실 재고 관리, Optimistic Locking | `api/reservation/inventory.py` |
| **Reservation Service** | 멱등적 예약 생성, 취소, 상태 전이 | `api/reservation/service.py` |

## API 설계

### Hotel / Room Type

| Method | Endpoint | 설명 |
|--------|----------|------|
| `POST` | `/api/v1/hotels` | 호텔 생성 |
| `GET` | `/api/v1/hotels` | 호텔 목록 |
| `GET` | `/api/v1/hotels/{id}` | 호텔 조회 |
| `DELETE` | `/api/v1/hotels/{id}` | 호텔 삭제 |
| `POST` | `/api/v1/hotels/{id}/room-types` | 객실 타입 생성 |
| `GET` | `/api/v1/hotels/{id}/room-types` | 객실 타입 목록 |

### Inventory

| Method | Endpoint | 설명 |
|--------|----------|------|
| `POST` | `/api/v1/inventory/init` | 날짜별 재고 초기화 |
| `GET` | `/api/v1/inventory` | 특정 날짜 재고 조회 |
| `GET` | `/api/v1/inventory/range` | 날짜 범위 재고 조회 |

### Reservation

| Method | Endpoint | 설명 |
|--------|----------|------|
| `POST` | `/api/v1/reservations` | 예약 생성 (멱등) |
| `GET` | `/api/v1/reservations/{id}` | 예약 조회 |
| `GET` | `/api/v1/hotels/{id}/reservations` | 호텔별 예약 목록 |
| `POST` | `/api/v1/reservations/{id}/cancel` | 예약 취소 |
| `POST` | `/api/v1/reservations/{id}/status` | 상태 변경 |

## 데이터 모델 (room_type_inventory)

핵심 데이터 구조는 `room_type_inventory` 로, 호텔/객실타입/날짜 조합별로
총 재고와 예약 수를 추적한다.

```
room_type_inventory {
    hotel_id:        "h1"
    room_type_id:    "rt1"
    date:            "2025-07-01"
    total_inventory: 100        # 총 객실 수
    total_reserved:  85         # 현재 예약된 수
    version:         12         # Optimistic lock 버전
}
```

### Redis 저장 구조

```
Key:   inventory:{hotel_id}:{room_type_id}:{date}
Value: JSON (RoomTypeInventory)

Key:   reservation:{reservation_id}
Value: JSON (Reservation)

Key:   hotel:{hotel_id}
Value: JSON (Hotel)

Key:   hotel_reservations:{hotel_id}
Value: SET of reservation_ids
```

**핵심 구현** -- 재고 키 생성:

```python
def _inventory_key(hotel_id: str, room_type_id: str, date: str) -> str:
    """Redis key for a single inventory record."""
    return f"inventory:{hotel_id}:{room_type_id}:{date}"
```

## 동시성 제어 (Concurrency Control)

여러 사용자가 동시에 같은 객실을 예약할 때 발생하는 race condition 을 해결해야 한다.

### 방식 비교

| 방식 | 장점 | 단점 | 적합한 상황 |
|------|------|------|------------|
| **Pessimistic Locking** | 충돌 시 데이터 일관성 보장 | 락 대기로 처리량 감소, 데드락 위험 | 충돌이 빈번한 경우 |
| **Optimistic Locking** | 락 없이 높은 처리량 | 충돌 시 재시도 필요, 재시도 비용 | 충돌이 드문 경우 (호텔 예약) |
| **DB Constraints** | 구현이 단순 | 유연성 부족, 복잡한 로직 표현 어려움 | 단순한 유니크 제약 |

### Optimistic Locking 구현

본 프로젝트는 Optimistic Locking + Redis WATCH/MULTI 를 사용한다.

```python
async def reserve_rooms(self, hotel_id, room_type_id, date, num_rooms,
                        overbooking_ratio=None):
    """Optimistic locking 으로 객실 예약.

    1. 현재 inventory 읽기 (version 포함)
    2. 오버부킹 한도 확인
    3. WATCH + MULTI 로 version 이 변경되지 않았을 때만 업데이트
    """
    key = _inventory_key(hotel_id, room_type_id, date)
    raw = await self._redis.get(key)
    inv = RoomTypeInventory(**json.loads(raw))

    # 오버부킹 한도 체크
    max_allowed = int(inv.total_inventory * overbooking_ratio)
    if inv.total_reserved + num_rooms > max_allowed:
        raise ValueError("Insufficient inventory")

    # Version 기반 낙관적 잠금
    new_inv = inv.model_copy(update={
        "total_reserved": inv.total_reserved + num_rooms,
        "version": inv.version + 1,
    })

    async with self._redis.pipeline(transaction=True) as pipe:
        await pipe.watch(key)
        current = RoomTypeInventory(**json.loads(await pipe.get(key)))

        # version 불일치 시 충돌 감지
        if current.version != inv.version:
            raise ValueError("Version conflict")

        pipe.multi()
        pipe.set(key, new_inv.model_dump_json())
        await pipe.execute()  # EXEC 실패 시 WatchError 발생
```

**동작 흐름**:

```
Client A                   Redis                  Client B
   │                         │                        │
   ├── GET inventory ───────▶│                        │
   │   (version=5)           │                        │
   │                         │◀── GET inventory ──────┤
   │                         │    (version=5)         │
   │                         │                        │
   ├── WATCH key ───────────▶│                        │
   ├── MULTI ───────────────▶│                        │
   ├── SET (version=6) ─────▶│                        │
   ├── EXEC ────────────────▶│  ✅ 성공               │
   │                         │                        │
   │                         │◀── WATCH key ──────────┤
   │                         │◀── MULTI ──────────────┤
   │                         │◀── SET (version=6) ────┤
   │                         │◀── EXEC ───────────────┤
   │                         │  ❌ version 불일치      │
```

## 오버부킹 (Overbooking)

호텔 업계에서는 no-show 를 고려하여 일정 비율의 초과 예약을 허용한다.

### 공식

```
max_allowed_reservations = floor(total_inventory * overbooking_ratio)
```

| total_inventory | overbooking_ratio | max_allowed |
|-----------------|-------------------|-------------|
| 100 | 1.0 (0%) | 100 |
| 100 | 1.1 (10%) | 110 |
| 100 | 1.15 (15%) | 115 |

**구현**:

```python
# 오버부킹 비율은 환경 변수로 설정 (기본값 1.1 = 110%)
OVERBOOKING_RATIO = float(os.getenv("OVERBOOKING_RATIO", "1.1"))

# 예약 가능 여부 확인
max_allowed = int(total_inventory * overbooking_ratio)
if total_reserved + num_rooms > max_allowed:
    raise ValueError("Insufficient inventory")
```

## 멱등적 예약 (Idempotent Reservation)

네트워크 장애나 클라이언트 재시도로 인한 중복 예약을 방지한다.

### 설계

클라이언트가 `reservation_id` (멱등성 키) 를 생성하여 요청에 포함한다.
서버는 해당 ID 로 기존 예약이 있는지 확인하고, 있으면 새로 생성하지 않고
기존 결과를 반환한다.

```python
async def create_reservation(self, req: ReservationRequest) -> Reservation:
    """멱등적 예약 생성.

    1. reservation_id 로 기존 예약 확인
    2. 이미 존재하면 기존 예약 반환 (재고 변경 없음)
    3. 없으면 날짜별 재고 확보 후 예약 생성
    """
    key = _reservation_key(req.reservation_id)
    existing = await self._redis.get(key)
    if existing is not None:
        return Reservation(**json.loads(existing))  # 멱등: 기존 반환

    # 날짜별 재고 확보 (실패 시 rollback)
    dates = _date_range(req.check_in, req.check_out)
    reserved_dates = []
    try:
        for date in dates:
            await self._inventory.reserve_rooms(
                hotel_id=req.hotel_id, room_type_id=req.room_type_id,
                date=date, num_rooms=req.num_rooms,
            )
            reserved_dates.append(date)
    except ValueError:
        for rdate in reserved_dates:  # rollback
            await self._inventory.release_rooms(...)
        raise

    reservation = Reservation(...)
    await self._redis.set(key, reservation.model_dump_json())
    return reservation
```

**시퀀스 다이어그램**:

```
Client              API Server              Redis
  │                      │                     │
  ├─ POST /reservations ▶│                     │
  │  (reservation_id=X)  │── GET res:X ───────▶│
  │                      │◀── NULL ────────────│  (신규)
  │                      │── RESERVE rooms ───▶│
  │                      │◀── OK ──────────────│
  │                      │── SET res:X ────────▶│
  │◀─ 200 CONFIRMED ────│                     │
  │                      │                     │
  ├─ POST /reservations ▶│  (재시도, 같은 ID)   │
  │  (reservation_id=X)  │── GET res:X ───────▶│
  │                      │◀── EXISTS ──────────│  (이미 존재)
  │◀─ 200 CONFIRMED ────│  (기존 반환, 재고 변경 없음)
```

## 예약 상태 머신 (State Machine)

```
                    ┌──────────┐
                    │ PENDING  │
                    └────┬─────┘
                         │
                    ┌────▼─────┐       ┌───────────┐
                    │CONFIRMED ├──────▶│ CANCELLED  │
                    └────┬─────┘       └───────────┘
                         │                    ▲
                    ┌────▼──────┐             │
                    │CHECKED_IN ├─────────────┘ (불가)
                    └────┬──────┘
                         │
                    ┌────▼───────┐
                    │CHECKED_OUT │
                    └────────────┘
```

**유효한 상태 전이**:

```python
VALID_TRANSITIONS = {
    PENDING:     {CONFIRMED, CANCELLED},
    CONFIRMED:   {CHECKED_IN, CANCELLED},
    CHECKED_IN:  {CHECKED_OUT},
    CHECKED_OUT: set(),     # 터미널 상태
    CANCELLED:   set(),     # 터미널 상태
}
```

**전이 검증 코드**:

```python
@staticmethod
def _validate_transition(current: ReservationStatus, target: ReservationStatus):
    allowed = VALID_TRANSITIONS.get(current, set())
    if target not in allowed:
        raise ValueError(f"Invalid transition: {current} -> {target}")
```

## Quick Start

```bash
# 서비스 시작
docker-compose up --build

# 헬스 체크
curl http://localhost:8023/health

# 호텔 생성
curl -X POST http://localhost:8023/api/v1/hotels \
  -H "Content-Type: application/json" \
  -d '{"name": "Grand Hotel", "address": "123 Main St"}'

# 객실 타입 생성
curl -X POST http://localhost:8023/api/v1/hotels/HOTEL_ID/room-types \
  -H "Content-Type: application/json" \
  -d '{"hotel_id": "HOTEL_ID", "name": "Deluxe", "total_inventory": 100, "price_per_night": 200}'

# 재고 초기화
curl -X POST "http://localhost:8023/api/v1/inventory/init?hotel_id=HOTEL_ID&room_type_id=RT_ID&date=2025-07-01&total_inventory=100"

# 예약 생성
curl -X POST http://localhost:8023/api/v1/reservations \
  -H "Content-Type: application/json" \
  -d '{"reservation_id": "res-001", "hotel_id": "HOTEL_ID", "room_type_id": "RT_ID", "guest_name": "Alice", "check_in": "2025-07-01", "check_out": "2025-07-03", "num_rooms": 2}'

# 예약 취소
curl -X POST http://localhost:8023/api/v1/reservations/res-001/cancel
```

## CLI 사용법

```bash
# 헬스 체크
python scripts/cli.py --health

# 호텔 생성
python scripts/cli.py hotel-create --name "Grand Hotel" --address "123 Main St"

# 재고 초기화
python scripts/cli.py inventory-init --hotel-id h1 --room-type-id rt1 --date 2025-07-01 --total 100

# 예약 생성
python scripts/cli.py reserve --reservation-id res-001 --hotel-id h1 --room-type-id rt1 \
  --guest "Alice" --check-in 2025-07-01 --check-out 2025-07-03 --num-rooms 2

# 예약 취소
python scripts/cli.py cancel --reservation-id res-001
```

## 환경 변수

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `REDIS_HOST` | `redis` | Redis 호스트 |
| `REDIS_PORT` | `6379` | Redis 포트 |
| `API_PORT` | `8023` | API 서버 포트 |
| `OVERBOOKING_RATIO` | `1.1` | 오버부킹 비율 (1.1 = 110%) |

## 확장 고려사항

### 수평 확장

- **API 계층**: 여러 FastAPI 인스턴스 + 로드 밸런서
- **저장 계층**: Redis Cluster 또는 PostgreSQL + 파티셔닝
- **캐시 계층**: 인기 호텔/날짜 조합 캐싱

### 고가용성

- Redis Sentinel 또는 Redis Cluster 로 자동 failover
- 예약 이벤트 Kafka 발행으로 비동기 처리 (결제, 알림)
- 재고 관리와 예약 서비스 분리 (마이크로서비스)

### 데이터베이스 마이그레이션

프로덕션에서는 Redis 대신 관계형 DB 를 사용하여 ACID 보장:

```sql
CREATE TABLE room_type_inventory (
    hotel_id       VARCHAR(36) NOT NULL,
    room_type_id   VARCHAR(36) NOT NULL,
    date           DATE        NOT NULL,
    total_inventory INT        NOT NULL,
    total_reserved  INT        NOT NULL DEFAULT 0,
    version         INT        NOT NULL DEFAULT 0,
    PRIMARY KEY (hotel_id, room_type_id, date)
);

-- Optimistic locking 쿼리
UPDATE room_type_inventory
SET total_reserved = total_reserved + :num_rooms,
    version = version + 1
WHERE hotel_id = :hotel_id
  AND room_type_id = :room_type_id
  AND date = :date
  AND version = :expected_version
  AND total_reserved + :num_rooms <= FLOOR(total_inventory * :overbooking_ratio);
```

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide Volume 2", Chapter 7
