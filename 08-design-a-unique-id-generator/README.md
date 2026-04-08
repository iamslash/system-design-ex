# Design A Unique ID Generator In Distributed Systems

분산 시스템에서 사용할 유일 ID 생성기(Unique ID Generator)를 설계한다.
단일 데이터베이스의 `auto_increment` 는 서버 한 대에서만 동작하므로
분산 환경에는 적합하지 않다. 여러 접근법을 비교하고, Twitter Snowflake
방식을 Python 으로 구현한다.

---

## 요구사항

| 항목 | 요구사항 |
|------|----------|
| ID 크기 | 64-bit 정수 |
| 정렬 | 시간순 정렬 가능 (time-sortable) |
| 처리량 | 초당 10,000+ ID 생성 |
| 고유성 | 분산 환경에서도 중복 없음 |
| 가용성 | 단일 장애점(SPOF) 없음 |

---

## 접근법 비교

| 접근법 | 장점 | 단점 |
|--------|------|------|
| Multi-master Replication | 기존 DB 활용, 구현 간단 | 서버 추가/제거 시 재설정 필요, 시간순 정렬 불가 |
| UUID | 서버 간 조율 불필요, 독립 생성 | 128-bit (너무 큼), 시간순 정렬 불가, 숫자가 아님 |
| Ticket Server (Flickr) | 구현 간단, 숫자 ID | SPOF, 서버 증설 시 동기화 필요 |
| **Twitter Snowflake** | **64-bit, 시간순 정렬, 분산 생성** | **시간 동기화(NTP) 의존** |

### Multi-master Replication

각 DB 서버가 `auto_increment` 를 사용하되, 증가 폭을 서버 수(N)로 설정한다.

```python
# 서버 2대일 때 (N=2)
# Server A: 1, 3, 5, 7, ...  (start=1, step=2)
# Server B: 2, 4, 6, 8, ...  (start=2, step=2)

# 문제점: 서버 3대로 늘리면?
# → step 을 3으로 바꿔야 하고, 기존 ID 와 충돌 가능
# → 시간순 정렬도 보장되지 않음 (Server A 가 느리면 id=7 이 id=8 보다 늦을 수 있음)
```

### UUID

```python
import uuid

# UUID v4: 128-bit, 랜덤 생성, 서버 간 조율 불필요
for _ in range(3):
    print(uuid.uuid4())
# 출력:
#   be5457a4-dd4a-4a4f-ae5e-1af0eb692b8c
#   c024f590-0ddc-489b-b7e0-c9d0eb66cdb0
#   10941c0c-2ef0-4d2e-8d5c-a86da3284050
#
# 단점:
#   - 128-bit → 64-bit 요구사항 미충족
#   - 시간순 정렬 불가 (랜덤 값)
#   - 문자열이라 인덱스 성능 저하
```

### Ticket Server (Flickr 방식)

중앙 서버 한 대가 순차 ID 를 발급한다.

```python
# 중앙 Ticket Server (단일 DB)
# CREATE TABLE tickets (id BIGINT AUTO_INCREMENT PRIMARY KEY);
# INSERT INTO tickets VALUES ();  -- id = 1
# INSERT INTO tickets VALUES ();  -- id = 2
#
# 장점: 구현이 매우 간단, 숫자 ID
# 단점: Ticket Server 가 SPOF (Single Point of Failure)
#        → 서버 2대로 이중화하면 Multi-master 문제로 회귀
```

---

## Twitter Snowflake

### 비트 구조 (64-bit)

```
| 1 bit (sign) | 41 bits (timestamp) | 5 bits (datacenter) | 5 bits (machine) | 12 bits (sequence) |
|    항상 0     |  밀리초 단위 타임스탬프  |   데이터센터 ID     |    서버 ID       |   일련번호          |
```

- **Sign (1 bit)**: 항상 0 (양수). 향후 확장용으로 예약.
- **Timestamp (41 bits)**: 커스텀 에포크 이후 경과한 밀리초. 약 69년간 사용 가능.
- **Datacenter ID (5 bits)**: 0-31, 최대 32개 데이터센터.
- **Machine ID (5 bits)**: 0-31, 데이터센터당 최대 32대 서버.
- **Sequence (12 bits)**: 0-4095, 같은 밀리초 내 순차 번호. 밀리초마다 리셋.

```
ID (decimal) : 2041935555478892544
ID (binary)  : 0001110001010110011010011001000110101110100011110011000000000000

Section         Bits                                           Value
--------------  ---------------------------------------------  ----------
Sign            0                                              0
Timestamp       00111000101011001101001100100011010111010      1775670348411
Datacenter      00111                                          7
Machine         10011                                          19
Sequence        000000000000                                   0
```

---

## 핵심 구현

### 상수 & 비트 시프트

```python
# Twitter snowflake epoch: Nov 04, 2010, 01:42:54.657 UTC
EPOCH = 1288834974657

# 비트 길이
SEQUENCE_BITS = 12       # 일련번호: 0-4095
MACHINE_ID_BITS = 5      # 머신 ID: 0-31
DATACENTER_ID_BITS = 5   # 데이터센터 ID: 0-31
TIMESTAMP_BITS = 41      # 타임스탬프: ~69년

# 최댓값 (비트 마스크)
MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1          # 4095
MAX_MACHINE_ID = (1 << MACHINE_ID_BITS) - 1      # 31
MAX_DATACENTER_ID = (1 << DATACENTER_ID_BITS) - 1  # 31

# 비트 시프트 (각 필드의 위치)
MACHINE_ID_SHIFT = SEQUENCE_BITS                           # 12
DATACENTER_ID_SHIFT = SEQUENCE_BITS + MACHINE_ID_BITS      # 17
TIMESTAMP_SHIFT = SEQUENCE_BITS + MACHINE_ID_BITS + DATACENTER_ID_BITS  # 22
```

### SnowflakeGenerator 클래스

```python
class SnowflakeGenerator:
    """Thread-safe Snowflake ID generator.

    각 인스턴스는 (datacenter_id, machine_id) 쌍으로 식별되며,
    전역적으로 유일한 64-bit ID 를 생성한다.
    """

    def __init__(
        self,
        datacenter_id: int,
        machine_id: int,
        epoch: int = EPOCH,
    ) -> None:
        # 범위 검증: datacenter_id 0-31, machine_id 0-31
        if not (0 <= datacenter_id <= MAX_DATACENTER_ID):
            raise ValueError(
                f"datacenter_id must be between 0 and {MAX_DATACENTER_ID}, "
                f"got {datacenter_id}"
            )
        if not (0 <= machine_id <= MAX_MACHINE_ID):
            raise ValueError(
                f"machine_id must be between 0 and {MAX_MACHINE_ID}, "
                f"got {machine_id}"
            )

        self._datacenter_id = datacenter_id
        self._machine_id = machine_id
        self._epoch = epoch
        self._sequence = 0
        self._last_timestamp = -1
        self._lock = threading.Lock()   # 스레드 안전성 보장
```

### ID 생성 로직

```python
def generate(self) -> int:
    """새로운 유일 64-bit ID 를 생성한다."""
    with self._lock:
        timestamp = self._current_millis()

        # 1. 시계가 역행하면 거부 (Clock backward)
        if timestamp < self._last_timestamp:
            raise RuntimeError(
                f"Clock moved backwards. "
                f"Refusing to generate ID for "
                f"{self._last_timestamp - timestamp} ms"
            )

        if timestamp == self._last_timestamp:
            # 2. 같은 밀리초: 일련번호 증가
            self._sequence = (self._sequence + 1) & MAX_SEQUENCE
            if self._sequence == 0:
                # 3. 일련번호 오버플로 (4096개 초과):
                #    다음 밀리초까지 대기 (spin-wait)
                timestamp = self._wait_next_millis(self._last_timestamp)
        else:
            # 4. 새 밀리초: 일련번호 리셋
            self._sequence = 0

        self._last_timestamp = timestamp

        # 5. 64-bit ID 조합 (비트 시프트 + OR)
        snowflake_id = (
            ((timestamp - self._epoch) << TIMESTAMP_SHIFT)   # 41 bits
            | (self._datacenter_id << DATACENTER_ID_SHIFT)   # 5 bits
            | (self._machine_id << MACHINE_ID_SHIFT)         # 5 bits
            | self._sequence                                  # 12 bits
        )
        return snowflake_id
```

**ID 조합 과정 시각화:**

```
timestamp - epoch = 486835373711 (41 bits)
                    ↓ << 22
0111000101011001101001100100011010111010 0000000000000000000000

datacenter_id = 7 (5 bits)
                  ↓ << 17
                                         00111 00000000000000000

machine_id = 19 (5 bits)
                ↓ << 12
                                               10011 000000000000

sequence = 0 (12 bits)
                                                     000000000000

OR 연산 → 최종 64-bit ID:
0001110001010110011010011001000110101110 00111 10011 000000000000
```

### ID 파싱 (역분해)

```python
@staticmethod
def parse(snowflake_id: int, epoch: int = EPOCH) -> dict:
    """Snowflake ID 를 구성 요소로 분해한다."""
    # 비트 마스크로 각 필드 추출
    sequence = snowflake_id & MAX_SEQUENCE                          # 하위 12 bits
    machine_id = (snowflake_id >> MACHINE_ID_SHIFT) & MAX_MACHINE_ID      # 다음 5 bits
    datacenter_id = (snowflake_id >> DATACENTER_ID_SHIFT) & MAX_DATACENTER_ID  # 다음 5 bits
    timestamp_offset = snowflake_id >> TIMESTAMP_SHIFT              # 상위 41 bits

    timestamp_ms = timestamp_offset + epoch   # 절대 타임스탬프 복원

    dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)

    return {
        "timestamp_ms": timestamp_ms,
        "datetime": dt.strftime("%Y-%m-%d %H:%M:%S.%f UTC"),
        "datacenter_id": datacenter_id,
        "machine_id": machine_id,
        "sequence": sequence,
    }
```

### Clock Backward 처리

시스템 시계가 NTP 동기화 등으로 역행하면, 같은 타임스탬프에 대해
중복 ID 가 생성될 수 있다. 이를 방지하기 위해 **즉시 에러를 발생**시킨다.

```python
# 시계가 역행하면 RuntimeError 발생
if timestamp < self._last_timestamp:
    raise RuntimeError(
        f"Clock moved backwards. "
        f"Refusing to generate ID for "
        f"{self._last_timestamp - timestamp} ms"
    )
```

실제 운영 환경에서의 대응 전략:
- NTP 서버와의 시간 차이를 모니터링
- 시계 역행 감지 시 짧은 대기 후 재시도
- 역행 폭이 크면 알림 발송 후 서비스 일시 중단

### Sequence Overflow 처리

같은 밀리초 내에 4,096개(12-bit 최대) 를 초과하는 ID 요청이 오면,
다음 밀리초까지 spin-wait 한다.

```python
if timestamp == self._last_timestamp:
    self._sequence = (self._sequence + 1) & MAX_SEQUENCE  # 비트 AND 로 0-4095 순환
    if self._sequence == 0:
        # 4096 번째에서 0 으로 순환 → 오버플로
        # 다음 밀리초까지 바쁜 대기
        timestamp = self._wait_next_millis(self._last_timestamp)

def _wait_next_millis(self, last_ts: int) -> int:
    """시계가 last_ts 를 넘어설 때까지 spin-wait."""
    ts = self._current_millis()
    while ts <= last_ts:
        ts = self._current_millis()
    return ts
```

---

## 실행 방법

### 사전 조건

- Python 3.11 이상

### 데모 실행

```bash
cd 08-design-a-unique-id-generator
python scripts/demo.py
```

### 테스트 실행

```bash
cd 08-design-a-unique-id-generator
pip install -r requirements.txt
pytest tests/ -v
```

---

## 샘플 출력

```
Snowflake ID Generator Demo
===========================

======================================================================
  1. Single Generator -- Generate & Parse IDs
======================================================================

  [ 1] ID: 2041935555297677312     ts=1775670348368  dc=1  mc=1  seq=0
  [ 2] ID: 2041935555297677313     ts=1775670348368  dc=1  mc=1  seq=1
  [ 3] ID: 2041935555297677314     ts=1775670348368  dc=1  mc=1  seq=2
  ...

======================================================================
  2. Throughput Benchmark
======================================================================

  Generated : 100,000 IDs
  Elapsed   : 0.037 s
  Throughput: 2,711,677 IDs/sec
  Unique    : 100,000 / 100,000

======================================================================
  3. Multi-Worker Simulation (4 workers, 1000 IDs each)
======================================================================

  Worker 0 (dc=0, mc=0): 1,000 IDs  range=[...]
  Worker 1 (dc=0, mc=1): 1,000 IDs  range=[...]
  Worker 2 (dc=1, mc=0): 1,000 IDs  range=[...]
  Worker 3 (dc=1, mc=1): 1,000 IDs  range=[...]

  Total IDs : 4,000
  Unique IDs: 4,000
  Duplicates: 0
  Result    : ALL UNIQUE

======================================================================
  4. ID Bit-Structure Visualization
======================================================================

  ID (decimal) : 2041935555478892544
  ID (binary)  : 0001110001010110011010011001000110101110100011110011000000000000

  Section         Bits                                           Value
  --------------  ---------------------------------------------  ----------
  Sign            0                                              0
  Timestamp       00111000101011001101001100100011010111010      1775670348411
  Datacenter      00111                                          7
  Machine         10011                                          19
  Sequence        000000000000                                   0

======================================================================
  5. Comparison With Other Approaches
======================================================================

  [UUID v4] 128-bit, random, not sortable:
    be5457a4-dd4a-4a4f-ae5e-1af0eb692b8c  (128 bits)
    c024f590-0ddc-489b-b7e0-c9d0eb66cdb0  (128 bits)
    ...

  [Snowflake] 64-bit, time-sortable:
    2041935555477901312  (61 bits)
    2041935555477901313  (61 bits)
    ...

  Snowflake IDs sortable by time? True

  [Auto-Increment] Why it fails in distributed systems:
    Server A: INSERT -> id=1, id=2, id=3, ...
    Server B: INSERT -> id=1, id=2, id=3, ...
                        ^^ COLLISION! ^^
```

> 참고: 실제 출력 값은 실행 시점에 따라 달라진다.

---

## 설계 고려사항

### Clock Synchronization (시계 동기화)

Snowflake 는 시스템 시계에 의존하므로 NTP(Network Time Protocol) 동기화가
필수적이다. 시계가 역행(clock backward)하면 중복 ID 가 생성될 수 있으므로
발견 즉시 에러를 발생시킨다.

- 모든 서버에 NTP 데몬을 설정하여 시계 오차를 최소화
- 시계 역행 발생 시 모니터링 알림 설정
- Google TrueTime 처럼 원자 시계 + GPS 를 사용하면 오차를 더 줄일 수 있음

### Section Length Tuning (비트 길이 조정)

비트 할당은 요구사항에 따라 조정 가능하다.

| 조정 시나리오 | Timestamp | Datacenter | Machine | Sequence |
|---------------|-----------|------------|---------|----------|
| 기본 (Snowflake) | 41 bits (~69년) | 5 bits (32) | 5 bits (32) | 12 bits (4096/ms) |
| 수명 연장 | 42 bits (~139년) | 5 bits | 5 bits | 11 bits (2048/ms) |
| 대량 생성 | 39 bits (~17년) | 5 bits | 5 bits | 14 bits (16384/ms) |
| 많은 서버 | 41 bits | 8 bits (256) | 8 bits (256) | 6 bits (64/ms) |

### High Availability (고가용성)

- 각 서버가 독립적으로 ID 를 생성하므로 **SPOF 가 없다**
- `datacenter_id` + `machine_id` 조합으로 서버를 구분하므로 서버 간 조율이 불필요
- 서버 추가/제거 시에도 기존 서버에 영향을 주지 않음
- 장애 발생 시 해당 서버만 교체하면 됨 (다른 서버는 계속 ID 생성)

---

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide", Chapter 7
