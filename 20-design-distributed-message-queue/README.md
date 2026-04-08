# Design Distributed Message Queue

분산 메시지 큐는 프로듀서(생산자)와 컨슈머(소비자)를 디커플링하여 비동기적으로
메시지를 전달하는 시스템이다. Apache Kafka 에서 영감을 받은 토픽-파티션 기반
아키텍처로, append-only 로그와 오프셋 기반 소비 모델을 구현한다.

## 아키텍처

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Producer A  │     │  Producer B  │     │  Producer C  │
│  (batching)  │     │  (batching)  │     │  (batching)  │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │  key hash / RR     │                     │
       ▼                    ▼                     ▼
┌─────────────────────────────────────────────────────────┐
│                        Broker                           │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │                  Topic: orders                   │    │
│  │                                                  │    │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐      │    │
│  │  │Partition 0│  │Partition 1│  │Partition 2│      │    │
│  │  │[seg0|seg1]│  │[seg0|seg1]│  │  [seg0]  │      │    │
│  │  │ Leader    │  │ Leader    │  │ Leader    │      │    │
│  │  │ F1, F2   │  │ F1, F2   │  │ F1, F2   │      │    │
│  │  └──────────┘  └──────────┘  └──────────┘      │    │
│  └─────────────────────────────────────────────────┘    │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │               Topic: events                      │    │
│  │  ┌──────────┐  ┌──────────┐                     │    │
│  │  │Partition 0│  │Partition 1│                     │    │
│  │  └──────────┘  └──────────┘                     │    │
│  └─────────────────────────────────────────────────┘    │
└────────────────────────┬────────────────────────────────┘
                         │  pull (offset-based)
       ┌─────────────────┼─────────────────┐
       ▼                 ▼                 ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  Consumer 1  │  │  Consumer 2  │  │  Consumer 3  │
│  Group: G1   │  │  Group: G1   │  │  Group: G2   │
│  P0, P1      │  │  P2          │  │  P0, P1, P2  │
└──────────────┘  └──────────────┘  └──────────────┘
```

### 핵심 컴포넌트

| 컴포넌트 | 역할 |
|---------|------|
| **Broker** | 토픽/파티션 관리, produce/consume 요청 라우팅 |
| **Topic** | 논리적 메시지 채널 (예: `orders`, `events`) |
| **Partition** | 토픽 내 순서 보장 단위, append-only 로그 |
| **Segment** | 파티션의 물리적 저장 단위 (WAL 파일) |
| **Producer** | 메시지 배치 처리, 파티션 라우팅 (key hash / round-robin) |
| **Consumer** | Pull 모델로 오프셋 기반 메시지 소비 |
| **Consumer Group** | 파티션을 컨슈머에 배타적으로 할당 |
| **Replication** | Leader-Follower 복제, ISR 추적 |

## Point-to-Point vs Pub/Sub

```
Point-to-Point (Consumer Group 내)         Pub/Sub (서로 다른 Group)
┌────────┐                                ┌────────┐
│  Msg   │──▶ Consumer Group G1           │  Msg   │──▶ Group G1 (Consumer 1)
│        │   (하나의 컨슈머만 처리)          │        │──▶ Group G2 (Consumer 2)
└────────┘                                └────────┘
```

- **Point-to-Point**: 같은 Consumer Group 내에서 각 파티션은 하나의 컨슈머에만 할당된다. 메시지가 중복 처리되지 않는다.
- **Pub/Sub**: 서로 다른 Consumer Group 은 독립적으로 같은 토픽을 구독한다. 각 그룹이 모든 메시지를 받는다.

## Consumer Group 과 파티션 할당

Consumer Group 은 파티션을 컨슈머에 **배타적으로** 할당하여 병렬 처리를 가능하게 한다.

```python
class ConsumerGroup:
    def rebalance(self) -> dict[str, list[tuple[str, int]]]:
        """Range 기반 파티션 할당. 파티션을 컨슈머 수로 균등 분배."""
        for c in self._consumers:
            c.revoke_all()

        for topic in sorted(self._subscriptions):
            num_parts = self.broker.num_partitions(topic)
            num_consumers = len(self._consumers)
            parts_per_consumer = num_parts // num_consumers
            remainder = num_parts % num_consumers

            idx = 0
            for i, consumer in enumerate(self._consumers):
                count = parts_per_consumer + (1 if i < remainder else 0)
                pids = list(range(idx, idx + count))
                consumer.assign(topic, pids)
                idx += count
```

**리밸런싱 시나리오:**

| 파티션 수 | 컨슈머 수 | 할당 결과 |
|----------|----------|----------|
| 6 | 2 | C1: [0,1,2], C2: [3,4,5] |
| 6 | 3 | C1: [0,1], C2: [2,3], C3: [4,5] |
| 5 | 2 | C1: [0,1,2], C2: [3,4] |
| 4 | 4 | C1: [0], C2: [1], C3: [2], C4: [3] |

> 컨슈머가 파티션 수보다 많으면 초과 컨슈머는 유휴 상태가 된다.

## WAL (Write-Ahead Log) 세그먼트

파티션은 **세그먼트** 단위로 물리적 저장을 관리한다. 세그먼트가 가득 차면 seal 되고
새 세그먼트가 생성된다.

```python
class Segment:
    """Append-only WAL 세그먼트. seal 후에는 쓰기 불가."""

    def __init__(self, base_offset: int, max_size: int = 1000) -> None:
        self.base_offset = base_offset
        self.max_size = max_size
        self.records: list[Record] = []
        self.sealed = False

    def append(self, key, value, timestamp) -> Record:
        if self.sealed:
            raise RuntimeError("Cannot append to a sealed segment")
        record = Record(offset=self.next_offset, key=key,
                        value=value, timestamp=timestamp)
        self.records.append(record)
        return record

    def seal(self) -> None:
        self.sealed = True
```

```
Partition 0 (topic: orders)
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│   Segment 0   │  │   Segment 1   │  │   Segment 2   │
│  offset 0-999 │  │ offset 1000-  │  │ offset 2000-  │
│   (sealed)    │  │  1999 (sealed) │  │   (active)    │
└───────────────┘  └───────────────┘  └───────────────┘
```

세그먼트 경계를 넘나드는 읽기도 투명하게 처리된다:

```python
class Partition:
    def read(self, offset: int, max_records: int = 100) -> list[Record]:
        """여러 세그먼트에 걸쳐 offset 기반 순차 읽기."""
        result: list[Record] = []
        remaining = max_records
        for seg in self._segments:
            if seg.next_offset <= offset:
                continue  # 이 세그먼트는 전부 이전 데이터
            records = seg.read(offset, remaining)
            result.extend(records)
            remaining -= len(records)
            if remaining <= 0:
                break
            if records:
                offset = records[-1].offset + 1
        return result
```

## ISR (In-Sync Replicas) 과 복제

Leader-Follower 모델로 데이터를 복제한다. ISR 은 Leader 의 LEO (Log End Offset) 에
가까이 따라잡은 Follower 집합이다.

```
Leader (Broker 0)          Follower (Broker 1)      Follower (Broker 2)
┌──────────────┐           ┌──────────────┐         ┌──────────────┐
│ offset 0..9  │ ──fetch──▶│ offset 0..9  │         │ offset 0..7  │
│ LEO = 10     │           │ LEO = 10     │         │ LEO = 8      │
└──────────────┘           └──────────────┘         └──────────────┘
                            ISR: {0, 1}               lag=2 > max
                                                      ISR 에서 제거됨

High Watermark = min(ISR LEOs) = 10
(Broker 2 는 ISR 에 없으므로 HW 계산에서 제외)
```

```python
class ReplicatedPartition:
    def produce(self, key, value, timestamp=None) -> Record:
        """Leader 에 쓰고, ack_mode 에 따라 복제."""
        record = self.leader.partition.append(key, value, timestamp)
        self.leader.leo = self.leader.partition.log_end_offset

        if self.ack_mode == AckMode.NONE:
            return record  # 복제 안 함

        if self.ack_mode == AckMode.ALL:
            self._replicate_to_isr()  # 동기 복제

        self._update_high_watermark()
        return record

    def _check_isr(self, broker_id, follower) -> None:
        """lag 과 fetch 시간으로 ISR 멤버십 판단."""
        leader_leo = self.leader.partition.log_end_offset
        lag = leader_leo - follower.leo
        time_since = time.time() - follower.last_fetch_time
        in_sync = (lag <= self.replica_lag_max
                   and time_since <= self.replica_time_max)
        if in_sync:
            self._isr_broker_ids.add(broker_id)
        else:
            self._isr_broker_ids.discard(broker_id)
            self._isr_broker_ids.add(self.leader.broker_id)  # Leader 는 항상 ISR
```

### ACK 모드

| ACK | 동작 | 장단점 |
|-----|------|--------|
| `0` (NONE) | Fire-and-forget, 복제 없음 | 최고 처리량, 데이터 유실 가능 |
| `1` (LEADER) | Leader 쓰기 완료 후 응답 | 균형 잡힌 성능/내구성 |
| `all` | 모든 ISR 복제 완료 후 응답 | 최고 내구성, 지연 증가 |

### High Watermark

컨슈머는 **High Watermark** 이하의 레코드만 읽을 수 있다. HW = ISR 내 모든
Replica 의 LEO 중 최솟값이다. 이를 통해 아직 복제가 완료되지 않은 데이터를
컨슈머가 읽는 것을 방지한다.

```python
def consume(self, offset: int, max_records: int = 100) -> list[Record]:
    """HW 이하의 커밋된 레코드만 반환."""
    records = self.leader.partition.read(offset, max_records)
    return [r for r in records if r.offset < self._hw]
```

## Delivery Semantics (전달 보장)

### At-Most-Once (최대 한 번)

오프셋을 **처리 전에 커밋**한다. 처리 중 실패하면 메시지는 유실된다.

```python
# auto_commit=True 로 설정
consumer = Consumer("c1", broker, group_id="g1", auto_commit=True)
consumer.assign("topic", [0])
records = consumer.poll()  # 오프셋 자동 커밋됨
process(records)            # 여기서 실패해도 재전달 없음
```

### At-Least-Once (최소 한 번)

메시지를 **처리 후에 커밋**한다. 처리 후 커밋 전에 실패하면 재전달된다.

```python
consumer = Consumer("c1", broker, group_id="g1", auto_commit=False)
consumer.assign("topic", [0])
records = consumer.poll()
process(records)    # 먼저 처리
consumer.commit()   # 그 다음 커밋 (실패하면 재전달)
```

### Exactly-Once (정확히 한 번)

멱등성(idempotent) 처리로 구현한다. 오프셋으로 중복을 감지한다.

```python
processed_offsets: set[int] = set()

def idempotent_process(records: list[Record]) -> None:
    for r in records:
        if r.offset not in processed_offsets:
            processed_offsets.add(r.offset)
            do_work(r)  # 실제 처리
```

## Producer 배칭 (Batching)

Producer 는 메시지를 파티션별 배치에 모은 뒤 한 번에 전송한다.

```python
class Producer:
    def send(self, record: ProducerRecord) -> list[Record]:
        """배치에 추가. batch_size 에 도달하면 자동 flush."""
        pid = self._select_partition(record.topic, record.key)
        batch = self._batches.setdefault((record.topic, pid), [])
        batch.append(record)
        if len(batch) >= self.config.batch_size:
            return self._flush_batch((record.topic, pid))
        return []

    def _select_partition(self, topic, key) -> int:
        """key 가 있으면 hash, 없으면 round-robin."""
        num_parts = self.broker.num_partitions(topic)
        if key is not None:
            h = int(hashlib.md5(key.encode()).hexdigest(), 16)
            return h % num_parts
        counter = self._rr_counters.get(topic, 0)
        self._rr_counters[topic] = counter + 1
        return counter % num_parts
```

### 배칭 트레이드오프

| | 작은 배치 | 큰 배치 |
|--|----------|---------|
| **지연(Latency)** | 낮음 (즉시 전송) | 높음 (배치 대기) |
| **처리량(Throughput)** | 낮음 (오버헤드 큼) | 높음 (I/O 최적화) |
| **메모리** | 적음 | 많음 (버퍼 크기) |
| **유실 위험** | 낮음 | 높음 (배치 내 메시지 유실) |

> 실제 시스템에서는 `batch_size` 와 `linger_ms` 를 조합하여 지연과 처리량 사이의
> 균형을 맞춘다.

## Quick Start

```bash
# 테스트 실행
cd 20-design-distributed-message-queue
pip install -r requirements.txt
pytest tests/ -v

# 데모 실행
python scripts/demo.py
```

## 테스트 구성 (48 tests)

| 카테고리 | 테스트 수 | 내용 |
|---------|----------|------|
| Partition Append/Read | 7 | 기본 append, offset 증가, FIFO 순서, 빈 파티션 읽기 |
| Segments | 4 | 세그먼트 로테이션, 경계 간 읽기, sealed 세그먼트 |
| Broker | 4 | 토픽 생성/삭제, produce/consume |
| Producer Batching | 4 | 배치 축적, 자동 flush, 수동 flush, sent count |
| Partition Routing | 2 | key hash 결정론적 라우팅, round-robin |
| Consumer Offset | 5 | 수동/자동 커밋, seek, 커밋된 오프셋에서 재개 |
| Consumer Groups | 6 | 균등/불균등 할당, 리밸런싱, 배타적 소비 |
| Replication | 6 | Leader 쓰기, follower fetch, ACK 모드별 동작, HW |
| ISR | 4 | 초기 ISR, lag 제거, 복구, leader 항상 ISR |
| Delivery Semantics | 3 | at-most-once, at-least-once, exactly-once |
| Integration | 3 | E2E produce/consume, consumer group E2E, replicated broker |
