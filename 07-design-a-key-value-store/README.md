# Design A Key-value Store

분산 키-값 저장소(Distributed Key-Value Store)의 핵심 개념을 구현한 예제입니다.

## 아키텍처

```
                          ┌─────────────────┐
                          │     Client      │
                          │  (CLI / HTTP)   │
                          └────────┬────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
               ┌────▼────┐   ┌────▼────┐   ┌────▼────┐
               │  Node 1  │   │  Node 2  │   │  Node 3  │
               │ :8071    │   │ :8072    │   │ :8073    │
               │          │   │          │   │          │
               │ ┌──────┐ │   │ ┌──────┐ │   │ ┌──────┐ │
               │ │Store │ │   │ │Store │ │   │ │Store │ │
               │ │Engine│ │   │ │Engine│ │   │ │Engine│ │
               │ └──────┘ │   │ └──────┘ │   │ └──────┘ │
               │ ┌──────┐ │   │ ┌──────┐ │   │ ┌──────┐ │
               │ │Gossip│ │   │ │Gossip│ │   │ │Gossip│ │
               │ └──────┘ │   │ └──────┘ │   │ └──────┘ │
               └────┬─────┘   └────┬─────┘   └────┬─────┘
                    │              │              │
                    └──────────────┴──────────────┘
                          Gossip Protocol
                       (Failure Detection)
```

## CAP 이론

이 시스템은 **AP(Availability + Partition Tolerance)** 시스템입니다.

- **가용성(A)**: 쿼럼을 만족하는 한 읽기/쓰기가 가능합니다.
- **분할 내성(P)**: 네트워크 분할 시에도 가용한 노드는 계속 동작합니다.
- **일관성(C)**: 최종 일관성(Eventual Consistency)을 제공합니다. Vector Clock을 사용하여 충돌을 감지하고, 충돌 시 모든 버전을 클라이언트에 반환합니다.

```
       Consistency
          ╱╲
         ╱  ╲
        ╱ CA ╲         CA = 현실에서 불가능 (네트워크 장애 불가피)
       ╱──────╲
      ╱        ╲
     ╱  CP  AP  ╲      CP = 일관성 우선 (은행 시스템)
    ╱            ╲     AP = 가용성 우선 (이 시스템) ← 우리 선택
   ╱──────────────╲
  Availability    Partition
                  Tolerance
```

## 핵심 컴포넌트

### 1. Consistent Hashing (안정 해싱)

가상 노드(Virtual Node)를 사용하여 키를 3개 노드에 균등하게 분배합니다. 노드 추가/제거 시 최소한의 키만 재배치됩니다.

```python
class ConsistentHashRing:
    def __init__(self, virtual_nodes: int = 150):
        self._virtual_nodes = virtual_nodes
        self._ring: list[tuple[int, str]] = []  # (hash, node) 정렬된 리스트
        self._nodes: set[str] = set()

    def get_replica_nodes(self, key: str, n: int) -> list[str]:
        """키에서 시계 방향으로 n개의 서로 다른 물리 노드를 찾는다."""
        h = self._hash(key)
        hashes = [item[0] for item in self._ring]
        idx = bisect_right(hashes, h) % len(self._ring)

        replicas, seen = [], set()
        for offset in range(len(self._ring)):
            _, node = self._ring[(idx + offset) % len(self._ring)]
            if node not in seen:
                seen.add(node)
                replicas.append(node)
            if len(replicas) >= n:
                break
        return replicas
```

### 2. Vector Clock (벡터 시계)

각 값에 벡터 시계를 부착하여 인과 관계를 추적합니다.
- 한 쪽이 다른 쪽의 조상이면 최신 값을 선택
- 동시 발생(concurrent) 쓰기이면 충돌로 감지하여 모든 버전을 반환

```python
class VectorClock:
    """노드 ID → 카운터 딕셔너리 기반 벡터 시계"""

    def __init__(self, clock: dict[str, int] | None = None):
        self._clock = dict(clock) if clock else {}

    def increment(self, node_id: str) -> VectorClock:
        """해당 노드의 카운터를 1 증가시킨 새 벡터 시계를 반환"""
        new_clock = dict(self._clock)
        new_clock[node_id] = new_clock.get(node_id, 0) + 1
        return VectorClock(new_clock)

    def merge(self, other: VectorClock) -> VectorClock:
        """두 벡터 시계의 element-wise max를 반환"""
        all_keys = set(self._clock) | set(other._clock)
        return VectorClock({
            k: max(self._clock.get(k, 0), other._clock.get(k, 0))
            for k in all_keys
        })

    def compare(self, other: VectorClock) -> Ordering:
        """
        BEFORE     – self가 other의 조상 (self ≤ other)
        AFTER      – self가 other의 자손 (self ≥ other)
        EQUAL      – 동일
        CONCURRENT – 충돌 (어느 쪽도 지배하지 않음)
        """
        all_keys = set(self._clock) | set(other._clock)
        self_leq = other_leq = True
        for k in all_keys:
            sv, ov = self._clock.get(k, 0), other._clock.get(k, 0)
            if sv > ov: self_leq = False
            if ov > sv: other_leq = False

        if self_leq and other_leq: return Ordering.EQUAL
        if self_leq:               return Ordering.BEFORE
        if other_leq:              return Ordering.AFTER
        return Ordering.CONCURRENT
```

**Vector Clock 동작 예시:**

```
1. 클라이언트 A → Node1에 put("name", "john")
   VC: {node1: 1}

2. 클라이언트 B → Node1에 put("name", "jane")  (A의 결과를 읽은 후)
   VC: {node1: 2}   → node1:1의 자손이므로 덮어쓰기

3. 동시 쓰기:
   클라이언트 C → Node1에 put("name", "alice")  VC: {node1: 2, node1_c: 1}
   클라이언트 D → Node2에 put("name", "bob")    VC: {node1: 2, node2: 1}
   → CONCURRENT! 두 버전 모두 클라이언트에 반환
```

### 3. Quorum (정족수)

- **N=3** (복제 인수): 각 키를 3개 노드에 복제
- **W=2** (쓰기 정족수): 2개 노드의 ACK를 받아야 쓰기 성공
- **R=2** (읽기 정족수): 2개 노드의 응답을 받아야 읽기 성공
- **W + R > N** 이므로 최신 데이터를 읽을 수 있습니다

```python
class QuorumController:
    async def quorum_put(self, key, value, replica_nodes, vector_clock=None):
        """코디네이터가 벡터 시계를 증가시키고 N개 복제 노드에 쓰기를 팬아웃"""
        vc = (vector_clock or VectorClock()).increment(self.node_id)

        # 모든 복제 노드에 동시 전송
        tasks = [self._send_put(node, key, value, vc) for node in replica_nodes]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # W개 이상 ACK 확인
        successes = [r for r in results if isinstance(r, dict) and r.get("ok")]
        if len(successes) >= self.w:
            return {"status": "ok", "vector_clock": vc.to_dict(), "acks": len(successes)}
        return {"status": "error", "message": f"Write quorum not met: {len(successes)}/{self.w}"}

    async def quorum_get(self, key, replica_nodes):
        """N개 노드에서 읽고, Vector Clock으로 최신 값을 선택 (또는 충돌 반환)"""
        tasks = [self._send_get(node, key) for node in replica_nodes]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        responses = [r for r in results if isinstance(r, dict) and r.get("found")]

        if len(responses) < self.r:
            return {"status": "not_found"}
        return self._reconcile(key, responses)  # Vector Clock 기반 최신 값 선택
```

**Reconciliation (충돌 해결):**

```python
@staticmethod
def _reconcile(key, responses):
    """Vector Clock으로 최신 값 선택. 동시 쓰기면 모든 버전 반환."""
    latest = []
    for candidate in responses:
        dominated = False
        new_latest = []
        for existing in latest:
            ordering = candidate.vector_clock.compare(existing.vector_clock)
            if ordering == Ordering.BEFORE:    # 후보가 더 오래됨 → 무시
                dominated = True
                new_latest.append(existing)
            elif ordering == Ordering.AFTER:    # 후보가 더 최신 → 기존 제거
                pass
            else:                               # CONCURRENT → 둘 다 유지
                new_latest.append(existing)
        if not dominated:
            new_latest.append(candidate)
        latest = new_latest

    if len(latest) == 1:
        return {"status": "ok", "value": latest[0].value}
    return {"status": "conflict", "versions": [...]}  # 클라이언트가 해결
```

### 4. Gossip Protocol (가십 프로토콜)

각 노드가 주기적으로(1초) 랜덤 피어에게 하트비트를 전송합니다.
- 5초 동안 하트비트가 없으면 **suspected** 상태
- 10초 동안 suspected 상태이면 **down** 으로 표시

```python
class GossipProtocol:
    SUSPECT_TIMEOUT = 5.0   # 초: 하트비트 없음 → suspected
    DOWN_TIMEOUT = 10.0     # 초: suspected 지속 → down

    async def _gossip_loop(self):
        while True:
            await asyncio.sleep(self.interval)  # 1초마다 실행
            self._heartbeat_counter += 1

            # 자신의 하트비트 갱신
            self.members[self.node_id].heartbeat = self._heartbeat_counter
            self.members[self.node_id].last_updated = time.time()

            # 다른 노드 상태 업데이트
            self._update_statuses()

            # 랜덤 피어에게 하트비트 전파
            await self._send_heartbeat_to_random_peer()

    def _update_statuses(self):
        now = time.time()
        for mid, member in self.members.items():
            if mid == self.node_id:
                continue
            elapsed = now - member.last_updated
            if elapsed > self.DOWN_TIMEOUT:
                member.status = NodeStatus.DOWN
            elif elapsed > self.SUSPECT_TIMEOUT:
                member.status = NodeStatus.SUSPECTED

    def receive_gossip(self, from_node, members):
        """수신한 가십 데이터 병합: 더 높은 하트비트만 수용"""
        for mid, info in members.items():
            incoming_hb = info.get("heartbeat", 0)
            existing = self.members.get(mid)
            if existing and incoming_hb > existing.heartbeat:
                existing.heartbeat = incoming_hb
                existing.last_updated = time.time()
                existing.status = NodeStatus.ALIVE  # 부활
```

### 5. Storage Engine (저장 엔진)

LSM-tree와 유사한 구조입니다:

```
Write Path:                          Read Path:
  Client                               Client
    │                                     │
    ▼                                     ▼
  ① WAL (디스크에 로그 기록)           ① Memtable 조회 (메모리)
    │                                     │ (없으면)
    ▼                                     ▼
  ② Memtable (메모리에 저장)           ② SSTable 조회 (디스크, 최신→오래된 순)
    │ (임계값 초과 시)
    ▼
  ③ SSTable 플러시 (디스크)
```

```python
class StorageEngine:
    def __init__(self, data_dir: str, memtable_threshold: int = 100):
        self._memtable: dict[str, StoredValue] = {}  # 메모리 저장소
        self._wal_path = os.path.join(data_dir, "wal.log")
        self._sstable = SSTable(os.path.join(data_dir, "sstables"))
        self._replay_wal()  # 시작 시 WAL 리플레이로 복구

    def put(self, key: str, value: str) -> float:
        ts = time.time()
        self._append_wal("PUT", key, value, ts)    # ① WAL 기록 (장애 복구용)
        self._memtable[key] = StoredValue(value=value, timestamp=ts)  # ② 메모리 저장
        self._maybe_flush()                         # ③ 임계값 초과 시 SSTable 플러시
        return ts

    def get(self, key: str) -> StoredValue | None:
        entry = self._memtable.get(key)             # ① 먼저 Memtable 확인
        if entry is not None:
            return None if entry.deleted else entry
        ss_entry = self._sstable.get(key)           # ② 없으면 SSTable 조회
        if ss_entry and ss_entry.value != "__TOMBSTONE__":
            return StoredValue(value=ss_entry.value, timestamp=ss_entry.timestamp)
        return None

    def _maybe_flush(self):
        """Memtable이 임계값 초과 시 SSTable로 플러시"""
        if len(self._memtable) < self._memtable_threshold:
            return
        flush_data = {k: (v.value, v.timestamp) for k, v in self._memtable.items()}
        self._sstable.flush(flush_data)   # 정렬된 key-value 파일로 디스크에 저장
        self._memtable.clear()
        # WAL 비우기 (플러시된 데이터는 SSTable에 안전하게 저장됨)
        with open(self._wal_path, "w"):
            pass
```

**SSTable** — 정렬된 키-값 파일:

```python
class SSTable:
    def flush(self, memtable: dict[str, tuple[str, float]]) -> str:
        """Memtable을 정렬된 키-값 파일로 디스크에 기록"""
        filepath = f"sstable_{int(time.time() * 1000)}.dat"
        sorted_items = sorted(memtable.items(), key=lambda kv: kv[0])
        with open(filepath, "w") as fh:
            for key, (value, ts) in sorted_items:
                fh.write(f"{key}\t{value}\t{ts}\n")  # TSV 포맷
        return filepath

    def get(self, key: str) -> SSTableEntry | None:
        """최신 SSTable부터 순차 탐색"""
        for filepath in self._list_files():  # 최신 파일 우선
            entry = self._scan_file(filepath, key)
            if entry is not None:
                return entry
        return None
```

## 핵심 기술 요약

| 목표/문제 | 기법 |
|-----------|------|
| 대용량 데이터 저장 | Consistent Hashing으로 분산 |
| 읽기 고가용성 | 데이터 복제 (N=3) |
| 쓰기 고가용성 | Vector Clock + 충돌 해결 |
| 데이터 파티셔닝 | Consistent Hashing |
| 조정 가능한 일관성 | Quorum (W + R > N) |
| 일시적 장애 처리 | Sloppy Quorum + Hinted Handoff |
| 영구적 장애 처리 | Merkle Tree (미구현) |
| 장애 감지 | Gossip Protocol |

## 빠른 시작

### 클러스터 실행

```bash
cd 07-design-a-key-value-store
docker-compose up --build
```

3개 노드가 실행됩니다:
- Node 1: http://localhost:8071
- Node 2: http://localhost:8072
- Node 3: http://localhost:8073

### 클러스터 종료

```bash
docker-compose down -v
```

## CLI 사용법

먼저 httpx를 설치합니다:

```bash
pip install httpx
```

### 값 저장 (PUT)

```bash
python scripts/cli.py put mykey "hello world"
```

### 값 조회 (GET)

```bash
python scripts/cli.py get mykey
```

### 값 삭제 (DELETE)

```bash
python scripts/cli.py delete mykey
```

### 키 목록 조회

```bash
python scripts/cli.py list
```

### 클러스터 정보

```bash
python scripts/cli.py cluster
```

### 다른 노드에 연결

```bash
# Node 1에 쓰기 → Node 2에서 읽기 (복제 확인)
python scripts/cli.py --node http://localhost:8071 put greeting "hello"
python scripts/cli.py --node http://localhost:8072 get greeting
```

## API 엔드포인트

| Method | Path | 설명 |
|--------|------|------|
| `GET` | `/health` | 노드 상태 및 멤버십 정보 |
| `PUT` | `/store/{key}` | 값 저장 (body: `{"value": "..."}`) |
| `GET` | `/store/{key}` | 값 조회 |
| `DELETE` | `/store/{key}` | 키 삭제 |
| `GET` | `/store` | 현재 노드의 모든 키 목록 |
| `GET` | `/cluster/info` | 클러스터 멤버십 및 해시 링 정보 |

### curl 예시

```bash
# 값 저장
curl -X PUT http://localhost:8071/store/greeting \
  -H "Content-Type: application/json" \
  -d '{"value": "hello world"}'

# 값 조회 (vector clock 포함)
curl http://localhost:8071/store/greeting
# → {"status":"ok","key":"greeting","value":"hello world","vector_clock":{"node1":1}}

# 다른 노드에서 조회 (복제 확인)
curl http://localhost:8072/store/greeting

# 삭제
curl -X DELETE http://localhost:8071/store/greeting
```

## 테스트

```bash
cd 07-design-a-key-value-store
pip install -r node/requirements.txt pytest
PYTHONPATH=. pytest tests/ -v
```

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide", Chapter 6
