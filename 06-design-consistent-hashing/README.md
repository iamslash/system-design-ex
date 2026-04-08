# Design Consistent Hashing

안정 해시(Consistent Hashing) 설계를 다룬다.

---

## 목차

1. [재해싱 문제 (Rehashing Problem)](#재해싱-문제-rehashing-problem)
2. [해시 링 (Hash Ring)](#해시-링-hash-ring)
3. [가상 노드 (Virtual Nodes)](#가상-노드-virtual-nodes)
4. [핵심 구현](#핵심-구현)
5. [실행 방법](#실행-방법)
6. [샘플 출력](#샘플-출력)

---

## 재해싱 문제 (Rehashing Problem)

가장 단순한 분산 해싱 방식은 **모듈러 해싱**이다.

```python
server_index = hash(key) % N   # N = 서버 수
```

서버가 N 대일 때는 잘 동작하지만, 서버를 추가하거나 제거하면 **거의 모든
키의 매핑이 바뀐다**. 서버 4대에서 1대를 제거하면 약 75%의 키가 재배치된다.

```python
# 모듈러 해싱의 문제점 시뮬레이션
servers = ["s0", "s1", "s2", "s3"]
keys = [f"key-{i}" for i in range(10000)]

# 4대일 때 매핑
mapping_4 = {k: servers[hash(k) % 4] for k in keys}

# s3 제거 → 3대로 변경
mapping_3 = {k: servers[hash(k) % 3] for k in keys}

# 재배치된 키 수
remapped = sum(1 for k in keys if mapping_4[k] != mapping_3.get(k, ""))
print(f"Keys remapped: {remapped}/10000 ({remapped/100:.1f}%)")
# → Keys remapped: ~7500/10000 (75.0%)
```

---

## 해시 링 (Hash Ring)

안정 해시는 해시 공간을 **원형 링**으로 취급한다.

```
            s0
           ╱    ╲
         k0      s3
        │          │
         k1      k3
           ╲    ╱
            s1
           ╱
         s2
          k2
```

1. 각 서버를 해시 함수로 링 위의 한 지점에 배치한다.
2. 키도 같은 해시 함수로 링 위에 배치한다.
3. 키에서 **시계 방향으로** 가장 가까운 서버가 해당 키를 담당한다.

서버가 추가/제거되면 **인접 구간의 키만** 이동한다. 이론적으로 서버 N 대일 때
평균 **K/N** 개의 키만 재배치된다 (K = 전체 키 수).

### 핵심 코드: 해시 함수 & 시계 방향 탐색

```python
import hashlib
from bisect import bisect_right

def _hash(key: str) -> int:
    """SHA-256 해시를 정수로 변환"""
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return int(digest, 16)

class ConsistentHashRing:
    def __init__(self, num_virtual_nodes: int = 150):
        self._num_virtual_nodes = num_virtual_nodes
        self._keys: list[int] = []       # 정렬된 해시 값 리스트
        self._ring: dict[int, str] = {}  # 해시 값 → 물리 노드 이름
        self._nodes: set[str] = set()    # 물리 노드 집합
```

---

## 가상 노드 (Virtual Nodes)

물리 서버를 링 위에 한 지점에만 배치하면 서버 간 담당 범위가 불균등해진다.
이를 해결하기 위해 각 물리 서버마다 **여러 개의 가상 노드(virtual node)**를
링 위에 분산 배치한다.

```
가상 노드 3개일 때:        가상 노드 150개일 때:
s0 ──── 넓은 구간          s0 ── 균등한 구간들
s1 ── 좁은 구간            s1 ── 균등한 구간들
s2 ────── 매우 넓은 구간    s2 ── 균등한 구간들
→ 불균등!                  → 균등!
```

| 가상 노드 수 | 분포 균등성 | 표준편차 | 메모리 사용 |
|:---:|:---:|:---:|:---:|
| 3 | 매우 불균등 | ~1000 | 적음 |
| 50 | 보통 | ~200 | 보통 |
| 150 | 균등 | ~45 | 보통 |
| 500 | 매우 균등 | ~25 | 많음 |

가상 노드가 많을수록 표준편차가 줄어들어 부하가 고르게 분산된다.

---

## 핵심 구현

### 노드 추가 (add_node)

물리 노드 하나를 추가하면 `num_virtual_nodes` 개의 가상 노드가 링에 배치된다:

```python
def add_node(self, node: str) -> None:
    if node in self._nodes:
        return
    self._nodes.add(node)
    for i in range(self._num_virtual_nodes):
        virtual_key = f"{node}#vn{i}"          # 가상 노드 키 생성
        h = _hash(virtual_key)                  # SHA-256 → 정수
        self._ring[h] = node                    # 해시 → 물리 노드 매핑
        bisect.insort(self._keys, h)            # 정렬 유지하며 삽입
```

### 키 조회 (get_node) — 시계 방향 탐색

키를 해싱한 후, 링에서 시계 방향으로 가장 가까운 가상 노드를 `bisect_right`로 O(log n) 탐색:

```python
def get_node(self, key: str) -> str:
    if not self._ring:
        raise RuntimeError("The hash ring is empty")

    h = _hash(key)
    idx = bisect.bisect_right(self._keys, h)  # 키 해시보다 큰 첫 위치
    if idx == len(self._keys):
        idx = 0                                 # 링의 끝 → 처음으로 wrap around
    return self._ring[self._keys[idx]]          # 해당 가상 노드의 물리 서버 반환
```

### 노드 제거 (remove_node)

```python
def remove_node(self, node: str) -> None:
    if node not in self._nodes:
        raise KeyError(f"Node {node!r} is not on the ring")
    self._nodes.discard(node)
    for i in range(self._num_virtual_nodes):
        virtual_key = f"{node}#vn{i}"
        h = _hash(virtual_key)
        del self._ring[h]
        # 정렬된 리스트에서 이진 탐색으로 제거
        idx = bisect.bisect_left(self._keys, h)
        if idx < len(self._keys) and self._keys[idx] == h:
            self._keys.pop(idx)
```

### 키 분포 조회 (get_distribution)

```python
def get_distribution(self, keys: list[str]) -> dict[str, int]:
    """각 물리 노드에 매핑된 키의 수를 반환"""
    dist = {node: 0 for node in sorted(self._nodes)}
    for key in keys:
        node = self.get_node(key)
        dist[node] += 1
    return dist
```

### 사용 예시

```python
ring = ConsistentHashRing(num_virtual_nodes=150)

# 서버 4대 추가
for i in range(4):
    ring.add_node(f"server-{i}")

# 키 조회
print(ring.get_node("user:1234"))  # → "server-2"
print(ring.get_node("order:5678")) # → "server-0"

# 분포 확인
keys = [f"key-{i}" for i in range(10000)]
print(ring.get_distribution(keys))
# → {'server-0': 2515, 'server-1': 2239, 'server-2': 2815, 'server-3': 2431}

# 서버 추가 → 최소한의 키만 재배치
ring.add_node("server-4")
new_dist = ring.get_distribution(keys)
# 약 20% 키만 이동 (K/N ≈ 10000/5 = 2000)
```

---

## 실행 방법

### 사전 조건

- Python 3.11 이상

### 데모 실행

```bash
cd 06-design-consistent-hashing
python scripts/demo.py
```

### 테스트 실행

```bash
cd 06-design-consistent-hashing
pip install -r requirements.txt
pytest tests/ -v
```

---

## 샘플 출력

```
Consistent Hashing Demo
=======================

============================================================
  Rehashing Problem (Modular Hash)
============================================================

Modular hash with 4 servers:
  server-0: 2518
  server-1: 2476
  server-2: 2524
  server-3: 2482

Remove server-3 -> modular hash with 3 servers:
  server-0: 3326
  server-1: 3340
  server-2: 3334

Keys remapped: 7523/10000 (75.2%)

============================================================
  Consistent Hashing
============================================================

Ring with 4 servers, 150 vnodes:
  server-0: 2550
  server-1: 2420
  server-2: 2510
  server-3: 2520
  std dev: 46.3

Remove server-3:
  server-0: 3400
  server-1: 3210
  server-2: 3390
  std dev: 87.9
  Keys remapped: 2520/10000 (25.2%)

============================================================
  Virtual Nodes Effect
============================================================

    vnodes   std dev     min     max
    ------   -------     ---     ---
         3    1042.1     438    4120
        10     593.2    1012    3401
        50     198.4    1650    2612
       150      46.3    1890    2130
       500      25.1    1940    2080

============================================================
  Add / Remove Server Simulation
============================================================

Initial ring (5 servers):
  server-0: 2040  server-1: 1930  ...
  std dev: 35.0

After adding server-5 (6 servers):
  Keys remapped: ~1650/10000 (16.5%)

After removing server-2 (4 servers):
  Keys remapped: ~2010/10000 (20.1%)
```

> 참고: 실제 출력 값은 해시 함수 결과에 따라 약간 다를 수 있다.

## 실제 활용 사례

- Amazon DynamoDB: 데이터 파티셔닝
- Apache Cassandra: 클러스터 데이터 분배
- Discord: 채팅 메시지 분산
- Akamai CDN: 콘텐츠 분배

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide", Chapter 5
