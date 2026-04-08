# Design Google Maps

Google Maps 는 경로 탐색, 실시간 교통 정보, ETA 계산, 지오코딩 등을 제공하는
위치 기반 서비스다. 이 예제는 핵심 구성 요소를 순수 Python 으로 구현한다.

## 아키텍처

```
┌──────────────────────────────────────────────────────────────┐
│                        Client (CLI)                          │
└──────────────────────────┬───────────────────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────────┐
│  Geocoding   │  │   Routing    │  │  ETA Estimator   │
│  Service     │  │   Engine     │  │                  │
│              │  │              │  │  distance/speed  │
│  name→latlng │  │  A* / Dijkstra│  │  + traffic model│
└──────────────┘  └──────┬───────┘  └────────┬─────────┘
                         │                   │
                ┌────────▼───────────────────▼──────┐
                │           Road Graph              │
                │                                   │
                │  Nodes (intersections: lat/lng)    │
                │  Edges (roads: distance, speed)    │
                │                                   │
                │  ┌─────────────────────────────┐  │
                │  │      Routing Tiles           │  │
                │  │  geohash 기반 타일 분할       │  │
                │  │  on-demand lazy loading       │  │
                │  └─────────────────────────────┘  │
                └───────────────────────────────────┘
```

### 요청 흐름

1. 클라이언트가 출발지/목적지 이름을 입력한다.
2. **Geocoding Service** 가 이름을 좌표 (lat/lng) 로 변환한다.
3. **Routing Engine** 이 A* 알고리즘으로 최단 경로를 탐색한다.
4. **ETA Estimator** 가 경로의 거리, 제한속도, 교통 상황을 반영하여 도착 예상 시간을 계산한다.

## Quick Start

```bash
# 데모 실행
python scripts/demo.py

# 테스트
pip install -r requirements.txt
pytest tests/ -v
```

## Map Tiling (지도 타일링)

대규모 도로 네트워크를 메모리에 모두 올리는 것은 비효율적이다. **Routing Tile** 은
지리적 좌표를 기반으로 그래프를 타일 단위로 분할하여 필요한 영역만 로드한다.

### Geohash 기반 타일 키

```python
# 위도/경도를 precision 자릿수로 양자화하여 타일 키 생성
def tile_key(lat: float, lng: float, precision: int = 2) -> str:
    factor = 10 ** precision
    qlat = math.floor(lat * factor) / factor
    qlng = math.floor(lng * factor) / factor
    return f"{qlat:.{precision}f},{qlng:.{precision}f}"
    # 예: tile_key(37.495, 127.025) -> "37.49,127.02"
```

- `precision=2` 일 때 타일 크기는 약 1 km
- `precision=1` 일 때 타일 크기는 약 11 km

### 타일 계층 구조 (Routing Tile Hierarchy)

```
┌─────────────────────────────────────────┐
│  Level 0 (precision=1) : ~11 km 타일   │  장거리 경로 개략 탐색
│  ┌──────────────────────────────────┐   │
│  │ Level 1 (precision=2) : ~1 km   │   │  도시 내 상세 경로
│  │  ┌───────────────────────────┐   │   │
│  │  │ Level 2 (precision=3)    │   │   │  블록 단위 정밀 경로
│  │  └───────────────────────────┘   │   │
│  └──────────────────────────────────┘   │
└─────────────────────────────────────────┘
```

실제 Google Maps 는 계층적 타일을 사용하여:
- **장거리**: 고속도로 위주의 coarse 타일로 빠르게 개략 경로 탐색
- **단거리**: fine 타일로 정밀 경로 탐색
- **Lazy Loading**: 경로가 통과하는 타일만 on-demand 로 메모리에 적재

### TileManager: 지연 로딩

```python
class TileManager:
    """타일을 지연 생성하는 매니저. 처음 요청 시 마스터 그래프에서 추출."""

    def __init__(self, master: RoadGraph, precision: int = 2) -> None:
        self._master = master
        self._precision = precision
        self._tiles: dict[str, RoutingTile] = {}
        self._node_tile: dict[str, str] = {}  # node_id -> tile_key

        # 노드-타일 매핑 사전 계산
        for nid, node in master.nodes.items():
            key = tile_key(node.lat, node.lng, precision)
            self._node_tile[nid] = key

    def get_tile(self, key: str) -> RoutingTile:
        """타일을 가져온다 (미적재 시 마스터 그래프에서 추출)."""
        if key in self._tiles and self._tiles[key].loaded:
            return self._tiles[key]
        tile = RoutingTile(key=key)
        node_ids = {nid for nid, tk in self._node_tile.items() if tk == key}
        for nid in node_ids:
            n = self._master.get_node(nid)
            tile.graph.add_node(n.id, n.lat, n.lng)
        # 타일 내 노드에서 출발하는 간선 포함
        for nid in node_ids:
            for edge in self._master.get_neighbors(nid):
                if not tile.graph.has_node(edge.dst):
                    dn = self._master.get_node(edge.dst)
                    tile.graph.add_node(dn.id, dn.lat, dn.lng)
                tile.graph._adj[nid].append(edge)
        tile.loaded = True
        self._tiles[key] = tile
        return tile
```

### 인접 타일 탐색

```python
# 3x3 이웃 타일 키 (중심 + 8방향) 계산
def neighbor_keys(lat: float, lng: float, precision: int = 2) -> list[str]:
    step = 1 / (10 ** precision)
    keys: list[str] = []
    for dlat in (-step, 0, step):
        for dlng in (-step, 0, step):
            keys.append(tile_key(lat + dlat, lng + dlng, precision))
    return keys
```

## Routing (경로 탐색)

### A* 알고리즘

A* 는 Dijkstra 에 **heuristic** (목표까지의 추정 거리) 을 더한 알고리즘이다.
`f(n) = g(n) + h(n)` 에서 `g(n)` 은 시작점에서 현재 노드까지의 실제 거리,
`h(n)` 은 현재 노드에서 목표까지의 Haversine 거리 (직선) 이다.

```python
def astar(graph: RoadGraph, start: str, end: str) -> RouteResult | None:
    """Haversine heuristic 을 사용한 A* 경로 탐색."""
    goal_node = graph.get_node(end)
    g_score: dict[str, float] = {start: 0.0}
    prev: dict[str, str | None] = {start: None}
    visited: set[str] = set()
    explored = 0

    h_start = haversine(graph.get_node(start), goal_node)
    heap: list[tuple[float, str]] = [(h_start, start)]

    while heap:
        _f, node = heapq.heappop(heap)
        if node in visited:
            continue
        visited.add(node)
        explored += 1

        if node == end:
            return RouteResult(
                path=_reconstruct(prev, end),
                distance_km=g_score[end],
                nodes_explored=explored,
            )

        for edge in graph.get_neighbors(node):
            if edge.dst in visited:
                continue
            tentative_g = g_score[node] + edge.distance_km
            if tentative_g < g_score.get(edge.dst, float("inf")):
                g_score[edge.dst] = tentative_g
                prev[edge.dst] = node
                h = haversine(graph.get_node(edge.dst), goal_node)
                heapq.heappush(heap, (tentative_g + h, edge.dst))

    return None  # 경로 없음
```

### Haversine 거리 (heuristic)

두 점 사이의 대원 거리를 계산한다. A* 의 heuristic 으로 사용되며,
실제 도로 거리보다 항상 작거나 같아 (admissible) 최적 경로를 보장한다.

```python
_EARTH_RADIUS_KM = 6371.0

def haversine(a: Node, b: Node) -> float:
    """두 노드 사이의 대원 거리 (km)."""
    lat1, lat2 = math.radians(a.lat), math.radians(b.lat)
    dlat = math.radians(b.lat - a.lat)
    dlng = math.radians(b.lng - a.lng)
    h = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlng / 2) ** 2
    )
    return 2 * _EARTH_RADIUS_KM * math.asin(math.sqrt(h))
```

### A* vs Dijkstra 비교

| 알고리즘 | 탐색 노드 수 | 시간 복잡도 | 특징 |
|----------|-------------|------------|------|
| Dijkstra | 모든 방향 균등 탐색 | O(E log V) | 최단 경로 보장 |
| A* | 목표 방향 우선 탐색 | O(E log V) | 최단 경로 보장 + 더 적은 탐색 |

데모 결과 (24 노드 그래프에서 A1 -> D6):
- **A***: 9 노드 탐색
- **Dijkstra**: 24 노드 탐색 (전체 그래프)
- A* 가 **15개 더 적은 노드** 를 탐색하여 동일한 최단 경로를 찾음

## ETA with Traffic (교통 상황 반영 도착 예상 시간)

### ETA 계산 공식

각 도로 구간의 이동 시간:

```
time = distance / speed_limit * traffic_multiplier
```

- `traffic_multiplier = 1.0`: 정상 흐름 (free-flow)
- `traffic_multiplier = 2.0`: 2배 느림 (혼잡)
- `traffic_multiplier = 3.0`: 3배 느림 (심한 정체)

```python
def compute_eta(
    graph: RoadGraph,
    path: list[str],
    traffic: TrafficModel | None = None,
) -> ETAResult:
    """경로의 ETA 를 계산한다. 각 구간별 거리/속도/교통을 반영."""
    total_dist = 0.0
    total_time = 0.0  # hours
    seg_times: list[float] = []

    for i in range(len(path) - 1):
        src, dst = path[i], path[i + 1]
        edge = _find_edge(graph, src, dst)
        multiplier = traffic.get_multiplier(src, dst) if traffic else 1.0
        travel_hours = (edge.distance_km / edge.speed_limit_kmh) * multiplier

        total_dist += edge.distance_km
        total_time += travel_hours
        seg_times.append(travel_hours * 60)  # 분으로 변환

    return ETAResult(
        path=path,
        total_distance_km=total_dist,
        total_time_hours=total_time,
        total_time_minutes=total_time * 60,
        segment_times=seg_times,
    )
```

### 교통 모델

```python
class TrafficModel:
    """도로 구간별 교통 상태 관리."""

    def set_traffic(self, src: str, dst: str, multiplier: float) -> None:
        """특정 구간의 교통 배율 설정."""
        self._conditions[(src, dst)] = multiplier

    def get_multiplier(self, src: str, dst: str) -> float:
        """구간의 교통 배율 조회 (기본값 1.0)."""
        return self._conditions.get((src, dst), 1.0)
```

실제 서비스에서는 실시간 GPS 데이터, 과거 패턴, 사고 정보 등을 결합하여
교통 모델을 업데이트한다.

## Location Service (위치 서비스 / Geocoding)

### Forward Geocoding (이름 -> 좌표)

```python
class GeocodingService:
    """Dict 기반 지오코딩: 이름 -> 좌표 변환."""

    def geocode(self, name: str) -> Location | None:
        """이름으로 좌표 검색 (대소문자 무시)."""
        return self._forward.get(name.lower())

    def reverse_geocode(self, lat: float, lng: float,
                        tolerance: float = 0.001) -> Location | None:
        """좌표로 가장 가까운 장소 검색 (tolerance 이내)."""
        exact = self._reverse.get((lat, lng))
        if exact is not None:
            return exact
        best: Location | None = None
        best_dist = float("inf")
        for (rlat, rlng), loc in self._reverse.items():
            d = abs(rlat - lat) + abs(rlng - lng)
            if d < best_dist and d <= tolerance:
                best_dist = d
                best = loc
        return best

    def search(self, query: str) -> list[Location]:
        """이름에 검색어가 포함된 장소 목록 반환."""
        q = query.lower()
        return [loc for key, loc in self._forward.items() if q in key]
```

실제 시스템에서는:
- **Forward Geocoding**: 텍스트 주소 -> 좌표 (Elasticsearch + 주소 DB)
- **Reverse Geocoding**: 좌표 -> 주소 (R-tree 공간 인덱스)
- **Autocomplete**: 접두사 검색 (Trie + 인기도 점수)

## Road Graph (도로 그래프)

### 데이터 모델

```python
@dataclass(frozen=True)
class Node:
    """교차로 (intersection)."""
    id: str
    lat: float   # 위도
    lng: float   # 경도

@dataclass(frozen=True)
class Edge:
    """방향 도로 구간."""
    src: str              # 출발 교차로
    dst: str              # 도착 교차로
    distance_km: float    # 거리 (km)
    speed_limit_kmh: float = 60.0  # 제한속도 (km/h)
```

### 가중 방향 그래프

```python
class RoadGraph:
    """도로 네트워크를 나타내는 가중 방향 그래프."""

    def add_edge(self, src, dst, distance_km=None,
                 speed_limit_kmh=60.0, bidirectional=True) -> Edge:
        """도로 구간 추가. distance_km 이 None 이면 Haversine 으로 계산."""
        if distance_km is None:
            distance_km = haversine(self._nodes[src], self._nodes[dst])
        edge = Edge(src=src, dst=dst, distance_km=distance_km,
                    speed_limit_kmh=speed_limit_kmh)
        self._adj[src].append(edge)
        if bidirectional:
            reverse = Edge(src=dst, dst=src, distance_km=distance_km,
                           speed_limit_kmh=speed_limit_kmh)
            self._adj[dst].append(reverse)
        return edge
```

## 데모 결과

```
============================================================
  1. Road Network
============================================================
  Intersections : 24
  Road segments : 84

============================================================
  3. Routing: A1 -> D6
============================================================
  A* path      : A1 -> B2 -> C3 -> D4 -> D5 -> D6
  A* distance  : 5.293 km
  A* explored  : 9 nodes

  Dijkstra path: A1 -> B2 -> C3 -> D4 -> D5 -> D6
  Dijkstra dist: 5.293 km
  Dijkstra exp : 24 nodes

  A* explored 15 fewer nodes than Dijkstra

============================================================
  4. ETA (no traffic)
============================================================
  Distance     : 5.293 km
  Time         : 5.6 minutes
  Avg speed    : 57.1 km/h

============================================================
  5. ETA (rush hour traffic)
============================================================
  Distance     : 5.293 km
  Time         : 11.9 minutes
  Avg speed    : 26.7 km/h
  Slowdown     : +6.4 min vs free-flow

============================================================
  6. Routing Tiles
============================================================
  Total tiles  : 18
  Loaded tiles : 0  (lazy loading 전)
  Tile for A1  : key='37.49,127.02' (4 nodes)
  Loaded tiles : 1  (A1 타일만 로드)
  Tiles near A1: 4 loaded (3x3 이웃 중 데이터 있는 타일)
```

## 테스트

```bash
pytest tests/ -v
```

52 개 테스트:
- **Graph** (12): 노드/간선 추가/삭제, 이웃 조회, 서브그래프, Haversine
- **Routing** (8): A* / Dijkstra 경로 탐색, 경로 없음, 동일 출발/도착, 그리드 순회
- **Routing Tiles** (9): 타일 키 생성, 이웃 타일, 지연 로딩, 타일 병합, 크로스 타일 라우팅
- **ETA** (11): 기본 ETA, 교통 반영, 배율 검증, 구간별 시간
- **Geocoding** (10): 정방향/역방향 지오코딩, 검색, 대소문자 무시, 허용 범위
