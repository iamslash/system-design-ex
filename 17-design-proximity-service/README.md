# Design Proximity Service

주변 검색(Proximity Service) 시스템 설계를 다룬다. 사용자의 위치(위도/경도)와
검색 반경이 주어지면, 주변의 비즈니스(식당, 카페, 상점 등)를 빠르게 찾아
반환하는 서비스이다.

---

## 목차

1. [아키텍처](#아키텍처)
2. [Geohash](#geohash)
3. [Quadtree](#quadtree)
4. [공간 인덱스 비교](#공간-인덱스-비교)
5. [경계 문제와 해결](#경계-문제와-해결)
6. [핵심 구현](#핵심-구현)
7. [실행 방법](#실행-방법)
8. [샘플 출력](#샘플-출력)

---

## 아키텍처

```
┌────────────┐     ┌────────────────┐     ┌──────────────────┐
│   Client   │────▶│   LB / API     │────▶│  Proximity       │
│  (Mobile)  │◀────│   Gateway      │◀────│  Service         │
└────────────┘     └────────────────┘     └──────┬───────────┘
                                                 │
                          ┌──────────────────────┼──────────────────────┐
                          │                      │                      │
                   ┌──────▼──────┐      ┌────────▼────────┐   ┌────────▼────────┐
                   │  Geohash    │      │   Quadtree      │   │   Business      │
                   │  Index      │      │   Index         │   │   DB            │
                   │  (Redis /   │      │   (In-Memory)   │   │   (MySQL /      │
                   │   DB prefix)│      │                 │   │    DynamoDB)    │
                   └─────────────┘      └─────────────────┘   └─────────────────┘
```

### 요청 흐름

1. 클라이언트가 `(lat, lng, radius)` 를 전송한다.
2. Proximity Service 가 공간 인덱스(Geohash 또는 Quadtree)를 조회한다.
3. 후보 비즈니스에 대해 Haversine 거리를 계산하여 반경 내의 결과만 필터링한다.
4. 거리순으로 정렬하여 상위 k 개를 반환한다.

---

## Geohash

Geohash 는 위도/경도를 **Base-32 문자열**로 인코딩하는 계층적 공간 인덱스이다.
문자를 하나 추가할 때마다 셀이 약 32 배 더 좁아진다.

### Geohash 정밀도 테이블

| 정밀도 | 셀 너비 (km) | 셀 높이 (km) | 용도 |
|:---:|:---:|:---:|:---:|
| 1 | 5,000 | 5,000 | 대륙 |
| 2 | 1,250 | 625 | 국가 |
| 3 | 156 | 156 | 광역시 |
| 4 | 39.1 | 19.5 | 도시 |
| 5 | 4.9 | 4.9 | 동네 |
| **6** | **1.2** | **0.61** | **블록 (기본값)** |
| 7 | 0.15 | 0.15 | 건물 |
| 8 | 0.038 | 0.019 | 정밀 |
| 9 | 0.005 | 0.005 | 초정밀 |

### 인코딩 과정

위도와 경도를 번갈아 이진 분할하여 5 비트씩 묶은 후 Base-32 로 변환한다:

```python
_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"

def encode(lat: float, lng: float, precision: int = 6) -> str:
    """위도/경도를 geohash 문자열로 인코딩"""
    lat_range = (-90.0, 90.0)
    lng_range = (-180.0, 180.0)
    is_lng = True          # 경도 비트부터 시작
    bit = 0
    ch_idx = 0
    geohash: list[str] = []

    while len(geohash) < precision:
        if is_lng:
            mid = (lng_range[0] + lng_range[1]) / 2
            if lng >= mid:
                ch_idx = ch_idx * 2 + 1          # 비트 1
                lng_range = (mid, lng_range[1])
            else:
                ch_idx = ch_idx * 2              # 비트 0
                lng_range = (lng_range[0], mid)
        else:
            mid = (lat_range[0] + lat_range[1]) / 2
            if lat >= mid:
                ch_idx = ch_idx * 2 + 1
                lat_range = (mid, lat_range[1])
            else:
                ch_idx = ch_idx * 2
                lat_range = (lat_range[0], mid)

        is_lng = not is_lng
        bit += 1
        if bit == 5:                             # 5비트 완성 → Base-32 문자
            geohash.append(_BASE32[ch_idx])
            bit = 0
            ch_idx = 0

    return "".join(geohash)
```

### 이웃 셀 탐색 (Neighbor Finding)

셀의 바운딩 박스를 구한 뒤, 셀 크기만큼 8 방향으로 오프셋하여
이웃 셀의 중심점을 계산하고 다시 인코딩한다:

```python
def neighbors(geohash: str) -> list[str]:
    """8개 이웃 셀의 geohash 반환 (N, NE, E, SE, S, SW, W, NW)"""
    precision = len(geohash)
    min_lat, min_lng, max_lat, max_lng = bounding_box(geohash)
    lat_delta = max_lat - min_lat       # 셀 높이
    lng_delta = max_lng - min_lng       # 셀 너비
    center_lat = (min_lat + max_lat) / 2
    center_lng = (min_lng + max_lng) / 2

    # 8방향 오프셋: (위도 변화, 경도 변화)
    offsets = [
        (lat_delta, 0),            # N
        (lat_delta, lng_delta),    # NE
        (0, lng_delta),            # E
        (-lat_delta, lng_delta),   # SE
        (-lat_delta, 0),           # S
        (-lat_delta, -lng_delta),  # SW
        (0, -lng_delta),           # W
        (lat_delta, -lng_delta),   # NW
    ]

    return [
        encode(
            max(-89.999999, min(89.999999, center_lat + dlat)),
            ((center_lng + dlng + 180) % 360) - 180,  # 경도 래핑
            precision,
        )
        for dlat, dlng in offsets
    ]
```

---

## Quadtree

Quadtree 는 2차원 공간을 재귀적으로 4등분하는 트리 구조이다. 리프 노드에
비즈니스가 `max_points` 개를 초과하면 분할된다.

```
┌───────────┬───────────┐
│           │           │
│    NW     │    NE     │
│  (3 biz)  │  (1 biz)  │
│           │           │
├─────┬─────┼───────────┤
│ SW  │ SW  │           │
│(2)  │(1)  │    SE     │
│     │     │  (0 biz)  │
├─────┼─────┤           │
│(1)  │(0)  │           │
└─────┴─────┴───────────┘
max_points = 3 → SW 가 4개 초과하여 재분할
```

### k-최근접 탐색

중심점에서 시작 반경의 바운딩 박스를 만들어 질의하고, 후보가 k 개
미만이면 반경을 2 배씩 확장한다:

```python
def find_nearest(self, lat, lng, k=5, initial_radius_km=1.0):
    """k 개의 가장 가까운 비즈니스를 찾는다"""
    radius = initial_radius_km
    candidates = []

    while radius <= max_radius_km:
        box = _radius_to_box(lat, lng, radius)  # 원 → 바운딩 박스 근사
        candidates = self.query_range(box)
        if len(candidates) >= k:
            break
        radius *= 2                              # 반경 2배 확장

    # Haversine 실제 거리로 정렬, 상위 k개 반환
    with_dist = [(biz, haversine_km(lat, lng, biz.lat, biz.lng))
                 for biz in candidates]
    with_dist.sort(key=lambda x: x[1])
    return with_dist[:k]
```

### Haversine 공식

지구 표면의 두 점 간 대원 거리를 계산한다:

```python
def haversine_km(lat1, lng1, lat2, lng2) -> float:
    """두 지점 간 거리 (km)"""
    R = 6371.0  # 지구 반지름 (km)
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlng/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))
```

---

## 공간 인덱스 비교

| 항목 | Geohash | Quadtree | S2 (Google) |
|:---:|:---:|:---:|:---:|
| **구조** | 문자열 접두사 | 재귀 4분할 트리 | 힐베르트 곡선 셀 |
| **저장** | DB / Redis prefix | 인메모리 트리 | DB 정수 범위 |
| **정밀도** | 고정 (1-12) | 적응형 (밀도 기반) | 적응형 (레벨 0-30) |
| **샤딩** | 접두사 기반 쉬움 | 트리 분할 어려움 | 셀 ID 범위 기반 |
| **경계 문제** | 있음 (이웃 셀 탐색 필요) | 없음 | 없음 |
| **k-최근접** | 간접적 (확장 탐색) | 네이티브 지원 | 간접적 |
| **실시간 갱신** | INSERT/DELETE 쉬움 | 트리 재구축 필요 | INSERT/DELETE 쉬움 |
| **대표 사용처** | Elasticsearch, Redis GEO | 인메모리 게임 서버 | Google Maps, Uber |

### 선택 기준

- **Geohash**: DB 친화적, 캐싱/샤딩이 쉽고, 대부분의 위치 기반 서비스에 적합
- **Quadtree**: 밀도가 불균등한 데이터에 적응적, 정확한 k-최근접 탐색이 필요할 때
- **S2**: 극지방/날짜변경선 근처에서도 균일한 셀, 대규모 글로벌 서비스

---

## 경계 문제와 해결

Geohash 의 가장 큰 약점은 **경계 문제(boundary issue)** 이다.

```
  ┌──────────┬──────────┐
  │          │          │
  │  9q8yyk  │  9q8yys  │
  │          │          │
  │     A •──┼──• B     │  A와 B는 매우 가까우나 다른 셀에 속함
  │          │          │
  └──────────┴──────────┘
```

같은 셀 접두사를 공유하지 않는 두 점이 실제로는 매우 가까울 수 있다.

### 해결 방법

1. **이웃 셀 탐색**: 현재 셀 + 8 개 이웃 셀의 비즈니스를 모두 후보로 수집
2. **Haversine 필터링**: 후보에 대해 실제 거리를 계산하여 반경 내만 반환
3. **적응형 정밀도**: 검색 반경에 맞는 정밀도를 자동 선택

```python
# 검색 반경에 따른 자동 정밀도 선택
def precision_for_radius_km(radius_km: float) -> int:
    """검색 반경을 커버하는 최소 geohash 정밀도 반환"""
    for prec in range(9, 0, -1):
        w, h = PRECISION_TABLE[prec]
        if w >= radius_km and h >= radius_km:
            return prec
    return 1
```

---

## 핵심 구현

### Proximity Service (Geohash 검색)

```python
def _search_geohash(self, lat, lng, radius_km, limit):
    """Geohash 인덱스를 사용한 주변 검색"""
    # 1. 반경에 맞는 정밀도 결정
    precision = precision_for_radius_km(radius_km)

    # 2. 쿼리 지점의 셀 + 8개 이웃 셀 수집
    query_gh = geohash_encode(lat, lng, precision)
    cells = [query_gh] + geohash_neighbors(query_gh)

    # 3. 해당 셀들의 모든 비즈니스를 후보로 수집
    candidates = []
    for cell in cells:
        candidates.extend(self._geohash_index.get(cell, []))

    # 4. Haversine 거리로 필터링 + 정렬
    results = []
    for biz in candidates:
        dist = haversine_km(lat, lng, biz.lat, biz.lng)
        if dist <= radius_km:
            results.append(SearchResult(business=biz, distance_km=dist))

    results.sort(key=lambda r: r.distance_km)
    return results[:limit]
```

### Proximity Service (Quadtree 검색)

```python
def _search_quadtree(self, lat, lng, radius_km, limit):
    """Quadtree 인덱스를 사용한 주변 검색"""
    # 1. 반경을 바운딩 박스로 변환
    box = _radius_to_box(lat, lng, radius_km)

    # 2. 바운딩 박스 범위 질의
    candidates = self._quadtree.query_range(box)

    # 3. 실제 거리로 필터링 + 정렬
    results = []
    for biz in candidates:
        dist = haversine_km(lat, lng, biz.lat, biz.lng)
        if dist <= radius_km:
            results.append(SearchResult(business=biz, distance_km=dist))

    results.sort(key=lambda r: r.distance_km)
    return results[:limit]
```

---

## 실행 방법

### 사전 조건

- Python 3.11 이상

### 데모 실행

```bash
cd 17-design-proximity-service
python scripts/demo.py
```

### 테스트 실행

```bash
cd 17-design-proximity-service
pip install -r requirements.txt
pytest tests/ -v
```

---

## 샘플 출력

```
Proximity Service Demo
======================

======================================================================
  1. Geohash Encoding & Decoding
======================================================================

  Precision table (approximate cell dimensions):
  Precision  Width (km)  Height (km)
  ---------  ----------  -----------
          1    5000.000     5000.000
          4      39.100       19.500
          6       1.200        0.610
          8       0.038        0.019

  San Francisco   prec=6 -> 9q8yyk        error=0.394 km
  Tokyo           prec=6 -> xn76cy        error=0.237 km
  London          prec=6 -> gcpvj0        error=0.131 km
  Sydney          prec=6 -> r3gx2f        error=0.122 km

======================================================================
  2. Geohash Neighbors
======================================================================

  User location: (37.7749, -122.4194)
  Geohash (precision 6): 9q8yyk

  Neighbors:
    N : 9q8yym    NE: 9q8yyt    E : 9q8yys
    SE: 9q8yye    S : 9q8yy7    SW: 9q8yy5
    W : 9q8yyh    NW: 9q8yyj

======================================================================
  3. Quadtree Build & Query
======================================================================

  Businesses inserted : 15
  Total nodes         : 69
  Max depth           : 16

  5 nearest to user (37.7749, -122.4194):
    1. Zuni Cafe                  0.202 km
    2. Sightglass Coffee          1.073 km
    3. Philz Coffee               1.199 km
    4. Blue Bottle Coffee         1.297 km
    5. Swan Oyster Depot          1.306 km

======================================================================
  4. Proximity Service - Geohash Backend
======================================================================

  Within 1.0 km (1 results):
    Zuni Cafe                  0.202 km

  Within 2.0 km (6 results):
    Zuni Cafe                  0.202 km
    Sightglass Coffee          1.073 km
    Philz Coffee               1.199 km
    Blue Bottle Coffee         1.297 km
    Swan Oyster Depot          1.306 km
    ... and 1 more

======================================================================
  6. Approach Comparison
======================================================================

  Geohash : 0.041s (243,902 QPS)
  Quadtree: 0.068s (147,059 QPS)

  +------------------+------------------+------------------+
  |   Approach       |   Pros           |   Cons           |
  +------------------+------------------+------------------+
  |   Geohash        | Simple, DB-      | Boundary issues, |
  |                  | friendly, easy   | fixed precision  |
  +------------------+------------------+------------------+
  |   Quadtree       | Adaptive density,| In-memory only,  |
  |                  | exact k-nearest  | harder to shard  |
  +------------------+------------------+------------------+
```

> 참고: 실제 출력 값은 실행 환경에 따라 약간 다를 수 있다.

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide", Chapter 17
