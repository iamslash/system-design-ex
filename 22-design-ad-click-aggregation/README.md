# Design Ad Click Event Aggregation

광고 클릭 이벤트를 실시간으로 집계하여 광고주에게 클릭 수, 상위 광고, 시계열
분석 데이터를 제공하는 시스템이다. MapReduce 스타일 파이프라인으로 대규모
클릭 스트림을 처리하며, tumbling/sliding window 기반 시간 집계를 지원한다.

## 아키텍처 (MapReduce DAG)

```
                         Raw Click Events
                               │
                               ▼
                    ┌─────────────────────┐
                    │     Filter Stage     │
                    │  (country/ip/user)   │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │     Map Stage        │
                    │  Partition by ad_id  │
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                 ▼
       ┌────────────┐  ┌────────────┐   ┌────────────┐
       │ ad_0001    │  │ ad_0002    │   │ ad_NNNN    │
       │ events     │  │ events     │   │ events     │
       └─────┬──────┘  └─────┬──────┘   └─────┬──────┘
             │               │                 │
             ▼               ▼                 ▼
       ┌────────────┐  ┌────────────┐   ┌────────────┐
       │ Aggregate  │  │ Aggregate  │   │ Aggregate  │
       │ count/min  │  │ count/min  │   │ count/min  │
       └─────┬──────┘  └─────┬──────┘   └─────┬──────┘
             │               │                 │
             └────────────────┼────────────────┘
                              │
                              ▼
                   ┌─────────────────────┐
                   │    Reduce Stage      │
                   │    Top-N by count    │
                   └──────────┬──────────┘
                              │
                              ▼
                   ┌─────────────────────┐
                   │  AggregatedStore    │
                   │  (Time-Series)      │
                   └─────────────────────┘
```

### 5대 컴포넌트

| 컴포넌트 | 역할 | 파일 |
|----------|------|------|
| **Event** | 광고 클릭 이벤트 모델 + 생성기 | `src/event.py` |
| **Aggregator** | MapReduce 파이프라인 (Map→Aggregate→Reduce) | `src/aggregator.py` |
| **Window** | Tumbling/Sliding 윈도우 집계 | `src/window.py` |
| **Storage** | 인메모리 시계열 저장소 + 쿼리 | `src/storage.py` |
| **Demo** | 100K 이벤트 생성 및 파이프라인 실행 | `scripts/demo.py` |

## Back-of-the-Envelope Estimation

| 항목 | 수치 |
|------|------|
| 일일 클릭 수 | 1B (10억) |
| 평균 QPS | 1B / 86,400 ≈ **11,574 QPS** |
| 피크 QPS (5x) | ≈ **50,000 QPS** |
| 이벤트 크기 | ≈ 200 bytes |
| 일일 원시 데이터 | 1B × 200B ≈ **200 GB/day** |
| 분당 집계 레코드 | 1B / 1,440 min ≈ 694K events/min |
| 광고 수 (cardinality) | ~100만 ad_id |
| 분당 집계 결과 | ~100만 (ad_id, minute) 레코드 |
| 집계 후 일일 저장량 | 100만 × 1,440 × 50B ≈ **72 GB/day** |

### 스토리지 추정

- 원시 이벤트 7일 보관: 200 GB × 7 = **1.4 TB**
- 집계 데이터 1년 보관: 72 GB × 365 = **26 TB**
- 집계 데이터는 원시 데이터 대비 **~1/3 크기** (중복 제거 + 압축 시 더 절감)

## Streaming vs Batching

| 항목 | Streaming | Batching |
|------|-----------|----------|
| 지연 시간 | 초 단위 (실시간) | 분~시간 단위 |
| 정확성 | Approximate (eventually exact) | Exact |
| 복잡도 | 높음 (상태 관리, 워터마크) | 낮음 |
| 처리량 | 이벤트 단위 | 대량 일괄 |
| 적합 사례 | 실시간 대시보드, 알림 | 일일 리포트, 정산 |

본 구현은 **마이크로 배치** 방식으로 streaming 의미론을 시뮬레이션한다.
이벤트를 분 단위 윈도우로 집계하여 실시간에 준하는 결과를 제공한다.

## Tumbling vs Sliding Window

### Tumbling Window (고정 윈도우)

겹침 없는 고정 크기 윈도우. 각 이벤트는 정확히 하나의 윈도우에 속한다.

```
Time:  |----1min----|----1min----|----1min----|
       [  Window 1  ][  Window 2 ][  Window 3 ]
```

```python
# src/window.py - tumbling_window 핵심 로직
def tumbling_window(
    events: Sequence[AdClickEvent],
    window_seconds: float = 60.0,
) -> dict[str, list[WindowBucket]]:
    buckets: dict[tuple[str, float], int] = defaultdict(int)
    for event in events:
        # 타임스탬프를 윈도우 경계로 내림
        w_start = event.timestamp - (event.timestamp % window_seconds)
        buckets[(event.ad_id, w_start)] += 1
    # ... WindowBucket 생성 및 정렬
```

### Sliding Window (슬라이딩 윈도우)

고정 크기 윈도우가 step 간격으로 이동. 이벤트가 여러 윈도우에 중복 포함된다.

```
Time:  |----------5min----------|
       |----step----|
       [    Window 1             ]
            [    Window 2             ]
                 [    Window 3             ]
```

```python
# src/window.py - sliding_window 핵심 로직
def sliding_window(
    events: Sequence[AdClickEvent],
    window_seconds: float = 300.0,
    step_seconds: float = 60.0,
) -> dict[str, list[WindowBucket]]:
    # 가장 이른 이벤트를 포함할 수 있는 첫 번째 윈도우 시작점 계산
    first_start = t_min - (t_min % step_seconds) - (window_seconds - step_seconds)
    # step 간격으로 윈도우 생성
    while ws <= t_max:
        window_starts.append(ws)
        ws += step_seconds
    # 각 이벤트를 포함하는 모든 윈도우에 카운트 추가
    for event in events:
        for ws in window_starts:
            if ws <= event.timestamp < ws + window_seconds:
                buckets[(event.ad_id, ws)] += 1
```

## Delivery Semantics (전달 보장)

| 방식 | 설명 | 적합 사례 |
|------|------|-----------|
| **At-most-once** | 유실 가능, 중복 없음 | 로그 분석 (정확성 덜 중요) |
| **At-least-once** | 유실 없음, 중복 가능 | 광고 클릭 (중복은 후처리로 제거) |
| **Exactly-once** | 유실/중복 없음 | 정산/과금 (비용 높음) |

본 구현은 **at-least-once** 를 기본으로 한다. 늦은 이벤트(late event)도
저장소에 반영하되, 워터마크를 통해 늦은 이벤트를 식별한다.

## Watermark for Late Events

워터마크는 "이 시점 이전의 모든 이벤트가 도착했다"는 시스템의 추정치다.
워터마크 이전 타임스탬프를 가진 이벤트는 **늦은 이벤트(late event)** 로 분류된다.

```python
# src/storage.py - 워터마크 기반 늦은 이벤트 감지
class AggregatedStore:
    def ingest(self, events, filters=None) -> int:
        late_count = 0
        partitions = map_partition(events, filters=filters)
        aggregated = aggregate_counts(partitions)
        for ad_id, buckets in aggregated.items():
            for minute_ts, mc in buckets.items():
                if minute_ts < self._watermark:
                    late_count += mc.count  # 늦은 이벤트 카운트
                self._upsert(ad_id, minute_ts, mc.count)  # 그래도 저장
        return late_count
```

전략:
- **Drop**: 늦은 이벤트 무시 (가장 단순)
- **Store & Flag**: 저장하되 플래그 표시 (본 구현)
- **Reprocess Window**: 해당 윈도우 재계산 (가장 정확, 비용 높음)

## Star Schema Filtering

필터링은 Map 단계에서 적용되어 다운스트림 처리량을 줄인다.

```
               ┌──────────────┐
               │  fact_clicks  │
               │  ─────────── │
               │  ad_id (FK)   │
               │  timestamp    │
               │  user_id (FK) │
               │  ip           │
               │  country (FK) │
               └──────┬───────┘
          ┌───────────┼───────────┐
          ▼           ▼           ▼
    ┌──────────┐ ┌──────────┐ ┌──────────┐
    │ dim_ad   │ │ dim_user │ │dim_country│
    └──────────┘ └──────────┘ └──────────┘
```

```python
# src/aggregator.py - 필터 조합
def filter_by_country(country: str) -> FilterFn:
    def _pred(e: AdClickEvent) -> bool:
        return e.country == country
    return _pred

def compose_filters(*filters: FilterFn) -> FilterFn:
    """여러 필터를 AND 조합"""
    def _pred(e: AdClickEvent) -> bool:
        return all(f(e) for f in filters)
    return _pred

# 사용: country='US' AND ip='10.0.0.1' 필터링
parts = map_partition(events, filters=[
    filter_by_country("US"),
    filter_by_ip("10.0.0.1"),
])
```

## MapReduce Pipeline 핵심 코드

### 1. Event 모델

```python
# src/event.py
@dataclass(frozen=True, slots=True)
class AdClickEvent:
    ad_id: str
    timestamp: float      # Unix epoch
    user_id: str
    ip: str
    country: str

    def minute_key(self) -> float:
        """타임스탬프를 분 경계로 내림"""
        return self.timestamp - (self.timestamp % 60)
```

### 2. Map: ad_id 로 파티셔닝

```python
# src/aggregator.py
def map_partition(
    events: Sequence[AdClickEvent],
    filters: Sequence[FilterFn] | None = None,
) -> dict[str, list[AdClickEvent]]:
    partitions: dict[str, list[AdClickEvent]] = defaultdict(list)
    for event in events:
        if combined and not combined(event):
            continue
        partitions[event.ad_id].append(event)
    return dict(partitions)
```

### 3. Aggregate: (ad_id, minute) 별 카운트

```python
# src/aggregator.py
def aggregate_counts(
    partitions: dict[str, list[AdClickEvent]],
) -> dict[str, dict[float, MinuteCount]]:
    result = {}
    for ad_id, events in partitions.items():
        buckets: dict[float, MinuteCount] = {}
        for event in events:
            mk = event.minute_key()
            if mk not in buckets:
                buckets[mk] = MinuteCount(ad_id=ad_id, minute_ts=mk)
            buckets[mk].count += 1
        result[ad_id] = buckets
    return result
```

### 4. Reduce: Top-N 추출

```python
# src/aggregator.py
def reduce_top_n(
    aggregated: dict[str, dict[float, MinuteCount]],
    n: int = 10,
) -> list[AdTotal]:
    totals = []
    for ad_id, buckets in aggregated.items():
        total = sum(mc.count for mc in buckets.values())
        totals.append(AdTotal(ad_id=ad_id, total_clicks=total))
    totals.sort(key=lambda t: t.total_clicks, reverse=True)
    return totals[:n]
```

### 5. 전체 파이프라인

```python
# src/aggregator.py
def run_pipeline(events, *, top_n=10, filters=None) -> list[AdTotal]:
    partitions = map_partition(events, filters=filters)
    aggregated = aggregate_counts(partitions)
    return reduce_top_n(aggregated, n=top_n)
```

## 실행

```bash
cd 22-design-ad-click-aggregation

# 의존성 설치
pip install -r requirements.txt

# 데모 실행 (100K 이벤트 생성 + 집계)
python scripts/demo.py

# 테스트
pytest tests/ -v
```

## 디렉토리 구조

```
22-design-ad-click-aggregation/
├── README.md
├── requirements.txt        # pytest only
├── src/
│   ├── __init__.py
│   ├── aggregator.py       # MapReduce 파이프라인 (Map→Aggregate→Reduce)
│   ├── event.py            # AdClickEvent 모델 + 생성기
│   ├── window.py           # Tumbling/Sliding 윈도우
│   └── storage.py          # 인메모리 시계열 저장소
├── scripts/
│   └── demo.py             # 데모 스크립트
└── tests/
    ├── __init__.py
    └── test_aggregation.py  # 36개 테스트
```
