# Design Metrics Monitoring & Alerting System

Metrics Monitoring System 은 서비스와 인프라에서 발생하는 메트릭 데이터를 수집, 저장,
쿼리, 시각화하고 이상 상태를 감지하여 알림을 보내는 시스템이다. 대규모 분산 환경에서
수십억 개의 시계열 데이터 포인트를 효율적으로 처리하는 것이 핵심 과제다.

## 아키텍처

```
                    ┌──────────────────────────────────────────────────────┐
                    │                   Client (CLI)                       │
                    └──────────────────┬───────────────────────────────────┘
                                       │ HTTP (Push Metrics / Query / Alerts)
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                          FastAPI Application (:8021)                         │
│                                                                              │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐  ┌───────────┐  │
│  │   Collector     │  │  Query Service │  │  Alert Engine  │  │ Notifier  │  │
│  │  (Push API)     │  │  (Aggregation  │  │  (Rule-based   │  │ (Email,   │  │
│  │                 │  │   Downsample)  │  │   Evaluation)  │  │  Webhook) │  │
│  └───────┬─────── ┘  └───────┬────────┘  └───────┬────────┘  └─────┬─────┘  │
│          │                   │                   │                  │        │
│          └───────────────────┼───────────────────┘                  │        │
│                              │                                      │        │
└──────────────────────────────┼──────────────────────────────────────┼────────┘
                               │ ZADD / ZRANGEBYSCORE                 │
                               ▼                                      │
                    ┌─────────────────────────┐                       │
                    │    Redis 7 (Alpine)      │ ◀─────────────────────┘
                    │                          │   HSET / RPUSH (alerts,
                    │  - Sorted Sets (TS)      │    notifications, rules)
                    │  - Hashes (Rules/Alerts) │
                    │  - Lists (Notifications) │
                    └─────────────────────────┘
```

### 5대 컴포넌트

| 컴포넌트 | 역할 | 파일 |
|----------|------|------|
| **Collector** | Push 방식으로 메트릭 데이터 수신 | `api/collector/metrics.py` |
| **Time-Series Storage** | Redis Sorted Set 기반 시계열 저장 | `api/storage/timeseries.py` |
| **Query Service** | 범위 쿼리, 집계(avg/max/min/sum/count), 다운샘플링 | `api/query/service.py` |
| **Alert Engine** | 규칙 기반 임계값 비교, 주기적 평가 | `api/alerting/rules.py` |
| **Notifier** | Email, Webhook, Slack 등 알림 전송 (시뮬레이션) | `api/alerting/notifier.py` |

## 데이터 모델 (Time Series)

시계열 데이터는 **metric name + labels** 로 식별되며, 각 데이터 포인트는
`(timestamp, value)` 쌍이다.

```
metric_name: "cpu.load"
labels:      {"host": "server1", "region": "us-east"}
data_points: [(t1, 0.72), (t2, 0.85), (t3, 0.91), ...]
```

### Redis 저장 구조

Redis Sorted Set 을 사용하여 score = timestamp, member = JSON 으로 저장한다.

```
Key:    ts:{metric_name}:{label_hash}
Score:  unix timestamp (float)
Member: {"value": 0.75, "timestamp": 1704067200.0, "labels": {"host": "s1"}}
```

**핵심 구현** -- 라벨 해싱과 키 생성:

```python
def label_hash(labels: dict[str, str]) -> str:
    """Deterministic hash of label key-value pairs."""
    if not labels:
        return "_"
    canonical = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
    return hashlib.md5(canonical.encode()).hexdigest()[:12]

def ts_key(metric_name: str, labels: dict[str, str]) -> str:
    return f"ts:{metric_name}:{label_hash(labels)}"
```

**데이터 포인트 저장**:

```python
async def add(self, metric_name, labels, value, timestamp=None):
    ts = timestamp if timestamp is not None else time.time()
    key = ts_key(metric_name, labels)
    member = json.dumps({"value": value, "timestamp": ts, "labels": labels})
    await self._redis.zadd(key, {member: ts})
```

## Pull vs Push 비교

| 구분 | Pull 방식 | Push 방식 |
|------|----------|----------|
| **동작** | 모니터링 시스템이 타겟에서 메트릭을 가져옴 | 타겟이 메트릭을 모니터링 시스템으로 전송 |
| **대표 시스템** | Prometheus | Datadog, Graphite, InfluxDB |
| **서비스 디스커버리** | 필요 (어디서 가져올지 알아야 함) | 불필요 (타겟이 알아서 전송) |
| **방화벽 친화성** | 모니터링 서버가 타겟에 접근 필요 | 타겟이 아웃바운드만 열면 됨 |
| **데이터 신선도** | 폴링 주기에 의존 | 실시간에 가까움 |
| **장애 감지** | 타겟이 응답 안 하면 감지 가능 | 타겟이 죽으면 데이터가 안 옴 (별도 감시 필요) |
| **네트워크 부하** | 예측 가능 (고정 주기) | 버스트 가능 |
| **본 프로젝트** | - | **Push 방식 채택** |

## Kafka 기반 수집 파이프라인 (확장 설계)

대규모 환경에서는 수집기와 저장소 사이에 Kafka 를 배치하여 버퍼링과 내구성을
확보한다.

```
┌──────────┐     ┌──────────┐     ┌─────────┐     ┌──────────┐
│  Agent   │────▶│ Collector│────▶│  Kafka   │────▶│ Consumer │────▶ TSDB
│ (target) │     │  API     │     │ (buffer) │     │ (writer) │
└──────────┘     └──────────┘     └─────────┘     └──────────┘
                                       │
                                       ├── Topic: metrics.raw
                                       ├── Topic: metrics.aggregated
                                       └── Partition by metric_name
```

- **장점**: 백프레셔 처리, 재처리 가능, 소비자 독립 확장
- **파티셔닝**: `metric_name` 기준으로 파티셔닝하여 같은 메트릭은 같은 소비자가 처리
- **보존 기간**: raw 데이터 7일, aggregated 데이터 90일

## Query Service

범위 쿼리와 집계 함수를 지원한다.

```python
class QueryService:
    async def query(self, req: QueryRequest) -> QueryResult:
        if req.downsample:
            data_points = await self._storage.downsample(
                metric_name=req.name, labels=req.labels,
                bucket_seconds=req.downsample, start=req.start, end=end,
            )
            return QueryResult(name=req.name, labels=req.labels, data_points=data_points)

        raw_points = await self._storage.query_range(
            metric_name=req.name, labels=req.labels, start=req.start, end=end,
        )
        if req.aggregation and raw_points:
            aggregated_value = self._aggregate(raw_points, req.aggregation)

        return QueryResult(
            name=req.name, labels=req.labels, data_points=raw_points,
            aggregation=req.aggregation, aggregated_value=aggregated_value,
        )

    @staticmethod
    def _aggregate(points, agg):
        values = [p["value"] for p in points]
        if agg == "avg":  return sum(values) / len(values)
        if agg == "max":  return max(values)
        if agg == "min":  return min(values)
        if agg == "sum":  return sum(values)
        if agg == "count": return float(len(values))
```

### 지원 집계 함수

| 함수 | 설명 | 예시 |
|------|------|------|
| `avg` | 평균값 | CPU 평균 사용률 |
| `max` | 최댓값 | 피크 메모리 사용량 |
| `min` | 최솟값 | 최저 응답 시간 |
| `sum` | 합계 | 총 요청 수 |
| `count` | 데이터 포인트 수 | 수집된 샘플 수 |

## Alert Flow

```
┌──────────────────────────────────────────────────────────────┐
│                    Alert Evaluation Loop                      │
│                                                              │
│  1. 등록된 모든 Alert Rule 조회                               │
│  2. 각 Rule 에 대해:                                         │
│     a. evaluation window 내 메트릭 데이터 조회                │
│     b. 평균값 계산                                           │
│     c. operator + threshold 비교                             │
│     d. 조건 충족 시 Alert 생성                                │
│  3. 발생한 Alert 에 대해 Notifier 호출                        │
│  4. ALERT_CHECK_INTERVAL 초 대기 후 반복                     │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

**규칙 평가 핵심 코드**:

```python
async def evaluate_rule(self, rule: AlertRule) -> Optional[Alert]:
    now = time.time()
    start = now - rule.duration
    points = await self._storage.query_range(
        metric_name=rule.metric_name, labels=rule.labels,
        start=start, end=now,
    )
    if not points:
        return None

    values = [p["value"] for p in points]
    avg_value = sum(values) / len(values)

    if _compare(avg_value, rule.operator, rule.threshold):
        return Alert(
            rule_name=rule.name, metric_name=rule.metric_name,
            status=AlertStatus.FIRING, value=avg_value,
            threshold=rule.threshold, severity=rule.severity, ...
        )
    return None
```

**비교 연산자**:

```python
def _compare(value, operator, threshold):
    if operator == "gt":  return value > threshold
    if operator == "gte": return value >= threshold
    if operator == "lt":  return value < threshold
    if operator == "lte": return value <= threshold
    if operator == "eq":  return value == threshold
    if operator == "neq": return value != threshold
```

## Downsampling

오래된 고해상도 데이터를 저해상도로 집계하여 저장 공간을 절약한다.

```
Raw Data (1s resolution):
|·|·|·|·|·|·|·|·|·|·|·|·|·|·|·|·|·|·|·|·|·|·|·|·|
0s                                               60s

Downsampled (1min buckets):
|=========== avg: 0.72 ===========|
0s                               60s

Downsampled (1hour buckets):
|====================== avg: 0.68 =======================|
0min                                                   60min
```

**구현**:

```python
async def downsample(self, metric_name, labels, bucket_seconds, start, end=None):
    points = await self.query_range(metric_name, labels, start, end)
    buckets: dict[float, list[float]] = {}
    for p in points:
        bucket_start = (p["timestamp"] // bucket_seconds) * bucket_seconds
        buckets.setdefault(bucket_start, []).append(p["value"])

    return [{
        "bucket_start": bs,
        "bucket_end": bs + bucket_seconds,
        "avg": sum(vals) / len(vals),
        "max": max(vals), "min": min(vals),
        "sum": sum(vals), "count": len(vals),
    } for bs, vals in sorted(buckets.items())]
```

### 보존 정책 (예시)

| 해상도 | 보존 기간 | 용도 |
|--------|----------|------|
| 1초 (raw) | 7일 | 실시간 디버깅 |
| 1분 | 30일 | 일일 트렌드 분석 |
| 1시간 | 1년 | 장기 용량 계획 |

## 시각화 (확장 설계)

실제 프로덕션에서는 Grafana 등의 시각화 도구와 연동한다.

```
┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│   Grafana    │────────▶│  Query API   │────────▶│  Time-Series │
│  Dashboard   │  HTTP   │  /api/v1/    │  Redis  │   Storage    │
│              │         │  query       │         │              │
└──────────────┘         └──────────────┘         └──────────────┘

Dashboard 구성:
- Row 1: CPU Load (line chart, 5min avg)
- Row 2: Memory Usage (area chart, max)
- Row 3: Request Rate (bar chart, sum)
- Row 4: Active Alerts (table)
```

## Quick Start

```bash
# 서비스 시작
docker-compose up --build

# 헬스 체크
curl http://localhost:8021/health

# 메트릭 Push
curl -X POST http://localhost:8021/api/v1/metrics \
  -H "Content-Type: application/json" \
  -d '{"name": "cpu.load", "labels": {"host": "server1"}, "value": 0.75}'

# 쿼리 (최근 1시간, 평균)
curl "http://localhost:8021/api/v1/query?name=cpu.load&labels=host=server1&start=$(date -v-1H +%s)&aggregation=avg"

# Alert Rule 생성
curl -X POST http://localhost:8021/api/v1/rules \
  -H "Content-Type: application/json" \
  -d '{"name": "High CPU", "metric_name": "cpu.load", "labels": {"host": "server1"}, "operator": "gt", "threshold": 0.8, "severity": "critical", "notification_channels": ["email", "webhook"]}'

# 알림 평가 수동 트리거
curl -X POST http://localhost:8021/api/v1/alerts/evaluate
```

## CLI 사용법

```bash
# 메트릭 Push
python scripts/cli.py push --metric cpu.load --labels host=server1 --value 0.75

# 쿼리 (최근 1시간, 평균 집계)
python scripts/cli.py query --metric cpu.load --labels host=server1 --start 1h --agg avg

# 활성 알림 조회
python scripts/cli.py alerts

# 알림 규칙 조회
python scripts/cli.py rules

# 헬스 체크
python scripts/cli.py --health
```

## 환경 변수

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `REDIS_HOST` | `redis` | Redis 호스트 |
| `REDIS_PORT` | `6379` | Redis 포트 |
| `API_PORT` | `8021` | API 서버 포트 |
| `ALERT_CHECK_INTERVAL` | `10` | 알림 평가 주기 (초) |
| `ALERT_EVALUATION_WINDOW` | `60` | 알림 평가 윈도우 (초) |

## 확장 고려사항

### 수평 확장

- **수집 계층**: 여러 Collector 인스턴스 + 로드 밸런서
- **저장 계층**: Redis Cluster 또는 전용 TSDB (InfluxDB, TimescaleDB, VictoriaMetrics)
- **쿼리 계층**: 읽기 전용 레플리카 + 캐싱

### 고가용성

- Redis Sentinel 또는 Redis Cluster 로 자동 failover
- 알림 엔진 이중화 (리더 선출 패턴)
- 수집 데이터 Kafka 버퍼링으로 유실 방지

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide Volume 2", Chapter 5
