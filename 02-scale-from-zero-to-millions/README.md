# Scale From Zero To Millions Of Users

수백만 사용자를 지원하는 시스템 설계. 단일 서버에서 시작하여 점진적으로 확장하는 과정을 다룬다.

## 아키텍처 진화

```
Single Server
    ↓
Web Server + Database (분리)
    ↓
Load Balancer + Multiple Web Servers
    ↓
Database Replication (Master-Slave)
    ↓
Cache Layer (Memcached/Redis)
    ↓
CDN (정적 콘텐츠)
    ↓
Stateless Web Tier (세션 → 외부 저장소)
    ↓
Multiple Data Centers (GeoDNS)
    ↓
Message Queue (비동기 처리)
    ↓
Database Sharding (수평 확장)
    ↓
Logging, Metrics, Automation
```

## 핵심 개념

### 1. Single Server Setup
- 모든 것이 하나의 서버에서 동작: 웹 앱, 데이터베이스, 캐시
- DNS → IP 주소 → HTTP 요청 → HTML/JSON 응답

### 2. Database 분리
- **관계형 DB (RDBMS)**: MySQL, PostgreSQL - JOIN 지원
- **비관계형 DB (NoSQL)**: Cassandra, DynamoDB - 초저지연, 비정형 데이터, 대용량

### 3. Vertical vs Horizontal Scaling
- **Vertical (Scale Up)**: CPU/RAM 증가 - 간단하지만 한계 존재, SPOF
- **Horizontal (Scale Out)**: 서버 추가 - 대규모 앱에 적합

### 4. Load Balancer
- 웹 서버 간 트래픽 균등 분배
- Public IP로 접근, Private IP로 서버 통신
- 서버 장애 시 자동 failover

### 5. Database Replication (Master-Slave)
- **Master**: 쓰기 전용
- **Slave**: 읽기 전용 (복제본)
- 장점: 성능 향상, 신뢰성, 고가용성

### 6. Cache
- 자주 접근하는 데이터를 메모리에 임시 저장
- **Read-through cache**: 캐시 → DB 순서로 조회
- 고려사항: 만료 정책, 일관성, SPOF 방지, 퇴거 정책 (LRU)

```python
# Read-through cache 패턴 (Memcached 예시)
SECONDS = 1
cache.set('myKey', 'hi there', 3600 * SECONDS)  # 1시간 TTL
cache.get('myKey')                                # → 'hi there'

# 의사코드
def get_user(user_id):
    user = cache.get(f"user:{user_id}")     # ① 캐시 조회
    if user is None:
        user = db.query(f"SELECT * FROM users WHERE id = {user_id}")  # ② DB 조회
        cache.set(f"user:{user_id}", user, ttl=3600)                  # ③ 캐시 저장
    return user
```

### 7. CDN (Content Delivery Network)
- 지리적으로 분산된 서버에서 정적 콘텐츠 제공
- TTL 기반 캐시 만료
- 고려사항: 비용, 적절한 캐시 만료, CDN 장애 대비

### 8. Stateless Web Tier
- 세션 데이터를 외부 저장소 (Redis/NoSQL)로 이동
- 웹 서버 Auto-scaling 가능

```python
# Stateful (나쁜 예): 세션이 서버 메모리에 묶임
# → 같은 사용자 요청이 항상 같은 서버로 가야 함 (sticky session)

# Stateless (좋은 예): 세션을 외부 저장소로 분리
import redis
session_store = redis.Redis(host='session-redis')

def handle_request(request):
    session = session_store.get(f"session:{request.token}")  # 어떤 서버든 접근 가능
    if not session:
        return redirect("/login")
    return process(request, session)
```

### 9. Data Centers
- GeoDNS: 사용자 위치 기반 가장 가까운 데이터센터로 라우팅
- 과제: 트래픽 리다이렉션, 데이터 동기화, 테스트/배포

### 10. Message Queue
- 비동기 통신을 위한 내구성 있는 컴포넌트
- Producer → Queue → Consumer
- 느슨한 결합, 독립적 스케일링

```python
# 사진 처리 예시: 웹 서버가 작업을 큐에 넣고, 워커가 비동기로 처리
# Producer (웹 서버)
queue.publish("photo_processing", {
    "user_id": 123,
    "photo_url": "s3://bucket/photo.jpg",
    "operations": ["crop", "sharpen", "blur"]
})

# Consumer (워커) — 독립적으로 스케일링 가능
def process_photo(message):
    photo = download(message["photo_url"])
    for op in message["operations"]:
        photo = apply_filter(photo, op)
    upload(photo)
```

### 11. Database Sharding
- 대규모 DB를 작은 샤드로 분할
- Sharding key (파티션 키)로 데이터 분배
- 과제: Resharding, Celebrity 문제, JOIN/비정규화

```python
# user_id 기반 샤딩: user_id % 4 → 샤드 0~3
def get_shard(user_id: int, num_shards: int = 4) -> int:
    return user_id % num_shards

# Shard 0: user_id 0, 4, 8, 12, ...
# Shard 1: user_id 1, 5, 9, 13, ...
# Shard 2: user_id 2, 6, 10, 14, ...
# Shard 3: user_id 3, 7, 11, 15, ...

# 문제점: 서버 추가/제거 시 대부분의 데이터 재배치 필요
#   → 해결: Consistent Hashing (06장 참조)
```

## 확장 요약

| 기법 | 목적 |
|------|------|
| Stateless Web Tier | 웹 서버 수평 확장 |
| 계층별 이중화 | 고가용성 |
| 캐시 | 응답 시간 단축 |
| 멀티 데이터센터 | 지리적 가용성 |
| CDN | 정적 콘텐츠 가속 |
| DB Sharding | 데이터 계층 확장 |
| 서비스 분리 | 독립적 확장 |
| 모니터링/자동화 | 운영 효율성 |
