# Back-of-the-envelope Estimation

시스템 설계 인터뷰에서 시스템 용량이나 성능 요구사항을 추정하는 기법.

## Power of Two

| Power | 근사값 | 이름 | 약칭 |
|-------|--------|------|------|
| 10 | 1 Thousand | 1 Kilobyte | 1 KB |
| 20 | 1 Million | 1 Megabyte | 1 MB |
| 30 | 1 Billion | 1 Gigabyte | 1 GB |
| 40 | 1 Trillion | 1 Terabyte | 1 TB |
| 50 | 1 Quadrillion | 1 Petabyte | 1 PB |

## Latency Numbers Every Programmer Should Know

| 연산 | 시간 |
|------|------|
| L1 캐시 참조 | 0.5 ns |
| Branch mispredict | 5 ns |
| L2 캐시 참조 | 7 ns |
| Mutex lock/unlock | 100 ns |
| 메인 메모리 참조 | 100 ns |
| Zippy로 1KB 압축 | 10 us |
| 1Gbps 네트워크로 2KB 전송 | 20 us |
| 메모리에서 1MB 순차 읽기 | 250 us |
| 같은 데이터센터 내 왕복 | 500 us |
| 디스크 탐색 | 10 ms |
| 네트워크에서 1MB 순차 읽기 | 10 ms |
| 디스크에서 1MB 순차 읽기 | 30 ms |
| CA → Netherlands → CA 패킷 전송 | 150 ms |

### 핵심 교훈

- **메모리는 빠르고 디스크는 느리다**
- 가능하면 디스크 탐색을 피하라
- 단순 압축 알고리즘은 빠르다
- 인터넷 전송 전 데이터를 압축하라
- 데이터센터 간 전송은 시간이 걸린다

## Availability Numbers

| 가용성 % | 일간 다운타임 | 주간 다운타임 | 월간 다운타임 | 연간 다운타임 |
|----------|-------------|-------------|-------------|-------------|
| 99% | 14.40분 | 1.68시간 | 7.31시간 | 3.65일 |
| 99.99% | 8.64초 | 1.01분 | 4.38분 | 52.60분 |
| 99.999% | 864ms | 6.05초 | 26.30초 | 5.26분 |
| 99.9999% | 86.4ms | 604.8ms | 2.63초 | 31.56초 |

## 추정 계산 코드

```python
# Back-of-the-envelope 계산을 Python으로 빠르게 검증

# === QPS 추정 ===
monthly_active_users = 300_000_000   # 3억 MAU
daily_active_ratio = 0.5              # 50% 일일 활성
tweets_per_user = 2                   # 일인당 일일 트윗

dau = monthly_active_users * daily_active_ratio          # 1.5억 DAU
qps = dau * tweets_per_user / 86400                      # ~3,472 QPS
peak_qps = qps * 2                                       # ~6,944 Peak QPS
print(f"DAU: {dau:,.0f}")
print(f"QPS: {qps:,.0f}, Peak: {peak_qps:,.0f}")

# === 저장소 추정 ===
media_ratio = 0.1                     # 10% 미디어 포함
media_size_mb = 1                     # 미디어 평균 1MB
retention_years = 5

daily_media_tb = dau * tweets_per_user * media_ratio * media_size_mb / 1e6
yearly_media_pb = daily_media_tb * 365 / 1000
total_media_pb = yearly_media_pb * retention_years
print(f"Daily media: {daily_media_tb:.0f} TB")
print(f"5-year media: {total_media_pb:.0f} PB")

# === 서버 수 추정 ===
# 단일 서버가 초당 처리 가능한 요청 수 (가정)
requests_per_server = 500
servers_needed = peak_qps / requests_per_server
print(f"Servers needed: {servers_needed:.0f}")
```

출력:
```
DAU: 150,000,000
QPS: 3,472, Peak: 6,944
Daily media: 30 TB
5-year media: 55 PB
Servers needed: 14
```

## 예제: Twitter QPS 및 저장소 추정

### 가정
- 월간 활성 사용자: 3억
- 일일 사용 비율: 50%
- 일인당 트윗 수: 2
- 미디어 포함 비율: 10%
- 데이터 보관 기간: 5년

### 추정
- **DAU**: 3억 x 50% = 1.5억
- **QPS**: 1.5억 x 2 / 86400 = ~3,500
- **Peak QPS**: ~7,000
- **일일 미디어 저장**: 1.5억 x 2 x 10% x 1MB = 30 TB/일
- **5년 미디어 저장**: 30TB x 365 x 5 = ~55 PB

## Tips

1. **반올림과 근사**: 복잡한 계산 대신 100,000 / 10 같은 간단한 수로
2. **가정 기록**: 나중에 참조할 수 있도록 가정을 적어둘 것
3. **단위 표기**: 5 KB인지 5 MB인지 명확히
4. **자주 묻는 추정**: QPS, Peak QPS, 저장소, 캐시, 서버 수
