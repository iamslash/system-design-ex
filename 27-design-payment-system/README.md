# Design A Payment System

결제 시스템은 e-commerce 의 핵심 인프라다. 구매자가 주문하면 돈이 판매자에게 전달되기까지
많은 내부/외부 서비스가 관여한다. 핵심 요구사항은 다음과 같다:

- **Pay-in flow**: 구매자의 신용카드에서 e-commerce 계좌로 입금
- **Pay-out flow**: e-commerce 계좌에서 판매자 계좌로 출금
- **복식부기 원장** (Double-entry ledger): 모든 거래의 DEBIT + CREDIT 합 = 0
- **멱등성** (Idempotency): 중복 결제 방지
- **재시도** (Retry): 실패한 결제를 지수 백오프로 재시도
- **정산** (Reconciliation): PSP/은행 데이터와 내부 원장 대조

## 아키텍처

```
                         ┌─────────────────────────┐
                         │       Client (CLI)       │
                         └────────────┬─────────────┘
                                      │  HTTP Request
                                      ▼
                         ┌─────────────────────────┐
                         │   FastAPI Application    │
                         │                          │
                         │  POST /v1/payments       │
                         │  GET  /v1/payments/{id}  │
                         │  POST /v1/payments/retry │
                         │  GET  /v1/ledger/{id}    │
                         │  GET  /v1/wallets/{id}   │
                         └────────────┬─────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                  ▼
            ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
            │   Payment    │ │   Ledger     │ │   Wallet     │
            │   Service    │ │   Service    │ │   Service    │
            │              │ │ (Double-     │ │ (Merchant    │
            │ Idempotency  │ │  entry)      │ │  balance)    │
            │ PSP Executor │ │              │ │              │
            └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
                   │                │                 │
                   └────────────────┼─────────────────┘
                                    ▼
                         ┌─────────────────────────┐
                         │     Redis 7 (Alpine)     │
                         └─────────────────────────┘
```

## Pay-in Flow

결제 서비스의 핵심 흐름은 다음과 같다:

1. 클라이언트가 결제 요청을 보낸다
2. **멱등성 체크**: 같은 idempotency key 가 이미 존재하면 기존 결과 반환
3. **PSP 호출**: Payment Service Provider (Stripe 등) 에 결제 실행
4. **지갑 업데이트**: 판매자 잔액 증가
5. **원장 기록**: DEBIT (구매자) + CREDIT (판매자) 복식부기
6. 결제 상태를 SUCCESS 로 변경

## 핵심 구현

### 결제 생성 (Pay-in Orchestration)

```python
# 결제 서비스가 전체 pay-in 흐름을 오케스트레이션
async def create_payment(self, buyer_id, merchant_id, amount,
                         currency, payment_method, idempotency_key) -> dict:
    payment_id = f"pay_{uuid.uuid4().hex[:16]}"
    payment = {"payment_id": payment_id, "status": "NOT_STARTED", ...}

    # 1. 멱등성 체크 — 같은 키로 중복 요청 방지
    existing = await self._idempotency.check_and_set(idempotency_key, payment)
    if existing is not None:
        return existing  # 이전 결과 반환, 중복 과금 없음

    # 2. PSP 호출
    payment["status"] = "EXECUTING"
    psp_response = await self._executor.execute(amount, currency, payment_method)

    if psp_response.result == PSPResult.FAILED:
        payment["status"] = "FAILED"
        return payment

    # 3. 판매자 지갑 잔액 증가
    await self._wallet.credit(merchant_id, amount, currency)

    # 4. 복식부기 원장 기록
    await self._ledger.record(
        payment_id=payment_id,
        buyer_account=f"buyer:{buyer_id}",
        merchant_account=f"merchant:{merchant_id}",
        amount=amount, currency=currency,
    )

    payment["status"] = "SUCCESS"
    return payment
```

### 멱등성 (Idempotency Key)

```python
# Redis SET NX 로 원자적 멱등성 보장
async def check_and_set(self, idempotency_key: str, payment_data: dict) -> dict | None:
    rkey = f"idempotency:{idempotency_key}"
    existing = await self._redis.get(rkey)
    if existing is not None:
        return json.loads(existing)  # 이미 처리된 결제

    # NX: 키가 없을 때만 설정, TTL 24시간
    was_set = await self._redis.set(rkey, json.dumps(payment_data), ex=86400, nx=True)
    if not was_set:
        # 동시 요청으로 다른 프로세스가 먼저 설정한 경우
        existing = await self._redis.get(rkey)
        return json.loads(existing) if existing else None
    return None  # 새 결제 진행
```

### 복식부기 원장 (Double-entry Ledger)

```python
# 모든 결제에 DEBIT + CREDIT 쌍을 기록
# 핵심 불변식: sum(DEBIT + CREDIT) == 0
async def record(self, payment_id, buyer_account, merchant_account,
                 amount, currency) -> list[dict]:
    debit_entry = {
        "entry_type": "DEBIT",
        "account": buyer_account,
        "amount": -amount,       # 돈이 나감
    }
    credit_entry = {
        "entry_type": "CREDIT",
        "account": merchant_account,
        "amount": amount,        # 돈이 들어옴
    }
    # DEBIT(-5000) + CREDIT(+5000) = 0 — 회계 무결성 보장
    return [debit_entry, credit_entry]
```

### 재시도 (Exponential Backoff)

```python
# 실패한 결제를 지수 백오프로 재시도
async def retry_payment(self, payment_id: str, max_retries: int = 3) -> dict:
    payment = await self.get_payment(payment_id)
    if payment["status"] != "FAILED":
        raise ValueError("only FAILED payments can be retried")

    for attempt in range(max_retries):
        if attempt > 0:
            delay = 0.1 * (2 ** (attempt - 1))  # 0.1s, 0.2s, 0.4s, ...
            await asyncio.sleep(delay)

        psp_response = await self._executor.execute(...)
        if psp_response.result == PSPResult.SUCCESS:
            # 지갑 + 원장 업데이트 후 SUCCESS
            return payment

    payment["status"] = "FAILED"  # 모든 재시도 소진
    return payment
```

## API 엔드포인트

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `POST` | `/v1/payments` | 결제 생성 |
| `GET` | `/v1/payments/{id}` | 결제 조회 |
| `POST` | `/v1/payments/retry` | 실패한 결제 재시도 |
| `GET` | `/v1/ledger/{id}` | 원장 항목 조회 |
| `GET` | `/v1/wallets/{id}` | 판매자 지갑 잔액 조회 |

## Quick Start

```bash
# Docker로 실행
docker-compose up --build

# 헬스 체크
curl http://localhost:8027/health

# 결제 생성
curl -X POST http://localhost:8027/v1/payments \
  -H "Content-Type: application/json" \
  -d '{
    "buyer_id": "alice",
    "merchant_id": "shop",
    "amount": 5000,
    "currency": "USD",
    "payment_method": "CREDIT_CARD",
    "idempotency_key": "pay-001"
  }'

# 원장 조회
curl http://localhost:8027/v1/ledger/{payment_id}

# 판매자 지갑 조회
curl http://localhost:8027/v1/wallets/shop
```

## 데모

```bash
pip install fakeredis redis pytest pytest-asyncio
python scripts/demo.py
```

## 테스트

```bash
pip install fakeredis redis pytest pytest-asyncio
pytest tests/ -v
```

## 결제 상태 전이

```
NOT_STARTED ──→ EXECUTING ──→ SUCCESS
                    │
                    └──→ FAILED ──→ (retry) ──→ EXECUTING ──→ SUCCESS
```

## 정산 (Reconciliation)

매일 밤 PSP/은행이 보내는 정산 파일(settlement file)과 내부 원장을 대조한다:

- **자동 조정 가능**: 원인이 알려진 불일치는 프로그램이 자동 수정
- **수동 조정 필요**: 원인은 알지만 자동화 비용이 큰 경우 재무팀이 수동 처리
- **분류 불가**: 원인 불명의 불일치는 별도 큐에 넣어 조사

## 보안 고려사항

| 위협 | 대응 |
|------|------|
| 도청 | HTTPS |
| 데이터 변조 | 암호화 + 무결성 검증 |
| MITM | SSL + Certificate pinning |
| DDoS | Rate limiting + Firewall |
| 카드 도난 | Tokenization (실제 카드번호 미저장) |
| PCI 준수 | Hosted payment page (PSP 가 카드정보 수집) |

## 환경 변수

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `REDIS_HOST` | `localhost` | Redis 호스트 |
| `REDIS_PORT` | `6379` | Redis 포트 |
| `PSP_FAILURE_RATE` | `0.0` | PSP 실패 확률 (0.0~1.0) |

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide Vol. 2", Chapter 27
