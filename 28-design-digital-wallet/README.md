# Design A Digital Wallet

디지털 지갑은 사용자 간 잔액 이체를 처리하는 시스템이다. PayPal 같은 결제 플랫폼에서
지갑 간 직접 이체는 은행 이체보다 빠르고 수수료가 없다. 핵심 요구사항은 다음과 같다:

- **잔액 이체**: 두 지갑 간 금액 이동 (1,000,000 TPS)
- **트랜잭션 보장**: 이체는 원자적이어야 함 (전부 성공 또는 전부 실패)
- **재현성** (Reproducibility): 이벤트를 재생하여 임의 시점의 잔액 복원 가능
- **가용성**: 99.99%

## 아키텍처

```
Command (이체 요청)
    │
    ▼
┌──────────────────────────────────────────────────┐
│                  Write Path                       │
│                                                   │
│  Command ──→ State Machine ──→ Event ──→ State    │
│              (검증+생성)       (불변 기록) (잔액)  │
│                                                   │
│  Event Store (append-only, 이벤트 로그)           │
│  Ledger (복식부기)                                │
└──────────────────────────┬───────────────────────┘
                           │ Publish Events
                           ▼
┌──────────────────────────────────────────────────┐
│                  Read Path (CQRS)                 │
│                                                   │
│  Events ──→ Read Model ──→ WalletView            │
│             (프로젝션)      TransferView          │
│                                                   │
│  Client Query ──→ Query Service ──→ Read Model   │
└──────────────────────────────────────────────────┘
```

## Event Sourcing

상태(잔액)를 직접 저장하는 대신, 모든 변경을 **불변 이벤트**로 기록한다.
현재 상태는 이벤트를 처음부터 재생하여 재구성할 수 있다.

```
Command ──→ [검증] ──→ Event ──→ [적용] ──→ State
   │                     │                    │
 "A→C $1"          "A에서 $1 차감"        A=$4, C=$4
 (의도)            (확정된 사실)           (현재 잔액)
```

### 핵심 구현: State Machine

```python
# 커맨드를 검증하고 이벤트를 생성하는 상태 머신
class WalletStateMachine:
    def __init__(self):
        self.event_store = EventStore()  # 불변 이벤트 로그
        self.ledger = Ledger()           # 복식부기
        self.wallets: dict[str, Wallet] = {}

    def handle(self, command: Command) -> CommandResult:
        """커맨드 검증 → 이벤트 생성 → 상태 업데이트"""
        handler = {
            CommandType.CREATE_WALLET: self._handle_create_wallet,
            CommandType.DEPOSIT: self._handle_deposit,
            CommandType.WITHDRAW: self._handle_withdraw,
            CommandType.TRANSFER: self._handle_transfer,
        }[command.command_type]
        return handler(command)
```

### 이체 처리 (Transfer)

```python
def _handle_transfer(self, cmd: Command) -> CommandResult:
    from_id = cmd.data["from_wallet_id"]
    to_id = cmd.data["to_wallet_id"]
    amount = cmd.data["amount"]

    # 검증: 지갑 존재 여부, 잔액 충분 여부
    source = self.wallets[from_id]
    if not source.has_sufficient_funds(amount):
        return CommandResult(success=False, error="Insufficient funds")

    # 이벤트 생성: TRANSFER_INITIATED → TRANSFER_COMPLETED
    initiated = self.event_store.append(
        EventType.TRANSFER_INITIATED, aggregate_id=transaction_id,
        data={"from_wallet_id": from_id, "to_wallet_id": to_id, "amount": amount},
    )
    completed = self.event_store.append(
        EventType.TRANSFER_COMPLETED, aggregate_id=transaction_id,
        data={"from_wallet_id": from_id, "to_wallet_id": to_id, "amount": amount},
    )

    # 이벤트 적용: 잔액 변경 + 원장 기록
    for event in [initiated, completed]:
        self._apply_event(event)
    return CommandResult(success=True, events=[initiated, completed])
```

### 이벤트 적용 (State Update)

```python
def _apply_event(self, event: Event) -> None:
    """이벤트를 상태에 적용 — 결정론적, 부작용 없음"""
    match event.event_type:
        case EventType.TRANSFER_COMPLETED:
            from_id = event.data["from_wallet_id"]
            to_id = event.data["to_wallet_id"]
            amount = event.data["amount"]
            self.wallets[from_id].withdraw(amount)  # 출금
            self.wallets[to_id].deposit(amount)      # 입금
            # 복식부기: DEBIT(출금) + CREDIT(입금) = 0
            self.ledger.record_transfer(
                transaction_id=event.data["transaction_id"],
                from_account=from_id, to_account=to_id,
                amount=amount,
            )
```

### 재현성 (Reproducibility)

```python
# 이벤트 리스트를 재생하여 과거 상태 복원
@classmethod
def from_events(cls, events: list[Event]) -> WalletStateMachine:
    sm = cls()
    for event in events:
        sm._apply_event(event)  # 동일한 이벤트 → 동일한 상태 (결정론적)
    return sm

# 스냅샷 + 이후 이벤트로 빠른 복원
@classmethod
def from_snapshot(cls, snapshot: Snapshot, events_after: list[Event]):
    sm = cls()
    for wallet_id, balance in snapshot.balances.items():
        sm.wallets[wallet_id] = Wallet(wallet_id=wallet_id, balance=balance, ...)
    for event in events_after:
        sm._apply_event(event)
    return sm
```

## CQRS (Command Query Responsibility Segregation)

쓰기 경로와 읽기 경로를 분리한다. 쓰기는 State Machine 을 통해, 읽기는
이벤트로 구축한 Read Model (프로젝션) 을 통해 처리한다.

```python
class CQRSWalletApp:
    def __init__(self):
        self.state_machine = WalletStateMachine()  # 쓰기 경로
        self.read_model = ReadModel()               # 읽기 경로

    def execute_command(self, command: Command) -> CommandResult:
        """쓰기: 커맨드 처리 후 읽기 모델 업데이트"""
        result = self.state_machine.handle(command)
        if result.success:
            for event in result.events:
                self.read_model.project_event(event)  # 프로젝션 갱신
        return result

    def query_balance(self, wallet_id: str) -> int | None:
        """읽기: Read Model 에서 잔액 조회"""
        return self.read_model.get_balance(wallet_id)
```

## 분산 트랜잭션

단일 노드로 1M TPS 를 처리할 수 없으므로 데이터를 파티셔닝한다.
두 지갑이 다른 파티션에 있을 때 분산 트랜잭션이 필요하다.

| 방식 | 특징 |
|------|------|
| **2PC** | DB 레벨, 락 오래 유지, 코디네이터 단일 장애점 |
| **TC/C** | 애플리케이션 레벨, 병렬 실행 가능, 보상 트랜잭션 필요 |
| **Saga** | 선형 순서 실행, 마이크로서비스 표준, 롤백은 보상으로 |

TC/C 에서는 Try 단계에서 출금만 먼저 실행하고, Confirm 단계에서 입금을
실행한다. 실패 시 Cancel 단계에서 출금을 되돌린다.

## 데모

```bash
python scripts/demo.py
```

## 테스트

```bash
pip install pytest
pytest tests/ -v
```

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide Vol. 2", Chapter 28
