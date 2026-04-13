# Design A Stock Exchange

주식 거래소는 매수자와 매도자를 효율적으로 매칭하는 시스템이다.
NYSE 는 하루 수십억 건, HKEX 는 하루 2000억 주를 처리한다. 핵심 요구사항:

- **주문 처리**: 지정가 주문 (limit order) 접수 및 취소
- **매칭 엔진**: FIFO (가격-시간 우선) 매칭 알고리즘
- **시장 데이터**: L1/L2/L3 호가창 + 캔들스틱 차트
- **결정론적 재생**: 동일한 주문 순서 → 동일한 체결 결과
- **가용성**: 99.99%, 밀리초 수준 지연시간

## 아키텍처

```
Client ──→ Broker ──→ Client Gateway ──→ Order Manager
                                              │
                                        Risk Check
                                        Wallet Check
                                              │
                                              ▼
                                    ┌─── Sequencer ───┐
                                    │   (inbound)      │
                                    └────────┬─────────┘
                                             ▼
                                      Matching Engine
                                       (Order Book)
                                             │
                                    ┌────────┴─────────┐
                                    ▼                   ▼
                            ┌─── Sequencer ───┐   Market Data
                            │  (outbound)      │   Publisher
                            └────────┬─────────┘      │
                                     ▼                 ▼
                              Order Manager      Data Service
                                     │           (L1/L2/Candle)
                                     ▼
                                  Reporter
                                  (DB 저장)
```

## Order Book

주문장은 doubly-linked list + hashmap 으로 구현한다.
모든 핵심 연산이 **O(1)** 에 수행된다.

```
Sell book (asks):
  100.13  [300]
  100.12  [100]
  100.11  [1100]
  100.10  [200][400]  ← best ask (최저 매도가)
  ─────────────────
  100.08  [500]       ← best bid (최고 매수가)
  100.07  [300]
  100.06  [200]
Buy book (bids)
```

### 핵심 구현: Order Book

```python
class OrderBook:
    def __init__(self, symbol: str):
        self._orders: dict[str, OrderNode] = {}       # order_id → Node (O(1) 취소)
        self._buy_levels: dict[float, PriceLevel] = {} # 가격 → 호가 단계
        self._sell_levels: dict[float, PriceLevel] = {}
        self._buy_prices: list[float] = []   # 정렬됨; best bid = 마지막
        self._sell_prices: list[float] = []  # 정렬됨; best ask = 첫번째

    def add_order(self, order: Order) -> None:
        """주문 추가 — O(1) amortized (새 가격: O(log N))"""
        node = OrderNode(order=order)
        self._orders[order.order_id] = node
        levels, prices = self._side_structures(order.side)
        if order.price not in levels:
            levels[order.price] = PriceLevel(order.price)
            bisect.insort(prices, order.price)  # 새 가격 삽입
        levels[order.price].append(node)  # FIFO 큐 끝에 추가

    def cancel_order(self, order_id: str) -> Order | None:
        """주문 취소 — O(1) doubly-linked list 에서 제거"""
        node = self._orders.pop(order_id, None)
        if node is None:
            return None
        level = levels[node.order.price]
        level.remove(node)  # O(1): prev/next 포인터로 즉시 제거
        return node.order
```

### PriceLevel (Doubly-Linked List)

```python
class PriceLevel:
    """동일 가격의 주문들을 FIFO 순서로 관리하는 이중 연결 리스트"""
    def append(self, node: OrderNode) -> None:
        """O(1) — 새 주문을 큐 끝에 추가"""
        node.prev = self.tail
        if self.tail is not None:
            self.tail.next = node
        else:
            self.head = node
        self.tail = node

    def pop_head(self) -> OrderNode | None:
        """O(1) — 가장 오래된 주문 제거 (매칭 시 사용)"""

    def remove(self, node: OrderNode) -> None:
        """O(1) — 임의 위치 노드 제거 (취소 시 사용)"""
```

## Matching Engine (FIFO)

매칭 엔진은 가격-시간 우선순위(price-time priority)로 주문을 매칭한다.
같은 가격에서는 먼저 들어온 주문이 먼저 체결된다 (FIFO).

```python
class MatchingEngine:
    def process_order(self, order: Order) -> list[Execution]:
        """주문을 반대편 호가와 매칭, 미체결분은 주문장에 등록"""
        book = self.get_or_create_book(order.symbol)
        executions = []
        remaining_qty = order.quantity

        if order.side is Side.BUY:
            # 매수 주문: 최저 매도가부터 sweep
            while remaining_qty > 0:
                best_sell = book.peek_best_sell_order()
                if best_sell is None or best_sell.price > order.price:
                    break  # 매칭 불가
                fill_qty = min(remaining_qty, best_sell.quantity)
                executions.append(Execution(
                    buy_order_id=order.order_id,
                    sell_order_id=best_sell.order_id,
                    price=best_sell.price,  # 호가 가격 우선
                    quantity=fill_qty,
                ))
                remaining_qty -= fill_qty

        # 미체결분은 주문장에 resting order 로 등록
        if remaining_qty > 0:
            book.add_order(Order(..., quantity=remaining_qty))
        return executions
```

## Sequencer

시퀀서는 모든 주문과 체결에 단조 증가하는 시퀀스 ID 를 부여한다.
이를 통해 **결정론적 재생**이 가능하다.

```python
class Sequencer:
    """모든 이벤트에 순서 번호를 부여하여 결정론적 재생 보장"""
    def sequence_order(self, order: Order) -> int:
        seq_id = self._next_id
        self._next_id += 1
        order.sequence_id = seq_id
        self._event_log.append(SequencedEvent(seq_id, EventType.NEW_ORDER, order))
        return seq_id

    def sequence_execution(self, execution: Execution) -> int:
        seq_id = self._next_id
        self._next_id += 1
        execution.sequence_id = seq_id
        self._event_log.append(SequencedEvent(seq_id, EventType.EXECUTION, execution))
        return seq_id
```

## Market Data (L1/L2 + Candlestick)

```python
class CandlestickAggregator:
    """체결 스트림을 OHLCV 캔들스틱으로 집계"""
    def record_execution(self, execution, timestamp):
        current = self._current.get(symbol)
        if current is None or timestamp >= current.interval_end:
            # 현재 캔들 완료, 새 캔들 시작
            self._candles[symbol].append(current)
            self._current[symbol] = Candlestick(
                open=price, high=price, low=price, close=price,
                volume=qty, trade_count=1,
            )
        else:
            # 현재 캔들 업데이트
            current.high = max(current.high, price)
            current.low = min(current.low, price)
            current.close = price
            current.volume += qty
```

## 성능 최적화

실제 거래소는 모든 컴포넌트를 **단일 서버**에 배치하고 `mmap` 으로
프로세스 간 통신하여 네트워크/디스크 지연을 제거한다:

| 기법 | 효과 |
|------|------|
| **Application Loop** | 단일 스레드 + CPU 핀닝, 컨텍스트 스위치 없음 |
| **mmap Event Store** | 네트워크/디스크 접근 없이 서브-마이크로초 메시지 전달 |
| **RocksDB** | 로컬 파일 기반 상태 저장 (LSM 트리, 쓰기 최적화) |
| **Ring Buffer** | 사전 할당, 락-프리, 객체 생성 없음 |

## 고가용성

**Hot-Warm** 구성 + **Raft 합의 알고리즘**:

```
Raft Node Group (5 nodes)
  Leader:   NewOrderEvent → Event Store → OrderFilledEvent
  Follower: Event Store 동기화 (읽기 전용)
  Follower: Event Store 동기화 (읽기 전용)
  Follower: (spare)
  Follower: (spare)

과반수(3/5) 가동이면 서비스 지속 가능
```

## 데모

```bash
python scripts/demo.py
```

## 테스트

```bash
pip install -r requirements.txt
pytest tests/ -v
```

## 참고

- Alex Xu, "System Design Interview - An Insider's Guide Vol. 2", Chapter 29
