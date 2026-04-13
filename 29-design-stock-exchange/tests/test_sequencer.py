"""Tests for the order/execution sequencer."""

from src.order_book import Order, Side
from src.matching import Execution
from src.sequencer import EventType, Sequencer


def test_sequence_order() -> None:
    """Orders should get monotonically increasing sequence IDs."""
    seq = Sequencer()
    o1 = Order(order_id="O1", symbol="AAPL", side=Side.BUY, price=100, quantity=10)
    o2 = Order(order_id="O2", symbol="AAPL", side=Side.SELL, price=101, quantity=20)

    id1 = seq.sequence_order(o1)
    id2 = seq.sequence_order(o2)

    assert id1 == 1
    assert id2 == 2
    assert o1.sequence_id == 1
    assert o2.sequence_id == 2


def test_sequence_execution() -> None:
    """Executions should also get monotonically increasing sequence IDs."""
    seq = Sequencer()
    ex = Execution(
        execution_id="E1", symbol="AAPL",
        buy_order_id="B1", sell_order_id="S1",
        price=100, quantity=50,
    )

    id1 = seq.sequence_execution(ex)
    assert id1 == 1
    assert ex.sequence_id == 1


def test_event_log() -> None:
    """Event log should contain all sequenced events in order."""
    seq = Sequencer()
    o1 = Order(order_id="O1", symbol="AAPL", side=Side.BUY, price=100, quantity=10)
    seq.sequence_order(o1)
    seq.sequence_cancel("AAPL", "O1")

    log = seq.event_log
    assert len(log) == 2
    assert log[0].event_type == EventType.NEW_ORDER
    assert log[1].event_type == EventType.CANCEL_ORDER


def test_reset() -> None:
    """Reset should clear the log and counter."""
    seq = Sequencer()
    o1 = Order(order_id="O1", symbol="AAPL", side=Side.BUY, price=100, quantity=10)
    seq.sequence_order(o1)
    seq.reset()

    assert seq.next_id == 1
    assert len(seq.event_log) == 0
