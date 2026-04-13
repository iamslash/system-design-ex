"""Tests for the order book with doubly-linked list price levels."""

import pytest
from src.order_book import Order, OrderBook, Side


def make_order(order_id: str, side: Side, price: float, qty: float) -> Order:
    return Order(order_id=order_id, symbol="AAPL", side=side, price=price, quantity=qty)


def test_add_and_best_bid_ask() -> None:
    """Adding orders should update best bid and ask."""
    book = OrderBook("AAPL")
    book.add_order(make_order("B1", Side.BUY, 100.0, 100))
    book.add_order(make_order("S1", Side.SELL, 101.0, 200))

    assert book.best_bid() == 100.0
    assert book.best_ask() == 101.0


def test_cancel_order() -> None:
    """Cancelled order should be removed from the book."""
    book = OrderBook("AAPL")
    book.add_order(make_order("B1", Side.BUY, 100.0, 100))
    book.add_order(make_order("B2", Side.BUY, 99.0, 50))

    cancelled = book.cancel_order("B1")
    assert cancelled is not None
    assert cancelled.order_id == "B1"
    assert book.best_bid() == 99.0


def test_cancel_nonexistent() -> None:
    """Cancelling a non-existent order should return None."""
    book = OrderBook("AAPL")
    assert book.cancel_order("NOPE") is None


def test_duplicate_order_id() -> None:
    """Adding duplicate order_id should raise ValueError."""
    book = OrderBook("AAPL")
    book.add_order(make_order("B1", Side.BUY, 100.0, 100))
    with pytest.raises(ValueError, match="Duplicate"):
        book.add_order(make_order("B1", Side.BUY, 100.0, 50))


def test_pop_best_buy_order() -> None:
    """pop_best_buy_order should return oldest order at best bid (FIFO)."""
    book = OrderBook("AAPL")
    book.add_order(make_order("B1", Side.BUY, 100.0, 100))
    book.add_order(make_order("B2", Side.BUY, 100.0, 200))

    popped = book.pop_best_buy_order()
    assert popped.order_id == "B1"  # FIFO: oldest first

    popped2 = book.pop_best_buy_order()
    assert popped2.order_id == "B2"


def test_pop_best_sell_order() -> None:
    """pop_best_sell_order should return oldest order at best ask (FIFO)."""
    book = OrderBook("AAPL")
    book.add_order(make_order("S1", Side.SELL, 101.0, 100))
    book.add_order(make_order("S2", Side.SELL, 101.0, 200))

    popped = book.pop_best_sell_order()
    assert popped.order_id == "S1"


def test_l1_data() -> None:
    """L1 data should show best bid, best ask, and spread."""
    book = OrderBook("AAPL")
    book.add_order(make_order("B1", Side.BUY, 100.0, 500))
    book.add_order(make_order("S1", Side.SELL, 100.5, 300))

    l1 = book.l1_data()
    assert l1["best_bid"] == 100.0
    assert l1["best_ask"] == 100.5
    assert l1["best_bid_qty"] == 500
    assert l1["spread"] == pytest.approx(0.5)


def test_l2_data() -> None:
    """L2 data should show multiple price levels per side."""
    book = OrderBook("AAPL")
    book.add_order(make_order("B1", Side.BUY, 100.0, 500))
    book.add_order(make_order("B2", Side.BUY, 99.0, 300))
    book.add_order(make_order("S1", Side.SELL, 101.0, 200))
    book.add_order(make_order("S2", Side.SELL, 102.0, 100))

    l2 = book.l2_data(depth=5)
    assert len(l2["bids"]) == 2
    assert len(l2["asks"]) == 2
    # Best bid first
    assert l2["bids"][0]["price"] == 100.0
    # Best ask first
    assert l2["asks"][0]["price"] == 101.0


def test_reduce_order_quantity() -> None:
    """Partial fill should reduce order quantity; full fill should remove it."""
    book = OrderBook("AAPL")
    book.add_order(make_order("B1", Side.BUY, 100.0, 500))

    book.reduce_order_quantity("B1", 200)
    order = book.get_order("B1")
    assert order is not None
    assert order.quantity == 300

    book.reduce_order_quantity("B1", 300)
    assert book.get_order("B1") is None
    assert book.order_count == 0


def test_empty_book() -> None:
    """Empty book should have None for best bid/ask."""
    book = OrderBook("AAPL")
    assert book.best_bid() is None
    assert book.best_ask() is None
    assert book.order_count == 0
