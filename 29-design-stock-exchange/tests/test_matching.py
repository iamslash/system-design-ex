"""Tests for the FIFO matching engine."""

import pytest
from src.order_book import Order, Side
from src.matching import MatchingEngine


def make_order(oid: str, side: Side, price: float, qty: float) -> Order:
    return Order(order_id=oid, symbol="AAPL", side=side, price=price, quantity=qty)


def test_no_match_no_crossing():
    """Buy below ask should rest on book, no execution."""
    engine = MatchingEngine()
    engine.process_order(make_order("S1", Side.SELL, 101.0, 100))
    execs = engine.process_order(make_order("B1", Side.BUY, 100.0, 100))

    assert len(execs) == 0
    book = engine.get_book("AAPL")
    assert book.order_count == 2


def test_exact_match():
    """Buy at ask price with equal quantity should produce one execution."""
    engine = MatchingEngine()
    engine.process_order(make_order("S1", Side.SELL, 100.0, 100))
    execs = engine.process_order(make_order("B1", Side.BUY, 100.0, 100))

    assert len(execs) == 1
    assert execs[0].price == 100.0
    assert execs[0].quantity == 100
    assert execs[0].buy_order_id == "B1"
    assert execs[0].sell_order_id == "S1"

    book = engine.get_book("AAPL")
    assert book.order_count == 0


def test_partial_fill_buy():
    """Buy for more than available sell should partially fill."""
    engine = MatchingEngine()
    engine.process_order(make_order("S1", Side.SELL, 100.0, 50))
    execs = engine.process_order(make_order("B1", Side.BUY, 100.0, 200))

    assert len(execs) == 1
    assert execs[0].quantity == 50

    # 150 remains on buy side
    book = engine.get_book("AAPL")
    assert book.best_bid() == 100.0


def test_multiple_fills():
    """One aggressive order should sweep through multiple resting orders."""
    engine = MatchingEngine()
    engine.process_order(make_order("S1", Side.SELL, 100.0, 100))
    engine.process_order(make_order("S2", Side.SELL, 100.5, 100))
    engine.process_order(make_order("S3", Side.SELL, 101.0, 100))

    execs = engine.process_order(make_order("B1", Side.BUY, 101.0, 250))

    assert len(execs) == 3
    assert execs[0].price == 100.0
    assert execs[1].price == 100.5
    assert execs[2].price == 101.0
    assert execs[2].quantity == 50  # partial fill on S3

    total_filled = sum(e.quantity for e in execs)
    assert total_filled == 250


def test_fifo_priority():
    """At the same price, earlier order should fill first."""
    engine = MatchingEngine()
    engine.process_order(make_order("S1", Side.SELL, 100.0, 100))
    engine.process_order(make_order("S2", Side.SELL, 100.0, 100))

    execs = engine.process_order(make_order("B1", Side.BUY, 100.0, 100))

    assert len(execs) == 1
    assert execs[0].sell_order_id == "S1"  # FIFO: S1 was first


def test_sell_matches_buy():
    """Incoming sell should match resting buy orders."""
    engine = MatchingEngine()
    engine.process_order(make_order("B1", Side.BUY, 100.0, 100))
    execs = engine.process_order(make_order("S1", Side.SELL, 100.0, 100))

    assert len(execs) == 1
    assert execs[0].buy_order_id == "B1"
    assert execs[0].sell_order_id == "S1"


def test_cancel_order():
    """Cancelled order should not be matchable."""
    engine = MatchingEngine()
    engine.process_order(make_order("S1", Side.SELL, 100.0, 100))
    engine.cancel_order("AAPL", "S1")

    execs = engine.process_order(make_order("B1", Side.BUY, 100.0, 100))
    assert len(execs) == 0

    book = engine.get_book("AAPL")
    assert book.best_bid() == 100.0  # resting buy
