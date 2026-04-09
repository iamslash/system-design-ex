"""Tests for market data publisher: L1/L2 and candlestick charts."""

import pytest
from src.matching import Execution
from src.market_data import CandlestickAggregator, MarketDataService
from src.order_book import Order, OrderBook, Side


def make_execution(eid: str, price: float, qty: float) -> Execution:
    return Execution(
        execution_id=eid, symbol="AAPL",
        buy_order_id="B1", sell_order_id="S1",
        price=price, quantity=qty,
    )


class TestCandlestickAggregator:
    def test_single_trade(self):
        """A single trade should create one in-progress candle."""
        agg = CandlestickAggregator(interval_seconds=60)
        agg.record_execution(make_execution("E1", 100.0, 50), timestamp=0.0)

        candle = agg.get_current_candle("AAPL")
        assert candle is not None
        assert candle.open == 100.0
        assert candle.close == 100.0
        assert candle.volume == 50

    def test_ohlcv_update(self):
        """Multiple trades within the same interval should update OHLCV."""
        agg = CandlestickAggregator(interval_seconds=60)
        agg.record_execution(make_execution("E1", 100.0, 50), timestamp=0.0)
        agg.record_execution(make_execution("E2", 105.0, 30), timestamp=10.0)
        agg.record_execution(make_execution("E3", 98.0, 20), timestamp=20.0)
        agg.record_execution(make_execution("E4", 102.0, 40), timestamp=30.0)

        candle = agg.get_current_candle("AAPL")
        assert candle.open == 100.0
        assert candle.high == 105.0
        assert candle.low == 98.0
        assert candle.close == 102.0
        assert candle.volume == 140
        assert candle.trade_count == 4

    def test_candle_roll(self):
        """Trade beyond the interval should start a new candle."""
        agg = CandlestickAggregator(interval_seconds=60)
        agg.record_execution(make_execution("E1", 100.0, 50), timestamp=0.0)
        agg.record_execution(make_execution("E2", 110.0, 30), timestamp=61.0)

        # First candle should be completed
        candles = agg.get_candles("AAPL")
        assert len(candles) == 1
        assert candles[0].close == 100.0

        # Current candle is the new one
        current = agg.get_current_candle("AAPL")
        assert current.open == 110.0

    def test_flush(self):
        """Flush should finalize in-progress candle."""
        agg = CandlestickAggregator(interval_seconds=60)
        agg.record_execution(make_execution("E1", 100.0, 50), timestamp=0.0)
        agg.flush("AAPL")

        candles = agg.get_candles("AAPL")
        assert len(candles) == 1
        assert agg.get_current_candle("AAPL") is None


class TestMarketDataService:
    def test_l1(self):
        """L1 should reflect best bid/ask from order book."""
        book = OrderBook("AAPL")
        book.add_order(Order("B1", "AAPL", Side.BUY, 100.0, 500))
        book.add_order(Order("S1", "AAPL", Side.SELL, 101.0, 300))

        mds = MarketDataService()
        l1 = mds.l1(book)
        assert l1["best_bid"] == 100.0
        assert l1["best_ask"] == 101.0

    def test_l2(self):
        """L2 should show multiple depth levels."""
        book = OrderBook("AAPL")
        book.add_order(Order("B1", "AAPL", Side.BUY, 100.0, 500))
        book.add_order(Order("B2", "AAPL", Side.BUY, 99.0, 300))
        book.add_order(Order("S1", "AAPL", Side.SELL, 101.0, 200))

        mds = MarketDataService()
        l2 = mds.l2(book, depth=5)
        assert len(l2["bids"]) == 2
        assert len(l2["asks"]) == 1

    def test_record_and_get_candles(self):
        """Service should aggregate executions into candles."""
        mds = MarketDataService(interval_seconds=60)
        mds.record_execution(make_execution("E1", 100.0, 50), timestamp=0.0)
        mds.record_execution(make_execution("E2", 105.0, 30), timestamp=61.0)

        candles = mds.get_candles("AAPL", flush=True)
        assert len(candles) == 2
