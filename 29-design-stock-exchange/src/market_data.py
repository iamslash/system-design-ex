"""Market data publisher: L1 / L2 order book snapshots and candlestick charts.

In a real exchange the market-data publisher is a separate service that
consumes the execution stream and order book updates, then fans them out
to subscribers via multicast (e.g. UDP) for minimal latency.

This simplified implementation provides:

- **L1 data**: best bid / ask (top-of-book).
- **L2 data**: depth-of-book (configurable number of levels).
- **Candlestick charts**: OHLCV (Open-High-Low-Close-Volume) aggregation
  over configurable time intervals.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .matching import Execution
from .order_book import OrderBook


@dataclass
class Candlestick:
    """A single OHLCV candlestick bar."""

    symbol: str
    interval_start: float
    interval_end: float
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int


class CandlestickAggregator:
    """Aggregates executions into candlestick bars by time interval.

    Parameters
    ----------
    interval_seconds:
        Duration of each candle in seconds (default 60 = 1 minute).
    """

    def __init__(self, interval_seconds: float = 60.0) -> None:
        self._interval = interval_seconds
        # symbol -> list of completed candles
        self._candles: dict[str, list[Candlestick]] = {}
        # symbol -> current (in-progress) candle
        self._current: dict[str, Candlestick] = {}

    def record_execution(self, execution: Execution, timestamp: float) -> None:
        """Record a trade and roll candles as needed."""
        symbol = execution.symbol
        price = execution.price
        qty = execution.quantity

        current = self._current.get(symbol)

        if current is None or timestamp >= current.interval_end:
            # Finalise the current candle if present.
            if current is not None:
                self._candles.setdefault(symbol, []).append(current)

            # Start a new candle.
            interval_start = (
                timestamp // self._interval
            ) * self._interval
            self._current[symbol] = Candlestick(
                symbol=symbol,
                interval_start=interval_start,
                interval_end=interval_start + self._interval,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=qty,
                trade_count=1,
            )
        else:
            # Update current candle.
            current.high = max(current.high, price)
            current.low = min(current.low, price)
            current.close = price
            current.volume += qty
            current.trade_count += 1

    def flush(self, symbol: str | None = None) -> None:
        """Finalise the current candle(s) so they appear in ``get_candles``."""
        symbols = [symbol] if symbol else list(self._current.keys())
        for sym in symbols:
            current = self._current.pop(sym, None)
            if current is not None:
                self._candles.setdefault(sym, []).append(current)

    def get_candles(self, symbol: str) -> list[Candlestick]:
        """Return all completed candles for *symbol*."""
        return list(self._candles.get(symbol, []))

    def get_current_candle(self, symbol: str) -> Candlestick | None:
        """Return the in-progress candle for *symbol*, or None."""
        return self._current.get(symbol)


class MarketDataService:
    """Aggregates L1/L2 data from order books and candles from executions."""

    def __init__(self, interval_seconds: float = 60.0) -> None:
        self._aggregator = CandlestickAggregator(interval_seconds=interval_seconds)

    @property
    def aggregator(self) -> CandlestickAggregator:
        return self._aggregator

    def l1(self, book: OrderBook) -> dict:
        """Return L1 (top-of-book) snapshot."""
        return book.l1_data()

    def l2(self, book: OrderBook, depth: int = 5) -> dict:
        """Return L2 (depth-of-book) snapshot."""
        return book.l2_data(depth=depth)

    def record_execution(self, execution: Execution, timestamp: float) -> None:
        """Feed an execution into the candlestick aggregator."""
        self._aggregator.record_execution(execution, timestamp)

    def get_candles(self, symbol: str, flush: bool = False) -> list[Candlestick]:
        """Return candle history. If *flush* is True, finalise the current bar."""
        if flush:
            self._aggregator.flush(symbol)
        return self._aggregator.get_candles(symbol)
