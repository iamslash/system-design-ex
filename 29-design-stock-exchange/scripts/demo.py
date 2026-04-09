"""Demo script for the stock exchange system.

Demonstrates order book, FIFO matching, sequencer, and market data
(L1/L2 + candlestick charts).

Usage:
    python scripts/demo.py
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.order_book import Order, Side
from src.matching import MatchingEngine
from src.sequencer import Sequencer
from src.market_data import MarketDataService


def main() -> None:
    engine = MatchingEngine()
    sequencer = Sequencer()
    mds = MarketDataService(interval_seconds=60)

    print("=" * 60)
    print("Stock Exchange Demo (AAPL)")
    print("=" * 60)

    # 1. Place sell orders
    print("\n[1] Placing sell (ask) orders")
    sells = [
        Order("S1", "AAPL", Side.SELL, 100.10, 200, timestamp=0.0),
        Order("S2", "AAPL", Side.SELL, 100.10, 400, timestamp=1.0),
        Order("S3", "AAPL", Side.SELL, 100.11, 1100, timestamp=2.0),
        Order("S4", "AAPL", Side.SELL, 100.12, 100, timestamp=3.0),
        Order("S5", "AAPL", Side.SELL, 100.13, 900, timestamp=4.0),
    ]
    for s in sells:
        sequencer.sequence_order(s)
        engine.process_order(s)
        print(f"    {s.order_id}: SELL {s.quantity} @ {s.price}")

    # 2. Place buy orders
    print("\n[2] Placing buy (bid) orders")
    buys = [
        Order("B1", "AAPL", Side.BUY, 100.08, 500, timestamp=5.0),
        Order("B2", "AAPL", Side.BUY, 100.07, 300, timestamp=6.0),
        Order("B3", "AAPL", Side.BUY, 100.06, 200, timestamp=7.0),
    ]
    for b in buys:
        sequencer.sequence_order(b)
        engine.process_order(b)
        print(f"    {b.order_id}: BUY  {b.quantity} @ {b.price}")

    # 3. L1 / L2 market data
    book = engine.get_book("AAPL")
    print("\n[3] Market data")
    l1 = mds.l1(book)
    print(f"    L1 - Best Bid: {l1['best_bid']} ({l1['best_bid_qty']}), "
          f"Best Ask: {l1['best_ask']} ({l1['best_ask_qty']}), "
          f"Spread: {l1['spread']}")

    l2 = mds.l2(book, depth=5)
    print("    L2 - Bids:")
    for lvl in l2["bids"]:
        print(f"           {lvl['price']:>8.2f}  qty={lvl['quantity']:.0f}  orders={lvl['orders']}")
    print("    L2 - Asks:")
    for lvl in l2["asks"]:
        print(f"           {lvl['price']:>8.2f}  qty={lvl['quantity']:.0f}  orders={lvl['orders']}")

    # 4. Aggressive buy that sweeps multiple price levels
    print("\n[4] Aggressive market buy: 2700 shares @ limit 100.13")
    aggressive = Order("B-AGG", "AAPL", Side.BUY, 100.13, 2700, timestamp=10.0)
    sequencer.sequence_order(aggressive)
    executions = engine.process_order(aggressive)

    print(f"    Fills: {len(executions)}")
    for ex in executions:
        sequencer.sequence_execution(ex)
        mds.record_execution(ex, timestamp=10.0)
        print(f"    {ex.execution_id}: {ex.quantity:.0f} @ {ex.price} "
              f"(buy={ex.buy_order_id}, sell={ex.sell_order_id})")

    total_filled = sum(e.quantity for e in executions)
    print(f"    Total filled: {total_filled:.0f}")

    # 5. Updated L1
    print("\n[5] Updated market data after aggressive buy")
    l1_new = mds.l1(book)
    print(f"    L1 - Best Bid: {l1_new['best_bid']}, Best Ask: {l1_new['best_ask']}")

    # 6. Candlestick
    print("\n[6] Candlestick chart")
    mds.aggregator.flush("AAPL")
    candles = mds.get_candles("AAPL")
    for c in candles:
        print(f"    O={c.open} H={c.high} L={c.low} C={c.close} "
              f"V={c.volume:.0f} trades={c.trade_count}")

    # 7. Sequencer log
    print(f"\n[7] Sequencer event log: {len(sequencer.event_log)} events")
    for evt in sequencer.event_log:
        print(f"    seq={evt.sequence_id:3d}  {evt.event_type.value}")

    print("\n" + "=" * 60)
    print("Demo complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
