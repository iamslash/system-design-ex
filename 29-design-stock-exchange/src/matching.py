"""FIFO matching engine for limit orders.

The matching engine receives sequenced orders and attempts to cross them
against the opposite side of the order book.  It enforces **price-time
priority** (FIFO within each price level).

Each successful match produces an ``Execution`` (fill) record.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .order_book import Order, OrderBook, Side


@dataclass
class Execution:
    """A single fill (trade) produced by the matching engine."""

    execution_id: str
    symbol: str
    buy_order_id: str
    sell_order_id: str
    price: float
    quantity: float
    sequence_id: int = 0


class MatchingEngine:
    """FIFO limit-order matching engine.

    For each incoming order the engine walks the opposite side of the book
    starting at the best price.  Within a price level, orders are matched
    in FIFO order (oldest first).

    If the incoming order is not fully filled, the remainder rests on the
    book.
    """

    def __init__(self) -> None:
        self._books: dict[str, OrderBook] = {}
        self._exec_counter: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_or_create_book(self, symbol: str) -> OrderBook:
        if symbol not in self._books:
            self._books[symbol] = OrderBook(symbol)
        return self._books[symbol]

    def get_book(self, symbol: str) -> Optional[OrderBook]:
        return self._books.get(symbol)

    def process_order(self, order: Order) -> list[Execution]:
        """Match *order* against the book and return a list of executions.

        Any unfilled remainder is added to the order book as a resting order.
        """
        book = self.get_or_create_book(order.symbol)
        executions: list[Execution] = []
        remaining_qty = order.quantity

        if order.side is Side.BUY:
            remaining_qty = self._match_buy(order, book, executions, remaining_qty)
        else:
            remaining_qty = self._match_sell(order, book, executions, remaining_qty)

        # Rest any unfilled quantity on the book.
        if remaining_qty > 0:
            resting = Order(
                order_id=order.order_id,
                symbol=order.symbol,
                side=order.side,
                price=order.price,
                quantity=remaining_qty,
                timestamp=order.timestamp,
                sequence_id=order.sequence_id,
            )
            book.add_order(resting)

        return executions

    def cancel_order(self, symbol: str, order_id: str) -> Optional[Order]:
        """Cancel an order from the book. Returns the order or None."""
        book = self._books.get(symbol)
        if book is None:
            return None
        return book.cancel_order(order_id)

    # ------------------------------------------------------------------
    # Match helpers
    # ------------------------------------------------------------------

    def _match_buy(
        self,
        incoming: Order,
        book: OrderBook,
        executions: list[Execution],
        remaining: float,
    ) -> float:
        """Match an incoming BUY order against resting SELL orders."""
        while remaining > 0:
            best_sell = book.peek_best_sell_order()
            if best_sell is None or best_sell.price > incoming.price:
                break
            fill_qty = min(remaining, best_sell.quantity)
            fill_price = best_sell.price  # price-time priority: resting price

            exec_record = self._create_execution(
                symbol=incoming.symbol,
                buy_order_id=incoming.order_id,
                sell_order_id=best_sell.order_id,
                price=fill_price,
                quantity=fill_qty,
            )
            executions.append(exec_record)
            remaining -= fill_qty

            if fill_qty >= best_sell.quantity:
                book.pop_best_sell_order()
            else:
                book.reduce_order_quantity(best_sell.order_id, fill_qty)

        return remaining

    def _match_sell(
        self,
        incoming: Order,
        book: OrderBook,
        executions: list[Execution],
        remaining: float,
    ) -> float:
        """Match an incoming SELL order against resting BUY orders."""
        while remaining > 0:
            best_buy = book.peek_best_buy_order()
            if best_buy is None or best_buy.price < incoming.price:
                break
            fill_qty = min(remaining, best_buy.quantity)
            fill_price = best_buy.price  # resting price

            exec_record = self._create_execution(
                symbol=incoming.symbol,
                buy_order_id=best_buy.order_id,
                sell_order_id=incoming.order_id,
                price=fill_price,
                quantity=fill_qty,
            )
            executions.append(exec_record)
            remaining -= fill_qty

            if fill_qty >= best_buy.quantity:
                book.pop_best_buy_order()
            else:
                book.reduce_order_quantity(best_buy.order_id, fill_qty)

        return remaining

    def _create_execution(
        self,
        symbol: str,
        buy_order_id: str,
        sell_order_id: str,
        price: float,
        quantity: float,
    ) -> Execution:
        self._exec_counter += 1
        return Execution(
            execution_id=f"E-{self._exec_counter:06d}",
            symbol=symbol,
            buy_order_id=buy_order_id,
            sell_order_id=sell_order_id,
            price=price,
            quantity=quantity,
        )
