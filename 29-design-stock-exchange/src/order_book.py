"""Order book with price levels using doubly-linked list + hashmap.

Each side (buy/sell) maintains a sorted collection of price levels.
Each price level is a FIFO queue of orders implemented as a doubly-linked
list for O(1) add / cancel / match at the head.

Internal data structures
------------------------
- ``_orders``: dict mapping order_id -> OrderNode  (O(1) lookup for cancel)
- ``_levels``: dict mapping price -> PriceLevel    (O(1) level access)
- ``_sorted_prices``: sorted list of active prices (for best bid/ask)
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class Side(Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class Order:
    """Immutable representation of an order placed by a client."""

    order_id: str
    symbol: str
    side: Side
    price: float
    quantity: float
    timestamp: float = 0.0
    sequence_id: int = 0


# -----------------------------------------------------------------------
# Doubly-linked list node for O(1) insertion / removal
# -----------------------------------------------------------------------

@dataclass
class OrderNode:
    """Node in the doubly-linked list that forms a price level's FIFO queue."""

    order: Order
    prev: Optional[OrderNode] = field(default=None, repr=False)
    next: Optional[OrderNode] = field(default=None, repr=False)


class PriceLevel:
    """FIFO queue of orders at a single price, backed by a doubly-linked list.

    Supports O(1) append, O(1) pop-head, and O(1) remove-by-node.
    """

    __slots__ = ("price", "head", "tail", "total_quantity", "order_count")

    def __init__(self, price: float) -> None:
        self.price = price
        self.head: Optional[OrderNode] = None
        self.tail: Optional[OrderNode] = None
        self.total_quantity: float = 0.0
        self.order_count: int = 0

    def is_empty(self) -> bool:
        return self.head is None

    def append(self, node: OrderNode) -> None:
        """Append *node* to the tail (newest order)."""
        node.prev = self.tail
        node.next = None
        if self.tail is not None:
            self.tail.next = node
        else:
            self.head = node
        self.tail = node
        self.total_quantity += node.order.quantity
        self.order_count += 1

    def pop_head(self) -> Optional[OrderNode]:
        """Remove and return the oldest order (head)."""
        if self.head is None:
            return None
        node = self.head
        self._unlink(node)
        return node

    def remove(self, node: OrderNode) -> None:
        """Remove an arbitrary node in O(1)."""
        self._unlink(node)

    def _unlink(self, node: OrderNode) -> None:
        if node.prev is not None:
            node.prev.next = node.next
        else:
            self.head = node.next
        if node.next is not None:
            node.next.prev = node.prev
        else:
            self.tail = node.prev
        node.prev = None
        node.next = None
        self.total_quantity -= node.order.quantity
        self.order_count -= 1


class OrderBook:
    """Order book for a single symbol with buy and sell sides.

    Provides O(1) add, O(1) cancel, and O(1) match at the best price level.
    Best bid/ask lookup is O(1) via maintaining indices into sorted price lists.
    """

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        # order_id -> OrderNode for O(1) cancel
        self._orders: dict[str, OrderNode] = {}
        # price -> PriceLevel per side
        self._buy_levels: dict[float, PriceLevel] = {}
        self._sell_levels: dict[float, PriceLevel] = {}
        # Sorted price lists for best bid/ask
        self._buy_prices: list[float] = []   # ascending; best bid = last
        self._sell_prices: list[float] = []  # ascending; best ask = first

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_order(self, order: Order) -> None:
        """Add an order to the book. O(1) amortised (O(log N) for new price)."""
        if order.order_id in self._orders:
            raise ValueError(f"Duplicate order_id: {order.order_id!r}")

        node = OrderNode(order=order)
        self._orders[order.order_id] = node

        levels, prices = self._side_structures(order.side)

        if order.price not in levels:
            levels[order.price] = PriceLevel(order.price)
            bisect.insort(prices, order.price)

        levels[order.price].append(node)

    def cancel_order(self, order_id: str) -> Optional[Order]:
        """Cancel an order by id in O(1). Returns the cancelled order or None."""
        node = self._orders.pop(order_id, None)
        if node is None:
            return None

        order = node.order
        levels, prices = self._side_structures(order.side)
        level = levels[order.price]
        level.remove(node)

        if level.is_empty():
            del levels[order.price]
            idx = bisect.bisect_left(prices, order.price)
            if idx < len(prices) and prices[idx] == order.price:
                prices.pop(idx)

        return order

    def best_bid(self) -> Optional[float]:
        """Return the highest buy price, or None if no bids."""
        return self._buy_prices[-1] if self._buy_prices else None

    def best_ask(self) -> Optional[float]:
        """Return the lowest sell price, or None if no asks."""
        return self._sell_prices[0] if self._sell_prices else None

    def peek_best_buy_order(self) -> Optional[Order]:
        """Return the oldest order at the best bid price without removing it."""
        if not self._buy_prices:
            return None
        level = self._buy_levels[self._buy_prices[-1]]
        return level.head.order if level.head else None

    def peek_best_sell_order(self) -> Optional[Order]:
        """Return the oldest order at the best ask price without removing it."""
        if not self._sell_prices:
            return None
        level = self._sell_levels[self._sell_prices[0]]
        return level.head.order if level.head else None

    def pop_best_buy_order(self) -> Optional[Order]:
        """Remove and return the oldest order at the best bid."""
        if not self._buy_prices:
            return None
        price = self._buy_prices[-1]
        level = self._buy_levels[price]
        node = level.pop_head()
        if node is None:
            return None
        self._orders.pop(node.order.order_id, None)
        if level.is_empty():
            del self._buy_levels[price]
            self._buy_prices.pop()
        return node.order

    def pop_best_sell_order(self) -> Optional[Order]:
        """Remove and return the oldest order at the best ask."""
        if not self._sell_prices:
            return None
        price = self._sell_prices[0]
        level = self._sell_levels[price]
        node = level.pop_head()
        if node is None:
            return None
        self._orders.pop(node.order.order_id, None)
        if level.is_empty():
            del self._sell_levels[price]
            self._sell_prices.pop(0)
        return node.order

    def reduce_order_quantity(self, order_id: str, filled_qty: float) -> None:
        """Reduce an order's remaining quantity after a partial fill.

        If the remaining quantity reaches zero the order is removed.
        """
        node = self._orders.get(order_id)
        if node is None:
            return

        order = node.order
        levels, prices = self._side_structures(order.side)
        level = levels.get(order.price)
        if level is None:
            return

        level.total_quantity -= filled_qty
        # Mutate the order's quantity in-place.
        remaining = order.quantity - filled_qty
        object.__setattr__(order, "quantity", remaining)

        if remaining <= 0:
            level.remove(node)
            self._orders.pop(order_id, None)
            if level.is_empty():
                del levels[order.price]
                idx = bisect.bisect_left(prices, order.price)
                if idx < len(prices) and prices[idx] == order.price:
                    prices.pop(idx)

    def get_order(self, order_id: str) -> Optional[Order]:
        """Look up an order by id. Returns None if not found."""
        node = self._orders.get(order_id)
        return node.order if node else None

    # ------------------------------------------------------------------
    # Market data helpers
    # ------------------------------------------------------------------

    def l1_data(self) -> dict:
        """Return Level-1 data: best bid/ask price and quantity."""
        bid_price = self.best_bid()
        ask_price = self.best_ask()
        bid_qty = (
            self._buy_levels[bid_price].total_quantity if bid_price is not None else 0.0
        )
        ask_qty = (
            self._sell_levels[ask_price].total_quantity
            if ask_price is not None
            else 0.0
        )
        return {
            "symbol": self.symbol,
            "best_bid": bid_price,
            "best_bid_qty": bid_qty,
            "best_ask": ask_price,
            "best_ask_qty": ask_qty,
            "spread": (ask_price - bid_price) if (bid_price and ask_price) else None,
        }

    def l2_data(self, depth: int = 5) -> dict:
        """Return Level-2 (depth-of-book) data.

        Returns up to *depth* price levels for each side, ordered by
        price (best first).
        """
        bids: list[dict] = []
        for price in reversed(self._buy_prices[-depth:]):
            lvl = self._buy_levels[price]
            bids.append(
                {"price": price, "quantity": lvl.total_quantity, "orders": lvl.order_count}
            )

        asks: list[dict] = []
        for price in self._sell_prices[:depth]:
            lvl = self._sell_levels[price]
            asks.append(
                {"price": price, "quantity": lvl.total_quantity, "orders": lvl.order_count}
            )

        return {"symbol": self.symbol, "bids": bids, "asks": asks}

    @property
    def order_count(self) -> int:
        return len(self._orders)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _side_structures(
        self, side: Side
    ) -> tuple[dict[float, PriceLevel], list[float]]:
        if side is Side.BUY:
            return self._buy_levels, self._buy_prices
        return self._sell_levels, self._sell_prices
