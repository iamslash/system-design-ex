"""Order and execution sequencer for deterministic replay.

In a real stock exchange the sequencer is the single-writer component that
stamps every inbound message with a monotonically increasing sequence ID
before handing it to the matching engine.  This guarantees:

1. Total ordering of all events (orders + executions).
2. Deterministic replay: replaying the sequenced log reproduces the same
   order book and execution history byte-for-byte.

This implementation stores a full event log that can be replayed through a
fresh matching engine to reconstruct state.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Union

from .matching import Execution
from .order_book import Order


class EventType(Enum):
    NEW_ORDER = "NEW_ORDER"
    CANCEL_ORDER = "CANCEL_ORDER"
    EXECUTION = "EXECUTION"


@dataclass
class SequencedEvent:
    """A single event in the sequenced log."""

    sequence_id: int
    event_type: EventType
    payload: Union[Order, Execution, dict]


class Sequencer:
    """Stamps monotonically increasing sequence IDs on orders and executions.

    The sequencer maintains an append-only event log for deterministic replay.
    """

    def __init__(self) -> None:
        self._next_id: int = 1
        self._event_log: list[SequencedEvent] = []

    @property
    def next_id(self) -> int:
        return self._next_id

    @property
    def event_log(self) -> list[SequencedEvent]:
        return list(self._event_log)

    def sequence_order(self, order: Order) -> int:
        """Assign a sequence ID to an inbound order and log it."""
        seq_id = self._next_id
        self._next_id += 1
        object.__setattr__(order, "sequence_id", seq_id)

        event = SequencedEvent(
            sequence_id=seq_id,
            event_type=EventType.NEW_ORDER,
            payload=order,
        )
        self._event_log.append(event)
        return seq_id

    def sequence_cancel(self, symbol: str, order_id: str) -> int:
        """Log a cancel request with a sequence ID."""
        seq_id = self._next_id
        self._next_id += 1

        event = SequencedEvent(
            sequence_id=seq_id,
            event_type=EventType.CANCEL_ORDER,
            payload={"symbol": symbol, "order_id": order_id},
        )
        self._event_log.append(event)
        return seq_id

    def sequence_execution(self, execution: Execution) -> int:
        """Assign a sequence ID to an execution and log it."""
        seq_id = self._next_id
        self._next_id += 1
        execution.sequence_id = seq_id

        event = SequencedEvent(
            sequence_id=seq_id,
            event_type=EventType.EXECUTION,
            payload=execution,
        )
        self._event_log.append(event)
        return seq_id

    def reset(self) -> None:
        """Clear the event log and reset the counter."""
        self._next_id = 1
        self._event_log.clear()
