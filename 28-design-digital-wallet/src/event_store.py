"""Immutable event log for event sourcing.

All state changes are captured as events. The event store is append-only;
events are never modified or deleted. State can be reconstructed by replaying
events from the beginning or from a snapshot.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class EventType(Enum):
    """All domain event types."""

    WALLET_CREATED = "WALLET_CREATED"
    MONEY_DEPOSITED = "MONEY_DEPOSITED"
    MONEY_WITHDRAWN = "MONEY_WITHDRAWN"
    TRANSFER_INITIATED = "TRANSFER_INITIATED"
    TRANSFER_COMPLETED = "TRANSFER_COMPLETED"
    TRANSFER_FAILED = "TRANSFER_FAILED"
    SNAPSHOT_TAKEN = "SNAPSHOT_TAKEN"


@dataclass(frozen=True)
class Event:
    """An immutable domain event."""

    event_id: str
    event_type: EventType
    aggregate_id: str  # wallet_id or transfer_id
    timestamp: datetime
    data: dict[str, Any]
    version: int  # monotonically increasing per aggregate

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "aggregate_id": self.aggregate_id,
            "timestamp": self.timestamp.isoformat(),
            "data": self.data,
            "version": self.version,
        }


class EventStore:
    """Append-only event log.

    Events are stored globally and indexed by aggregate_id for efficient
    replay of a single aggregate's history.
    """

    def __init__(self) -> None:
        self._events: list[Event] = []
        self._index: dict[str, list[int]] = {}  # aggregate_id -> positions
        self._idempotency_keys: set[str] = set()

    def append(
        self,
        event_type: EventType,
        aggregate_id: str,
        data: dict[str, Any],
        *,
        idempotency_key: str | None = None,
    ) -> Event:
        """Append an event. Returns the created event.

        If an idempotency_key is provided and was already used, the original
        event is returned without creating a duplicate.
        """
        if idempotency_key and idempotency_key in self._idempotency_keys:
            # Return the existing event with this key
            for evt in reversed(self._events):
                if evt.data.get("idempotency_key") == idempotency_key:
                    return evt
            # Fallback: key tracked but event not found (shouldn't happen)
            raise RuntimeError(f"Idempotency key {idempotency_key} tracked but event missing")

        # Compute next version for this aggregate
        agg_positions = self._index.get(aggregate_id, [])
        version = len(agg_positions) + 1

        if idempotency_key:
            data = {**data, "idempotency_key": idempotency_key}

        event = Event(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            aggregate_id=aggregate_id,
            timestamp=datetime.now(timezone.utc),
            data=data,
            version=version,
        )

        pos = len(self._events)
        self._events.append(event)
        self._index.setdefault(aggregate_id, []).append(pos)

        if idempotency_key:
            self._idempotency_keys.add(idempotency_key)

        return event

    def get_events(
        self,
        aggregate_id: str | None = None,
        *,
        after_version: int = 0,
    ) -> list[Event]:
        """Retrieve events, optionally filtered by aggregate and version."""
        if aggregate_id is not None:
            positions = self._index.get(aggregate_id, [])
            events = [self._events[p] for p in positions]
        else:
            events = list(self._events)

        if after_version > 0:
            events = [e for e in events if e.version > after_version]

        return events

    def get_all_events(self) -> list[Event]:
        """Return all events in insertion order."""
        return list(self._events)

    def __len__(self) -> int:
        return len(self._events)
