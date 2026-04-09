"""Tests for the immutable event store."""

from src.event_store import EventStore, EventType


def test_append_and_retrieve():
    """Appended events should be retrievable."""
    store = EventStore()
    event = store.append(EventType.WALLET_CREATED, "w1", {"wallet_id": "w1", "owner": "alice"})

    assert event.event_type == EventType.WALLET_CREATED
    assert event.version == 1
    assert len(store) == 1


def test_idempotency():
    """Same idempotency key should return the original event."""
    store = EventStore()
    first = store.append(
        EventType.MONEY_DEPOSITED,
        "w1",
        {"wallet_id": "w1", "amount": 100},
        idempotency_key="dep-001",
    )
    second = store.append(
        EventType.MONEY_DEPOSITED,
        "w1",
        {"wallet_id": "w1", "amount": 100},
        idempotency_key="dep-001",
    )

    assert first.event_id == second.event_id
    assert len(store) == 1


def test_get_events_by_aggregate():
    """Should filter events by aggregate_id."""
    store = EventStore()
    store.append(EventType.WALLET_CREATED, "w1", {"wallet_id": "w1", "owner": "alice"})
    store.append(EventType.WALLET_CREATED, "w2", {"wallet_id": "w2", "owner": "bob"})
    store.append(EventType.MONEY_DEPOSITED, "w1", {"wallet_id": "w1", "amount": 100})

    w1_events = store.get_events("w1")
    assert len(w1_events) == 2

    w2_events = store.get_events("w2")
    assert len(w2_events) == 1


def test_get_events_after_version():
    """Should filter events after a specific version."""
    store = EventStore()
    store.append(EventType.WALLET_CREATED, "w1", {"wallet_id": "w1", "owner": "alice"})
    store.append(EventType.MONEY_DEPOSITED, "w1", {"wallet_id": "w1", "amount": 100})
    store.append(EventType.MONEY_DEPOSITED, "w1", {"wallet_id": "w1", "amount": 200})

    events = store.get_events("w1", after_version=1)
    assert len(events) == 2
    assert events[0].version == 2


def test_event_immutability():
    """Events should be frozen dataclasses."""
    store = EventStore()
    event = store.append(EventType.WALLET_CREATED, "w1", {"wallet_id": "w1", "owner": "alice"})

    import dataclasses
    assert dataclasses.is_dataclass(event)
    # frozen=True means we can't set attributes
    with __import__("pytest").raises(AttributeError):
        event.event_id = "changed"
