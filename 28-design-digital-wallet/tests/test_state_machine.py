"""Tests for the wallet state machine (command -> event -> state)."""

import pytest
from src.state_machine import Command, CommandType, WalletStateMachine


def test_create_wallet() -> None:
    """CREATE_WALLET should produce a WALLET_CREATED event."""
    sm = WalletStateMachine()
    result = sm.handle(Command(CommandType.CREATE_WALLET, {"wallet_id": "w1", "owner": "alice"}))

    assert result.success is True
    assert len(result.events) == 1
    assert "w1" in sm.wallets
    assert sm.wallets["w1"].owner == "alice"


def test_duplicate_wallet() -> None:
    """Creating a wallet with the same ID should fail."""
    sm = WalletStateMachine()
    sm.handle(Command(CommandType.CREATE_WALLET, {"wallet_id": "w1", "owner": "alice"}))
    result = sm.handle(Command(CommandType.CREATE_WALLET, {"wallet_id": "w1", "owner": "bob"}))

    assert result.success is False
    assert "already exists" in result.error


def test_deposit() -> None:
    """DEPOSIT should increase wallet balance."""
    sm = WalletStateMachine()
    sm.handle(Command(CommandType.CREATE_WALLET, {"wallet_id": "w1", "owner": "alice"}))
    result = sm.handle(Command(CommandType.DEPOSIT, {"wallet_id": "w1", "amount": 5000}))

    assert result.success is True
    assert sm.wallets["w1"].balance == 5000


def test_withdraw() -> None:
    """WITHDRAW should decrease wallet balance."""
    sm = WalletStateMachine()
    sm.handle(Command(CommandType.CREATE_WALLET, {"wallet_id": "w1", "owner": "alice"}))
    sm.handle(Command(CommandType.DEPOSIT, {"wallet_id": "w1", "amount": 10000}))
    result = sm.handle(Command(CommandType.WITHDRAW, {"wallet_id": "w1", "amount": 3000}))

    assert result.success is True
    assert sm.wallets["w1"].balance == 7000


def test_withdraw_insufficient_funds() -> None:
    """WITHDRAW exceeding balance should fail."""
    sm = WalletStateMachine()
    sm.handle(Command(CommandType.CREATE_WALLET, {"wallet_id": "w1", "owner": "alice"}))
    sm.handle(Command(CommandType.DEPOSIT, {"wallet_id": "w1", "amount": 1000}))
    result = sm.handle(Command(CommandType.WITHDRAW, {"wallet_id": "w1", "amount": 5000}))

    assert result.success is False
    assert "Insufficient funds" in result.error


def test_transfer() -> None:
    """TRANSFER should move money between wallets atomically."""
    sm = WalletStateMachine()
    sm.handle(Command(CommandType.CREATE_WALLET, {"wallet_id": "w1", "owner": "alice"}))
    sm.handle(Command(CommandType.CREATE_WALLET, {"wallet_id": "w2", "owner": "bob"}))
    sm.handle(Command(CommandType.DEPOSIT, {"wallet_id": "w1", "amount": 10000}))

    result = sm.handle(Command(
        CommandType.TRANSFER,
        {"from_wallet_id": "w1", "to_wallet_id": "w2", "amount": 4000},
    ))

    assert result.success is True
    assert sm.wallets["w1"].balance == 6000
    assert sm.wallets["w2"].balance == 4000


def test_transfer_insufficient_funds() -> None:
    """Transfer exceeding source balance should fail."""
    sm = WalletStateMachine()
    sm.handle(Command(CommandType.CREATE_WALLET, {"wallet_id": "w1", "owner": "alice"}))
    sm.handle(Command(CommandType.CREATE_WALLET, {"wallet_id": "w2", "owner": "bob"}))
    sm.handle(Command(CommandType.DEPOSIT, {"wallet_id": "w1", "amount": 1000}))

    result = sm.handle(Command(
        CommandType.TRANSFER,
        {"from_wallet_id": "w1", "to_wallet_id": "w2", "amount": 5000},
    ))

    assert result.success is False
    assert sm.wallets["w1"].balance == 1000  # unchanged


def test_transfer_self() -> None:
    """Transfer to the same wallet should fail."""
    sm = WalletStateMachine()
    sm.handle(Command(CommandType.CREATE_WALLET, {"wallet_id": "w1", "owner": "alice"}))
    sm.handle(Command(CommandType.DEPOSIT, {"wallet_id": "w1", "amount": 5000}))

    result = sm.handle(Command(
        CommandType.TRANSFER,
        {"from_wallet_id": "w1", "to_wallet_id": "w1", "amount": 1000},
    ))

    assert result.success is False


def test_snapshot_and_replay() -> None:
    """State reconstructed from events should match the original."""
    sm = WalletStateMachine()
    sm.handle(Command(CommandType.CREATE_WALLET, {"wallet_id": "w1", "owner": "alice"}))
    sm.handle(Command(CommandType.CREATE_WALLET, {"wallet_id": "w2", "owner": "bob"}))
    sm.handle(Command(CommandType.DEPOSIT, {"wallet_id": "w1", "amount": 10000}))
    sm.handle(Command(CommandType.TRANSFER, {
        "from_wallet_id": "w1", "to_wallet_id": "w2", "amount": 3000,
    }))

    # Replay from events
    events = sm.event_store.get_all_events()
    replayed = WalletStateMachine.from_events(events)

    assert replayed.wallets["w1"].balance == sm.wallets["w1"].balance
    assert replayed.wallets["w2"].balance == sm.wallets["w2"].balance


def test_snapshot_restore() -> None:
    """State restored from snapshot + subsequent events should be correct."""
    sm = WalletStateMachine()
    sm.handle(Command(CommandType.CREATE_WALLET, {"wallet_id": "w1", "owner": "alice"}))
    sm.handle(Command(CommandType.DEPOSIT, {"wallet_id": "w1", "amount": 10000}))

    snap = sm.take_snapshot()

    sm.handle(Command(CommandType.WITHDRAW, {"wallet_id": "w1", "amount": 2000}))
    events_after = sm.event_store.get_events("w1", after_version=snap.version)

    restored = WalletStateMachine.from_snapshot(snap, events_after)
    assert restored.wallets["w1"].balance == 8000


def test_ledger_invariant() -> None:
    """Ledger total balance should always be zero after any operation."""
    sm = WalletStateMachine()
    sm.handle(Command(CommandType.CREATE_WALLET, {"wallet_id": "w1", "owner": "alice"}))
    sm.handle(Command(CommandType.CREATE_WALLET, {"wallet_id": "w2", "owner": "bob"}))
    sm.handle(Command(CommandType.DEPOSIT, {"wallet_id": "w1", "amount": 10000}))
    sm.handle(Command(CommandType.TRANSFER, {
        "from_wallet_id": "w1", "to_wallet_id": "w2", "amount": 3000,
    }))
    sm.handle(Command(CommandType.WITHDRAW, {"wallet_id": "w2", "amount": 1000}))

    assert sm.ledger.total_balance() == 0
