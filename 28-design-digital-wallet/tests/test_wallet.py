"""Tests for the Wallet data model."""

import pytest
from src.wallet import InsufficientFundsError, Wallet


def test_deposit() -> None:
    """Deposit should increase balance."""
    w = Wallet(wallet_id="w1", owner="alice", balance=0)
    w.deposit(5000)
    assert w.balance == 5000


def test_withdraw() -> None:
    """Withdraw should decrease balance."""
    w = Wallet(wallet_id="w1", owner="alice", balance=10000)
    w.withdraw(3000)
    assert w.balance == 7000


def test_withdraw_insufficient() -> None:
    """Withdraw more than balance should raise InsufficientFundsError."""
    w = Wallet(wallet_id="w1", owner="alice", balance=1000)
    with pytest.raises(InsufficientFundsError):
        w.withdraw(5000)


def test_deposit_negative() -> None:
    """Negative deposit should raise ValueError."""
    w = Wallet(wallet_id="w1", owner="alice")
    with pytest.raises(ValueError, match="positive"):
        w.deposit(-100)


def test_withdraw_negative() -> None:
    """Negative withdrawal should raise ValueError."""
    w = Wallet(wallet_id="w1", owner="alice", balance=1000)
    with pytest.raises(ValueError, match="positive"):
        w.withdraw(-100)


def test_has_sufficient_funds() -> None:
    w = Wallet(wallet_id="w1", owner="alice", balance=5000)
    assert w.has_sufficient_funds(5000) is True
    assert w.has_sufficient_funds(5001) is False


def test_balance_dollars() -> None:
    """balance_dollars should return Decimal dollars."""
    from decimal import Decimal

    w = Wallet(wallet_id="w1", owner="alice", balance=12345)
    assert w.balance_dollars() == Decimal("123.45")
