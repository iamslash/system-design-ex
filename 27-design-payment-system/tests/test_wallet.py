"""Tests for the merchant wallet service."""

from __future__ import annotations

import pytest

from wallet.service import WalletService


@pytest.mark.asyncio
async def test_credit_increases_balance(wallet: WalletService) -> None:
    """Crediting a merchant should increase their balance."""
    new_balance = await wallet.credit("merchant_1", 5000, "USD")
    assert new_balance == 5000

    new_balance = await wallet.credit("merchant_1", 3000, "USD")
    assert new_balance == 8000


@pytest.mark.asyncio
async def test_debit_decreases_balance(wallet: WalletService) -> None:
    """Debiting a merchant should decrease their balance."""
    await wallet.credit("merchant_1", 10000, "USD")
    new_balance = await wallet.debit("merchant_1", 4000, "USD")
    assert new_balance == 6000


@pytest.mark.asyncio
async def test_debit_insufficient_funds(wallet: WalletService) -> None:
    """Debiting more than available should raise ValueError."""
    await wallet.credit("merchant_1", 1000, "USD")
    with pytest.raises(ValueError, match="Insufficient balance"):
        await wallet.debit("merchant_1", 5000, "USD")


@pytest.mark.asyncio
async def test_get_balance_default_zero(wallet: WalletService) -> None:
    """New merchant should have zero balance."""
    balance = await wallet.get_balance("new_merchant")
    assert balance == 0


@pytest.mark.asyncio
async def test_multi_currency(wallet: WalletService) -> None:
    """Different currencies should have independent balances."""
    await wallet.credit("merchant_1", 5000, "USD")
    await wallet.credit("merchant_1", 3000, "EUR")

    usd = await wallet.get_balance("merchant_1", "USD")
    eur = await wallet.get_balance("merchant_1", "EUR")

    assert usd == 5000
    assert eur == 3000
