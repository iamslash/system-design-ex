"""Tests for the double-entry ledger service."""

from __future__ import annotations

import pytest

from ledger.service import LedgerService


@pytest.mark.asyncio
async def test_record_creates_two_entries(ledger: LedgerService):
    """A payment should produce exactly one DEBIT and one CREDIT entry."""
    entries = await ledger.record(
        payment_id="pay_001",
        buyer_account="buyer:alice",
        merchant_account="merchant:shop",
        amount=5000,
        currency="USD",
    )

    assert len(entries) == 2
    debit = entries[0]
    credit = entries[1]
    assert debit["entry_type"] == "DEBIT"
    assert debit["amount"] == -5000
    assert credit["entry_type"] == "CREDIT"
    assert credit["amount"] == 5000


@pytest.mark.asyncio
async def test_balance_check_is_zero(ledger: LedgerService):
    """Sum of DEBIT + CREDIT for a payment should always be zero."""
    await ledger.record(
        payment_id="pay_002",
        buyer_account="buyer:bob",
        merchant_account="merchant:shop",
        amount=3000,
        currency="USD",
    )

    balance = await ledger.balance_check("pay_002")
    assert balance == 0


@pytest.mark.asyncio
async def test_get_entries(ledger: LedgerService):
    """Should retrieve stored entries by payment ID."""
    await ledger.record(
        payment_id="pay_003",
        buyer_account="buyer:carol",
        merchant_account="merchant:store",
        amount=1000,
        currency="USD",
    )

    entries = await ledger.get_entries("pay_003")
    assert len(entries) == 2


@pytest.mark.asyncio
async def test_get_entries_empty(ledger: LedgerService):
    """Should return empty list for a non-existent payment."""
    entries = await ledger.get_entries("pay_nonexistent")
    assert entries == []
