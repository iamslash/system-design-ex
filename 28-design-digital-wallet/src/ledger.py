"""Double-entry accounting ledger.

Every transaction creates two entries: a debit on one account and a credit
on another. The fundamental invariant is that the sum of all entries across
all accounts is always zero.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class EntryType(Enum):
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


@dataclass(frozen=True)
class LedgerEntry:
    """A single ledger entry (one half of a double-entry pair)."""

    entry_id: str
    transaction_id: str
    account_id: str
    entry_type: EntryType
    amount: int  # always positive, direction determined by entry_type
    timestamp: datetime
    description: str = ""

    def signed_amount(self) -> int:
        """Debit is negative (money leaves), credit is positive (money arrives)."""
        return self.amount if self.entry_type == EntryType.CREDIT else -self.amount


class Ledger:
    """Double-entry accounting ledger.

    Invariant: sum of all signed amounts across all entries == 0.
    """

    def __init__(self) -> None:
        self._entries: list[LedgerEntry] = []
        self._by_account: dict[str, list[int]] = {}  # account_id -> positions
        self._by_transaction: dict[str, list[int]] = {}

    def record_transfer(
        self,
        transaction_id: str,
        from_account: str,
        to_account: str,
        amount: int,
        *,
        description: str = "",
        timestamp: datetime | None = None,
    ) -> tuple[LedgerEntry, LedgerEntry]:
        """Record a double-entry transfer: debit source, credit destination.

        Returns the (debit_entry, credit_entry) pair.
        """
        if amount <= 0:
            raise ValueError("Transfer amount must be positive")

        ts = timestamp or datetime.now(timezone.utc)

        debit_entry = LedgerEntry(
            entry_id=f"{transaction_id}-debit",
            transaction_id=transaction_id,
            account_id=from_account,
            entry_type=EntryType.DEBIT,
            amount=amount,
            timestamp=ts,
            description=description,
        )
        credit_entry = LedgerEntry(
            entry_id=f"{transaction_id}-credit",
            transaction_id=transaction_id,
            account_id=to_account,
            entry_type=EntryType.CREDIT,
            amount=amount,
            timestamp=ts,
            description=description,
        )

        for entry in (debit_entry, credit_entry):
            pos = len(self._entries)
            self._entries.append(entry)
            self._by_account.setdefault(entry.account_id, []).append(pos)
            self._by_transaction.setdefault(entry.transaction_id, []).append(pos)

        return debit_entry, credit_entry

    def record_deposit(
        self,
        transaction_id: str,
        account_id: str,
        amount: int,
        *,
        description: str = "",
        timestamp: datetime | None = None,
    ) -> tuple[LedgerEntry, LedgerEntry]:
        """Record a deposit: debit from EXTERNAL_SOURCE, credit to account."""
        return self.record_transfer(
            transaction_id=transaction_id,
            from_account="EXTERNAL_SOURCE",
            to_account=account_id,
            amount=amount,
            description=description or "deposit",
            timestamp=timestamp,
        )

    def record_withdrawal(
        self,
        transaction_id: str,
        account_id: str,
        amount: int,
        *,
        description: str = "",
        timestamp: datetime | None = None,
    ) -> tuple[LedgerEntry, LedgerEntry]:
        """Record a withdrawal: debit from account, credit to EXTERNAL_SINK."""
        return self.record_transfer(
            transaction_id=transaction_id,
            from_account=account_id,
            to_account="EXTERNAL_SINK",
            amount=amount,
            description=description or "withdrawal",
            timestamp=timestamp,
        )

    def account_balance(self, account_id: str) -> int:
        """Compute balance by summing all signed entries for an account."""
        positions = self._by_account.get(account_id, [])
        return sum(self._entries[p].signed_amount() for p in positions)

    def total_balance(self) -> int:
        """Sum of all signed entries. Must always be zero."""
        return sum(e.signed_amount() for e in self._entries)

    def get_entries(self, account_id: str | None = None) -> list[LedgerEntry]:
        """Get entries, optionally filtered by account."""
        if account_id is not None:
            positions = self._by_account.get(account_id, [])
            return [self._entries[p] for p in positions]
        return list(self._entries)

    def get_transaction_entries(self, transaction_id: str) -> list[LedgerEntry]:
        """Get both entries of a transaction."""
        positions = self._by_transaction.get(transaction_id, [])
        return [self._entries[p] for p in positions]

    def __len__(self) -> int:
        return len(self._entries)
