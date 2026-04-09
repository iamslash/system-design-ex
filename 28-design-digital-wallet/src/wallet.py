"""Digital wallet with balance management.

A wallet holds a balance denominated in integer cents to avoid floating-point
issues. Wallets interact through the state machine which validates commands
and produces events.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal


@dataclass
class Wallet:
    """A single digital wallet account."""

    wallet_id: str
    owner: str
    balance: int = 0  # stored in cents to avoid floating-point errors

    def deposit(self, amount: int) -> None:
        """Add funds. Amount must be positive (in cents)."""
        if amount <= 0:
            raise ValueError("Deposit amount must be positive")
        self.balance += amount

    def withdraw(self, amount: int) -> None:
        """Remove funds. Raises if insufficient balance."""
        if amount <= 0:
            raise ValueError("Withdrawal amount must be positive")
        if amount > self.balance:
            raise InsufficientFundsError(
                f"Wallet {self.wallet_id}: requested {amount}, available {self.balance}"
            )
        self.balance -= amount

    def has_sufficient_funds(self, amount: int) -> bool:
        return self.balance >= amount

    def balance_dollars(self) -> Decimal:
        """Return balance as a Decimal dollar amount."""
        return Decimal(self.balance) / Decimal(100)

    def to_dict(self) -> dict:
        return {
            "wallet_id": self.wallet_id,
            "owner": self.owner,
            "balance": self.balance,
            "balance_dollars": str(self.balance_dollars()),
        }


class InsufficientFundsError(Exception):
    """Raised when a wallet has insufficient funds for an operation."""
