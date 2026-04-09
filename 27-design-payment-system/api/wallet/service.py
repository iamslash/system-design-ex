"""Merchant wallet service.

Tracks merchant balances in Redis. Each merchant has a balance per currency.
Credits increase the balance (pay-in), debits decrease it (pay-out/withdrawal).
"""

from __future__ import annotations

import redis.asyncio as aioredis


class WalletService:
    """Manages merchant wallet balances in Redis."""

    PREFIX = "wallet:"

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    def _key(self, merchant_id: str, currency: str) -> str:
        return f"{self.PREFIX}{merchant_id}:{currency}"

    async def credit(self, merchant_id: str, amount: int, currency: str) -> int:
        """Credit (add) amount to a merchant's wallet.

        Args:
            merchant_id: Merchant identifier.
            amount: Amount in cents to add.
            currency: ISO 4217 currency code.

        Returns:
            New balance after credit.
        """
        new_balance = await self._redis.incrby(
            self._key(merchant_id, currency), amount
        )
        return int(new_balance)

    async def debit(self, merchant_id: str, amount: int, currency: str) -> int:
        """Debit (subtract) amount from a merchant's wallet.

        Args:
            merchant_id: Merchant identifier.
            amount: Amount in cents to subtract.
            currency: ISO 4217 currency code.

        Returns:
            New balance after debit.

        Raises:
            ValueError: If insufficient balance.
        """
        current = await self.get_balance(merchant_id, currency)
        if current < amount:
            raise ValueError(
                f"Insufficient balance: {current} < {amount} for merchant {merchant_id}"
            )
        new_balance = await self._redis.decrby(
            self._key(merchant_id, currency), amount
        )
        return int(new_balance)

    async def get_balance(self, merchant_id: str, currency: str = "USD") -> int:
        """Get a merchant's wallet balance.

        Args:
            merchant_id: Merchant identifier.
            currency: ISO 4217 currency code.

        Returns:
            Current balance in cents.
        """
        balance = await self._redis.get(self._key(merchant_id, currency))
        return int(balance) if balance else 0
