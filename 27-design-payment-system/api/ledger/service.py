"""Double-entry ledger service.

Every transaction produces exactly two entries: a DEBIT and a CREDIT
of equal amount. The sum of all entries for a payment is always zero,
ensuring accounting integrity.

    DEBIT  (buyer account)    : -amount  (money leaves buyer)
    CREDIT (merchant account) : +amount  (money enters merchant)
"""

from __future__ import annotations

import json
import uuid

import redis.asyncio as aioredis


class LedgerService:
    """Manages double-entry bookkeeping in Redis."""

    PREFIX = "ledger:"

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    def _key(self, payment_id: str) -> str:
        return f"{self.PREFIX}{payment_id}"

    async def record(
        self,
        payment_id: str,
        buyer_account: str,
        merchant_account: str,
        amount: int,
        currency: str,
    ) -> list[dict]:
        """Record a double-entry for a payment.

        Creates two entries:
            - DEBIT on buyer_account for -amount
            - CREDIT on merchant_account for +amount

        Returns:
            List of the two ledger entry dicts.
        """
        debit_entry = {
            "entry_id": f"le_{uuid.uuid4().hex[:16]}",
            "payment_id": payment_id,
            "account": buyer_account,
            "entry_type": "DEBIT",
            "amount": -amount,
            "currency": currency,
        }
        credit_entry = {
            "entry_id": f"le_{uuid.uuid4().hex[:16]}",
            "payment_id": payment_id,
            "account": merchant_account,
            "entry_type": "CREDIT",
            "amount": amount,
            "currency": currency,
        }

        entries = [debit_entry, credit_entry]
        await self._redis.set(self._key(payment_id), json.dumps(entries))
        return entries

    async def get_entries(self, payment_id: str) -> list[dict]:
        """Retrieve ledger entries for a payment."""
        data = await self._redis.get(self._key(payment_id))
        if data is None:
            return []
        return json.loads(data)

    async def balance_check(self, payment_id: str) -> int:
        """Verify that entries for a payment sum to zero.

        Returns:
            Sum of all entry amounts (should be 0 for a valid ledger).
        """
        entries = await self.get_entries(payment_id)
        return sum(e["amount"] for e in entries)

    async def get_account_entries(self, account: str) -> list[dict]:
        """Retrieve all ledger entries for an account.

        Scans ledger keys to find entries matching the account.
        In production, this would use an index; here we scan for simplicity.
        """
        result: list[dict] = []
        cursor = 0
        while True:
            cursor, keys = await self._redis.scan(
                cursor, match=f"{self.PREFIX}*", count=100
            )
            for key in keys:
                data = await self._redis.get(key)
                if data:
                    entries = json.loads(data)
                    for entry in entries:
                        if entry["account"] == account:
                            result.append(entry)
            if cursor == 0:
                break
        return result
