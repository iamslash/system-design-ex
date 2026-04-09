"""Payment service — orchestrates the pay-in flow.

Flow: validate -> check idempotency -> execute PSP -> update wallet -> record ledger.

Payment status lifecycle:
    NOT_STARTED -> EXECUTING -> SUCCESS
                             -> FAILED (can be retried)
"""

from __future__ import annotations

import asyncio
import json
import uuid

import redis.asyncio as aioredis

from payment.executor import PaymentExecutor, PSPResult
from payment.idempotency import IdempotencyStore
from ledger.service import LedgerService
from wallet.service import WalletService


class PaymentService:
    """Orchestrates the full pay-in flow."""

    PREFIX = "payment:"

    def __init__(
        self,
        redis: aioredis.Redis,
        executor: PaymentExecutor,
        idempotency: IdempotencyStore,
        ledger: LedgerService,
        wallet: WalletService,
    ) -> None:
        self._redis = redis
        self._executor = executor
        self._idempotency = idempotency
        self._ledger = ledger
        self._wallet = wallet

    def _key(self, payment_id: str) -> str:
        return f"{self.PREFIX}{payment_id}"

    async def _store_payment(self, payment: dict) -> None:
        """Persist payment record to Redis."""
        await self._redis.set(self._key(payment["payment_id"]), json.dumps(payment))

    async def get_payment(self, payment_id: str) -> dict | None:
        """Retrieve a payment by ID."""
        data = await self._redis.get(self._key(payment_id))
        return json.loads(data) if data else None

    async def create_payment(
        self,
        buyer_id: str,
        merchant_id: str,
        amount: int,
        currency: str,
        payment_method: str,
        idempotency_key: str,
    ) -> dict:
        """Create and process a payment.

        Args:
            buyer_id: Buyer identifier.
            merchant_id: Merchant identifier.
            amount: Amount in cents.
            currency: ISO 4217 currency code.
            payment_method: Payment method type.
            idempotency_key: Client-provided idempotency key.

        Returns:
            Payment record dict.

        Raises:
            ValueError: If amount is not positive.
        """
        # 1. Validate
        if amount <= 0:
            raise ValueError("Amount must be positive")

        payment_id = f"pay_{uuid.uuid4().hex[:16]}"

        payment = {
            "payment_id": payment_id,
            "buyer_id": buyer_id,
            "merchant_id": merchant_id,
            "amount": amount,
            "currency": currency,
            "payment_method": payment_method,
            "status": "NOT_STARTED",
            "idempotency_key": idempotency_key,
            "psp_reference": None,
        }

        # 2. Check idempotency
        existing = await self._idempotency.check_and_set(idempotency_key, payment)
        if existing is not None:
            return existing

        # 3. Transition to EXECUTING
        payment["status"] = "EXECUTING"
        await self._store_payment(payment)
        await self._idempotency.update(idempotency_key, payment)

        # 4. Execute PSP call
        psp_response = await self._executor.execute(amount, currency, payment_method)
        payment["psp_reference"] = psp_response.psp_reference

        if psp_response.result == PSPResult.FAILED:
            payment["status"] = "FAILED"
            await self._store_payment(payment)
            await self._idempotency.update(idempotency_key, payment)
            return payment

        # 5. Update wallet (credit merchant)
        await self._wallet.credit(merchant_id, amount, currency)

        # 6. Record double-entry ledger
        await self._ledger.record(
            payment_id=payment_id,
            buyer_account=f"buyer:{buyer_id}",
            merchant_account=f"merchant:{merchant_id}",
            amount=amount,
            currency=currency,
        )

        # 7. Mark SUCCESS
        payment["status"] = "SUCCESS"
        await self._store_payment(payment)
        await self._idempotency.update(idempotency_key, payment)

        return payment

    async def retry_payment(
        self, payment_id: str, max_retries: int = 3
    ) -> dict:
        """Retry a failed payment with exponential backoff.

        Args:
            payment_id: ID of the failed payment.
            max_retries: Maximum number of retry attempts.

        Returns:
            Updated payment record.

        Raises:
            ValueError: If payment not found or not in FAILED status.
        """
        payment = await self.get_payment(payment_id)
        if payment is None:
            raise ValueError(f"Payment {payment_id} not found")
        if payment["status"] != "FAILED":
            raise ValueError(
                f"Payment {payment_id} is in {payment['status']} status, "
                "only FAILED payments can be retried"
            )

        for attempt in range(max_retries):
            # Exponential backoff: 0.1s, 0.2s, 0.4s, ...
            if attempt > 0:
                delay = 0.1 * (2 ** (attempt - 1))
                await asyncio.sleep(delay)

            payment["status"] = "EXECUTING"
            await self._store_payment(payment)

            psp_response = await self._executor.execute(
                payment["amount"],
                payment["currency"],
                payment["payment_method"],
            )
            payment["psp_reference"] = psp_response.psp_reference

            if psp_response.result == PSPResult.SUCCESS:
                # Update wallet and ledger on success
                await self._wallet.credit(
                    payment["merchant_id"],
                    payment["amount"],
                    payment["currency"],
                )
                await self._ledger.record(
                    payment_id=payment["payment_id"],
                    buyer_account=f"buyer:{payment['buyer_id']}",
                    merchant_account=f"merchant:{payment['merchant_id']}",
                    amount=payment["amount"],
                    currency=payment["currency"],
                )
                payment["status"] = "SUCCESS"
                await self._store_payment(payment)
                await self._idempotency.update(
                    payment["idempotency_key"], payment
                )
                return payment

        # All retries exhausted
        payment["status"] = "FAILED"
        await self._store_payment(payment)
        await self._idempotency.update(payment["idempotency_key"], payment)
        return payment
