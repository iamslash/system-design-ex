"""Demo script for the payment system.

Simulates the pay-in flow: create payment, verify ledger double-entry,
check wallet balance, and test idempotency.

Usage:
    python scripts/demo.py
"""

from __future__ import annotations

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

import fakeredis.aioredis

from payment.executor import PaymentExecutor
from payment.idempotency import IdempotencyStore
from payment.service import PaymentService
from ledger.service import LedgerService
from wallet.service import WalletService


async def main() -> None:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    executor = PaymentExecutor(failure_rate=0.0)
    idempotency = IdempotencyStore(redis)
    ledger = LedgerService(redis)
    wallet = WalletService(redis)
    payment_svc = PaymentService(
        redis=redis,
        executor=executor,
        idempotency=idempotency,
        ledger=ledger,
        wallet=wallet,
    )

    print("=" * 60)
    print("Payment System Demo")
    print("=" * 60)

    # 1. Create a payment
    print("\n[1] Creating payment: buyer_alice -> merchant_shop, $50.00")
    result = await payment_svc.create_payment(
        buyer_id="alice",
        merchant_id="shop",
        amount=5000,
        currency="USD",
        payment_method="CREDIT_CARD",
        idempotency_key="demo-pay-001",
    )
    print(f"    Payment ID : {result['payment_id']}")
    print(f"    Status     : {result['status']}")
    print(f"    PSP Ref    : {result['psp_reference']}")

    # 2. Verify double-entry ledger
    print("\n[2] Verifying double-entry ledger")
    entries = await ledger.get_entries(result["payment_id"])
    for e in entries:
        sign = "+" if e["amount"] > 0 else ""
        print(f"    {e['entry_type']:6s}  {e['account']:25s}  {sign}{e['amount']} {e['currency']}")
    balance = await ledger.balance_check(result["payment_id"])
    print(f"    Sum of entries: {balance} (should be 0)")

    # 3. Check wallet balance
    print("\n[3] Merchant wallet balance")
    bal = await wallet.get_balance("shop", "USD")
    print(f"    merchant:shop  =  ${bal / 100:.2f}")

    # 4. Test idempotency
    print("\n[4] Testing idempotency (same key again)")
    dup = await payment_svc.create_payment(
        buyer_id="alice",
        merchant_id="shop",
        amount=5000,
        currency="USD",
        payment_method="CREDIT_CARD",
        idempotency_key="demo-pay-001",
    )
    print(f"    Same payment? {dup['payment_id'] == result['payment_id']}")
    bal2 = await wallet.get_balance("shop", "USD")
    print(f"    Wallet unchanged? ${bal2 / 100:.2f} (no double charge)")

    # 5. Second payment
    print("\n[5] Second payment: buyer_bob -> merchant_shop, $25.00")
    result2 = await payment_svc.create_payment(
        buyer_id="bob",
        merchant_id="shop",
        amount=2500,
        currency="USD",
        payment_method="DEBIT_CARD",
        idempotency_key="demo-pay-002",
    )
    print(f"    Status: {result2['status']}")
    bal3 = await wallet.get_balance("shop", "USD")
    print(f"    Total wallet: ${bal3 / 100:.2f}")

    print("\n" + "=" * 60)
    print("Demo complete!")
    print("=" * 60)

    await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
