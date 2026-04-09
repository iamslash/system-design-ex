"""FastAPI application entry point for the payment system."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException

from config import settings
from models import (
    LedgerResponse,
    PaymentRequest,
    PaymentResponse,
    PaymentStatus,
    RetryPaymentRequest,
    WalletResponse,
)
from payment.executor import PaymentExecutor
from payment.idempotency import IdempotencyStore
from payment.service import PaymentService
from ledger.service import LedgerService
from wallet.service import WalletService


# ---------------------------------------------------------------------------
# Application lifespan — manage Redis connection pool
# ---------------------------------------------------------------------------

_redis: aioredis.Redis | None = None
_payment_svc: PaymentService | None = None
_ledger_svc: LedgerService | None = None
_wallet_svc: WalletService | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open and close the Redis connection pool around the app lifetime."""
    global _redis, _payment_svc, _ledger_svc, _wallet_svc
    _redis = aioredis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        decode_responses=True,
    )
    executor = PaymentExecutor(failure_rate=settings.PSP_FAILURE_RATE)
    idempotency = IdempotencyStore(_redis)
    _ledger_svc = LedgerService(_redis)
    _wallet_svc = WalletService(_redis)
    _payment_svc = PaymentService(
        redis=_redis,
        executor=executor,
        idempotency=idempotency,
        ledger=_ledger_svc,
        wallet=_wallet_svc,
    )
    yield
    await _redis.aclose()


app = FastAPI(title="Payment System", version="1.0.0", lifespan=lifespan)


def _pay() -> PaymentService:
    assert _payment_svc is not None, "Application not started"
    return _payment_svc


def _ledger() -> LedgerService:
    assert _ledger_svc is not None, "Application not started"
    return _ledger_svc


def _wallet() -> WalletService:
    assert _wallet_svc is not None, "Application not started"
    return _wallet_svc


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Payment endpoints
# ---------------------------------------------------------------------------


@app.post("/v1/payments", response_model=PaymentResponse, status_code=201)
async def create_payment(body: PaymentRequest) -> PaymentResponse:
    """Create and process a payment."""
    try:
        result = await _pay().create_payment(
            buyer_id=body.buyer_id,
            merchant_id=body.merchant_id,
            amount=body.amount,
            currency=body.currency,
            payment_method=body.payment_method.value,
            idempotency_key=body.idempotency_key,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return PaymentResponse(**result)


@app.get("/v1/payments/{payment_id}", response_model=PaymentResponse)
async def get_payment(payment_id: str) -> PaymentResponse:
    """Retrieve a payment by ID."""
    result = await _pay().get_payment(payment_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Payment '{payment_id}' not found")
    return PaymentResponse(**result)


@app.post("/v1/payments/retry", response_model=PaymentResponse)
async def retry_payment(body: RetryPaymentRequest) -> PaymentResponse:
    """Retry a failed payment with exponential backoff."""
    try:
        result = await _pay().retry_payment(
            payment_id=body.payment_id,
            max_retries=body.max_retries,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return PaymentResponse(**result)


# ---------------------------------------------------------------------------
# Ledger endpoints
# ---------------------------------------------------------------------------


@app.get("/v1/ledger/{payment_id}", response_model=LedgerResponse)
async def get_ledger(payment_id: str) -> LedgerResponse:
    """Retrieve ledger entries for a payment."""
    entries = await _ledger().get_entries(payment_id)
    if not entries:
        raise HTTPException(
            status_code=404, detail=f"No ledger entries for payment '{payment_id}'"
        )
    balance = sum(e["amount"] for e in entries)
    return LedgerResponse(
        payment_id=payment_id,
        entries=entries,
        balance_check=balance,
    )


# ---------------------------------------------------------------------------
# Wallet endpoints
# ---------------------------------------------------------------------------


@app.get("/v1/wallets/{merchant_id}", response_model=WalletResponse)
async def get_wallet(merchant_id: str, currency: str = "USD") -> WalletResponse:
    """Get a merchant's wallet balance."""
    balance = await _wallet().get_balance(merchant_id, currency)
    return WalletResponse(
        merchant_id=merchant_id,
        balance=balance,
        currency=currency,
    )
