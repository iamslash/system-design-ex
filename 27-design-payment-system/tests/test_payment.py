"""Tests for the payment service pay-in flow."""

from __future__ import annotations

import pytest

from payment.service import PaymentService


@pytest.mark.asyncio
async def test_create_payment_success(payment_service: PaymentService) -> None:
    """Successful payment should transition NOT_STARTED -> EXECUTING -> SUCCESS."""
    result = await payment_service.create_payment(
        buyer_id="buyer_1",
        merchant_id="merchant_1",
        amount=5000,
        currency="USD",
        payment_method="CREDIT_CARD",
        idempotency_key="idem-001",
    )

    assert result["status"] == "SUCCESS"
    assert result["buyer_id"] == "buyer_1"
    assert result["merchant_id"] == "merchant_1"
    assert result["amount"] == 5000
    assert result["psp_reference"] is not None


@pytest.mark.asyncio
async def test_idempotency_prevents_duplicate(payment_service: PaymentService) -> None:
    """Same idempotency key should return the original payment, not create a new one."""
    first = await payment_service.create_payment(
        buyer_id="buyer_1",
        merchant_id="merchant_1",
        amount=5000,
        currency="USD",
        payment_method="CREDIT_CARD",
        idempotency_key="idem-dup",
    )
    second = await payment_service.create_payment(
        buyer_id="buyer_1",
        merchant_id="merchant_1",
        amount=5000,
        currency="USD",
        payment_method="CREDIT_CARD",
        idempotency_key="idem-dup",
    )

    assert first["payment_id"] == second["payment_id"]


@pytest.mark.asyncio
async def test_get_payment(payment_service: PaymentService) -> None:
    """Should retrieve a stored payment by ID."""
    created = await payment_service.create_payment(
        buyer_id="buyer_1",
        merchant_id="merchant_1",
        amount=1000,
        currency="USD",
        payment_method="CREDIT_CARD",
        idempotency_key="idem-get",
    )

    fetched = await payment_service.get_payment(created["payment_id"])
    assert fetched is not None
    assert fetched["payment_id"] == created["payment_id"]
    assert fetched["status"] == "SUCCESS"


@pytest.mark.asyncio
async def test_get_payment_not_found(payment_service: PaymentService) -> None:
    """Should return None for a non-existent payment."""
    result = await payment_service.get_payment("pay_nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_failed_payment(failing_payment_service: PaymentService) -> None:
    """Payment with a failing PSP should end in FAILED status."""
    result = await failing_payment_service.create_payment(
        buyer_id="buyer_1",
        merchant_id="merchant_1",
        amount=2000,
        currency="USD",
        payment_method="CREDIT_CARD",
        idempotency_key="idem-fail",
    )

    assert result["status"] == "FAILED"


@pytest.mark.asyncio
async def test_retry_failed_payment(failing_payment_service: PaymentService, payment_service: PaymentService) -> None:
    """Retry should attempt to re-execute a FAILED payment."""
    failed = await failing_payment_service.create_payment(
        buyer_id="buyer_1",
        merchant_id="merchant_1",
        amount=3000,
        currency="USD",
        payment_method="CREDIT_CARD",
        idempotency_key="idem-retry",
    )
    assert failed["status"] == "FAILED"

    # Retry with the always-failing service — should remain FAILED
    retried = await failing_payment_service.retry_payment(
        payment_id=failed["payment_id"], max_retries=1
    )
    assert retried["status"] == "FAILED"


@pytest.mark.asyncio
async def test_retry_non_failed_payment_raises(payment_service: PaymentService) -> None:
    """Retry on a SUCCESS payment should raise ValueError."""
    created = await payment_service.create_payment(
        buyer_id="buyer_1",
        merchant_id="merchant_1",
        amount=1000,
        currency="USD",
        payment_method="CREDIT_CARD",
        idempotency_key="idem-retry-err",
    )

    with pytest.raises(ValueError, match="only FAILED payments"):
        await payment_service.retry_payment(created["payment_id"])


@pytest.mark.asyncio
async def test_invalid_amount(payment_service: PaymentService) -> None:
    """Negative amount should raise ValueError."""
    with pytest.raises(ValueError, match="Amount must be positive"):
        await payment_service.create_payment(
            buyer_id="buyer_1",
            merchant_id="merchant_1",
            amount=-100,
            currency="USD",
            payment_method="CREDIT_CARD",
            idempotency_key="idem-neg",
        )
