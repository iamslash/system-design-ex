"""Pydantic models for API request/response schemas."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class PaymentStatus(str, Enum):
    """Payment status lifecycle: NOT_STARTED -> EXECUTING -> SUCCESS/FAILED."""

    NOT_STARTED = "NOT_STARTED"
    EXECUTING = "EXECUTING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class PaymentMethod(str, Enum):
    """Supported payment methods."""

    CREDIT_CARD = "CREDIT_CARD"
    DEBIT_CARD = "DEBIT_CARD"


class PaymentRequest(BaseModel):
    """Request body for creating a payment."""

    buyer_id: str = Field(..., min_length=1, description="Buyer identifier")
    merchant_id: str = Field(..., min_length=1, description="Merchant identifier")
    amount: int = Field(..., gt=0, description="Amount in cents")
    currency: str = Field(default="USD", min_length=3, max_length=3)
    payment_method: PaymentMethod = Field(default=PaymentMethod.CREDIT_CARD)
    idempotency_key: str = Field(..., min_length=1, description="Client-provided idempotency key")


class PaymentResponse(BaseModel):
    """Response after creating or querying a payment."""

    payment_id: str
    buyer_id: str
    merchant_id: str
    amount: int
    currency: str
    status: PaymentStatus
    idempotency_key: str
    psp_reference: str | None = None


class LedgerEntry(BaseModel):
    """A single ledger entry (debit or credit)."""

    entry_id: str
    payment_id: str
    account: str
    entry_type: str = Field(..., description="DEBIT or CREDIT")
    amount: int
    currency: str


class LedgerResponse(BaseModel):
    """Response containing ledger entries for a payment."""

    payment_id: str
    entries: list[LedgerEntry]
    balance_check: int = Field(..., description="Sum of all entries (should be 0)")


class WalletResponse(BaseModel):
    """Response for a merchant wallet balance query."""

    merchant_id: str
    balance: int
    currency: str


class RetryPaymentRequest(BaseModel):
    """Request body for retrying a failed payment."""

    payment_id: str = Field(..., min_length=1, description="Payment ID to retry")
    max_retries: int = Field(default=3, ge=1, le=10)
