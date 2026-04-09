"""Payment executor — simulated PSP (Payment Service Provider) integration.

Mimics a Stripe-like PSP that processes credit card charges. The failure
rate is configurable via PSP_FAILURE_RATE (0.0 = always succeed, 1.0 = always fail).
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from enum import Enum


class PSPResult(str, Enum):
    """Result from the PSP call."""

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


@dataclass
class PSPResponse:
    """Response from the simulated PSP."""

    result: PSPResult
    psp_reference: str
    reason: str | None = None


class PaymentExecutor:
    """Simulated PSP integration for processing payments."""

    def __init__(self, failure_rate: float = 0.0) -> None:
        """Initialize with configurable failure rate.

        Args:
            failure_rate: Probability of payment failure (0.0 to 1.0).
        """
        if not 0.0 <= failure_rate <= 1.0:
            raise ValueError("failure_rate must be between 0.0 and 1.0")
        self._failure_rate = failure_rate

    async def execute(
        self,
        amount: int,
        currency: str,
        payment_method: str,
    ) -> PSPResponse:
        """Execute a payment through the simulated PSP.

        Args:
            amount: Amount in cents.
            currency: ISO 4217 currency code.
            payment_method: Payment method type.

        Returns:
            PSPResponse with success/failure and a reference ID.
        """
        psp_reference = f"psp_{uuid.uuid4().hex[:16]}"

        if random.random() < self._failure_rate:
            return PSPResponse(
                result=PSPResult.FAILED,
                psp_reference=psp_reference,
                reason="Payment declined by issuer",
            )

        return PSPResponse(
            result=PSPResult.SUCCESS,
            psp_reference=psp_reference,
        )
