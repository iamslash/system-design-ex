"""Shared fixtures for payment system tests."""

from __future__ import annotations

import os
import sys

import pytest_asyncio
import fakeredis.aioredis

# Add the api directory to the path so we can import the service modules.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

from payment.executor import PaymentExecutor
from payment.idempotency import IdempotencyStore
from payment.service import PaymentService
from ledger.service import LedgerService
from wallet.service import WalletService


@pytest_asyncio.fixture
async def redis_client() -> fakeredis.aioredis.FakeRedis:
    """Create a fake async Redis client for testing."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.flushall()
    await client.aclose()


@pytest_asyncio.fixture
async def executor() -> PaymentExecutor:
    """Create a payment executor that always succeeds."""
    return PaymentExecutor(failure_rate=0.0)


@pytest_asyncio.fixture
async def failing_executor() -> PaymentExecutor:
    """Create a payment executor that always fails."""
    return PaymentExecutor(failure_rate=1.0)


@pytest_asyncio.fixture
async def idempotency(redis_client: fakeredis.aioredis.FakeRedis) -> IdempotencyStore:
    """Create an idempotency store."""
    return IdempotencyStore(redis_client)


@pytest_asyncio.fixture
async def ledger(redis_client: fakeredis.aioredis.FakeRedis) -> LedgerService:
    """Create a ledger service."""
    return LedgerService(redis_client)


@pytest_asyncio.fixture
async def wallet(redis_client: fakeredis.aioredis.FakeRedis) -> WalletService:
    """Create a wallet service."""
    return WalletService(redis_client)


@pytest_asyncio.fixture
async def payment_service(
    redis_client: fakeredis.aioredis.FakeRedis,
    executor: PaymentExecutor,
    idempotency: IdempotencyStore,
    ledger: LedgerService,
    wallet: WalletService,
) -> PaymentService:
    """Create a payment service with all dependencies."""
    return PaymentService(
        redis=redis_client,
        executor=executor,
        idempotency=idempotency,
        ledger=ledger,
        wallet=wallet,
    )


@pytest_asyncio.fixture
async def failing_payment_service(
    redis_client: fakeredis.aioredis.FakeRedis,
    failing_executor: PaymentExecutor,
    idempotency: IdempotencyStore,
    ledger: LedgerService,
    wallet: WalletService,
) -> PaymentService:
    """Create a payment service that always fails PSP calls."""
    return PaymentService(
        redis=redis_client,
        executor=failing_executor,
        idempotency=idempotency,
        ledger=ledger,
        wallet=wallet,
    )
