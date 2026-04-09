"""State machine: command validation, event generation, state update.

The state machine is the core orchestrator. It receives commands (requests),
validates them against current state, generates events on success, and
applies those events to update state. This ensures a clean separation between
intent (commands), facts (events), and current reality (state).

Flow: Command -> Validate -> Generate Event(s) -> Apply to State
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from .event_store import Event, EventStore, EventType
from .ledger import Ledger
from .wallet import InsufficientFundsError, Wallet


class CommandType(Enum):
    CREATE_WALLET = "CREATE_WALLET"
    DEPOSIT = "DEPOSIT"
    WITHDRAW = "WITHDRAW"
    TRANSFER = "TRANSFER"


@dataclass
class Command:
    """A request to change state. Commands can be rejected."""

    command_type: CommandType
    data: dict[str, Any]
    idempotency_key: str | None = None


@dataclass
class CommandResult:
    """Result of processing a command."""

    success: bool
    events: list[Event]
    error: str | None = None


@dataclass
class Snapshot:
    """A point-in-time snapshot of all wallet balances."""

    snapshot_id: str
    timestamp: datetime
    version: int  # event store length at snapshot time
    balances: dict[str, int]  # wallet_id -> balance
    owners: dict[str, str]  # wallet_id -> owner


class WalletStateMachine:
    """Processes commands, produces events, maintains state.

    The state machine owns the wallets, event store, and ledger. All
    mutations go through commands.
    """

    def __init__(self) -> None:
        self.event_store = EventStore()
        self.ledger = Ledger()
        self.wallets: dict[str, Wallet] = {}
        self._snapshots: list[Snapshot] = []

    # -- Command dispatch --------------------------------------------------

    def handle(self, command: Command) -> CommandResult:
        """Validate and execute a command."""
        handler = {
            CommandType.CREATE_WALLET: self._handle_create_wallet,
            CommandType.DEPOSIT: self._handle_deposit,
            CommandType.WITHDRAW: self._handle_withdraw,
            CommandType.TRANSFER: self._handle_transfer,
        }.get(command.command_type)

        if handler is None:
            return CommandResult(success=False, events=[], error="Unknown command type")

        return handler(command)

    # -- Command handlers --------------------------------------------------

    def _handle_create_wallet(self, cmd: Command) -> CommandResult:
        wallet_id = cmd.data.get("wallet_id", str(uuid.uuid4()))
        owner = cmd.data.get("owner", "anonymous")

        if wallet_id in self.wallets:
            return CommandResult(
                success=False, events=[], error=f"Wallet {wallet_id} already exists"
            )

        event = self.event_store.append(
            EventType.WALLET_CREATED,
            aggregate_id=wallet_id,
            data={"wallet_id": wallet_id, "owner": owner},
            idempotency_key=cmd.idempotency_key,
        )
        self._apply_event(event)
        return CommandResult(success=True, events=[event])

    def _handle_deposit(self, cmd: Command) -> CommandResult:
        wallet_id = cmd.data["wallet_id"]
        amount = cmd.data["amount"]

        if wallet_id not in self.wallets:
            return CommandResult(
                success=False, events=[], error=f"Wallet {wallet_id} not found"
            )
        if amount <= 0:
            return CommandResult(
                success=False, events=[], error="Amount must be positive"
            )

        transaction_id = str(uuid.uuid4())
        event = self.event_store.append(
            EventType.MONEY_DEPOSITED,
            aggregate_id=wallet_id,
            data={
                "wallet_id": wallet_id,
                "amount": amount,
                "transaction_id": transaction_id,
            },
            idempotency_key=cmd.idempotency_key,
        )
        self._apply_event(event)
        return CommandResult(success=True, events=[event])

    def _handle_withdraw(self, cmd: Command) -> CommandResult:
        wallet_id = cmd.data["wallet_id"]
        amount = cmd.data["amount"]

        if wallet_id not in self.wallets:
            return CommandResult(
                success=False, events=[], error=f"Wallet {wallet_id} not found"
            )
        if amount <= 0:
            return CommandResult(
                success=False, events=[], error="Amount must be positive"
            )

        wallet = self.wallets[wallet_id]
        if not wallet.has_sufficient_funds(amount):
            return CommandResult(
                success=False,
                events=[],
                error=f"Insufficient funds: requested {amount}, available {wallet.balance}",
            )

        transaction_id = str(uuid.uuid4())
        event = self.event_store.append(
            EventType.MONEY_WITHDRAWN,
            aggregate_id=wallet_id,
            data={
                "wallet_id": wallet_id,
                "amount": amount,
                "transaction_id": transaction_id,
            },
            idempotency_key=cmd.idempotency_key,
        )
        self._apply_event(event)
        return CommandResult(success=True, events=[event])

    def _handle_transfer(self, cmd: Command) -> CommandResult:
        from_id = cmd.data["from_wallet_id"]
        to_id = cmd.data["to_wallet_id"]
        amount = cmd.data["amount"]

        # Validate
        if from_id not in self.wallets:
            return CommandResult(
                success=False, events=[], error=f"Source wallet {from_id} not found"
            )
        if to_id not in self.wallets:
            return CommandResult(
                success=False, events=[], error=f"Destination wallet {to_id} not found"
            )
        if from_id == to_id:
            return CommandResult(
                success=False, events=[], error="Cannot transfer to the same wallet"
            )
        if amount <= 0:
            return CommandResult(
                success=False, events=[], error="Amount must be positive"
            )

        source = self.wallets[from_id]
        if not source.has_sufficient_funds(amount):
            return CommandResult(
                success=False,
                events=[],
                error=f"Insufficient funds: requested {amount}, available {source.balance}",
            )

        transaction_id = str(uuid.uuid4())
        events: list[Event] = []

        # Initiated event
        initiated = self.event_store.append(
            EventType.TRANSFER_INITIATED,
            aggregate_id=transaction_id,
            data={
                "transaction_id": transaction_id,
                "from_wallet_id": from_id,
                "to_wallet_id": to_id,
                "amount": amount,
            },
            idempotency_key=cmd.idempotency_key,
        )
        events.append(initiated)

        # Completed event (in-memory: always succeeds after validation)
        completed = self.event_store.append(
            EventType.TRANSFER_COMPLETED,
            aggregate_id=transaction_id,
            data={
                "transaction_id": transaction_id,
                "from_wallet_id": from_id,
                "to_wallet_id": to_id,
                "amount": amount,
            },
        )
        events.append(completed)

        # Apply both events
        for event in events:
            self._apply_event(event)

        return CommandResult(success=True, events=events)

    # -- Event application -------------------------------------------------

    def _apply_event(self, event: Event) -> None:
        """Apply a single event to update in-memory state."""
        match event.event_type:
            case EventType.WALLET_CREATED:
                wallet_id = event.data["wallet_id"]
                owner = event.data["owner"]
                self.wallets[wallet_id] = Wallet(wallet_id=wallet_id, owner=owner)

            case EventType.MONEY_DEPOSITED:
                wallet_id = event.data["wallet_id"]
                amount = event.data["amount"]
                self.wallets[wallet_id].deposit(amount)
                self.ledger.record_deposit(
                    transaction_id=event.data["transaction_id"],
                    account_id=wallet_id,
                    amount=amount,
                    timestamp=event.timestamp,
                )

            case EventType.MONEY_WITHDRAWN:
                wallet_id = event.data["wallet_id"]
                amount = event.data["amount"]
                self.wallets[wallet_id].withdraw(amount)
                self.ledger.record_withdrawal(
                    transaction_id=event.data["transaction_id"],
                    account_id=wallet_id,
                    amount=amount,
                    timestamp=event.timestamp,
                )

            case EventType.TRANSFER_COMPLETED:
                from_id = event.data["from_wallet_id"]
                to_id = event.data["to_wallet_id"]
                amount = event.data["amount"]
                self.wallets[from_id].withdraw(amount)
                self.wallets[to_id].deposit(amount)
                self.ledger.record_transfer(
                    transaction_id=event.data["transaction_id"],
                    from_account=from_id,
                    to_account=to_id,
                    amount=amount,
                    description=f"Transfer {amount} cents from {from_id} to {to_id}",
                    timestamp=event.timestamp,
                )

            case EventType.TRANSFER_INITIATED | EventType.TRANSFER_FAILED:
                pass  # No state change for these event types

    # -- Snapshot & replay -------------------------------------------------

    def take_snapshot(self) -> Snapshot:
        """Capture current state as a snapshot for fast recovery."""
        snapshot = Snapshot(
            snapshot_id=str(uuid.uuid4()),
            timestamp=datetime.now(timezone.utc),
            version=len(self.event_store),
            balances={wid: w.balance for wid, w in self.wallets.items()},
            owners={wid: w.owner for wid, w in self.wallets.items()},
        )
        self._snapshots.append(snapshot)
        return snapshot

    def get_latest_snapshot(self) -> Snapshot | None:
        """Return the most recent snapshot, or None."""
        return self._snapshots[-1] if self._snapshots else None

    @classmethod
    def from_events(cls, events: list[Event]) -> WalletStateMachine:
        """Reconstruct state by replaying a list of events."""
        sm = cls()
        for event in events:
            sm._apply_event(event)
        # Rebuild event store (replay scenario)
        sm.event_store = EventStore()
        for event in events:
            sm.event_store.append(
                event.event_type,
                aggregate_id=event.aggregate_id,
                data=event.data,
            )
        return sm

    @classmethod
    def from_snapshot(cls, snapshot: Snapshot, events_after: list[Event]) -> WalletStateMachine:
        """Restore from snapshot, then replay subsequent events."""
        sm = cls()
        # Restore snapshot state
        for wallet_id, balance in snapshot.balances.items():
            owner = snapshot.owners.get(wallet_id, "unknown")
            w = Wallet(wallet_id=wallet_id, owner=owner, balance=balance)
            sm.wallets[wallet_id] = w

        # Replay events after snapshot
        for event in events_after:
            sm._apply_event(event)

        return sm

    def get_balance(self, wallet_id: str) -> int | None:
        """Return balance for a wallet, or None if not found."""
        wallet = self.wallets.get(wallet_id)
        return wallet.balance if wallet else None

    def get_all_balances(self) -> dict[str, int]:
        """Return all wallet balances."""
        return {wid: w.balance for wid, w in self.wallets.items()}
