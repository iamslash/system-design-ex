"""CQRS: Command Query Responsibility Segregation.

Write path: Command -> StateMachine -> Event -> State update
Read path:  Query -> ReadModel (optimized projection of state)

The read model is eventually consistent with the write model. In this
in-memory implementation they share the same process, but the separation
allows independent scaling in a distributed system.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .event_store import Event, EventType
from .state_machine import Command, CommandResult, WalletStateMachine


# ---------------------------------------------------------------------------
# Read models (projections)
# ---------------------------------------------------------------------------

@dataclass
class WalletView:
    """Read-optimized view of a single wallet."""

    wallet_id: str
    owner: str
    balance: int
    transaction_count: int = 0


@dataclass
class TransferView:
    """Read-optimized view of a transfer."""

    transaction_id: str
    from_wallet_id: str
    to_wallet_id: str
    amount: int
    status: str  # "initiated", "completed", "failed"
    timestamp: str


class ReadModel:
    """Maintains read-optimized projections updated by events.

    In production this would be a separate database/cache. Here it
    is an in-memory dictionary kept in sync by processing events.
    """

    def __init__(self) -> None:
        self._wallets: dict[str, WalletView] = {}
        self._transfers: list[TransferView] = []
        self._events_processed: int = 0

    def project_event(self, event: Event) -> None:
        """Update projections based on a new event."""
        match event.event_type:
            case EventType.WALLET_CREATED:
                wid = event.data["wallet_id"]
                self._wallets[wid] = WalletView(
                    wallet_id=wid,
                    owner=event.data["owner"],
                    balance=0,
                )

            case EventType.MONEY_DEPOSITED:
                wid = event.data["wallet_id"]
                if wid in self._wallets:
                    self._wallets[wid].balance += event.data["amount"]
                    self._wallets[wid].transaction_count += 1

            case EventType.MONEY_WITHDRAWN:
                wid = event.data["wallet_id"]
                if wid in self._wallets:
                    self._wallets[wid].balance -= event.data["amount"]
                    self._wallets[wid].transaction_count += 1

            case EventType.TRANSFER_INITIATED:
                self._transfers.append(TransferView(
                    transaction_id=event.data["transaction_id"],
                    from_wallet_id=event.data["from_wallet_id"],
                    to_wallet_id=event.data["to_wallet_id"],
                    amount=event.data["amount"],
                    status="initiated",
                    timestamp=event.timestamp.isoformat(),
                ))

            case EventType.TRANSFER_COMPLETED:
                tid = event.data["transaction_id"]
                from_id = event.data["from_wallet_id"]
                to_id = event.data["to_wallet_id"]
                amount = event.data["amount"]

                # Update transfer status
                for t in self._transfers:
                    if t.transaction_id == tid:
                        t.status = "completed"
                        break

                # Update balances
                if from_id in self._wallets:
                    self._wallets[from_id].balance -= amount
                    self._wallets[from_id].transaction_count += 1
                if to_id in self._wallets:
                    self._wallets[to_id].balance += amount
                    self._wallets[to_id].transaction_count += 1

            case EventType.TRANSFER_FAILED:
                tid = event.data["transaction_id"]
                for t in self._transfers:
                    if t.transaction_id == tid:
                        t.status = "failed"
                        break

        self._events_processed += 1

    # -- Query methods (read path) -----------------------------------------

    def get_wallet(self, wallet_id: str) -> WalletView | None:
        return self._wallets.get(wallet_id)

    def get_balance(self, wallet_id: str) -> int | None:
        view = self._wallets.get(wallet_id)
        return view.balance if view else None

    def get_all_wallets(self) -> list[WalletView]:
        return list(self._wallets.values())

    def get_transfers(
        self,
        *,
        wallet_id: str | None = None,
        status: str | None = None,
    ) -> list[TransferView]:
        results = self._transfers
        if wallet_id:
            results = [
                t for t in results
                if t.from_wallet_id == wallet_id or t.to_wallet_id == wallet_id
            ]
        if status:
            results = [t for t in results if t.status == status]
        return results

    def get_total_transaction_count(self) -> int:
        return sum(w.transaction_count for w in self._wallets.values())

    @property
    def events_processed(self) -> int:
        return self._events_processed


# ---------------------------------------------------------------------------
# CQRS Application (combines write + read paths)
# ---------------------------------------------------------------------------

class CQRSWalletApp:
    """Unified CQRS application.

    Write path: commands go through the state machine.
    Read path: queries go through the read model (projection).
    Events bridge the two: the state machine produces events, and
    the read model consumes them.
    """

    def __init__(self) -> None:
        self.state_machine = WalletStateMachine()
        self.read_model = ReadModel()

    def execute_command(self, command: Command) -> CommandResult:
        """Write path: process command and update read model."""
        result = self.state_machine.handle(command)
        if result.success:
            for event in result.events:
                self.read_model.project_event(event)
        return result

    # -- Read path convenience methods ------------------------------------

    def query_balance(self, wallet_id: str) -> int | None:
        """Read path: get balance from read model."""
        return self.read_model.get_balance(wallet_id)

    def query_wallet(self, wallet_id: str) -> WalletView | None:
        """Read path: get full wallet view."""
        return self.read_model.get_wallet(wallet_id)

    def query_all_wallets(self) -> list[WalletView]:
        """Read path: list all wallets."""
        return self.read_model.get_all_wallets()

    def query_transfers(
        self,
        *,
        wallet_id: str | None = None,
        status: str | None = None,
    ) -> list[TransferView]:
        """Read path: query transfers with optional filters."""
        return self.read_model.get_transfers(wallet_id=wallet_id, status=status)
