"""Demo script for the digital wallet system.

Demonstrates event sourcing, CQRS, and reproducibility:
create wallets, deposit, transfer, then replay events.

Usage:
    python scripts/demo.py
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.cqrs import CQRSWalletApp
from src.state_machine import Command, CommandType, WalletStateMachine


def main() -> None:
    app = CQRSWalletApp()

    print("=" * 60)
    print("Digital Wallet Demo (Event Sourcing + CQRS)")
    print("=" * 60)

    # 1. Create wallets
    print("\n[1] Creating wallets")
    app.execute_command(Command(CommandType.CREATE_WALLET, {"wallet_id": "alice", "owner": "Alice"}))
    app.execute_command(Command(CommandType.CREATE_WALLET, {"wallet_id": "bob", "owner": "Bob"}))
    print("    Created: alice, bob")

    # 2. Deposit
    print("\n[2] Depositing $100 to Alice")
    app.execute_command(Command(CommandType.DEPOSIT, {"wallet_id": "alice", "amount": 10000}))
    print(f"    Alice balance: ${app.query_balance('alice') / 100:.2f}")

    # 3. Transfer
    print("\n[3] Transferring $30 from Alice to Bob")
    result = app.execute_command(Command(
        CommandType.TRANSFER,
        {"from_wallet_id": "alice", "to_wallet_id": "bob", "amount": 3000},
    ))
    print(f"    Success: {result.success}")
    print(f"    Alice: ${app.query_balance('alice') / 100:.2f}")
    print(f"    Bob:   ${app.query_balance('bob') / 100:.2f}")

    # 4. Verify ledger invariant
    print("\n[4] Ledger invariant check")
    total = app.state_machine.ledger.total_balance()
    print(f"    Total balance across all accounts: {total} (should be 0)")

    # 5. Query transfers via read model
    print("\n[5] Transfer history (CQRS read path)")
    transfers = app.query_transfers(wallet_id="alice")
    for t in transfers:
        print(f"    {t.from_wallet_id} -> {t.to_wallet_id}: "
              f"${t.amount / 100:.2f} [{t.status}]")

    # 6. Snapshot
    print("\n[6] Taking snapshot")
    snap = app.state_machine.take_snapshot()
    print(f"    Snapshot at version {snap.version}")
    print(f"    Balances: {snap.balances}")

    # 7. Reproducibility: replay from events
    print("\n[7] Reproducing state by replaying events")
    events = app.state_machine.event_store.get_all_events()
    replayed = WalletStateMachine.from_events(events)
    print(f"    Original - Alice: {app.state_machine.wallets['alice'].balance}, "
          f"Bob: {app.state_machine.wallets['bob'].balance}")
    print(f"    Replayed - Alice: {replayed.wallets['alice'].balance}, "
          f"Bob: {replayed.wallets['bob'].balance}")
    match = (
        replayed.wallets["alice"].balance == app.state_machine.wallets["alice"].balance
        and replayed.wallets["bob"].balance == app.state_machine.wallets["bob"].balance
    )
    print(f"    States match: {match}")

    # 8. Event log
    print(f"\n[8] Event store: {len(events)} events total")
    for e in events:
        print(f"    v{e.version} {e.event_type.value:25s} agg={e.aggregate_id}")

    print("\n" + "=" * 60)
    print("Demo complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
