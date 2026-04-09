"""Tests for CQRS: write path + read path consistency."""

from src.cqrs import CQRSWalletApp
from src.state_machine import Command, CommandType


def test_cqrs_write_and_read():
    """Read model should reflect state after write commands."""
    app = CQRSWalletApp()
    app.execute_command(Command(CommandType.CREATE_WALLET, {"wallet_id": "w1", "owner": "alice"}))
    app.execute_command(Command(CommandType.DEPOSIT, {"wallet_id": "w1", "amount": 5000}))

    assert app.query_balance("w1") == 5000

    view = app.query_wallet("w1")
    assert view is not None
    assert view.owner == "alice"
    assert view.transaction_count == 1


def test_cqrs_transfer():
    """Transfer should update both read model balances."""
    app = CQRSWalletApp()
    app.execute_command(Command(CommandType.CREATE_WALLET, {"wallet_id": "w1", "owner": "alice"}))
    app.execute_command(Command(CommandType.CREATE_WALLET, {"wallet_id": "w2", "owner": "bob"}))
    app.execute_command(Command(CommandType.DEPOSIT, {"wallet_id": "w1", "amount": 10000}))
    app.execute_command(Command(
        CommandType.TRANSFER,
        {"from_wallet_id": "w1", "to_wallet_id": "w2", "amount": 4000},
    ))

    assert app.query_balance("w1") == 6000
    assert app.query_balance("w2") == 4000


def test_cqrs_transfer_view():
    """Transfers should appear in the read model query."""
    app = CQRSWalletApp()
    app.execute_command(Command(CommandType.CREATE_WALLET, {"wallet_id": "w1", "owner": "alice"}))
    app.execute_command(Command(CommandType.CREATE_WALLET, {"wallet_id": "w2", "owner": "bob"}))
    app.execute_command(Command(CommandType.DEPOSIT, {"wallet_id": "w1", "amount": 10000}))
    app.execute_command(Command(
        CommandType.TRANSFER,
        {"from_wallet_id": "w1", "to_wallet_id": "w2", "amount": 3000},
    ))

    transfers = app.query_transfers(wallet_id="w1")
    assert len(transfers) == 1
    assert transfers[0].status == "completed"
    assert transfers[0].amount == 3000


def test_cqrs_failed_command_no_read_update():
    """Failed command should not update the read model."""
    app = CQRSWalletApp()
    app.execute_command(Command(CommandType.CREATE_WALLET, {"wallet_id": "w1", "owner": "alice"}))

    result = app.execute_command(Command(CommandType.WITHDRAW, {"wallet_id": "w1", "amount": 100}))
    assert result.success is False
    assert app.query_balance("w1") == 0


def test_cqrs_all_wallets():
    """query_all_wallets should list all created wallets."""
    app = CQRSWalletApp()
    app.execute_command(Command(CommandType.CREATE_WALLET, {"wallet_id": "w1", "owner": "alice"}))
    app.execute_command(Command(CommandType.CREATE_WALLET, {"wallet_id": "w2", "owner": "bob"}))

    wallets = app.query_all_wallets()
    assert len(wallets) == 2
