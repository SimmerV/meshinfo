"""An isolated concurrency experiment must preserve fixture ownership and cleanup."""

import asyncio
import json
import stat
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from scripts import benchmark_history_concurrency as benchmark
from scripts.benchmark_history_read_plans import RecordedAcquisition


def test_live_target_rejected_before_connection_or_output(monkeypatch, tmp_path):
    monkeypatch.setenv("CONCURRENCY_TEST_DSN", "postgresql://localhost/meshinfo")
    connector = AsyncMock()
    monkeypatch.setattr(benchmark.asyncpg, "connect", connector)
    with pytest.raises(ValueError, match="dedicated"):
        asyncio.run(benchmark.run(SimpleNamespace(dsn_env="CONCURRENCY_TEST_DSN")))
    connector.assert_not_awaited()
    assert not list(tmp_path.iterdir())


def test_ownership_report_is_private_exclusive_and_atomically_replaceable(tmp_path):
    target = tmp_path / "ownership.json"
    benchmark.persist_report(target, {"owned": 1}, create=True)
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    with pytest.raises(FileExistsError):
        benchmark.persist_report(target, {"owned": 2}, create=True)
    assert json.loads(target.read_text()) == {"owned": 1}
    benchmark.persist_report(target, {"owned": 1, "cleanup_complete": True})
    assert json.loads(target.read_text())["cleanup_complete"]
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert list(tmp_path.iterdir()) == [target]


def test_ownership_creation_does_not_follow_existing_symlink(tmp_path):
    victim = tmp_path / "existing"
    victim.write_text("keep")
    target = tmp_path / "ownership.json"
    target.symlink_to(victim)
    with pytest.raises(FileExistsError):
        benchmark.persist_report(target, {"owned": 1}, create=True)
    assert victim.read_text() == "keep"


def test_insert_target_must_be_heap_before_commit():
    connection = SimpleNamespace(fetchval=AsyncMock(return_value="columnar"))
    with pytest.raises(ValueError, match="heap"):
        asyncio.run(benchmark.require_heap(connection, 123))


def test_failed_planner_transaction_setup_always_releases_pool_connection():
    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            raise RuntimeError("rollback failed")

    connection = SimpleNamespace(transaction=lambda **kwargs: Transaction(),
        execute=AsyncMock(side_effect=ValueError("setting failed")))
    acquisition = MagicMock()
    acquisition.__aenter__ = AsyncMock(return_value=connection)
    acquisition.__aexit__ = AsyncMock(return_value=False)
    pool = SimpleNamespace(acquire=lambda: acquisition)
    recorded = RecordedAcquisition(pool, {"planner_profile": "custom"})
    with pytest.raises(RuntimeError, match="rollback failed"):
        asyncio.run(recorded.__aenter__())
    acquisition.__aexit__.assert_awaited_once()


def test_recorded_connection_preserves_native_transaction_method_for_real_reader():
    native_transaction = MagicMock()
    connection = SimpleNamespace(transaction=native_transaction)
    acquisition = MagicMock()
    acquisition.__aenter__ = AsyncMock(return_value=connection)
    acquisition.__aexit__ = AsyncMock(return_value=False)
    recorded = RecordedAcquisition(SimpleNamespace(acquire=lambda: acquisition), {})
    async def scenario():
        async with recorded as proxy:
            assert proxy.transaction is native_transaction
            proxy.transaction(readonly=True)
    asyncio.run(scenario())
    native_transaction.assert_called_once_with(readonly=True)


def test_cleanup_uses_both_exact_owned_id_and_hot_timestamp_bounds():
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock()
    transaction.__aexit__ = AsyncMock(return_value=False)
    connection = SimpleNamespace(transaction=lambda: transaction,
        execute=AsyncMock(), fetchval=AsyncMock(return_value=0))
    asyncio.run(benchmark.remove_owned(connection, {"heap": "benchmark_heap", "columnar": "public"},
        "benchmark_shared", {"first_id": 100, "exclusive_last_id": 104,
                             "created_at": "2026-10-15T00:00:00+00:00"}))
    assert connection.execute.await_count == 3
    for call in connection.execute.await_args_list:
        sql, low, high, start, end = call.args
        assert low == 100 and high == 104 and (end-start).total_seconds() == 1
        assert ">=$1" in sql and "<$2" in sql
        assert "created_at>=$3 AND created_at<$4" in sql
