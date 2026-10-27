"""Durable retention status, public redaction, and scheduler failure paths."""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import maintenance
from storage.db import maintenance_status, retention_executor


@pytest.mark.parametrize("operation", ["retention", "compression", "backup"])
def test_status_is_saved_as_one_bounded_operation_record(operation):
    connection = SimpleNamespace(execute=AsyncMock())
    detail = {"status": "partial", "scopes": {"packets": {"status": "blocked"}}}
    asyncio.run(maintenance_status.save_status(connection, operation, detail))
    call = connection.execute.await_args
    assert "ON CONFLICT (operation) DO UPDATE" in call.args[0]
    assert call.args[1] == operation
    assert json.loads(call.args[2]) == detail
    assert call.kwargs["timeout"] == 3


def test_unknown_status_operation_is_rejected_before_sql():
    connection = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(ValueError):
        asyncio.run(maintenance_status.save_status(connection, "unknown", {}))
    connection.execute.assert_not_called()


def test_read_status_handles_upgrade_without_table_and_decodes_json():
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=None), fetch=AsyncMock())
    assert asyncio.run(maintenance_status.read_status(connection)) == {}
    connection.fetch.assert_not_called()
    now = datetime(2026, 10, 27, tzinfo=timezone.utc)
    connection.fetchval.return_value = "storage_maintenance_status"
    connection.fetch.return_value = [
        {"operation": "retention", "updated_at": now, "result": '{"status":"complete"}'},
        {"operation": "backup", "updated_at": now, "result": {"status": "ok"}},
    ]
    result = asyncio.run(maintenance_status.read_status(connection))
    assert result["retention"] == {"updated_at": now.isoformat(), "result": {"status": "complete"}}
    assert result["backup"]["result"] == {"status": "ok"}


def test_public_status_omits_operator_paths_sql_and_destination_details():
    statuses = {
        "retention": {
            "updated_at": "2026-10-27T00:00:00+00:00",
            "result": {
                "status": "partial",
                "reason": "SQLSTATE; operator-only diagnostic",
                "scopes": {
                    "packets": {
                        "status": "blocked", "reason": "private SQL and partition name",
                        "partitions_dropped": ["mqtt_messages_202001"],
                    },
                    "telemetry": {"status": "complete", "rows_deleted": 120},
                },
            },
        },
        "backup": {
            "updated_at": "2026-10-27T00:00:00+00:00",
            "result": {
                "status": "error", "destination": "private@host:/secret/path",
                "password": "secret-password", "reason": "private traceback",
            },
        },
        "unknown_operation": {"updated_at": "private", "result": {"status": "private"}},
    }
    public = maintenance_status.public_status(statuses)
    assert public == {
        "retention": {
            "updated_at": "2026-10-27T00:00:00+00:00",
            "status": "partial",
            "scopes": {"packets": {"status": "blocked"}, "telemetry": {"status": "complete"}},
        },
        "backup": {
            "updated_at": "2026-10-27T00:00:00+00:00", "status": "error", "scopes": {},
        },
    }
    serialized = json.dumps(public)
    for secret in ("private", "secret", "SQLSTATE", "partitions_dropped", "rows_deleted"):
        assert secret not in serialized


def test_public_status_restricts_scope_names_and_status_values():
    public = maintenance_status.public_status({
        "retention": {
            "updated_at": "2026-10-27T00:00:00+00:00",
            "result": {
                "status": "unexpected SQL with private credentials",
                "scopes": {
                    "packets": {"status": "unexpected private diagnostic"},
                    "private@host:/destination": {"status": "blocked"},
                },
            },
        },
    })
    assert public["retention"]["status"] == "unknown"
    assert public["retention"]["scopes"] == {"packets": {"status": "unknown"}}
    assert "private" not in json.dumps(public)


class Pool:
    def __init__(self, connection, error=None):
        self.connection = connection
        self.error = error
        self.released = False

    @asynccontextmanager
    async def acquire(self, *, timeout):
        assert timeout == 3
        if self.error is not None:
            raise self.error
        try:
            yield self.connection
        finally:
            self.released = True


def install_runner(monkeypatch, *, outcome=None, execution_error=None, index_error=None, save_error=None):
    connection = object()
    pool = Pool(connection)
    prepare = AsyncMock(side_effect=index_error)
    enforce = AsyncMock(return_value=outcome or {"status": "complete", "scopes": {}}, side_effect=execution_error)
    save = AsyncMock(side_effect=save_error)
    monkeypatch.setattr(maintenance_status, "prepare_retention_index", prepare)
    monkeypatch.setattr(retention_executor, "enforce_retention", enforce)
    monkeypatch.setattr(maintenance_status, "save_status", save)
    return SimpleNamespace(pool=pool), connection, prepare, enforce, save


def test_scope_partial_progress_is_persisted_and_returned(monkeypatch):
    outcome = {
        "status": "partial",
        "scopes": {
            "packets": {"status": "blocked", "reason": "unsupported layout"},
            "telemetry": {"status": "complete", "rows_deleted": 100},
        },
    }
    storage, connection, _, enforce, save = install_runner(monkeypatch, outcome=outcome)
    result = asyncio.run(maintenance.run_retention({}, storage))
    assert result == outcome
    enforce.assert_awaited_once_with(connection, {})
    save.assert_awaited_once_with(connection, "retention", outcome)
    assert storage.pool.released


def test_failed_index_preparation_does_not_block_other_history_families(monkeypatch):
    storage, connection, _, enforce, save = install_runner(monkeypatch, index_error=RuntimeError("index invalid"))
    result = asyncio.run(maintenance.run_retention({}, storage))
    assert result["status"] == "complete"
    enforce.assert_awaited_once_with(connection, {})
    save.assert_awaited_once()
    assert storage.pool.released


def test_unexpected_executor_failure_is_durable_and_sanitized(monkeypatch):
    storage, connection, _, _, save = install_runner(monkeypatch, execution_error=RuntimeError("password=private"))
    result = asyncio.run(maintenance.run_retention({}, storage))
    assert result["status"] == "error"
    save.assert_awaited_once()
    assert save.await_args.args[:2] == (connection, "retention")
    assert save.await_args.args[2]["status"] == "error"
    assert "private" not in json.dumps(save.await_args.args[2])
    assert storage.pool.released


def test_status_write_failure_is_not_reported_as_completed_maintenance(monkeypatch):
    storage, _, _, _, _ = install_runner(monkeypatch, save_error=OSError("status store unavailable"))
    result = asyncio.run(maintenance.run_retention({}, storage))
    assert result["status"] == "error"
    assert result["status_persisted"] is False
    assert storage.pool.released


def test_pool_outage_returns_failure_without_executing_retention(monkeypatch):
    storage, _, _, enforce, save = install_runner(monkeypatch)
    storage.pool.error = TimeoutError("pool unavailable")
    result = asyncio.run(maintenance.run_retention({}, storage))
    assert result["status"] == "error"
    assert result["status_persisted"] is False
    enforce.assert_not_awaited()
    save.assert_not_awaited()


def test_cancellation_releases_connection_and_is_not_saved_as_success(monkeypatch):
    storage, _, _, _, save = install_runner(monkeypatch, execution_error=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(maintenance.run_retention({}, storage))
    save.assert_not_awaited()
    assert storage.pool.released


def test_scheduler_keeps_each_worker_single_flight_and_awaits_cancellation(monkeypatch):
    import main as application

    async def run():
        starts = {"retention": 0, "backup": 0}
        started = {name: asyncio.Event() for name in starts}
        stopped = {name: asyncio.Event() for name in starts}

        async def worker(name):
            starts[name] += 1
            started[name].set()
            try:
                await asyncio.Event().wait()
            finally:
                # Verify the loop waits for asynchronous worker cleanup too.
                await asyncio.sleep(0)
                stopped[name].set()

        monkeypatch.setattr(maintenance, "scheduled_backup_due", AsyncMock(return_value=True))

        async def retention(*_args):
            return await worker("retention")
        async def snapshot(*_args, **_kwargs):
            return await worker("backup")
        monkeypatch.setattr(maintenance, "run_retention", retention)
        monkeypatch.setattr(maintenance, "run_backup", snapshot)
        storage = SimpleNamespace(
            mark_nodes_inactive_by_age=AsyncMock(return_value=0),
            ensure_mqtt_partitions=AsyncMock(),
        )
        task = asyncio.create_task(application.maintenance_loop(
            {"server": {"node_activity_prune_threshold": 60}},
            SimpleNamespace(pg_storage=storage), interval_seconds=0.001,
        ))
        try:
            await asyncio.wait_for(asyncio.gather(*(event.wait() for event in started.values())), 2)
            await asyncio.sleep(0.01)
            assert storage.mark_nodes_inactive_by_age.await_count > 1
            assert starts == {"retention": 1, "backup": 1}
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 2)
        assert all(event.is_set() for event in stopped.values())

    asyncio.run(run())
