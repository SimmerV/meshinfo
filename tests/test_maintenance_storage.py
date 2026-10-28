"""Durable retention status, public redaction, and scheduler failure paths."""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import maintenance
from storage.db import maintenance_connection, maintenance_status, retention_executor


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


class ControlContext:
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
    control = ControlContext(connection)

    class ForbiddenPool:
        def acquire(self, **_kwargs):
            raise AssertionError("Storage maintenance must not borrow the application pool")

    storage = SimpleNamespace(pool=ForbiddenPool(), control=control)

    @asynccontextmanager
    async def dedicated(actual_storage, config, *, application_name):
        assert actual_storage is storage
        assert application_name in {"meshinfo-retention-control", "meshinfo-compression-control"}
        async with control.acquire(timeout=3) as conn:
            yield conn

    monkeypatch.setattr(maintenance_connection, "maintenance_connection", dedicated)
    prepare = AsyncMock(side_effect=index_error)
    enforce = AsyncMock(return_value=outcome or {"status": "complete", "scopes": {}}, side_effect=execution_error)
    save = AsyncMock(side_effect=save_error)
    monkeypatch.setattr(maintenance_status, "prepare_retention_index", prepare)
    monkeypatch.setattr(retention_executor, "enforce_retention", enforce)
    monkeypatch.setattr(maintenance_status, "save_status", save)
    return storage, connection, prepare, enforce, save


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
    assert storage.control.released


def test_failed_index_preparation_does_not_block_other_history_families(monkeypatch):
    storage, connection, _, enforce, save = install_runner(monkeypatch, index_error=RuntimeError("index invalid"))
    result = asyncio.run(maintenance.run_retention({}, storage))
    assert result["status"] == "complete"
    enforce.assert_awaited_once_with(connection, {})
    save.assert_awaited_once()
    assert storage.control.released


def test_unexpected_executor_failure_is_durable_and_sanitized(monkeypatch):
    storage, connection, _, _, save = install_runner(monkeypatch, execution_error=RuntimeError("password=private"))
    result = asyncio.run(maintenance.run_retention({}, storage))
    assert result["status"] == "error"
    save.assert_awaited_once()
    assert save.await_args.args[:2] == (connection, "retention")
    assert save.await_args.args[2]["status"] == "error"
    assert "private" not in json.dumps(save.await_args.args[2])
    assert storage.control.released


def test_status_write_failure_is_not_reported_as_completed_maintenance(monkeypatch):
    storage, _, _, _, _ = install_runner(monkeypatch, save_error=OSError("status store unavailable"))
    result = asyncio.run(maintenance.run_retention({}, storage))
    assert result["status"] == "error"
    assert result["status_persisted"] is False
    assert storage.control.released


def test_dedicated_connection_outage_returns_failure_without_executing_retention(monkeypatch):
    storage, _, _, enforce, save = install_runner(monkeypatch)
    storage.control.error = TimeoutError("maintenance connection unavailable")
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
    assert storage.control.released


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


class SingleConnectionPool:
    """A one-slot application pool so regressions deterministically block reads."""

    def __init__(self):
        self.available = asyncio.Semaphore(1)
        self.connection = object()
        self.active = 0

    @asynccontextmanager
    async def acquire(self, *, timeout=0.1):
        await asyncio.wait_for(self.available.acquire(), timeout)
        self.active += 1
        try:
            yield self.connection
        finally:
            self.active -= 1
            self.available.release()


@pytest.mark.parametrize("stage", ["index_preparation", "expiration"])
def test_retention_keeps_single_connection_application_pool_available(monkeypatch, stage):
    async def scenario():
        storage, connection, _, _, save = install_runner(monkeypatch)
        storage.pool = SingleConnectionPool()
        started, finish = asyncio.Event(), asyncio.Event()

        async def controlled(actual, config):
            assert actual is connection
            started.set()
            await finish.wait()
            return {"status": "complete", "scopes": {}}

        if stage == "index_preparation":
            monkeypatch.setattr(maintenance_status, "prepare_retention_index", controlled)
        else:
            monkeypatch.setattr(retention_executor, "enforce_retention", controlled)
        task = asyncio.create_task(maintenance.run_retention({}, storage))
        try:
            await asyncio.wait_for(started.wait(), 1)
            assert not task.done() and not storage.control.released
            # These stand in for concurrent history reads and ingestion. With
            # the old pool-held maintenance path they time out at this point.
            for _ in range(3):
                async with storage.pool.acquire() as application:
                    assert application is not connection
                    assert storage.pool.active == 1
        finally:
            finish.set()
        assert (await asyncio.wait_for(task, 1))["status"] == "complete"
        assert storage.control.released
        save.assert_awaited_once()

    asyncio.run(scenario())


def test_index_packing_keeps_single_connection_application_pool_available(monkeypatch):
    from storage.db import index_maintenance

    async def scenario():
        storage, connection, _, _, save = install_runner(monkeypatch)
        storage.pool = SingleConnectionPool()
        started, finish = asyncio.Event(), asyncio.Event()
        preceding = asyncio.get_running_loop().create_future()

        async def controlled(actual, config):
            assert actual is connection
            started.set()
            await finish.wait()
            return {"status": "packed", "index": "synthetic_history_index"}

        monkeypatch.setattr(index_maintenance, "run_index_packing_cycle", controlled)
        task = asyncio.create_task(maintenance.run_compression({}, storage, after=preceding))
        await asyncio.sleep(0)
        assert not started.is_set()
        preceding.set_result({"status": "complete"})
        try:
            await asyncio.wait_for(started.wait(), 1)
            assert not task.done() and not storage.control.released
            for _ in range(3):
                async with storage.pool.acquire() as application:
                    assert application is not connection
        finally:
            finish.set()
        result = await asyncio.wait_for(task, 1)
        assert result == {"status": "packed", "index": "synthetic_history_index"}
        assert storage.control.released
        save.assert_awaited_once_with(connection, "compression", result)

    asyncio.run(scenario())


def test_cancelled_index_preparation_closes_dedicated_connection(monkeypatch):
    storage, _, _, enforce, save = install_runner(monkeypatch, index_error=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(maintenance.run_retention({}, storage))
    assert storage.control.released
    enforce.assert_not_awaited()
    save.assert_not_awaited()


def test_cancelled_index_packing_closes_dedicated_connection(monkeypatch):
    from storage.db import index_maintenance

    storage, _, _, _, save = install_runner(monkeypatch)
    monkeypatch.setattr(index_maintenance, "run_index_packing_cycle", AsyncMock(side_effect=asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(maintenance.run_compression({}, storage))
    assert storage.control.released
    save.assert_not_awaited()


def test_packing_failure_remains_visible_with_dedicated_connection(monkeypatch):
    from storage.db import index_maintenance
    from storage.db.compression import CompressionRefused

    storage, connection, _, _, save = install_runner(monkeypatch)
    monkeypatch.setattr(index_maintenance, "run_index_packing_cycle", AsyncMock(side_effect=CompressionRefused("insufficient headroom")))
    result = asyncio.run(maintenance.run_compression({}, storage))
    assert result == {"status": "blocked", "reason": "insufficient headroom"}
    save.assert_awaited_once_with(connection, "compression", result)
    assert storage.control.released
