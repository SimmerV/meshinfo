"""Fast safety checks for opt-in execution and maintenance coordination."""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from storage.db import retention_executor as executor
from storage.db.retention_executor import MAINTENANCE_LOCK_KEY, RetentionBlocked, enforce_retention, maintenance_lock
from test_storage_report import paired_catalog


class LockConnection:
    def __init__(self, acquired=True):
        self.acquired = acquired
        self.calls = []

    async def fetchval(self, query, *args, **kwargs):
        self.calls.append((query, args))
        if "pg_try_advisory_lock" in query:
            return self.acquired
        if "pg_advisory_unlock" in query:
            return True
        if "clock_timestamp" in query:
            return datetime(2026, 10, 27, 10, 0, tzinfo=timezone.utc)
        raise AssertionError(query)


def enabled_config(**overrides):
    return {"storage": {"retention": {"enabled": True, "months": 0, "overrides": overrides}}}


def test_disabled_and_forever_do_not_touch_a_database():
    assert asyncio.run(enforce_retention(object(), {}))["status"] == "disabled"
    assert asyncio.run(enforce_retention(object(), enabled_config()))["status"] == "forever"


def test_busy_lock_returns_without_data_queries_or_unlocking_someone_elses_lock():
    conn = LockConnection(acquired=False)
    result = asyncio.run(enforce_retention(conn, enabled_config(packets=3)))
    assert result["status"] == "busy"
    assert len(conn.calls) == 1
    assert conn.calls[0][1] == (MAINTENANCE_LOCK_KEY,)


def test_lock_released_on_cancellation():
    conn = LockConnection()

    async def task():
        async with maintenance_lock(conn) as acquired:
            assert acquired
            raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(task())
    assert "pg_advisory_unlock" in conn.calls[-1][0]


def test_cutoff_uses_database_clock_and_scope_overrides(monkeypatch):
    conn = LockConnection()
    packet = AsyncMock(return_value=executor._empty_scope(datetime(2026, 7, 1, tzinfo=timezone.utc)))
    rows = AsyncMock(return_value=executor._empty_scope(datetime(2026, 4, 1, tzinfo=timezone.utc)))
    monkeypatch.setattr(executor, "_expire_packets", packet)
    monkeypatch.setattr(executor, "_expire_rows", rows)
    result = asyncio.run(enforce_retention(conn, enabled_config(packets=3, telemetry=6)))
    assert result["status"] == "complete"
    assert packet.await_args.args[1] == datetime(2026, 7, 1, tzinfo=timezone.utc)
    assert rows.await_args.args[1:3] == ("telemetry", datetime(2026, 4, 1, tzinfo=timezone.utc))
    assert result["scopes"]["chat"]["status"] == "forever"
    assert "pg_advisory_unlock" in conn.calls[-1][0]


def test_scope_failure_does_not_hide_committed_progress(monkeypatch):
    conn = LockConnection()
    monkeypatch.setattr(executor, "_expire_packets", AsyncMock(side_effect=RetentionBlocked("unsupported layout")))
    progress = executor._empty_scope(datetime(2026, 7, 1, tzinfo=timezone.utc))
    progress["rows_deleted"] = 3
    monkeypatch.setattr(executor, "_expire_rows", AsyncMock(return_value=progress))
    result = asyncio.run(enforce_retention(conn, enabled_config(packets=3, telemetry=3)))
    assert result["status"] == "partial"
    assert result["scopes"]["packets"]["status"] == "blocked"
    assert result["scopes"]["packets"]["partitions_dropped"] == []
    assert result["scopes"]["telemetry"]["rows_deleted"] == 3


def test_cancellation_propagates_instead_of_becoming_a_success(monkeypatch):
    conn = LockConnection()
    monkeypatch.setattr(executor, "_expire_packets", AsyncMock(side_effect=asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(enforce_retention(conn, enabled_config(packets=1)))
    assert "pg_advisory_unlock" in conn.calls[-1][0]


@pytest.mark.parametrize("kwargs", [{"batch_size": 0}, {"batch_size": True}, {"max_seconds": -1}, {"max_seconds": float("nan")}, {"max_seconds": 301}, {"tail_limit": 0}, {"max_packet_months": 1.5}])
def test_invalid_execution_limits_fail_before_database_access(kwargs):
    with pytest.raises(ValueError):
        asyncio.run(enforce_retention(object(), enabled_config(packets=3), **kwargs))


def test_known_months_are_grouped_by_catalog_bounds():
    catalog = [r for r in paired_catalog() if r["parent_name"] in ("mqtt_messages", "packet_receptions")]
    parents, children, defaults, months = executor._packet_layout(catalog)
    assert len(parents) == 2 and len(children) == 10 and len(defaults) == 2
    assert all(set(group) == {"mqtt_messages", "packet_receptions"} for group in months.values())


@pytest.mark.parametrize("mutate", ["legacy", "multicolumn", "nested", "foreign", "unknown_bound"])
def test_unknown_layouts_are_rejected_before_any_deletion(mutate):
    catalog = [r for r in paired_catalog() if r["parent_name"] in ("mqtt_messages", "packet_receptions")]
    if mutate == "legacy":
        catalog[0]["relation_kind"] = "r"
    elif mutate == "multicolumn":
        catalog[0]["partition_key"] = "RANGE (created_at, id)"
    elif mutate == "unknown_bound":
        catalog[1]["partition_bound"] = "FOR VALUES FROM (MINVALUE) TO (MAXVALUE)"
    else:
        catalog[1]["relation_kind"] = "p" if mutate == "nested" else "f"
    with pytest.raises(RetentionBlocked):
        executor._packet_layout(catalog)


def test_identifiers_are_quoted_even_when_catalog_names_contain_quotes():
    assert executor._quoted('public', 'odd"name') == '"public"."odd""name"'


def test_cancellation_while_lock_acquire_is_in_flight_waits_for_result_and_unlocks():
    class DelayedLock(LockConnection):
        def __init__(self):
            super().__init__()
            self.started = asyncio.Event()
            self.release_response = asyncio.Event()

        async def fetchval(self, query, *args, **kwargs):
            if "pg_try_advisory_lock" in query:
                self.started.set()
                await self.release_response.wait()
            return await super().fetchval(query, *args, **kwargs)

    async def scenario():
        conn = DelayedLock()

        async def operation():
            async with maintenance_lock(conn):
                raise AssertionError("Cancelled acquisition must never enter the operation")

        task = asyncio.create_task(operation())
        await conn.started.wait()
        task.cancel()
        conn.release_response.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert "pg_advisory_unlock" in conn.calls[-1][0]

    asyncio.run(scenario())


def test_missing_created_at_index_blocks_row_expiration():
    class MissingIndex:
        async def fetchrow(self, *args):
            return {"oid": 1, "kind": "r", "inherited": False, "has_timestamp": True}

        async def fetch(self, *args):
            return []

    with pytest.raises(RetentionBlocked, match="created_at btree index"):
        asyncio.run(executor._row_table_ready(MissingIndex(), "node_neighborinfo_history"))
