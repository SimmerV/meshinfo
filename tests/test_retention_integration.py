"""Synthetic retention cases; NEVER point this suite at a deployed mesh DB.

Requires MESHINFO_STORAGE_TEST_DSN naming a dedicated *_storage_test database.
Fixtures occupy January/February 1900 and reserved high row IDs only. Base schema
must already be installed, so these tests exercise the same partition/index and
constraint definitions as an installation without copying an entire database.
"""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from copy import deepcopy
import json
import os
from urllib.parse import urlsplit

import asyncpg
import pytest

from storage.db.retention_executor import MAINTENANCE_LOCK_KEY, enforce_retention
from storage.retention import complete_month_cutoff

DSN = os.environ.get("MESHINFO_STORAGE_TEST_DSN", "")
pytestmark = pytest.mark.skipif(not DSN, reason="MESHINFO_STORAGE_TEST_DSN not set")
BASE = 8_800_000_000_000_000


def policy(**scopes):
    return {"storage": {"retention": {"enabled": True, "months": 0, "overrides": scopes}}}


def part(table, month):
    return f"{table}_retention_test_1900_{month:02d}"


@asynccontextmanager
async def fixture(*, months=(1,), paired=True):
    database = urlsplit(DSN).path.lstrip("/")
    if not database.endswith("_storage_test"):
        pytest.fail("Retention integration tests require a dedicated *_storage_test database")
    conn = await asyncpg.connect(DSN, command_timeout=5)
    assert await conn.fetchval("SELECT current_database()") == database
    try:
        for month in months:
            for table in ("mqtt_messages", "packet_receptions") if paired else ("mqtt_messages",):
                await conn.execute(f"CREATE TABLE {part(table, month)} PARTITION OF {table} FOR VALUES FROM ('1900-{month:02d}-01 00:00:00+00') TO ('1900-{month+1:02d}-01 00:00:00+00')")
        yield conn
    finally:
        await conn.execute("DROP VIEW IF EXISTS retention_test_dependent")
        await conn.execute("DROP TABLE IF EXISTS retention_test_inbound")
        await conn.execute("DELETE FROM packet_receptions WHERE mqtt_row_id BETWEEN $1 AND $2", BASE, BASE + 9999)
        await conn.execute("DELETE FROM mqtt_messages WHERE id BETWEEN $1 AND $2", BASE, BASE + 9999)
        await conn.execute("DELETE FROM chat_messages WHERE id BETWEEN $1 AND $2", BASE, BASE + 9999)
        await conn.execute("DELETE FROM nodes WHERE id='abcd1900'")
        await conn.execute("DELETE FROM mqtt_topics WHERE topic LIKE 'msh/retention-test-api/%'")
        await conn.execute("DROP INDEX IF EXISTS retention_test_neighbor_created_at")
        for month in months:
            for table in ("packet_receptions", "mqtt_messages"):
                await conn.execute(f"DROP TABLE IF EXISTS {part(table, month)}")
        await conn.close()


async def packet(conn, offset, when="1900-01-15T00:00:00+00:00"):
    timestamp = datetime.fromisoformat(when) if isinstance(when, str) else when
    await conn.execute("INSERT INTO mqtt_messages(id,topic,payload,created_at) VALUES($1,'retention-test','{}',$2)", BASE + offset, timestamp)


async def reception(conn, offset, when="1900-01-15T00:00:00+00:00"):
    timestamp = datetime.fromisoformat(when) if isinstance(when, str) else when
    await conn.execute("INSERT INTO packet_receptions(mqtt_row_id,created_at) VALUES($1,$2)", BASE + offset, timestamp)


async def now(conn):
    return await conn.fetchval("SELECT clock_timestamp()")


async def exists(conn, table):
    return await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", table)


async def count_receptions(conn, offset):
    return await conn.fetchval("SELECT count(*) FROM packet_receptions WHERE mqtt_row_id=$1", BASE + offset)


def test_complete_month_drop_removes_cross_month_tails_but_keeps_current_packets():
    async def scenario():
        async with fixture() as conn:
            current = await now(conn)
            await packet(conn, 1)
            await reception(conn, 1)
            await reception(conn, 1, current)
            await packet(conn, 100, current)
            await reception(conn, 100, current)
            result = await enforce_retention(conn, policy(packets=1))
            assert result["status"] == "complete", result
            assert result["scopes"]["packets"]["receptions_deleted"] == 1
            assert len(result["scopes"]["packets"]["partitions_dropped"]) == 2
            assert not await exists(conn, part("mqtt_messages", 1))
            assert await count_receptions(conn, 1) == 0
            assert await count_receptions(conn, 100) == 1
    asyncio.run(scenario())


def test_midnight_id_interleaving_preserves_retained_packet_and_its_reception():
    async def scenario():
        async with fixture() as conn:
            current = await now(conn)
            for offset in (1, 3):
                await packet(conn, offset)
                await reception(conn, offset)
            await packet(conn, 2, current)
            await reception(conn, 2, current)
            await reception(conn, 1, current)
            result = await enforce_retention(conn, policy(packets=1))
            assert result["status"] == "complete", result
            assert await count_receptions(conn, 1) == 0
            assert await count_receptions(conn, 2) == 1
            assert await conn.fetchval("SELECT count(*) FROM mqtt_messages WHERE id=$1", BASE + 2) == 1
    asyncio.run(scenario())


def test_unrelated_orphan_in_id_gap_is_not_silently_deleted():
    async def scenario():
        async with fixture() as conn:
            current = await now(conn)
            for offset in (1, 3):
                await packet(conn, offset)
                await reception(conn, offset)
            await reception(conn, 2, current)  # no canonical ever existed
            await reception(conn, 1, current)
            result = await enforce_retention(conn, policy(packets=1))
            assert result["status"] == "complete", result
            assert await count_receptions(conn, 2) == 1
            assert result["scopes"]["packets"]["receptions_deleted"] == 1
    asyncio.run(scenario())


@pytest.mark.parametrize("case", ["duplicate_id", "old_reception_for_kept_packet", "tail_budget", "default_nonempty", "dependency", "repeated_retained_ids"])
def test_unsafe_or_failed_packet_expiration_rolls_back_every_change(case):
    async def scenario():
        async with fixture() as conn:
            current = await now(conn)
            await packet(conn, 1)
            await reception(conn, 1)
            await reception(conn, 1, current)
            kwargs = {}
            if case == "duplicate_id":
                await packet(conn, 1, current)
            elif case == "old_reception_for_kept_packet":
                await packet(conn, 100, current)
                await reception(conn, 100)
            elif case == "tail_budget":
                await reception(conn, 1, current)
                kwargs["tail_limit"] = 1
            elif case == "default_nonempty":
                await packet(conn, 2, "1850-01-01T00:00:00+00:00")
            elif case == "dependency":
                await conn.execute(f"CREATE VIEW retention_test_dependent AS SELECT * FROM {part('mqtt_messages', 1)}")
            elif case == "repeated_retained_ids":
                await packet(conn, 2, current)
                await packet(conn, 2, current + timedelta(microseconds=1))
                await packet(conn, 50, current)
                await reception(conn, 50)
                kwargs["interleaved_limit"] = 1
            before = await count_receptions(conn, 1)
            result = await enforce_retention(conn, policy(packets=1), **kwargs)
            assert result["status"] in ("blocked", "error"), result
            assert await exists(conn, part("mqtt_messages", 1))
            assert await exists(conn, part("packet_receptions", 1))
            assert await count_receptions(conn, 1) == before
            assert result["scopes"]["packets"]["partitions_dropped"] == []
            assert result["scopes"]["packets"]["receptions_deleted"] == 0
    asyncio.run(scenario())


def test_empty_default_with_allocated_heap_pages_does_not_permanently_block():
    async def scenario():
        async with fixture() as conn:
            await packet(conn, 2, "1850-01-01T00:00:00+00:00")
            await conn.execute("DELETE FROM mqtt_messages_default WHERE id=$1", BASE + 2)
            await packet(conn, 1)
            await reception(conn, 1)
            result = await enforce_retention(conn, policy(packets=1))
            assert result["status"] == "complete", result
    asyncio.run(scenario())


def test_unpaired_month_is_visible_and_left_intact():
    async def scenario():
        async with fixture(paired=False) as conn:
            await packet(conn, 1)
            result = await enforce_retention(conn, policy(packets=1))
            assert result["status"] == "blocked", result
            assert "unpaired" in result["scopes"]["packets"]["reason"]
            assert await exists(conn, part("mqtt_messages", 1))
    asyncio.run(scenario())


def test_month_limit_makes_atomic_progress_and_can_resume():
    async def scenario():
        async with fixture(months=(1, 2)) as conn:
            await packet(conn, 1)
            await reception(conn, 1)
            await reception(conn, 1, "1900-02-01T00:00:00+00:00")
            await packet(conn, 2, "1900-02-15T00:00:00+00:00")
            await reception(conn, 2, "1900-02-15T00:00:00+00:00")
            first = await enforce_retention(conn, policy(packets=1), max_packet_months=1)
            assert first["status"] == "partial", first
            assert not await exists(conn, part("mqtt_messages", 1))
            assert await exists(conn, part("mqtt_messages", 2))
            assert await count_receptions(conn, 1) == 0
            assert await count_receptions(conn, 2) == 1
            second = await enforce_retention(conn, policy(packets=1), max_packet_months=1)
            assert second["status"] == "complete", second
            assert not await exists(conn, part("mqtt_messages", 2))
    asyncio.run(scenario())


def test_shared_maintenance_lock_prevents_retention_during_snapshot():
    async def scenario():
        async with fixture() as conn:
            other = await asyncpg.connect(DSN)
            try:
                assert await other.fetchval("SELECT pg_try_advisory_lock($1::bigint)", MAINTENANCE_LOCK_KEY)
                result = await enforce_retention(conn, policy(packets=1))
                assert result["status"] == "busy"
                assert await exists(conn, part("mqtt_messages", 1))
            finally:
                await other.close()
    asyncio.run(scenario())


def test_active_history_reader_causes_bounded_lock_failure_without_deletion():
    async def scenario():
        async with fixture() as conn:
            await packet(conn, 1)
            other = await asyncpg.connect(DSN)
            try:
                async with other.transaction():
                    await other.fetchval(f"SELECT count(*) FROM {part('mqtt_messages', 1)}")
                    result = await enforce_retention(conn, policy(packets=1))
                    assert result["status"] == "error", result
                    assert "55P03" in result["scopes"]["packets"]["reason"]
                    assert await exists(conn, part("mqtt_messages", 1))
            finally:
                await other.close()
    asyncio.run(scenario())


def test_row_expiration_is_bounded_and_preserves_exact_month_boundary():
    async def scenario():
        async with fixture(months=()) as conn:
            cutoff = complete_month_cutoff(await now(conn), 1)
            await conn.executemany("INSERT INTO chat_messages(id,text,created_at) VALUES($1,'retention-test',$2)",
                                   [(BASE + i, datetime(1900, 1, 1, tzinfo=timezone.utc)) for i in range(1, 6)] + [(BASE + 6, cutoff)])
            first = await enforce_retention(conn, policy(chat=1), batch_size=2, max_batches_per_scope=1)
            assert first["status"] == "partial", first
            assert first["scopes"]["chat"]["rows_deleted"] == 2
            second = await enforce_retention(conn, policy(chat=1), batch_size=2, max_batches_per_scope=3)
            assert second["status"] == "complete", second
            assert second["scopes"]["chat"]["rows_deleted"] == 3
            assert await conn.fetchval("SELECT count(*) FROM chat_messages WHERE id BETWEEN $1 AND $2", BASE, BASE + 9999) == 1
            assert await conn.fetchval("SELECT created_at FROM chat_messages WHERE id=$1", BASE + 6) == cutoff
    asyncio.run(scenario())


def test_locked_expired_row_is_reported_pending_then_removed_on_retry():
    async def scenario():
        async with fixture(months=()) as conn:
            await conn.execute("INSERT INTO chat_messages(id,created_at) VALUES($1,'1900-01-01')", BASE + 1)
            other = await asyncpg.connect(DSN)
            try:
                async with other.transaction():
                    await other.fetchrow("SELECT id FROM chat_messages WHERE id=$1 FOR UPDATE", BASE + 1)
                    first = await enforce_retention(conn, policy(chat=1))
                    assert first["status"] == "partial", first
                    assert first["scopes"]["chat"]["rows_deleted"] == 0
                second = await enforce_retention(conn, policy(chat=1))
                assert second["status"] == "complete", second
                assert second["scopes"]["chat"]["rows_deleted"] == 1
            finally:
                await other.close()
    asyncio.run(scenario())


def test_inbound_foreign_key_blocks_history_expiration_without_cascading():
    async def scenario():
        async with fixture(months=()) as conn:
            await conn.execute("INSERT INTO chat_messages(id,created_at) VALUES($1,'1900-01-01')", BASE + 1)
            await conn.execute("CREATE TABLE retention_test_inbound(id bigint REFERENCES chat_messages(id) ON DELETE CASCADE)")
            await conn.execute("INSERT INTO retention_test_inbound VALUES($1)", BASE + 1)
            result = await enforce_retention(conn, policy(chat=1))
            assert result["status"] == "blocked", result
            assert await conn.fetchval("SELECT count(*) FROM retention_test_inbound") == 1
            assert await conn.fetchval("SELECT count(*) FROM chat_messages WHERE id=$1", BASE + 1) == 1
    asyncio.run(scenario())


def test_retained_search_pagination_and_exact_copy_reconstruction_are_unchanged():
    from storage.db.postgres import PostgresStorage

    async def scenario():
        async with fixture() as conn:
            storage = PostgresStorage({"server": {"timezone": "UTC"}, "storage": {"postgres": {"enabled": True}}})
            storage.pool = await asyncpg.create_pool(DSN, min_size=1, max_size=1)
            try:
                current = await now(conn)
                await packet(conn, 1)
                await reception(conn, 1)
                originals = {}
                for offset in (100, 101):
                    canonical = {"from": "!abcd1900", "id": offset, "sender": "!00000001",
                                 "topic": "msh/retention-test-api/!00000001", "rx_rssi": -90, "rx_snr": 3.5,
                                 "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": "aGVsbG8="}}
                    copy = deepcopy(canonical)
                    copy.update(sender="!00000002", topic="msh/retention-test-api/!00000002", rx_rssi=-105, rx_snr=1.25,
                                extension={"retain_exactly": [1, "two", None]})
                    await conn.execute("INSERT INTO mqtt_messages(id,topic,payload,created_at) VALUES($1,$2,$3,$4)",
                                       BASE + offset, canonical["topic"], json.dumps(canonical), current - timedelta(seconds=102-offset))
                    await storage._write_reception(conn, BASE + offset, canonical, canonical)
                    await storage._write_reception(conn, BASE + offset, copy, canonical)
                    originals[offset] = [canonical, copy]
                before = await storage.query_mqtt_message_by_id(BASE + 100, include_copies=True)
                assert before and [r["reconstructed"] for r in before["receptions"]] == originals[100]
                first = await storage.query_mqtt_messages(limit=1, search="retention-test-api")
                assert first["next_cursor"]
                second = await storage.query_mqtt_messages(limit=1, search="retention-test-api", before=first["next_cursor"])
                assert len(first["messages"]) == len(second["messages"]) == 1
                assert first["messages"][0]["mqtt_row_id"] != second["messages"][0]["mqtt_row_id"]
                result = await enforce_retention(conn, policy(packets=1))
                assert result["status"] == "complete", result
                assert await storage.query_mqtt_message_by_id(BASE + 1, include_copies=True) is None
                assert await storage.query_mqtt_message_by_id(BASE + 100, include_copies=True) == before
                assert await storage.query_mqtt_messages(limit=1, search="retention-test-api") == first
                assert await storage.query_mqtt_messages(limit=1, search="retention-test-api", before=first["next_cursor"]) == second
            finally:
                await storage.pool.close()
    asyncio.run(scenario())


def test_every_row_scope_expires_history_and_preserves_current_state():
    async def scenario():
        async with fixture(months=()) as conn:
            cutoff = complete_month_cutoff(await now(conn), 1)
            await conn.execute("INSERT INTO nodes(id,longname,created_at) VALUES('abcd1900','Retention current state','1900-01-01')")
            await conn.execute("INSERT INTO node_telemetry_current(node_id,battery_level,created_at) VALUES('abcd1900',87,'1900-01-01')")
            await conn.execute("INSERT INTO node_positions(node_id,latitude_i,longitude_i,created_at) VALUES('abcd1900',123,456,'1900-01-01')")
            await conn.execute("INSERT INTO node_neighborinfo(node_id,neighbors,created_at) VALUES('abcd1900','[]','1900-01-01')")
            await conn.execute("CREATE INDEX retention_test_neighbor_created_at ON node_neighborinfo_history(created_at)")
            for offset, timestamp in ((1, datetime(1900, 1, 1, tzinfo=timezone.utc)), (2, cutoff)):
                for table in ("telemetry", "traceroutes"):
                    await conn.execute(f"INSERT INTO {table}(id,from_node_id,message_id,created_at) VALUES($1,'abcd1900',$1,$2)", BASE + offset, timestamp)
                await conn.execute("INSERT INTO chat_messages(id,from_node_id,created_at) VALUES($1,'abcd1900',$2)", BASE + offset, timestamp)
                await conn.execute("INSERT INTO node_neighborinfo_history(id,node_id,neighbors,created_at) VALUES($1,'abcd1900','[]',$2)", BASE + offset, timestamp)
            state_tables = ("nodes", "node_telemetry_current", "node_positions", "node_neighborinfo")
            before = {}
            for table in state_tables:
                column = "id" if table == "nodes" else "node_id"
                before[table] = dict(await conn.fetchrow(f"SELECT * FROM {table} WHERE {column}='abcd1900'"))
            result = await enforce_retention(conn, policy(telemetry=1, chat=1, traceroutes=1, neighbors=1))
            assert result["status"] == "complete", result
            for scope, table in (("telemetry", "telemetry"), ("chat", "chat_messages"), ("traceroutes", "traceroutes"), ("neighbors", "node_neighborinfo_history")):
                assert result["scopes"][scope]["rows_deleted"] == 1
                assert await conn.fetchval(f"SELECT count(*) FROM {table} WHERE id=$1", BASE + 1) == 0
                assert await conn.fetchval(f"SELECT count(*) FROM {table} WHERE id=$1", BASE + 2) == 1
            for table in state_tables:
                column = "id" if table == "nodes" else "node_id"
                assert dict(await conn.fetchrow(f"SELECT * FROM {table} WHERE {column}='abcd1900'")) == before[table]
    asyncio.run(scenario())
