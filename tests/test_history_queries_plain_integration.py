"""Actual packet details and node pages without Columnar loaded or installed.

MESHINFO_PLAIN_HISTORY_TEST_CONNECTION names a private JSON connection file for
an explicitly disposable *_plain_reader_test database. No schema initializer,
extension installation, production database or network discovery is used.
"""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import stat
import uuid

import asyncpg
import pytest

from storage.db.postgres import PostgresStorage
from storage.db import history_queries


CONNECTION_FILE = os.environ.get("MESHINFO_PLAIN_HISTORY_TEST_CONNECTION", "")
pytestmark = pytest.mark.skipif(not CONNECTION_FILE, reason="Dedicated plain PostgreSQL connection not set")
JANUARY = datetime(2020, 1, 15, tzinfo=timezone.utc)
BOUNDARY = datetime(2020, 1, 31, 23, 59, 59, tzinfo=timezone.utc)
FEBRUARY = datetime(2020, 2, 4, tzinfo=timezone.utc)
TOPIC = "receipt_%\\雪"
CANONICAL = {"from": 1, "to": 2, "id": 77, "topic": "canonical", "sender": "00000001",
             "qos": 0, "retain": False, "text": "雪", "nullable": None}
ORIGINAL_COPY = {"from": 1, "id": 77, "topic": TOPIC, "sender": "00000002",
                 "qos": 0, "retain": False, "text": "snow ☃", "nullable": None,
                 "rx_rssi": -70, "rx_snr": 4.5, "new": [1, None]}


def connection_settings():
    fd = os.open(Path(CONNECTION_FILE), os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd) as source:
        identity = os.fstat(source.fileno())
        if (not stat.S_ISREG(identity.st_mode) or stat.S_IMODE(identity.st_mode) != 0o600
                or identity.st_uid != os.geteuid()):
            raise ValueError("Plain-reader connection file must be private and owned")
        settings = json.load(source)
    if (set(settings) - {"host", "port", "database", "user", "password"}
            or not settings.get("database", "").endswith("_plain_reader_test")):
        raise ValueError("Plain-reader tests require an explicitly disposable database")
    return settings


@asynccontextmanager
async def fixture():
    settings = connection_settings()
    conn = await asyncpg.connect(**settings, timeout=5, command_timeout=5)
    schema = "plain_detail_" + uuid.uuid4().hex[:16]
    pool = None
    created = False
    try:
        assert await conn.fetchval("SELECT current_database()") == settings["database"]
        assert int(await conn.fetchval("SHOW server_version_num")) // 10000 == 18
        assert await conn.fetchval("SHOW shared_preload_libraries") == ""
        assert await conn.fetchval("SELECT current_setting('columnar.enable_custom_scan',true)") is None
        assert not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname<>'plpgsql')")
        assert not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_am WHERE amname='columnar')")
        await conn.execute(f'CREATE SCHEMA "{schema}"')
        created = True
        await conn.execute(f'SET search_path="{schema}",public')
        await conn.execute("""CREATE TABLE mqtt_messages (
            id bigint,topic text,payload text,qos integer,retain boolean,timestamp bigint,
            created_at timestamptz NOT NULL,from_node_id varchar(8),to_node_id varchar(8),packet_id bigint,
            PRIMARY KEY(id,created_at)) PARTITION BY RANGE(created_at);
            CREATE TABLE packets_january PARTITION OF mqtt_messages
                FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
            CREATE TABLE packets_february PARTITION OF mqtt_messages
                FOR VALUES FROM ('2020-02-01') TO ('2020-03-01');
            CREATE INDEX sender_packet ON mqtt_messages(from_node_id,packet_id,created_at DESC,id DESC);
            CREATE INDEX sender_history ON mqtt_messages(from_node_id,created_at DESC,id DESC);
            CREATE INDEX recipient_history ON mqtt_messages(to_node_id,created_at DESC,id DESC);
            CREATE TABLE mqtt_topics(id integer PRIMARY KEY,topic text NOT NULL);
            CREATE TABLE packet_receptions(mqtt_row_id bigint,created_at timestamptz,
                gateway varchar(8),topic_id integer,rx_rssi smallint,rx_snr double precision,
                rx_time bigint,hop_limit smallint,hops_away smallint,relay_node integer,transport smallint,extras jsonb);
            CREATE INDEX reception_lookup ON packet_receptions(mqtt_row_id,created_at)""")
        rows = [
            (10, "canonical", json.dumps(CANONICAL, ensure_ascii=False), 0, False, 123, JANUARY, "00000001", "00000002", 77),
            (11, "malformed", '{"broken":', 0, False, 124, JANUARY, "00000001", None, 78),
            (12, "scalar", "42", 0, False, 125, JANUARY, "00000001", None, 79),
            (13, None, None, 0, False, None, JANUARY, None, None, None),
            (14, "array", '[1,null,"雪"]', 0, False, 126, JANUARY, None, None, None),
            (15, "json null", "null", 0, False, 127, JANUARY, None, None, None),
            (50, "older", '{"text":"older exact identity"}', 0, False, 150, BOUNDARY, "00000001", None, 500),
            (50, "newer", '{"text":"newer different sender"}', 0, False, 250, FEBRUARY, "00000002", None, 600),
            (60, "old packet", '{"text":"old packet id"}', 0, False, 160, JANUARY, "00000003", None, 777),
            (61, "tie low", '{"text":"tie low"}', 0, False, 261, FEBRUARY, "00000003", None, 777),
            (62, "tie high", '{"text":"tie high"}', 0, False, 262, FEBRUARY, "00000003", None, 777),
            (64, "recipient only", '{"text":"null sender"}', 0, False, 164,
             JANUARY+timedelta(days=6), None, "00000001", 864),
            (65, "self addressed", '{"text":"self addressed"}', 0, False, 265,
             FEBRUARY+timedelta(days=1), "00000001", "00000001", 865),
            (66, "rare node", '{"text":"rare node"}', 0, False, 266,
             FEBRUARY-timedelta(days=1), "000000ff", None, 866),
        ]
        await conn.copy_records_to_table("mqtt_messages", schema_name=schema, records=rows)
        await conn.execute("INSERT INTO mqtt_topics VALUES(1,$1),(2,'newer receipt')", TOPIC)
        receipts = [
            (10, JANUARY+timedelta(seconds=1), "00000002", 1, -70, 4.5, None, None, None, None, None,
             json.dumps({"set": {"text": "snow ☃", "nullable": None, "new": [1, None]}, "del": ["to"]})),
            (11, JANUARY+timedelta(seconds=1), "00000002", 1, None, None, None, None, None, None, None, None),
            (12, JANUARY+timedelta(seconds=1), "00000002", 1, None, None, None, None, None, None, None, None),
            (13, JANUARY+timedelta(seconds=1), "00000002", 1, None, None, None, None, None, None, None, None),
            (14, JANUARY+timedelta(seconds=1), "00000002", 1, None, None, None, None, None, None, None, None),
            (15, JANUARY+timedelta(seconds=1), "00000002", 1, None, None, None, None, None, None, None, None),
            (50, BOUNDARY+timedelta(seconds=2), "00000004", 1, None, None, None, None, None, None, None, None),
            (50, FEBRUARY+timedelta(seconds=1), "00000005", 2, None, None, None, None, None, None, None, None),
        ]
        await conn.copy_records_to_table("packet_receptions", schema_name=schema, records=receipts)
        pool = await asyncpg.create_pool(**settings, min_size=1, max_size=1, timeout=5, command_timeout=5,
            server_settings={"search_path": f'"{schema}",public', "timezone": "UTC",
                             "statement_timeout": "5000", "lock_timeout": "100ms"})
        storage = PostgresStorage({"server": {"timezone": "UTC"}, "storage": {"postgres": {"enabled": True}}})
        storage.pool = pool
        yield conn, storage
        async with pool.acquire() as check:
            assert not check.is_in_transaction()
            assert await check.fetchval("SHOW plan_cache_mode") == "auto"
            assert await check.fetchval("SELECT current_setting('columnar.enable_custom_scan',true)") is None
        assert await conn.fetchval("SELECT count(*) FROM mqtt_messages") == len(rows)
    finally:
        try:
            if pool is not None:
                try:
                    await asyncio.wait_for(pool.close(), 5)
                except BaseException:
                    pool.terminate()
                    raise
        finally:
            try:
                if created:
                    await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
            finally:
                await conn.close(timeout=3)


@pytest.mark.parametrize("case", ["by_id", "by_packet", "copies", "missing", "non_object", "reused_numeric_id", "snapshot"])
def test_plain_postgres_packet_details_without_columnar(case):
    async def scenario():
        async with fixture() as (writer, storage):
            if case == "by_id":
                result = await storage.query_mqtt_message_by_id(10)
                assert result["mqtt_row_id"] == 10 and result["text"] == "雪"
                assert result["reception_count"] == 1
                assert "reconstructed" not in result["receptions"][0]
            elif case == "by_packet":
                result = await storage.query_mqtt_message_by_packet("00000003", 777)
                assert result["mqtt_row_id"] == 62 and result["text"] == "tie high"
            elif case == "copies":
                for result in [await storage.query_mqtt_message_by_id(10, True),
                               await storage.query_mqtt_message_by_packet("00000001", 77, True)]:
                    assert result["receptions"][0]["reconstructed"] == ORIGINAL_COPY
            elif case == "missing":
                assert await storage.query_mqtt_message_by_id(999999, True) is None
                assert await storage.query_mqtt_message_by_packet("ffffffff", 999999, True) is None
            elif case == "non_object":
                for row_id, expected in [(11, '{"broken":'), (12, 42), (14, [1, None, "雪"]), (15, None)]:
                    result = await storage.query_mqtt_message_by_id(row_id, True)
                    assert result["raw"] == expected and result["reception_count"] == 1
                    assert "reconstructed" not in result["receptions"][0]
                null = await storage.query_mqtt_message_by_id(13, True)
                assert "raw" not in null and null["topic"] is None and null["timestamp"] is None
                assert null["reception_count"] == 1 and "reconstructed" not in null["receptions"][0]
            elif case == "reused_numeric_id":
                newest = await storage.query_mqtt_message_by_id(50, True)
                assert newest["text"] == "newer different sender"
                assert [r["gateway"] for r in newest["receptions"]] == ["00000005"]
                selected = await storage.query_mqtt_message_by_packet("00000001", 500, True)
                assert selected["mqtt_row_id"] == 50 and selected["text"] == "older exact identity"
                assert [r["gateway"] for r in selected["receptions"]] == ["00000004"]
                assert selected["receptions"][0]["created_at"].month == 2
            else:
                pool = storage.pool
                class Hooked:
                    def __init__(self, connection):
                        self.connection = connection
                    def __getattr__(self, name):
                        return getattr(self.connection, name)
                    async def fetch(self, query, *args):
                        result = await self.connection.fetch(query, *args)
                        if "FROM mqtt_messages" in query:
                            await writer.execute("UPDATE packet_receptions SET gateway='00000009' WHERE mqtt_row_id=10")
                        return result
                class Pool:
                    @asynccontextmanager
                    async def acquire(self, **kwargs):
                        async with pool.acquire(**kwargs) as connection:
                            yield Hooked(connection)
                storage.pool = Pool()
                result = await storage.query_mqtt_message_by_id(10, True)
                assert result["receptions"][0]["reconstructed"] == ORIGINAL_COPY
                assert await writer.fetchval("SELECT gateway FROM packet_receptions WHERE mqtt_row_id=10") == "00000009"
    asyncio.run(scenario())


def message_ids(page):
    return [message["mqtt_row_id"] for message in page["messages"]]


@pytest.mark.parametrize("limit", [1, 50, 50_000])
@pytest.mark.parametrize("node,expected", [
    ("00000001", [65, 50, 64, 12, 11, 10]),
    ("000000ff", [66]),
    ("ffffffff", []),
])
def test_plain_postgres_node_page_limits(node, expected, limit):
    async def scenario():
        async with fixture() as (_, storage):
            page = await storage.query_node_mqtt_messages(node, limit=limit)
            assert message_ids(page) == expected[:limit]
            assert bool(page["next_cursor"]) is (len(expected) > limit)
            if node == "00000001" and limit > 1:
                # A self-addressed row belongs to one UNION branch; a NULL
                # sender must still qualify through the recipient branch.
                assert message_ids(page).count(65) == 1 and 64 in message_ids(page)
                by_id = {message["mqtt_row_id"]: message for message in page["messages"]}
                assert by_id[11]["raw"] == '{"broken":' and by_id[12]["raw"] == 42
                assert by_id[10]["reception_count"] == 1
    asyncio.run(scenario())


@pytest.mark.parametrize("start,end,expected", [
    (JANUARY, BOUNDARY, [50, 64, 12, 11, 10]),
    (BOUNDARY+timedelta(seconds=1), FEBRUARY+timedelta(days=1), [65]),
    (JANUARY, FEBRUARY+timedelta(days=1), [65, 50, 64, 12, 11, 10]),
    (JANUARY, JANUARY, [12, 11, 10]),
])
def test_plain_postgres_node_windows(start, end, expected):
    async def scenario():
        async with fixture() as (_, storage):
            page = await storage.query_node_mqtt_messages("00000001", start=start, end=end)
            assert message_ids(page) == expected and page["next_cursor"] is None
    asyncio.run(scenario())


def test_plain_postgres_node_cursor_ties_and_identity():
    async def scenario():
        async with fixture() as (_, storage):
            cursor, seen = None, []
            for _ in range(7):
                page = await storage.query_node_mqtt_messages("!00000001", limit=1,
                    start=JANUARY, end=FEBRUARY+timedelta(days=1), before=cursor)
                seen.extend(message_ids(page))
                cursor = page["next_cursor"]
                if cursor is None:
                    break
            assert cursor is None and seen == [65, 50, 64, 12, 11, 10]
            # The same numeric ID also exists in February for a different
            # sender. Neither partition nor sender may leak through a cursor.
            other = await storage.query_node_mqtt_messages("00000002")
            assert message_ids(other) == [50, 10]
    asyncio.run(scenario())


@pytest.mark.parametrize("failure", [None, "sql", "deadline", "cancel"])
def test_plain_postgres_node_read_scope_and_recovery(failure, monkeypatch):
    async def scenario():
        async with fixture() as (_, storage):
            pool = storage.pool
            entered = asyncio.Event()
            async with pool.acquire() as check:
                original_pid = await check.fetchval("SELECT pg_backend_pid()")

            class Hooked:
                def __init__(self, connection):
                    self.connection = connection

                def __getattr__(self, name):
                    return getattr(self.connection, name)

                async def fetch(self, query, *args):
                    assert self.connection.is_in_transaction()
                    assert await self.connection.fetchval("SHOW transaction_isolation") == "repeatable read"
                    assert await self.connection.fetchval("SHOW transaction_read_only") == "on"
                    assert await self.connection.fetchval("SHOW plan_cache_mode") == "force_generic_plan"
                    assert await self.connection.fetchval(
                        "SELECT current_setting('columnar.enable_custom_scan',true)") is None
                    entered.set()
                    if failure == "sql":
                        await self.connection.execute("SELECT 1/0")
                    elif failure in {"deadline", "cancel"}:
                        await self.connection.execute("SELECT pg_sleep(1)")
                    return await self.connection.fetch(query, *args)

            class Pool:
                @asynccontextmanager
                async def acquire(self, **kwargs):
                    async with pool.acquire(**kwargs) as connection:
                        yield Hooked(connection)

            storage.pool = Pool()
            if failure == "deadline":
                monkeypatch.setattr(history_queries, "HISTORY_READ_TIMEOUT_SECONDS", 0.1)
            if failure == "cancel":
                task = asyncio.create_task(storage.query_node_mqtt_messages("00000001"))
                try:
                    await asyncio.wait_for(entered.wait(), 2)
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                finally:
                    if not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
            elif failure:
                with pytest.raises(history_queries.HistoryReadError) as raised:
                    await storage.query_node_mqtt_messages("00000001")
                assert raised.value.operation == "query_node_mqtt_messages"
                assert raised.value.timed_out is (failure == "deadline")
            else:
                assert message_ids(await storage.query_node_mqtt_messages("00000001")) == [65, 50, 64, 12, 11, 10]
            assert entered.is_set()
            storage.pool = pool
            async with pool.acquire() as check:
                assert await check.fetchval("SELECT pg_backend_pid()") == original_pid
                assert not check.is_in_transaction()
                assert await check.fetchval("SHOW plan_cache_mode") == "auto"
                assert await check.fetchval("SELECT current_setting('columnar.enable_custom_scan',true)") is None
                assert await check.fetchval("SELECT 1") == 1
            assert message_ids(await storage.query_node_mqtt_messages("000000ff")) == [66]
    asyncio.run(scenario())
