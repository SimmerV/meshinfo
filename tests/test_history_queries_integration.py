"""Real SQL semantics in an explicitly isolated, disposable test schema.

Set MESHINFO_HISTORY_TEST_DSN to a database ending in _columnar_test. No source
database is opened. Both page plans are checked on heap and, when installed,
mixed heap/columnar partitions using the actual PostgresStorage reader methods.
"""

import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
from unittest.mock import patch
from urllib.parse import unquote, urlsplit
import uuid

import asyncpg
import pytest

from storage.db.history_queries import (
    packet_page_sql, fetch_topic_packet_page, literal_topic_pattern,
    topic_match_cte, TOPIC_MATCH_ARRAY_LIMIT,
)
from storage.db.postgres import PostgresStorage


DSN = os.environ.get("MESHINFO_HISTORY_TEST_DSN", "")
pytestmark = pytest.mark.skipif(not DSN, reason="MESHINFO_HISTORY_TEST_DSN not set")
HOT = datetime(2020, 2, 1, tzinfo=timezone.utc)
TIE = HOT - timedelta(seconds=1, microseconds=123456)
NODE = "00000001"
TOPIC = "My_%\\Channel"


def test_packet_query_semantics_on_heap_and_available_columnar():
    asyncio.run(scenario())


async def scenario():
    database = unquote(urlsplit(DSN).path.lstrip("/"))
    if not database.endswith("_columnar_test"):
        raise ValueError("History integration tests require an isolated *_columnar_test database")
    connection = await asyncpg.connect(DSN, timeout=5, command_timeout=10,
        server_settings={"timezone": "UTC", "statement_timeout": "10000", "lock_timeout": "500ms"})
    try:
        assert await connection.fetchval("SELECT current_database()") == database
        methods = ["heap"]
        if await connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_am WHERE amname='columnar')"):
            methods.append("columnar")
        for method in methods:
            await exercise_method(connection, method)
    finally:
        await connection.close()


async def exercise_method(connection, method):
    schema = "history_queries_test_" + uuid.uuid4().hex[:12]
    await connection.execute(f'CREATE SCHEMA "{schema}"')
    storage = PostgresStorage({"server": {"timezone": "UTC"},
                              "storage": {"postgres": {"enabled": True}}})
    try:
        await connection.execute(f'SET search_path="{schema}",public')
        await connection.execute("""CREATE TABLE mqtt_messages (
            id bigint,topic text,payload text,qos integer,retain boolean,timestamp bigint,
            created_at timestamptz NOT NULL,from_node_id varchar(8),to_node_id varchar(8),packet_id bigint,
            PRIMARY KEY(id,created_at)) PARTITION BY RANGE(created_at);
            CREATE TABLE mqtt_topics(id integer PRIMARY KEY,topic text NOT NULL);
            CREATE TABLE packet_receptions(mqtt_row_id bigint,created_at timestamptz,
            gateway varchar(8),topic_id integer,rx_rssi smallint,rx_snr double precision,
            rx_time bigint,hop_limit smallint,hops_away smallint,relay_node integer,transport smallint,extras jsonb);
            CREATE INDEX reception_lookup ON packet_receptions(mqtt_row_id,created_at)""")
        await connection.execute(f"""CREATE TABLE packets_cold PARTITION OF mqtt_messages
            FOR VALUES FROM ('2020-01-01') TO ('2020-02-01') USING {method};
            CREATE TABLE packets_legacy PARTITION OF mqtt_messages
            FOR VALUES FROM ('2019-12-01') TO ('2020-01-01') USING {method};
            CREATE TABLE packets_hot PARTITION OF mqtt_messages
            FOR VALUES FROM ('2020-02-01') TO ('2020-03-01')""")
        packets = [
            (1, None, None, 0, False, 1, HOT-timedelta(microseconds=1), None, None, None),
            (2, TOPIC, '{"from":1,"to":2,"text":"sensor_A foo"}', 0, False, 2, TIE, NODE, "00000002", 99),
            (3, "self", "42", 0, False, 3, TIE, NODE, NODE, 100),
            (4, TOPIC, '{"bad":', 0, False, 4, HOT+timedelta(seconds=1), NODE, None, 99),
            (5, "hot", '{"text":"雪"}', 0, False, 5, HOT+timedelta(seconds=1), NODE, None, 99),
            (6, None, None, 0, False, None, HOT-timedelta(days=2), None, None, None),
            (7, "MyXq\\Channel", '{"text":"sensorZZ"}', 0, False, 7, HOT-timedelta(days=1), None, None, None),
            (8, None, '{"text":"back\\\\slash"}', 0, False, 8, HOT-timedelta(days=1, seconds=1), None, NODE, None),
            (9, TOPIC, '{"text":"legacy without receipts"}', 0, False, 9,
             datetime(2019,12,15,tzinfo=timezone.utc), None, None, None),
        ]
        await connection.copy_records_to_table("mqtt_messages", schema_name=schema, records=packets)
        await connection.execute("INSERT INTO mqtt_topics VALUES (1,'receipt-only-marker'),(2,'another topic')")
        receptions = [
            (1, HOT+timedelta(seconds=1), "00000002", 1, None, None, None, None, None, None, None, None),
            (1, HOT+timedelta(seconds=2), "00000003", 1, None, None, None, None, None, None, None, None),
            (6, HOT+timedelta(days=1), "00000002", 1, None, None, None, None, None, None, None, None),
            (2, TIE-timedelta(seconds=1), "00000002", 1, None, None, None, None, None, None, None, None),
            (2, TIE+timedelta(seconds=1), "00000002", 2, 0, None, None, None, None, None, 0,
             json.dumps({"set": {"nullable": None, "text": "雪"}, "del": ["to"]})),
            (7, HOT, "00000002", None, None, None, None, None, None, None, None, None),
        ]
        await connection.copy_records_to_table("packet_receptions", schema_name=schema, records=receptions)
        storage.pool = await asyncpg.create_pool(DSN, min_size=1, max_size=1, command_timeout=10,
            server_settings={"search_path": f'"{schema}",public', "timezone": "UTC",
                             "default_transaction_read_only": "on", "work_mem": "4MB"})
        async with storage.pool.acquire() as pooled:
            initial_settings = await pooled.fetchrow("SELECT current_setting('plan_cache_mode') AS plan, "
                "current_setting('columnar.enable_custom_scan',true) AS columnar")
        previous = None
        for strategy in ("direct", "candidates", "candidate_array", "node_branches", "node_branch_array", "node_rows"):
            def build(conditions, limit_parameter, reception_count_sql, *, _strategy=strategy, **_production_options):
                if _strategy in {"node_branches", "node_branch_array", "node_rows"} and not (
                        conditions and conditions[0] == "(from_node_id = $1 OR to_node_id = $1)"):
                    _strategy = "candidate_array" if _strategy == "node_branch_array" else "candidates"
                _production_options.pop("strategy", None)
                return packet_page_sql(conditions, limit_parameter, reception_count_sql,
                                       strategy=_strategy, **_production_options)
            with patch("storage.db.postgres.packet_page_sql", side_effect=build):
                checks = await check_readers(connection, storage)
            if previous is not None:
                assert checks == previous
            previous = checks
        # Only January is covered. December legacy packets and February hot
        # packets must continue through the native fallback without duplicates.
        await connection.execute("""CREATE TABLE mqtt_archive_topics(id bigint,created_at timestamptz,topic text,
            PRIMARY KEY(id,created_at)) PARTITION BY RANGE(created_at);
            CREATE TABLE topic_lookup_january PARTITION OF mqtt_archive_topics
            FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
            INSERT INTO mqtt_archive_topics SELECT id,created_at,topic FROM mqtt_messages
            WHERE created_at>='2020-01-01' AND created_at<'2020-02-01'""")
        assert await check_readers(connection, storage) == previous
        await check_dictionary_overflow(connection, storage)
        if method == "columnar":
            await check_topic_lookup_snapshot(connection, storage)
        async with storage.pool.acquire() as pooled:
            assert await pooled.fetchrow("SELECT current_setting('plan_cache_mode') AS plan, "
                "current_setting('columnar.enable_custom_scan',true) AS columnar") == initial_settings
    finally:
        if storage.pool is not None:
            await storage.pool.close()
        await connection.execute("SET search_path=public")
        await connection.execute(f'DROP SCHEMA "{schema}" CASCADE')


async def check_topic_lookup_snapshot(writer, storage):
    """A hot-row update between key selection and payload fetch cannot mix versions."""
    class ConcurrentUpdate:
        def __init__(self, connection):
            self.connection = connection
            self.changed = False

        def __getattr__(self, name):
            return getattr(self.connection, name)

        async def fetch(self, query, *parameters):
            rows = await self.connection.fetch(query, *parameters)
            if "FROM mqtt_archive_topics WHERE" in query:
                await writer.execute("UPDATE mqtt_messages SET topic='changed-concurrently' WHERE id=4 "
                                     "AND created_at>='2020-02-01' AND created_at<'2020-03-01'")
                self.changed = True
            return rows

    try:
        async with storage.pool.acquire() as connection:
            hooked = ConcurrentUpdate(connection)
            rows = await fetch_topic_packet_page(hooked,
                ["topic ILIKE '%' || $1 || '%' ESCAPE '\\'"],
                [literal_topic_pattern(TOPIC), 51], 2, storage._reception_count_sql)
        assert hooked.changed
        assert [(row["id"], row["topic"]) for row in rows] == [(4, TOPIC), (2, TOPIC), (9, TOPIC)]
        assert await writer.fetchval("SELECT topic FROM mqtt_messages WHERE id=4") == "changed-concurrently"
    finally:
        await writer.execute("UPDATE mqtt_messages SET topic=$1 WHERE id=4 "
                             "AND created_at>='2020-02-01' AND created_at<'2020-03-01'", TOPIC)


async def check_dictionary_overflow(connection, storage):
    await connection.execute("INSERT INTO mqtt_topics SELECT n,'bounded-overflow-witness-'||n "
                             "FROM generate_series(1000,$1) n", 1000+TOPIC_MATCH_ARRAY_LIMIT+1)
    matched = await connection.fetchrow("WITH " + topic_match_cte(1) +
        " SELECT ids,overflow FROM packet_topic_matches", "bounded-overflow-witness")
    assert matched["overflow"] and len(matched["ids"]) == TOPIC_MATCH_ARRAY_LIMIT+1
    beyond_cap = await connection.fetchval("SELECT id FROM mqtt_topics "
        "WHERE topic LIKE 'bounded-overflow-witness-%' AND NOT(id=ANY($1::integer[])) ORDER BY id LIMIT 1",
        matched["ids"])
    assert beyond_cap is not None
    await connection.execute("INSERT INTO packet_receptions(mqtt_row_id,created_at,topic_id,gateway) "
                             "VALUES(2,$1,$2,'00000004')", TIE+timedelta(seconds=2), beyond_cap)
    for term in ("bounded-overflow-witness", "bounded%overflow_witness%"):
        for topic in (None, TOPIC):
            result = await storage.query_mqtt_messages(search=term, topic=topic, limit=1)
            assert [row["mqtt_row_id"] for row in result["messages"]] == [2]
            assert result["next_cursor"] is None


async def check_readers(connection, storage):
    outcomes = {}
    for term in ("receipt-only-marker", "sensor_%", "%", "_", "\\", "no matching topic", "' OR true --", "雪"):
        expected = await connection.fetch("""SELECT id FROM mqtt_messages WHERE (
            topic ILIKE '%'||$1||'%' OR payload ILIKE '%'||$1||'%' OR EXISTS (
            SELECT 1 FROM packet_receptions pr JOIN mqtt_topics t ON t.id=pr.topic_id
            WHERE pr.mqtt_row_id=mqtt_messages.id AND pr.created_at>=mqtt_messages.created_at
            AND pr.created_at<mqtt_messages.created_at+interval '86400 seconds'
            AND t.topic ILIKE '%'||$1||'%')) ORDER BY created_at DESC,id DESC""", term)
        result = await storage.query_mqtt_messages(search=term, limit=200)
        identities = [row["mqtt_row_id"] for row in result["messages"]]
        assert identities == [row["id"] for row in expected]
        if term == "receipt-only-marker":
            assert identities == [1]  # duplicate and cross-month receipts stay one packet
        outcomes[term] = result
    literal = await storage.query_mqtt_messages(topic=TOPIC)
    assert [row["mqtt_row_id"] for row in literal["messages"]] == [4, 2, 9]
    assert not (await storage.query_mqtt_messages(topic="receipt-only-marker"))["messages"]
    literal_ids, cursor = [], None
    for _ in range(4):
        page = await storage.query_mqtt_messages(topic=TOPIC, limit=1, before=cursor)
        literal_ids.extend(row["mqtt_row_id"] for row in page["messages"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert literal_ids == [4, 2, 9] and cursor is None
    for term, expected_ids in (("sensor", [2]), ("legacy", [9]), ("another topic", [2]),
                               ("absent-literal-topic-term", []), ("%", [4, 2, 9])):
        combined = await storage.query_mqtt_messages(topic=TOPIC, search=term, limit=1)
        combined_ids = [row["mqtt_row_id"] for row in combined["messages"]]
        cursor = combined["next_cursor"]
        while cursor:
            page = await storage.query_mqtt_messages(topic=TOPIC, search=term, limit=1, before=cursor)
            combined_ids.extend(row["mqtt_row_id"] for row in page["messages"])
            cursor = page["next_cursor"]
        assert combined_ids == expected_ids
        outcomes["topic_search:"+term] = combined_ids
    with patch("time.time", return_value=10):
        reported_time = await storage.query_mqtt_messages(topic=TOPIC, range_seconds=7, limit=1)
        assert [row["mqtt_row_id"] for row in reported_time["messages"]] == [4]
        reported_tail = await storage.query_mqtt_messages(topic=TOPIC, range_seconds=7, limit=1,
                                                        before=reported_time["next_cursor"])
        assert [row["mqtt_row_id"] for row in reported_tail["messages"]] == [9]
        assert reported_tail["next_cursor"] is None
    node = await storage.query_node_mqtt_messages(NODE)
    assert [row["mqtt_row_id"] for row in node["messages"]] == [5, 4, 3, 2, 8]
    reused = await storage.query_mqtt_message_by_packet(NODE, 99)
    assert reused["mqtt_row_id"] == 5
    copied = await storage.query_mqtt_message_by_id(2, include_copies=True)
    assert copied["reception_count"] == 1
    assert copied["receptions"][0]["reconstructed"] == {
        "from": 1, "text": "雪", "nullable": None, "rx_rssi": 0, "transport_mechanism": 0}
    assert await storage.query_mqtt_message_by_id(999999) is None
    ids, before = [], None
    for _ in range(10):
        page = await storage.query_mqtt_messages(limit=1, before=before)
        ids.extend(row["mqtt_row_id"] for row in page["messages"])
        before = page["next_cursor"]
        if before is None:
            break
    assert ids == [5, 4, 1, 3, 2, 7, 8, 6, 9]
    assert before is None
    outcomes.update(literal=literal, node=node, reused=reused, copied=copied, pagination=ids)
    return outcomes
