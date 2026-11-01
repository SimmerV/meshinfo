"""Bounded UNION pages must retain ordered index access as archives grow."""

import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
from urllib.parse import unquote, urlsplit
import uuid

import asyncpg
import pytest

from storage.db.history_queries import literal_topic_pattern, topic_candidate_sql


DSN = os.environ.get("MESHINFO_HISTORY_TEST_DSN", "")
pytestmark = pytest.mark.skipif(not DSN, reason="MESHINFO_HISTORY_TEST_DSN not set")
JAN = datetime(2020, 1, 1, tzinfo=timezone.utc)
FEB = datetime(2020, 2, 1, tzinfo=timezone.utc)
MAR = datetime(2020, 3, 1, tzinfo=timezone.utc)
APR = datetime(2020, 4, 1, tzinfo=timezone.utc)


def walk(plan):
    yield plan
    for child in plan.get("Plans", []):
        yield from walk(child)


def test_branch_limits_preserve_global_pages_and_bounded_ordered_index_plans():
    asyncio.run(exercise())


async def exercise():
    database = unquote(urlsplit(DSN).path.lstrip("/"))
    if not database.endswith("_columnar_test"):
        raise ValueError("History integration tests require an isolated *_columnar_test database")
    connection = await asyncpg.connect(DSN, timeout=5, command_timeout=10,
        server_settings={"timezone": "UTC", "statement_timeout": "10000", "lock_timeout": "500ms",
                         "work_mem": "2MB", "max_parallel_workers_per_gather": "0"})
    schema = "history_candidates_test_" + uuid.uuid4().hex[:12]
    created = False
    try:
        assert await connection.fetchval("SELECT current_database()") == database
        if not await connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_am WHERE amname='columnar')"):
            pytest.skip("Columnar extension is not installed in isolated database")
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        created = True
        await connection.execute(f'SET search_path="{schema}",public')
        await connection.execute("""CREATE TABLE mqtt_messages(id bigint NOT NULL,
            created_at timestamptz NOT NULL,topic text,PRIMARY KEY(id,created_at)) PARTITION BY RANGE(created_at);
            CREATE TABLE packets_legacy PARTITION OF mqtt_messages
                FOR VALUES FROM ('2019-12-01') TO ('2020-01-01') USING columnar;
            CREATE TABLE packets_january PARTITION OF mqtt_messages
                FOR VALUES FROM ('2020-01-01') TO ('2020-02-01') USING columnar;
            CREATE TABLE packets_hot PARTITION OF mqtt_messages
                FOR VALUES FROM ('2020-02-01') TO ('2020-03-01');
            CREATE TABLE packets_march PARTITION OF mqtt_messages
                FOR VALUES FROM ('2020-03-01') TO ('2020-04-01') USING columnar;
            CREATE INDEX packets_created ON mqtt_messages(created_at DESC,id DESC);
            CREATE TABLE mqtt_archive_topics(LIKE mqtt_messages INCLUDING ALL) PARTITION BY RANGE(created_at);
            CREATE TABLE topics_january PARTITION OF mqtt_archive_topics
                FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
            CREATE TABLE topics_march PARTITION OF mqtt_archive_topics
                FOR VALUES FROM ('2020-03-01') TO ('2020-04-01')""")
        # Dense timestamp ties and reused IDs across months exercise the full
        # composite ordering. Topic literals include LIKE metacharacters.
        literal = "literal_%\\channel"
        records = [(number, JAN+timedelta(microseconds=number//10),
                    literal if number % 20 == 0 else None if number % 31 == 0 else "ordinary")
                   for number in range(1, 60_001)]
        records.extend((number, FEB+timedelta(microseconds=number//10), literal)
                       for number in range(1, 54))
        records.extend([(1, JAN-timedelta(microseconds=1), literal),
                        (1, MAR, literal), (2, MAR, None), (3, MAR, literal)])
        await connection.copy_records_to_table("mqtt_messages", schema_name=schema, records=records)
        await connection.execute("""INSERT INTO mqtt_archive_topics SELECT id,created_at,topic
            FROM mqtt_messages WHERE (created_at>='2020-01-01' AND created_at<'2020-02-01')
                OR (created_at>='2020-03-01' AND created_at<'2020-04-01');
            ANALYZE mqtt_messages; ANALYZE mqtt_archive_topics""")
        intervals = [(JAN, FEB), (MAR, APR)]  # uncovered legacy month plus an internal hot gap
        cases = [([], []),
            (["topic ILIKE '%' || $1 || '%' ESCAPE '\\'"], [literal_topic_pattern(literal)]),
            (["created_at >= $1", "created_at < $2"], [JAN, FEB]),
            (["created_at >= $1", "created_at <= $2"], [JAN-timedelta(days=1), MAR]),
            (["(created_at,id)<($1::timestamptz,$2::bigint)"], [FEB+timedelta(microseconds=3), 33]),
            (["(created_at,id)<($1::timestamptz,$2::bigint)"], [JAN+timedelta(microseconds=5999), 59995]),
            (["created_at >= $1", "created_at < $2"], [FEB, MAR]),
            (["topic IS NULL"], []),
            (["topic=$1"], ["absent"])]
        async with connection.transaction(readonly=True):
            await connection.execute("SET LOCAL plan_cache_mode=force_custom_plan")
            for conditions, arguments in cases:
                for count in (1, 7, 51):
                    limit_parameter = len(arguments)+1
                    query, extra = topic_candidate_sql(conditions, limit_parameter, intervals)
                    actual = await connection.fetch(query, *arguments, count, *extra)
                    expected = await connection.fetch("SELECT id,created_at FROM mqtt_messages WHERE "
                        + (" AND ".join(conditions) or "true")
                        + f" ORDER BY created_at DESC,id DESC LIMIT ${limit_parameter}::integer",
                        *arguments, count)
                    assert actual == expected
                    assert len({(row["id"],row["created_at"]) for row in actual}) == len(actual)
            # Test topology, not milliseconds: a large archive-only page must
            # read an ordered index prefix and never sort/scan all 60k keys.
            for topic in (False, True):
                conditions, arguments = ["created_at >= $1", "created_at < $2"], [JAN, FEB]
                if topic:
                    conditions.append("topic ILIKE '%' || $3 || '%' ESCAPE '\\'")
                    arguments.append(literal_topic_pattern(literal))
                limit_parameter = len(arguments)+1
                query, extra = topic_candidate_sql(conditions, limit_parameter, intervals)
                result = json.loads(await connection.fetchval("EXPLAIN (ANALYZE,FORMAT JSON) " + query,
                    *arguments, 51, *extra))[0]["Plan"]
                scans = [node for node in walk(result) if node.get("Relation Name") == "topics_january"]
                assert len(scans) == 1
                assert scans[0]["Node Type"] in ("Index Scan", "Index Only Scan")
                assert scans[0]["Actual Rows"] == 51
                assert scans[0].get("Rows Removed by Filter", 0) <= 51*20
                assert all(node["Actual Rows"] <= 51 for node in walk(result)
                           if node["Node Type"] in ("Sort", "Limit", "Merge Append"))
    finally:
        if created:
            await connection.execute("SET search_path=public")
            await connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await connection.close()
