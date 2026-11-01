"""Bounded real topic pilots, restricted to the shared isolated archive fixture."""
import asyncio
import json
import re

import pytest

from storage.db import archive, archive_topics
from test_archive_integration import DSN, fixture


pytestmark = pytest.mark.skipif(not DSN, reason="MESHINFO_ARCHIVE_TEST_DSN not set")


def plan_nodes(value):
    if isinstance(value, dict):
        yield value
        for child in value.get("Plans", []):
            yield from plan_nodes(child)


@pytest.mark.parametrize("method", ["heap", "columnar"])
def test_topic_pilot_limits_metadata_before_heap_sample_sort_and_preserves_budgets(method):
    async def scenario():
        async with fixture() as (conn, _expected):
            await conn.execute(archive_topics.SCHEMA_SQL)
            async with conn.transaction():
                source_name = "archive_topic_pilot_input"
                await conn.execute(f"CREATE TABLE public.{source_name} (id bigint,created_at timestamptz,"
                                   f"topic text,PRIMARY KEY(id,created_at)) USING {method}")
                count = 250000 if method == "heap" else 4096
                # Duplicate IDs at distinct timestamps exercise the exact
                # composite-key join; NULL and Unicode topics retain semantics.
                await conn.execute(f"""INSERT INTO public.{source_name}
                    SELECT g/2,'1901-01-02'::timestamptz+(g%2)*interval '1 microsecond',
                        CASE WHEN g%11=0 THEN NULL ELSE 'pilot/雪/'||(g%37) END
                    FROM generate_series(1,$1::int) g""", count)
                await conn.execute(f"ANALYZE public.{source_name}")
                if method == "heap":
                    sampled = await conn.fetchval(f"SELECT count(*) FROM public.{source_name} "
                                                   "TABLESAMPLE SYSTEM(1) REPEATABLE(731)")
                    assert sampled > 1000, "Witness must exceed the metadata sample cap"
                source = {"name": source_name, "method": method,
                          "oid": await conn.fetchval(f"SELECT 'public.{source_name}'::regclass::oid"),
                          "table_bytes": await archive._size(conn, source_name)}
                policy = archive.ArchivePolicy(batch_mib=1, max_wal_mib=16)
                plans, cohorts = [], []

                class MeasuredPilot:
                    def __getattr__(self, name):
                        return getattr(conn, name)

                    async def execute(self, query, *arguments, **options):
                        if query.startswith("WITH sample AS MATERIALIZED"):
                            # Execute the real INSERT exactly once and retain its
                            # actual plan, rather than asserting SQL spelling.
                            result = json.loads(await conn.fetchval(
                                "EXPLAIN (ANALYZE, FORMAT JSON) "+query, *arguments))[0]["Plan"]
                            plans.append(result)
                            pilot = re.search(r'INSERT INTO "public"\."([a-z0-9_]+)"', query)[1]
                            cohort = dict(await conn.fetchrow(f"""SELECT count(*) AS rows,
                                sum(128+coalesce(octet_length(topic),0)) AS bytes,
                                sum(128*(1+coalesce(octet_length(topic),0)::bigint)) AS index_work
                                FROM {archive_topics.q(pilot)}"""))
                            cohorts.append(cohort)
                            assert not await conn.fetchval(f"""SELECT EXISTS(
                                SELECT 1 FROM {archive_topics.q(pilot)} p LEFT JOIN public.{source_name} s
                                USING(id,created_at) WHERE s.id IS NULL
                                  OR convert_to(p.topic,'UTF8') IS DISTINCT FROM convert_to(s.topic,'UTF8'))""")
                            return f"INSERT 0 {cohort['rows']}"
                        return await conn.execute(query, *arguments, **options)

                estimate = await archive_topics.estimate(MeasuredPilot(), source, policy)
                assert estimate > 16*1024**2 and len(plans) == len(cohorts) == 1
                assert 0 < cohorts[0]["rows"] <= 1000
                assert cohorts[0]["bytes"] <= policy.batch_bytes
                assert cohorts[0]["index_work"] <= policy.max_wal_bytes//4
                nodes = list(plan_nodes(plans[0]))
                if method == "heap":
                    sampled_limits = [node for node in nodes if node.get("Node Type") == "Limit"
                                      and len(node.get("Plans", [])) == 1
                                      and node["Plans"][0].get("Node Type") == "Sample Scan"]
                    assert len(sampled_limits) == 1, plans[0]
                    assert sampled_limits[0]["Actual Rows"] == 1000
                    assert sampled_limits[0]["Plans"][0]["Actual Rows"] == 1000
                    assert all(node["Actual Rows"] <= 1000 for node in nodes
                               if node.get("Node Type") in {"Sort", "WindowAgg", "CTE Scan"})
                else:
                    assert not any(node.get("Node Type") == "Sample Scan" for node in nodes)
                assert await conn.fetchval(f"SELECT count(*) FROM public.{source_name}") == count
                assert not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_class "
                                               "WHERE relname LIKE 'meshinfo_topics_pilot_%')")
                await conn.execute(f"DROP TABLE public.{source_name}")
    asyncio.run(scenario())


@pytest.mark.parametrize("budget", ["bytes", "gin"])
def test_topic_pilot_refuses_single_topic_over_budget_and_rolls_back_stage(budget):
    async def scenario():
        async with fixture() as (conn, _expected):
            await conn.execute(archive_topics.SCHEMA_SQL)
            # Byte-bound case previously slipped through the one-row fallback
            # because it omitted the per-row 128-byte allowance.
            policy = archive.ArchivePolicy(batch_mib=1, max_wal_mib=1024 if budget == "bytes" else 16)
            topic_bytes = policy.batch_bytes if budget == "bytes" else policy.max_wal_bytes//(4*128)
            with pytest.raises(archive.ArchiveBlocked, match="GIN/WAL pilot budget"):
                async with conn.transaction():
                    await conn.execute("CREATE TABLE public.archive_topic_oversized "
                                       "(id bigint,created_at timestamptz,topic text,PRIMARY KEY(id,created_at))")
                    await conn.execute("INSERT INTO public.archive_topic_oversized VALUES"
                                       "(1,'1901-01-02',repeat('x',$1))", topic_bytes)
                    source = {"name": "archive_topic_oversized", "method": "heap",
                              "oid": await conn.fetchval("SELECT 'public.archive_topic_oversized'::regclass::oid"),
                              "table_bytes": await archive._size(conn, "archive_topic_oversized")}
                    await archive_topics.estimate(conn, source, policy)
            assert await conn.fetchval("SELECT to_regclass('public.archive_topic_oversized')") is None
            assert not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_class "
                                           "WHERE relname LIKE 'meshinfo_topics_pilot_%')")
    asyncio.run(scenario())
