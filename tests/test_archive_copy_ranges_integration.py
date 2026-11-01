"""Large-source copy cohorts retain exact cursor semantics and indexed plans."""
import asyncio
from datetime import timedelta
import json

import pytest

from storage.db import archive, archive_topics
from test_archive_integration import DSN, FREE, STAMP, fixture


pytestmark = pytest.mark.skipif(not DSN, reason="MESHINFO_ARCHIVE_TEST_DSN not set")


def nodes(plan):
    yield plan
    for child in plan.get("Plans", []):
        yield from nodes(child)


def test_large_copy_cohorts_use_indexed_scalar_envelopes_without_losing_timestamp_ties():
    async def scenario():
        async with fixture() as (conn, _original):
            await conn.execute("""INSERT INTO public.mqtt_messages(id,created_at,topic,payload)
                SELECT g,'1901-01-02'::timestamptz,'msh/range/'||(g%31),'{"text":"copy range"}'
                FROM generate_series(1000,60999) g;
                INSERT INTO public.mqtt_messages(id,created_at,topic,payload)
                SELECT 30000,'1901-01-02'::timestamptz+g*interval '1 microsecond',
                    CASE WHEN g%2=0 THEN NULL ELSE 'msh/雪/%_literal' END,'{"text":"same id"}'
                FROM generate_series(1,110) g;
                ANALYZE public.mqtt_messages_1901_01""")
            count_before = await conn.fetchval("SELECT count(*) FROM public.mqtt_messages")
            config = {"storage": {"archive": {"enabled": True,"after_months": 1,
                      "batch_rows": 93,"batch_mib": 1,"statement_seconds": 3}}}
            policy = archive.archive_policy(config)
            async with archive.maintenance_lock(conn) as acquired:
                assert acquired
                plan = await archive.plan_archive(conn, config)
                source = next(row for row in plan["partitions"] if row["eligible"])
                job_id = await archive._start(conn, source, policy, FREE)
                job = await archive._job(conn, job_id)
                before_settings = await conn.fetchrow("SELECT current_setting('enable_seqscan'),"
                    "current_setting('enable_indexscan'),current_setting('columnar.enable_custom_scan')")

                # NULL initial bound, narrow middle, same-ID upper/lower seams,
                # and a short final cohort all exercise the actual writer SQL.
                for previous_id, previous_time in (
                    (None, None), (45000, STAMP), (30000, STAMP),
                    (30000, STAMP+timedelta(microseconds=93)), (60990, STAMP),
                ):
                    async with conn.transaction():
                        await conn.execute("SELECT set_config('meshinfo.archive_job',$1,true)", str(job_id))
                        await conn.execute(f"TRUNCATE {archive._q(job['stage_name'])},"
                                           f"{archive_topics.q(job['topic_stage_name'])}")
                        await conn.execute("""UPDATE public.storage_archive_jobs SET
                            last_id=$2,last_created_at=$3,rows_copied=0,copied_logical_bytes=0,
                            inflight_bytes=NULL,phase='copying' WHERE job_id=$1""",
                            job_id, previous_id, previous_time)
                    job = await archive._job(conn, job_id)
                    expected = await conn.fetch(f"""SELECT {archive._columns()} FROM {archive._q(source['name'])}
                        WHERE ($1::bigint IS NULL OR (id,created_at)>($1,$2::timestamptz))
                        ORDER BY id,created_at LIMIT 93""", previous_id, previous_time)
                    captured = []

                    class MeasuredCopy:
                        def __getattr__(self, name):
                            return getattr(conn, name)

                        async def execute(self, query, *arguments, **options):
                            if query.startswith("INSERT INTO ") and any(
                                    archive._q(name) in query for name in (job['stage_name'],job['topic_stage_name'])):
                                captured.append(json.loads(await conn.fetchval(
                                    "EXPLAIN (ANALYZE, FORMAT JSON) "+query, *arguments))[0]["Plan"])
                                return f"INSERT 0 {len(expected)}"
                            return await conn.execute(query, *arguments, **options)

                    await archive._copy_chunk(MeasuredCopy(), job, policy, FREE)
                    assert len(captured) == 2
                    for actual in captured:
                        scans = [node for node in nodes(actual)
                                 if node.get('Relation Name') == source['name']]
                        assert scans and all(node['Node Type'] in ('Index Scan','Index Only Scan','Bitmap Heap Scan')
                                             for node in scans), actual
                        assert all(node['Actual Rows'] == len(expected) for node in scans), actual
                        if previous_id is not None:
                            assert any(node.get('Index Cond') and 'id >=' in node['Index Cond']
                                       for node in nodes(actual)), actual
                        assert any(node.get('Index Cond') and 'id <=' in node['Index Cond']
                                   for node in nodes(actual)), actual
                    assert [tuple(row) for row in await conn.fetch(
                        f"SELECT {archive._columns()} FROM {archive._q(job['stage_name'])} ORDER BY id,created_at")
                    ] == [tuple(row) for row in expected]
                    assert [tuple(row) for row in await conn.fetch(
                        f"SELECT id,created_at,topic FROM {archive_topics.q(job['topic_stage_name'])} ORDER BY id,created_at")
                    ] == [(row['id'],row['created_at'],row['topic']) for row in expected]
                    saved = await archive._job(conn, job_id)
                    assert saved['rows_copied'] == len(expected) and saved['inflight_bytes'] is None
                    assert (saved['last_id'],saved['last_created_at']) == (expected[-1]['id'],expected[-1]['created_at'])
                    assert await conn.fetchrow("SELECT current_setting('enable_seqscan'),"
                        "current_setting('enable_indexscan'),current_setting('columnar.enable_custom_scan')") == before_settings
                await archive.cancel_archive(conn, job_id, config=config)
            assert await conn.fetchval("SELECT count(*) FROM public.mqtt_messages") == count_before
    asyncio.run(scenario())
