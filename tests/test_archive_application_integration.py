"""Actual mixed-history maintenance; uses the guarded archive-only fixture."""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pytest

from test_archive_integration import DSN, FREE, POLICY, create_paused_job, fixture, until_done
from storage.db import archive, storage_report
from storage.db.postgres import PostgresStorage
from storage.db.retention_executor import enforce_retention

pytestmark = pytest.mark.skipif(not DSN, reason='MESHINFO_ARCHIVE_TEST_DSN not set')


@pytest.mark.parametrize('state', ['copying', 'complete'])
def test_retention_expires_compressed_history_or_cancels_its_stage(state):
    async def scenario():
        async with fixture() as (conn, expected):
            now = datetime.now(timezone.utc)
            start = now.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
            end = start.replace(year=start.year+1,month=1) if start.month == 12 else start.replace(month=start.month+1)
            await conn.execute(f"""CREATE TABLE public.packet_receptions (
                mqtt_row_id BIGINT NOT NULL,created_at TIMESTAMPTZ NOT NULL) PARTITION BY RANGE(created_at);
                CREATE TABLE public.packet_receptions_1901_01 PARTITION OF public.packet_receptions
                    FOR VALUES FROM ('1901-01-01') TO ('1901-02-01');
                CREATE TABLE public.packet_receptions_hot PARTITION OF public.packet_receptions
                    FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}');
                CREATE TABLE public.packet_receptions_default PARTITION OF public.packet_receptions DEFAULT;
                CREATE INDEX receipt_identity ON public.packet_receptions(mqtt_row_id,created_at)""")
            try:
                await conn.execute("INSERT INTO public.packet_receptions VALUES(1,'1901-01-02'),(1,now()),(888,now())")
                await conn.execute("INSERT INTO public.mqtt_messages(id,created_at,payload) VALUES(888,now(),'kept')")
                job = await create_paused_job(conn)
                if state == 'complete':
                    await until_done(conn)
                result = await enforce_retention(conn, {'storage': {'retention': {
                    'enabled': True, 'months': 0, 'overrides': {'packets': 1}}}})
                assert result['status'] == 'complete', result
                assert result['scopes']['packets']['receptions_deleted'] == 1
                assert await conn.fetchval('SELECT array_agg(id) FROM public.mqtt_messages') == [888]
                assert await conn.fetchval('SELECT array_agg(mqtt_row_id) FROM public.packet_receptions') == [888]
                assert await conn.fetchval('SELECT to_regclass($1)', 'public.'+job['stage_name']) is None
                assert await conn.fetchval('SELECT count(*) FROM public.storage_archive_jobs') == 0
            finally:
                await conn.execute('DROP TABLE public.packet_receptions')
    asyncio.run(scenario())


def test_backfill_skips_frozen_and_compressed_history_but_repairs_hot_heap():
    async def scenario():
        async with fixture() as (conn, expected):
            class Pool:
                @asynccontextmanager
                async def acquire(self):
                    yield conn
            storage = PostgresStorage({'server': {'timezone': 'UTC'}})
            storage.pool = Pool()
            job = await create_paused_job(conn)
            # A restored journal can temporarily contain source OIDs from the
            # original cluster. Name matching must also prevent modification.
            await conn.execute('UPDATE public.storage_archive_jobs SET source_oid=1 WHERE job_id=$1', job['job_id'])
            await storage._backfill_mqtt_node_ids()
            assert [tuple(row) for row in await conn.fetch('SELECT * FROM public.mqtt_messages_1901_01 ORDER BY id,created_at')] == expected
            await until_done(conn)
            # Load a legacy hot row bypassing only the application extraction
            # trigger, then prove startup repair does not UPDATE the parent.
            await conn.execute('ALTER TABLE public.mqtt_messages_hot DISABLE TRIGGER trg_mqtt_messages_extract_node_ids')
            await conn.execute("INSERT INTO public.mqtt_messages_hot(id,created_at,payload) VALUES(888,now(),'{\"from\":123,\"to\":456}')")
            await conn.execute('ALTER TABLE public.mqtt_messages_hot ENABLE TRIGGER trg_mqtt_messages_extract_node_ids')
            await storage._backfill_mqtt_node_ids()
            assert await conn.fetchval('SELECT from_node_id FROM public.mqtt_messages WHERE id=888') == '0000007b'
            assert [tuple(row) for row in await conn.fetch('SELECT * FROM public.mqtt_messages_1901_01 ORDER BY id,created_at')] == expected
            report = await storage_report.build_storage_report(conn, {})
            assert report['public']['archive']['compressed_partitions'] == 1
            assert report['details']['archive']['metadata_bytes'] > 0
            assert report['details']['archive']['staging_bytes'] == 0
            assert 'jobs' not in report['public']['archive']
    asyncio.run(scenario())
