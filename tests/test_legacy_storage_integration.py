"""Legacy compaction cannot mutate real frozen or compressed archive fixtures."""
import asyncio

import pytest

from scripts.compact_mqtt_partitions import compact_month
from test_archive_integration import DSN, create_paused_job, fixture, until_done

pytestmark = pytest.mark.skipif(not DSN, reason='MESHINFO_ARCHIVE_TEST_DSN not set')


@pytest.mark.parametrize('state', ['copying', 'complete'])
def test_legacy_compactor_refuses_actual_archive_storage_before_any_mutation(state):
    async def scenario():
        async with fixture() as (conn, expected):
            job = await create_paused_job(conn)
            if state == 'complete':
                await until_done(conn)
            before = dict(await conn.fetchrow('SELECT * FROM public.storage_archive_jobs WHERE job_id=$1', job['job_id']))
            assert not await compact_month(conn, '1901_01', 900, False, True, True)
            assert [tuple(row) for row in await conn.fetch('SELECT * FROM public.mqtt_messages_1901_01 ORDER BY id,created_at')] == expected
            assert dict(await conn.fetchrow('SELECT * FROM public.storage_archive_jobs WHERE job_id=$1', job['job_id'])) == before
            assert await conn.fetchval("SELECT to_regclass('public.mqtt_messages_1901_01_compact')") is None
            assert await conn.fetchval("SELECT to_regclass('public.mqtt_messages_1901_01_precompact')") is None
    asyncio.run(scenario())
