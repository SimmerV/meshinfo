"""Real transaction/deadline checks using the small dedicated archive fixture."""
import asyncio
from copy import deepcopy

import pytest

from storage.db import archive
from test_archive_integration import DSN, FREE, POLICY, create_ready_job, fixture

pytestmark = pytest.mark.skipif(not DSN, reason='MESHINFO_ARCHIVE_TEST_DSN not set')


def test_real_analyze_budget_outlives_copy_cycle_and_commits_only_one_relation():
    async def scenario():
        async with fixture() as (conn, expected):
            job = await create_ready_job(conn)
            config = deepcopy(POLICY)
            config['storage']['archive'].update(cycle_seconds=1, statement_seconds=.5, analyze_seconds=3)
            calls = []

            class DelayedAnalysis:
                def __getattr__(self, name):
                    return getattr(conn, name)

                async def execute(self, query, *arguments, **options):
                    if query.startswith('ANALYZE '):
                        calls.append(query)
                        # Actual server work exceeds the old whole-cycle limit.
                        await conn.execute('SELECT pg_sleep(1.1)', timeout=options['timeout'])
                    return await conn.execute(query, *arguments, **options)

            first = await archive.run_archive_cycle(DelayedAnalysis(), config, free_bytes=FREE)
            assert first['status'] == 'ready' and first['elapsed_seconds'] > 1
            saved = await archive._job(conn, job['job_id'])
            assert saved['stage_analyzed'] and not saved['topic_analyzed'] and not saved['analyzed']
            assert calls == ['ANALYZE '+archive._q(job['stage_name'])]
            second = await archive.run_archive_cycle(DelayedAnalysis(), config, free_bytes=FREE)
            assert second['status'] == 'ready'
            saved = await archive._job(conn, job['job_id'])
            assert saved['analyzed'] and len(calls) == 2
            assert calls[-1] == 'ANALYZE '+archive._q(job['topic_stage_name'])
            assert saved['rows_copied'] == saved['rows_verified'] == len(expected)
            assert [tuple(row) for row in await conn.fetch('SELECT * FROM public.mqtt_messages ORDER BY id,created_at')] == expected
    asyncio.run(scenario())


def test_real_analyze_timeout_rolls_back_only_current_relation_and_resets_settings():
    async def scenario():
        async with fixture() as (conn, expected):
            job = await create_ready_job(conn)
            await archive._analyze_stage(conn, job, archive.archive_policy(POLICY))
            assert job['stage_analyzed'] and not job['topic_analyzed']
            before = await conn.fetchval('SHOW statement_timeout')
            config = deepcopy(POLICY)
            config['storage']['archive'].update(cycle_seconds=1, statement_seconds=.5, analyze_seconds=.1)

            class SlowTopicAnalysis:
                def __getattr__(self, name):
                    return getattr(conn, name)

                async def execute(self, query, *arguments, **options):
                    if query.startswith('ANALYZE '):
                        assert query == 'ANALYZE '+archive._q(job['topic_stage_name'])
                        await conn.execute('SELECT pg_sleep(1)', timeout=options['timeout'])
                    return await conn.execute(query, *arguments, **options)

            result = await archive.run_archive_cycle(SlowTopicAnalysis(), config, free_bytes=FREE)
            assert result['status'] == 'blocked' and '57014' in result['reason']
            saved = await archive._job(conn, job['job_id'])
            assert saved['stage_analyzed'] and not saved['topic_analyzed'] and not saved['analyzed']
            assert saved['rows_copied'] == saved['rows_verified'] == len(expected)
            assert await conn.fetchval('SHOW statement_timeout') == before
            assert not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory')")
            assert [tuple(row) for row in await conn.fetch('SELECT * FROM public.mqtt_messages ORDER BY id,created_at')] == expected
    asyncio.run(scenario())
