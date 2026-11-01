"""ANALYZE has a bounded independent budget without extending copy cycles."""
import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
import time
import uuid
from unittest.mock import AsyncMock

import pytest

from config import DEFAULT_CONFIG
from storage.db import archive


def test_analysis_default_and_ceiling_are_independent_of_short_copy_cycles():
    policy = archive.ArchivePolicy()
    assert policy.analyze_seconds == DEFAULT_CONFIG['storage']['archive']['analyze_seconds'] == 300
    assert (policy.cycle_seconds, policy.statement_seconds) == (10, 3)
    assert archive.ArchivePolicy(analyze_seconds=1800).analyze_seconds == 1800
    for invalid in (1800.01, 0, True, float('inf'), float('nan')):
        with pytest.raises(ValueError):
            archive.ArchivePolicy(analyze_seconds=invalid)
    with pytest.raises(ValueError, match='shorter than'):
        archive.ArchivePolicy(cycle_seconds=1, statement_seconds=1)


def ready_connection(monkeypatch):
    job = {'job_id': uuid.uuid4(), 'phase': 'ready', 'source_name': 'source', 'stage_name': 'stage',
           'topic_stage_name': 'topics', 'target_method': 'columnar', 'abort_count': 0,
           'analyzed': False, 'stage_analyzed': False, 'topic_analyzed': False,
           'rows_copied': 33, 'rows_verified': 33}

    class Connection:
        delay = 0
        released = False

        def __init__(self):
            self.analyze_calls = []
            self.entered = asyncio.Event()

        def is_in_transaction(self):
            return False

        def is_closed(self):
            return False

        @asynccontextmanager
        async def transaction(self):
            previous = deepcopy(job)
            try:
                yield
            except BaseException:
                job.clear()
                job.update(previous)
                raise

        async def fetchval(self, sql, *args, **kwargs):
            if 'pg_wal_lsn_diff' in sql:
                return 0
            if 'pg_current_wal_insert_lsn' in sql:
                return '0/0'
            if 'SELECT job_id FROM' in sql:
                return job['job_id']
            raise AssertionError(sql)

        async def execute(self, sql, *args, **kwargs):
            if sql.startswith('ANALYZE '):
                self.analyze_calls.append((sql, kwargs['timeout']))
                self.entered.set()
                # Deliberately ignore the per-command deadline here: the
                # outer phase deadline must still bound an unresponsive call.
                await asyncio.sleep(self.delay)
            elif 'SET stage_analyzed=$2' in sql:
                job.update(stage_analyzed=args[1], topic_analyzed=args[2], analyzed=args[1] and args[2])

    conn = Connection()

    @asynccontextmanager
    async def locked(_conn):
        try:
            yield True
        finally:
            conn.released = True

    async def get_job(*_args):
        return deepcopy(job)

    monkeypatch.setattr(archive, 'maintenance_lock', locked)
    monkeypatch.setattr(archive, '_environment', AsyncMock())
    monkeypatch.setattr(archive, '_validate_job', AsyncMock())
    monkeypatch.setattr(archive, '_recover_inflight', AsyncMock())
    monkeypatch.setattr(archive, '_job', get_job)
    for name in ('_copy_chunk', '_verify_chunk', '_cutover'):
        monkeypatch.setattr(archive, name, AsyncMock(side_effect=AssertionError('Unexpected '+name)))
    return conn, job


def settings(analyze_seconds=3):
    return {'storage': {'archive': {'enabled': True, 'cycle_seconds': 1,
                                   'statement_seconds': .1, 'analyze_seconds': analyze_seconds}}}


def test_one_analysis_can_outlive_copy_cycle_and_each_relation_commits_once(monkeypatch):
    async def scenario():
        conn, job = ready_connection(monkeypatch)
        conn.delay = 1.05
        first = await archive.run_archive_cycle(conn, settings(), free_bytes=lambda: 20*1024**3)
        assert first['status'] == 'ready' and first['elapsed_seconds'] > 1
        assert job['stage_analyzed'] and not job['topic_analyzed'] and not job['analyzed']
        assert conn.analyze_calls == [('ANALYZE "public"."stage"', 3.25)]
        second = await archive.run_archive_cycle(conn, settings(), free_bytes=lambda: 20*1024**3)
        assert second['status'] == 'ready' and job['analyzed']
        assert conn.analyze_calls == [('ANALYZE "public"."stage"', 3.25),
                                      ('ANALYZE "public"."topics"', 3.25)]
        assert job['rows_copied'] == job['rows_verified'] == 33
    asyncio.run(scenario())


def test_analysis_phase_deadline_preserves_completed_relation_checkpoint(monkeypatch):
    async def scenario():
        conn, job = ready_connection(monkeypatch)
        job['stage_analyzed'] = True
        conn.delay = 30
        started = time.monotonic()
        result = await archive.run_archive_cycle(conn, settings(.1), free_bytes=lambda: 20*1024**3)
        assert result['status'] == 'blocked' and result['reason'] == 'TimeoutError'
        assert time.monotonic()-started < 4
        assert job['stage_analyzed'] and not job['topic_analyzed'] and not job['analyzed']
        assert job['rows_copied'] == job['rows_verified'] == 33 and conn.released
        assert [call[0] for call in conn.analyze_calls] == ['ANALYZE "public"."topics"']
    asyncio.run(scenario())


def test_external_shutdown_cancellation_is_not_swallowed_by_extended_budget(monkeypatch):
    async def scenario():
        conn, job = ready_connection(monkeypatch)
        conn.delay = 30
        task = asyncio.create_task(archive.run_archive_cycle(conn, settings(), free_bytes=lambda: 20*1024**3))
        await asyncio.wait_for(conn.entered.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert conn.released and not job['stage_analyzed'] and not job['analyzed']
        assert job['rows_copied'] == job['rows_verified'] == 33
    asyncio.run(scenario())


def test_large_analysis_budget_does_not_extend_copy_work(monkeypatch):
    async def scenario():
        conn, job = ready_connection(monkeypatch)
        job['phase'] = 'copying'

        async def slow_copy(*_args):
            await asyncio.sleep(30)

        monkeypatch.setattr(archive, '_copy_chunk', slow_copy)
        started = time.monotonic()
        result = await archive.run_archive_cycle(conn, settings(300), free_bytes=lambda: 20*1024**3)
        assert result['status'] == 'blocked' and result['reason'] == 'TimeoutError'
        assert time.monotonic()-started < 2
        assert not conn.analyze_calls and conn.released
    asyncio.run(scenario())
