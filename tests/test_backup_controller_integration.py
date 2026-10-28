"""Real maintenance sessions with a one-slot application pool and synthetic producer.

Opt in with MESHINFO_STORAGE_TEST_DSN pointing to a dedicated *_storage_test DB
initialized with MeshInfo's schema. No actual dump is made. The controlled
producer keeps the real advisory lock/session open while application reads and
writes exercise the only pool slot; cleanup restores prior backup status.
"""

import asyncio
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import re
from types import SimpleNamespace
from urllib.parse import unquote, urlsplit

import asyncpg
import pytest

from storage import backup_controller
from storage.backup import BackupResult
from storage.db.retention_executor import MAINTENANCE_LOCK_KEY

DSN = os.environ.get('MESHINFO_STORAGE_TEST_DSN','')
pytestmark = pytest.mark.skipif(not DSN,reason='MESHINFO_STORAGE_TEST_DSN not set')


@asynccontextmanager
async def fixture(tmp_path):
    url = urlsplit(DSN)
    database = unquote(url.path.lstrip('/'))
    if not re.fullmatch(r'[A-Za-z0-9_]+_storage_test',database):
        pytest.fail('Controller integration requires a dedicated *_storage_test database')
    pg = {'host':url.hostname,'port':url.port or 5432,'database':database,
          'username':unquote(url.username or 'postgres'),'password':unquote(url.password or ''),
          'min_pool_size':1,'max_pool_size':1}
    pool = await asyncpg.create_pool(DSN,min_size=1,max_size=1,timeout=5,command_timeout=3)
    previous = None
    try:
        async with pool.acquire(timeout=1) as conn:
            assert await conn.fetchval('SELECT current_database()') == database
            assert await conn.fetchval("SELECT to_regclass('public.storage_identity') IS NOT NULL")
            previous = await conn.fetchrow("SELECT updated_at,result::text AS result FROM storage_maintenance_status WHERE operation='backup'")
        cfg = {'storage':{'postgres':pg},'backups':{'schedule':'off','dir':str(tmp_path),'mode':'local'}}
        yield cfg, SimpleNamespace(pool=pool,pg_config=pg)
    finally:
        async with pool.acquire(timeout=1) as conn:
            if previous is None:
                await conn.execute("DELETE FROM storage_maintenance_status WHERE operation='backup'")
            else:
                await conn.execute("""INSERT INTO storage_maintenance_status(operation,updated_at,result)
                    VALUES('backup',$1,$2::jsonb) ON CONFLICT(operation) DO UPDATE
                    SET updated_at=EXCLUDED.updated_at,result=EXCLUDED.result""",previous['updated_at'],previous['result'])
        await pool.close()
        backup_controller._BACKOFF_UNTIL.clear()


async def control_pid(conn):
    rows = await conn.fetch("""SELECT pid FROM pg_stat_activity WHERE datname=current_database()
        AND application_name='meshinfo-backup-control' AND pid<>pg_backend_pid()""")
    assert len(rows) == 1
    return rows[0]['pid']


async def assert_session_closed(conn,pid):
    for _ in range(50):
        if not await conn.fetchval('SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)',pid):
            return
        await asyncio.sleep(.01)
    pytest.fail('maintenance control connection was not closed')


def test_one_slot_pool_reads_and_writes_continue_while_real_backup_lock_is_held(monkeypatch,tmp_path):
    async def scenario():
        async with fixture(tmp_path) as (cfg,storage):
            started,finish = asyncio.Event(),asyncio.Event()
            async def producer(*args,**kwargs):
                started.set()
                await finish.wait()
                return BackupResult('synthetic','synthetic.dump',123,'a'*64,1,'archive_decode',())
            monkeypatch.setattr(backup_controller,'create_snapshot',producer)
            task = asyncio.create_task(backup_controller.run_backup(cfg,storage,Path('/unused'),manual=True))
            try:
                await asyncio.wait_for(started.wait(),3)
                async with storage.pool.acquire(timeout=.5) as conn:
                    pid = await control_pid(conn)
                    assert pid != conn.get_server_pid()
                    assert not await conn.fetchval('SELECT pg_try_advisory_lock($1::bigint)',MAINTENANCE_LOCK_KEY)
                    assert await conn.fetchval('SELECT 1') == 1
                    await conn.execute('CREATE TEMP TABLE controller_pool_probe(value integer)')
                    await conn.execute('INSERT INTO controller_pool_probe VALUES(7)')
                    assert await conn.fetchval('SELECT value FROM controller_pool_probe') == 7
                    assert not task.done()
                finish.set()
                result = await asyncio.wait_for(task,3)
                assert result['status'] == 'complete' and result['status_persisted']
                async with storage.pool.acquire(timeout=.5) as conn:
                    await assert_session_closed(conn,pid)
                    assert await conn.fetchval('SELECT pg_try_advisory_lock($1::bigint)',MAINTENANCE_LOCK_KEY)
                    await conn.fetchval('SELECT pg_advisory_unlock($1::bigint)',MAINTENANCE_LOCK_KEY)
                    saved = json.loads(await conn.fetchval("SELECT result::text FROM storage_maintenance_status WHERE operation='backup'"))
                    assert saved['status'] == 'complete' and saved['snapshot_completed']
            finally:
                finish.set()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task,return_exceptions=True)
    asyncio.run(scenario())


@pytest.mark.parametrize('outcome',['cancel','terminate'])
def test_control_session_loss_or_cancellation_stops_producer_and_closes_connection(outcome,monkeypatch,tmp_path):
    async def scenario():
        async with fixture(tmp_path) as (cfg,storage):
            started,cleaned = asyncio.Event(),asyncio.Event()
            async def producer(*args,**kwargs):
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    await asyncio.sleep(.01)
                    cleaned.set()
            monkeypatch.setattr(backup_controller,'create_snapshot',producer)
            task = asyncio.create_task(backup_controller.run_backup(cfg,storage,Path('/unused'),manual=True))
            try:
                await asyncio.wait_for(started.wait(),3)
                async with storage.pool.acquire(timeout=.5) as conn:
                    pid = await control_pid(conn)
                    if outcome == 'terminate':
                        assert await conn.fetchval('SELECT pg_terminate_backend($1)',pid)
                if outcome == 'cancel':
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await asyncio.wait_for(task,4)
                else:
                    result = await asyncio.wait_for(task,8)
                    assert result['status'] == 'error'
                assert cleaned.is_set()
                async with storage.pool.acquire(timeout=.5) as conn:
                    await assert_session_closed(conn,pid)
                    assert await conn.fetchval('SELECT 1') == 1
                    assert await conn.fetchval('SELECT pg_try_advisory_lock($1::bigint)',MAINTENANCE_LOCK_KEY)
                    await conn.fetchval('SELECT pg_advisory_unlock($1::bigint)',MAINTENANCE_LOCK_KEY)
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task,return_exceptions=True)
    asyncio.run(scenario())
