"""Real cursor durability and shared-lock exclusion, without rebuilding an index."""

import asyncio
from contextlib import asynccontextmanager
import os
import re
from urllib.parse import unquote, urlsplit

import asyncpg
import pytest

from storage.db import index_maintenance as packing
from storage.db.maintenance_status import save_status
from storage.db.retention_executor import MAINTENANCE_LOCK_KEY

DSN = os.environ.get('MESHINFO_STORAGE_TEST_DSN', '')
pytestmark = pytest.mark.skipif(not DSN, reason='MESHINFO_STORAGE_TEST_DSN not set')
OPERATIONS = ['compression', 'compression_cursor']


@asynccontextmanager
async def fixture():
    database = unquote(urlsplit(DSN).path.lstrip('/'))
    if not re.fullmatch(r'[A-Za-z0-9_]+_storage_test', database):
        pytest.fail('Rotation integration requires a dedicated *_storage_test database')
    conn = await asyncpg.connect(DSN, timeout=5, command_timeout=3)
    saved = None
    try:
        assert await conn.fetchval('SELECT current_database()') == database
        saved = await conn.fetch('SELECT operation,updated_at,result::text AS result '
                                 'FROM storage_maintenance_status WHERE operation=ANY($1::text[])', OPERATIONS)
        await conn.execute('DELETE FROM storage_maintenance_status WHERE operation=ANY($1::text[])', OPERATIONS)
        yield conn
    finally:
        if saved is not None:
            await conn.execute('DELETE FROM storage_maintenance_status WHERE operation=ANY($1::text[])', OPERATIONS)
            for row in saved:
                await conn.execute('INSERT INTO storage_maintenance_status(operation,updated_at,result) '
                                   'VALUES($1,$2,$3::jsonb)', row['operation'], row['updated_at'], row['result'])
        await conn.close()


@pytest.mark.parametrize('interruption', ['failure', 'cancel'])
def test_cursor_survives_restart_and_status_writes_and_releases_real_lock(interruption, monkeypatch, tmp_path):
    async def scenario():
        async with fixture() as observer:
            conn = await asyncpg.connect(DSN, timeout=5, command_timeout=3)
            cfg = {'storage': {'compression': {'enabled': True, 'database_filesystem': str(tmp_path)}}}
            attempts = []
            started = asyncio.Event()
            async def plan(*args, **kwargs):
                return {'indexes': [{'index': name, 'eligible': True, 'tablespace': None} for name in ('b', 'a')]}
            async def controlled_rebuild(connection, name, **kwargs):
                attempts.append(name)
                assert not connection.is_in_transaction()
                assert await observer.fetchval("SELECT result->>'last_index' FROM storage_maintenance_status "
                                               "WHERE operation='compression_cursor'") == name
                assert not await observer.fetchval('SELECT pg_try_advisory_lock($1::bigint)', MAINTENANCE_LOCK_KEY)
                if len(attempts) == 1:
                    if interruption == 'failure':
                        raise TimeoutError('simulated bounded rebuild failure')
                    started.set()
                    await asyncio.Event().wait()
                return {'status': 'unchanged', 'index': name}
            monkeypatch.setattr(packing, 'plan_index_packing', plan)
            monkeypatch.setattr(packing, '_pack_index_locked', controlled_rebuild)
            task = asyncio.create_task(packing.run_index_packing_cycle(conn, cfg))
            try:
                if interruption == 'cancel':
                    await asyncio.wait_for(started.wait(), 3)
                    task.cancel()
                with pytest.raises(asyncio.CancelledError if interruption == 'cancel' else TimeoutError):
                    await asyncio.wait_for(task, 3)
                # Every lock claim must be gone even though the cursor remains.
                assert await observer.fetchval('SELECT pg_try_advisory_lock($1::bigint)', MAINTENANCE_LOCK_KEY)
                try:
                    assert await packing.run_index_packing_cycle(conn, cfg) == {'status': 'busy'}
                finally:
                    await observer.fetchval('SELECT pg_advisory_unlock($1::bigint)', MAINTENANCE_LOCK_KEY)
                await save_status(observer, 'compression', {'status': 'busy'})
                assert await observer.fetchval("SELECT result->>'last_index' FROM storage_maintenance_status "
                                               "WHERE operation='compression_cursor'") == 'a'
                await conn.close()
                conn = await asyncpg.connect(DSN, timeout=5, command_timeout=3)
                result = await packing.run_index_packing_cycle(conn, cfg)
                assert result == {'status': 'unchanged', 'index': 'b'} and attempts == ['a', 'b']
                assert await observer.fetchval('SELECT pg_try_advisory_lock($1::bigint)', MAINTENANCE_LOCK_KEY)
                await observer.fetchval('SELECT pg_advisory_unlock($1::bigint)', MAINTENANCE_LOCK_KEY)
                assert await observer.fetchval("SELECT count(*) FROM storage_maintenance_status "
                                               "WHERE operation='compression_cursor'") == 1
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                await conn.close()
    asyncio.run(scenario())
