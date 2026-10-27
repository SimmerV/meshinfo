"""Small PostgreSQL index packing cases; only a dedicated *_compression_test DB."""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import os
from urllib.parse import urlsplit

import asyncpg
import pytest

from storage.db.compression import CompressionRefused, GIB
from storage.db.index_maintenance import IndexPackingLimits, pack_index, plan_index_packing
from storage.db.retention_executor import MAINTENANCE_LOCK_KEY

DSN = os.environ.get("MESHINFO_COMPRESSION_TEST_DSN", "")
pytestmark = pytest.mark.skipif(not DSN, reason="MESHINFO_COMPRESSION_TEST_DSN not set")
PART = 'mqtt_messages_compression_test_1900_01'
RECV = 'packet_receptions_compression_test_1900_01'
BASE = 8_700_000_000_000_000


@asynccontextmanager
async def fixture():
    database = urlsplit(DSN).path.lstrip('/')
    if not database.endswith('_compression_test'):
        pytest.fail('Packing tests require a dedicated *_compression_test database')
    conn = await asyncpg.connect(DSN, command_timeout=10)
    assert await conn.fetchval('SELECT current_database()') == database
    try:
        for table, parent in ((PART, 'mqtt_messages'), (RECV, 'packet_receptions')):
            await conn.execute(f"CREATE TABLE {table} PARTITION OF {parent} FOR VALUES FROM ('1900-01-01+00') TO ('1900-02-01+00')")
        start = datetime(1900, 1, 1, tzinfo=timezone.utc)
        rows = [(BASE + i, 'msh/compression-test', '{"text":"' + ('payload ☃ ' * 30) + str(i) + '"}',
                 start + timedelta(seconds=i), 'abcd1900', i) for i in range(4000)]
        await conn.copy_records_to_table(PART, columns=('id','topic','payload','created_at','from_node_id','packet_id'), records=rows)
        await conn.copy_records_to_table(RECV, columns=('mqtt_row_id','created_at','gateway'),
                                         records=[(BASE + i, start + timedelta(seconds=i, milliseconds=1), 'ffff1900') for i in range(4000)])
        await conn.execute(f'ANALYZE {PART}')
        name = await conn.fetchval("""SELECT c.relname FROM pg_index x JOIN pg_class c ON c.oid=x.indexrelid
            JOIN pg_attribute a ON a.attrelid=x.indrelid AND a.attnum=x.indkey[0]
            WHERE x.indrelid=$1::regclass AND x.indnkeyatts=1 AND a.attname='created_at'""", PART)
        yield conn, name
    finally:
        await conn.execute(f'DROP TABLE IF EXISTS {RECV}, {PART}')
        await conn.close()


async def snapshot(conn):
    packets = [tuple(r) for r in await conn.fetch(f'SELECT * FROM {PART} ORDER BY id,created_at')]
    receipts = [tuple(r) for r in await conn.fetch(f'SELECT * FROM {RECV} ORDER BY mqtt_row_id,created_at')]
    return packets, receipts


async def index_state(conn, name):
    return dict(await conn.fetchrow("""SELECT c.oid::bigint,c.relfilenode,c.reloptions,pg_relation_size(c.oid) AS bytes,
        i.indisvalid,(SELECT inhparent FROM pg_inherits WHERE inhrelid=c.oid) AS parent
        FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid WHERE c.oid=$1::regclass""", name))


def test_pack_saves_space_preserves_all_data_and_attachment_and_is_idempotently_skipped():
    async def scenario():
        async with fixture() as (conn, name):
            before_data = await snapshot(conn)
            before = await index_state(conn, name)
            result = await pack_index(conn, name, free_bytes=lambda: 10 * GIB)
            assert result['status'] == 'packed' and result['bytes_saved'] > 0
            assert await snapshot(conn) == before_data
            after = await index_state(conn, name)
            assert after['oid'] == before['oid'] and after['parent'] == before['parent'] and after['indisvalid']
            assert after['bytes'] < before['bytes']
            plan = await plan_index_packing(conn)
            detail = next(row for row in plan['indexes'] if row['index'] == name)
            assert detail['reason'] == 'already_configured' and not detail['eligible']
    asyncio.run(scenario())


@pytest.mark.parametrize('failure', ['headroom', 'post_build_floor', 'cancel_after_build', 'timeout'])
def test_failure_rolls_back_index_options_files_and_data_without_staging_leftovers(failure):
    class Proxy:
        def __init__(self, conn):
            self.conn = conn
        def __getattr__(self, name):
            return getattr(self.conn, name)
        async def execute(self, query, *args, **kwargs):
            if query.startswith('REINDEX INDEX') and failure == 'timeout':
                await asyncio.sleep(1)
            result = await self.conn.execute(query, *args, **kwargs)
            if query.startswith('REINDEX INDEX') and failure == 'cancel_after_build':
                raise asyncio.CancelledError()
            return result
    async def scenario():
        async with fixture() as (conn, name):
            data, before = await snapshot(conn), await index_state(conn, name)
            indexes_before = await conn.fetchval('SELECT count(*) FROM pg_index WHERE indrelid=$1::regclass', PART)
            calls = 0
            def free():
                nonlocal calls
                calls += 1
                return 0 if failure == 'headroom' or (failure == 'post_build_floor' and calls > 2) else 10 * GIB
            expected = asyncio.CancelledError if failure == 'cancel_after_build' else TimeoutError if failure == 'timeout' else CompressionRefused
            with pytest.raises(expected):
                await pack_index(Proxy(conn), name, free_bytes=free,
                                 limits=IndexPackingLimits(max_seconds=.1 if failure == 'timeout' else 5))
            assert await index_state(conn, name) == before
            assert await snapshot(conn) == data
            assert await conn.fetchval('SELECT count(*) FROM pg_index WHERE indrelid=$1::regclass', PART) == indexes_before
            # A fresh session must be able to acquire the maintenance lock.
            other = await asyncpg.connect(DSN)
            try:
                assert await other.fetchval('SELECT pg_try_advisory_lock($1::bigint)', MAINTENANCE_LOCK_KEY)
            finally:
                await other.close()
    asyncio.run(scenario())


def test_writer_contention_is_refused_without_changing_index():
    async def scenario():
        async with fixture() as (conn, name):
            other = await asyncpg.connect(DSN)
            try:
                before = await index_state(conn, name)
                async with other.transaction():
                    await other.execute(f'LOCK TABLE {PART} IN ROW EXCLUSIVE MODE')
                    with pytest.raises(asyncpg.LockNotAvailableError):
                        await pack_index(conn, name, free_bytes=lambda: 10 * GIB)
                assert await index_state(conn, name) == before
            finally:
                await other.close()
    asyncio.run(scenario())


def test_shared_maintenance_lock_prevents_overlapping_work():
    async def scenario():
        async with fixture() as (conn, name):
            other = await asyncpg.connect(DSN)
            try:
                await other.fetchval('SELECT pg_advisory_lock($1::bigint)', MAINTENANCE_LOCK_KEY)
                result = await pack_index(conn, name, free_bytes=lambda: 10 * GIB)
                assert result['status'] == 'busy'
            finally:
                await other.close()
    asyncio.run(scenario())


def test_index_growth_between_preview_and_lock_rechecks_size_limit():
    class GrowingIndex:
        def __init__(self, conn):
            self.conn, self.reads = conn, 0
        def __getattr__(self, name):
            return getattr(self.conn, name)
        async def fetchrow(self, query, *args, **kwargs):
            row = await self.conn.fetchrow(query, *args, **kwargs)
            if 'AND idx.relname=$1' in query:
                self.reads += 1
                if self.reads == 2:
                    return {**dict(row), 'index_bytes': 65 * 1024**2}
            return row
    async def scenario():
        async with fixture() as (conn, name):
            before = await index_state(conn, name)
            with pytest.raises(CompressionRefused, match='above_size_limit'):
                await pack_index(GrowingIndex(conn), name, free_bytes=lambda: 10 * GIB)
            assert await index_state(conn, name) == before
    asyncio.run(scenario())
