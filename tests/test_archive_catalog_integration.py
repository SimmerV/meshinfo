"""Actual startup GIN compatibility on the dedicated archive-test database."""
import asyncio
import os

import pytest

from storage.db import archive_catalog as catalog
from test_archive_integration import fixture, until_done

pytestmark = pytest.mark.skipif(not os.environ.get('MESHINFO_ARCHIVE_TEST_DSN'),
                                reason='MESHINFO_ARCHIVE_TEST_DSN not set')


def test_populated_legacy_heap_without_topic_index_gets_automatic_parent_repair():
    async def scenario():
        async with fixture() as (conn, expected):
            await conn.execute('DROP INDEX public.mqtt_topic_leaf')
            assert not await catalog.has_topic_index(conn, {'oid': await conn.fetchval(
                "SELECT 'public.mqtt_messages_1901_01'::regclass::oid")})
            await catalog.ensure_heap_topic_indexes(conn, allow_legacy_parent=True)
            parents = await catalog.parent_gin_indexes(conn)
            assert len(parents) == 1 and catalog.supported_parent(parents[0])
            assert [tuple(row) for row in await conn.fetch('SELECT * FROM mqtt_messages ORDER BY id,created_at')] == expected
    asyncio.run(scenario())


def test_mixed_history_refuses_parent_gin_and_still_indexes_empty_rollover_leaf():
    async def scenario():
        async with fixture() as (conn, expected):
            await until_done(conn)
            await conn.execute("INSERT INTO public.mqtt_messages(id,created_at,payload) VALUES(90001,now(),'hot data')")
            with pytest.raises(RuntimeError, match='space-checked'):
                await catalog.ensure_heap_topic_indexes(conn, allow_legacy_parent=True)
            assert await catalog.parent_gin_indexes(conn) == []
            default = next(row for row in await catalog.heap_leaves(conn) if row['name'] == 'mqtt_messages_default')
            assert await catalog.has_topic_index(conn, default)
            assert [tuple(row) for row in await conn.fetch('SELECT * FROM mqtt_messages_1901_01 ORDER BY id,created_at')] == expected
            assert await conn.fetchval('SELECT payload FROM mqtt_messages WHERE id=90001') == 'hot data'
    asyncio.run(scenario())
