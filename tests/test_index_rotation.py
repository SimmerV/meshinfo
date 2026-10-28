"""Automatic packing makes bounded durable progress even when candidates fail."""

import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from storage.db import index_maintenance as packing
from storage.db.maintenance_status import public_status


class Connection:
    def __init__(self, durable=None):
        self.durable = durable if durable is not None else {}
        self.lock_depth = 0
        self.busy = False
        self.in_transaction = False
        self.fail_read = False
        self.fail_write = False
        self.events = []

    def is_in_transaction(self):
        return self.in_transaction

    async def fetchval(self, query, *args, **kwargs):
        if 'pg_try_advisory_lock' in query:
            self.events.append('lock')
            if self.busy:
                return False
            self.lock_depth += 1
            return True
        if 'pg_advisory_unlock' in query:
            self.events.append('unlock')
            self.lock_depth -= 1
            return True
        assert "operation='compression_cursor'" in query
        assert kwargs['timeout'] == 3 and self.lock_depth == 1
        self.events.append('read_cursor')
        if self.fail_read:
            raise RuntimeError('cursor read failed')
        return self.durable.get('last_index')

    async def execute(self, query, data, **kwargs):
        assert "VALUES ('compression_cursor'" in query
        assert kwargs['timeout'] == 3 and self.lock_depth == 1
        self.events.append('save_cursor')
        if self.fail_write:
            raise RuntimeError('cursor save failed')
        self.durable.update(json.loads(data))


def candidate(name, **kwargs):
    return {'index': name, 'eligible': True, 'tablespace': None, **kwargs}


def config(tmp_path):
    return {'storage': {'compression': {'enabled': True, 'database_filesystem': str(tmp_path)}}}


def prepare(monkeypatch, candidates):
    async def plan(conn, **kwargs):
        assert conn.lock_depth == 1
        return {'indexes': candidates}
    plan_mock = AsyncMock(side_effect=plan)
    monkeypatch.setattr(packing, 'plan_index_packing', plan_mock)
    return plan_mock


@pytest.mark.parametrize('previous,names,expected', [
    (None, ['z', 'b', 'a'], 'a'),
    ('a', ['c', 'b'], 'b'),  # previous candidate was removed
    ('b', ['b', 'a'], 'a'),  # wrap around after candidate removal
    ('b', ['d', 'a', 'c'], 'c'),  # candidates added on both sides
    ('z', ['b', 'a'], 'a'),
    (False, ['b', 'a'], 'a'),
    ('a', [], None),
])
def test_stable_successor_handles_candidate_churn_and_wrap(previous, names, expected):
    result = packing._next_packing_candidate([candidate(name) for name in names], previous)
    assert (result['index'] if result else None) == expected


def test_repeated_failure_rotates_across_restarted_connections(monkeypatch, tmp_path):
    prepare(monkeypatch, [candidate('z', index_bytes=1), candidate('b'), candidate('a')])
    durable, attempts = {}, []
    async def fail(conn, name, **kwargs):
        assert conn.lock_depth == 1 and conn.durable['last_index'] == name
        attempts.append(name)
        raise TimeoutError('bounded rebuild exceeded limit')
    monkeypatch.setattr(packing, '_pack_index_locked', fail)
    for _ in range(5):
        conn = Connection(durable)  # no process-local cursor survives
        with pytest.raises(TimeoutError):
            asyncio.run(packing.run_index_packing_cycle(conn, config(tmp_path)))
        assert conn.lock_depth == 0
        assert conn.events == ['lock', 'read_cursor', 'save_cursor', 'unlock']
    assert attempts == ['a', 'b', 'z', 'a', 'b']
    assert durable == {'last_index': 'b'}


def test_busy_preserves_cursor_and_does_not_plan_or_pack(monkeypatch, tmp_path):
    plan = prepare(monkeypatch, [candidate('b')])
    rebuild = AsyncMock()
    monkeypatch.setattr(packing, '_pack_index_locked', rebuild)
    conn = Connection({'last_index': 'a'})
    conn.busy = True
    assert asyncio.run(packing.run_index_packing_cycle(conn, config(tmp_path))) == {'status': 'busy'}
    assert conn.durable == {'last_index': 'a'} and conn.events == ['lock']
    plan.assert_not_awaited()
    rebuild.assert_not_awaited()


def test_no_eligible_candidates_keeps_position(monkeypatch, tmp_path):
    prepare(monkeypatch, [candidate('a', eligible=False), candidate('b', tablespace='custom')])
    rebuild = AsyncMock()
    monkeypatch.setattr(packing, '_pack_index_locked', rebuild)
    conn = Connection({'last_index': 'previous'})
    assert asyncio.run(packing.run_index_packing_cycle(conn, config(tmp_path))) == {'status': 'no_candidates'}
    assert conn.durable == {'last_index': 'previous'} and conn.lock_depth == 0
    assert 'save_cursor' not in conn.events
    rebuild.assert_not_awaited()


@pytest.mark.parametrize('failure', ['read', 'write'])
def test_cursor_database_failure_prevents_rebuild(monkeypatch, tmp_path, failure):
    prepare(monkeypatch, [candidate('b')])
    rebuild = AsyncMock()
    monkeypatch.setattr(packing, '_pack_index_locked', rebuild)
    conn = Connection({'last_index': 'a'})
    setattr(conn, f'fail_{failure}', True)
    with pytest.raises(RuntimeError):
        asyncio.run(packing.run_index_packing_cycle(conn, config(tmp_path)))
    assert conn.durable == {'last_index': 'a'} and conn.lock_depth == 0
    rebuild.assert_not_awaited()


def test_caller_transaction_is_rejected_before_lock_or_cursor_write(monkeypatch, tmp_path):
    plan = prepare(monkeypatch, [candidate('a')])
    conn = Connection()
    conn.in_transaction = True
    with pytest.raises(ValueError, match='outside a transaction'):
        asyncio.run(packing.run_index_packing_cycle(conn, config(tmp_path)))
    assert not conn.events and not conn.durable
    plan.assert_not_awaited()


def test_cancelled_attempt_preserves_cursor_and_releases_lock(monkeypatch, tmp_path):
    async def scenario():
        prepare(monkeypatch, [candidate('a'), candidate('b')])
        started = asyncio.Event()
        async def rebuild(conn, name, **kwargs):
            assert conn.durable['last_index'] == name and conn.lock_depth == 1
            started.set()
            await asyncio.Event().wait()
        monkeypatch.setattr(packing, '_pack_index_locked', rebuild)
        conn = Connection()
        task = asyncio.create_task(packing.run_index_packing_cycle(conn, config(tmp_path)))
        await asyncio.wait_for(started.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert conn.durable == {'last_index': 'a'} and conn.lock_depth == 0
        next_rebuild = AsyncMock(return_value={'status': 'unchanged'})
        monkeypatch.setattr(packing, '_pack_index_locked', next_rebuild)
        restarted = Connection(conn.durable)
        await packing.run_index_packing_cycle(restarted, config(tmp_path))
        assert next_rebuild.await_args.args[1] == 'b'
    asyncio.run(scenario())


def test_manual_pack_has_one_lock_and_never_advances_automatic_cursor(monkeypatch):
    conn = Connection({'last_index': 'previous'})
    async def rebuild(connection, name, **kwargs):
        assert connection.lock_depth == 1
        assert name == 'explicit_index'
        return {'status': 'packed'}
    monkeypatch.setattr(packing, '_pack_index_locked', rebuild)
    assert asyncio.run(packing.pack_index(conn, 'explicit_index', free_bytes=lambda: 10**12)) == {'status': 'packed'}
    assert conn.events == ['lock', 'unlock'] and conn.durable == {'last_index': 'previous'}


def test_private_scheduler_cursor_is_not_exposed_in_public_status():
    assert public_status({'compression_cursor': {'updated_at': 'now', 'result': {'last_index': 'private_index'}}}) == {}
