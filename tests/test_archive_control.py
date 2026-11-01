"""Operator opt-in, cancellation and public failure contracts."""
import argparse
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripts import manage_archive
from storage.db import archive_control
from storage.db.history_queries import HistoryReadError


def test_default_configuration_does_not_create_archive_objects(monkeypatch):
    conn = SimpleNamespace(fetchval=AsyncMock(return_value=None))
    create = AsyncMock()
    monkeypatch.setattr(archive_control, 'ensure_archive_schema', create)
    assert asyncio.run(archive_control.run_archive_work(conn, {})) == {'status': 'disabled'}
    create.assert_not_called()


def test_disabling_compression_unfreezes_an_unfinished_original(monkeypatch):
    conn = SimpleNamespace(fetchval=AsyncMock(return_value='storage_archive_jobs'),
                           fetchrow=AsyncMock(return_value={'job_id': 'job', 'target_method': 'columnar'}))
    cancel = AsyncMock(return_value={'status': 'cancelled'})
    monkeypatch.setattr(archive_control, 'cancel_archive', cancel)
    result = asyncio.run(archive_control.run_archive_work(conn, {}))
    assert result['status'] == 'cancelled'
    cancel.assert_awaited_once_with(conn, 'job', config={})


def test_explicit_heap_restore_continues_with_automatic_compression_off(monkeypatch):
    conn = SimpleNamespace(fetchval=AsyncMock(return_value=True),
                           fetchrow=AsyncMock(return_value={'job_id': 'job', 'target_method': 'heap'}))
    cancel = AsyncMock()
    indexes = AsyncMock()
    runner = AsyncMock(return_value={'status': 'copying', 'target_method': 'heap'})
    monkeypatch.setattr(archive_control, 'cancel_archive', cancel)
    monkeypatch.setattr(archive_control, 'prepare_archive_indexes', indexes)
    monkeypatch.setattr(archive_control, 'run_archive_cycle', runner)
    result = asyncio.run(archive_control.run_archive_work(conn, {}, free_bytes=lambda: 10**10))
    assert result['target_method'] == 'heap'
    cancel.assert_not_called()
    indexes.assert_not_called()
    runner.assert_awaited_once()


def test_restore_requires_disabling_automatic_recompression(monkeypatch):
    start = AsyncMock()
    monkeypatch.setattr(archive_control, 'start_restore_archive', start)
    result = asyncio.run(archive_control.restore_archive_work(object(), 'partition', {
        'storage': {'archive': {'enabled': True}}}, free_bytes=lambda: 10**10))
    assert result['status'] == 'blocked'
    start.assert_not_called()


@pytest.mark.parametrize('reason', ['extension', 'filesystem'])
def test_preflight_refusal_never_starts_a_migration(monkeypatch, reason):
    conn = SimpleNamespace(fetchval=AsyncMock(side_effect=['storage_archive_jobs', reason != 'extension']))
    runner = AsyncMock()
    indexes = AsyncMock()
    monkeypatch.setattr(archive_control, 'run_archive_cycle', runner)
    monkeypatch.setattr(archive_control, 'prepare_archive_indexes', indexes)
    result = asyncio.run(archive_control.run_archive_work(conn, {'storage': {'archive': {'enabled': True}}}))
    assert result['status'] == 'blocked'
    runner.assert_not_called()
    indexes.assert_not_called()


def test_partial_index_preparation_defers_compression(monkeypatch):
    conn = SimpleNamespace(fetchval=AsyncMock(return_value=True))
    indexes = AsyncMock(return_value={'status': 'partial', 'reason': 'one index built'})
    runner = AsyncMock()
    monkeypatch.setattr(archive_control, 'prepare_archive_indexes', indexes)
    monkeypatch.setattr(archive_control, 'run_archive_cycle', runner)
    config = {'storage': {'archive': {'enabled': True}}}
    result = asyncio.run(archive_control.run_archive_work(conn, config, free_bytes=lambda: 10**10))
    assert result['status'] == 'partial'
    runner.assert_not_called()


@pytest.mark.parametrize('command', ['run', 'cancel', 'restore'])
def test_cli_requires_explicit_apply_before_opening_database(monkeypatch, command):
    monkeypatch.setattr(manage_archive, 'load_config', lambda _: {})
    connect = AsyncMock()
    monkeypatch.setattr(manage_archive, 'maintenance_connection', connect)
    with pytest.raises(ValueError, match='require --apply'):
        asyncio.run(manage_archive.run(manage_archive.parser().parse_args([command])))
    connect.assert_not_called()


def test_cli_cycle_limits_and_help_are_bounded():
    assert len(manage_archive.parser().format_help()) < 3000
    assert manage_archive.bounded_cycles('10000') == 10000
    for value in ('0', '10001', 'no', '1.2'):
        with pytest.raises(argparse.ArgumentTypeError):
            manage_archive.bounded_cycles(value)


@pytest.mark.parametrize('timed_out,code,message', [(True,504,'history_query_timeout'), (False,503,'history_unavailable')])
def test_history_errors_are_retryable_and_never_cached_as_empty(timed_out, code, message):
    from api.api import app, history_read_error
    assert app.exception_handlers[HistoryReadError] is history_read_error
    result = asyncio.run(history_read_error(None, HistoryReadError('private operation', timed_out=timed_out)))
    assert result.status_code == code
    assert json.loads(result.body) == {'error': message, 'retryable': True}
    assert result.headers['cache-control'] == 'no-store'
    assert result.headers['retry-after'] == '2'
    assert b'private' not in result.body
