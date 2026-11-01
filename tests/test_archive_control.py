"""Operator opt-in, cancellation and public failure contracts."""
import argparse
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from contextlib import asynccontextmanager
import uuid

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


@pytest.mark.parametrize('status', ['busy', 'cleanup_pending', 'blocked', 'cancelled'])
@pytest.mark.parametrize('existing_reason', [None, 'Source ownership could not be established'])
def test_disabled_policy_reports_actual_cancellation_progress_and_preserves_refusals(
        monkeypatch, status, existing_reason):
    job = uuid.uuid4()
    conn = SimpleNamespace(fetchval=AsyncMock(return_value=True),
                           fetchrow=AsyncMock(return_value={
                               'job_id': job, 'target_method': 'columnar', 'phase': 'copying'}))
    outcome = {'status': status, 'job_id': str(job)}
    if status == 'cleanup_pending':
        outcome.update(cleanup_requested=True, cleanup_topics_dropped=True,
                       cleanup_stage_dropped=False, cleanup_error='Source reader holds a lock')
    if existing_reason is not None:
        outcome['reason'] = existing_reason
    expected = dict(outcome)
    cancel = AsyncMock(return_value=outcome)
    runner, indexes = AsyncMock(), AsyncMock()
    monkeypatch.setattr(archive_control, 'cancel_archive', cancel)
    monkeypatch.setattr(archive_control, 'run_archive_cycle', runner)
    monkeypatch.setattr(archive_control, 'prepare_archive_indexes', indexes)

    result = asyncio.run(archive_control.run_archive_work(conn, {}, target_job_id=job))

    assert all(result[key] == value for key, value in expected.items())
    if existing_reason is not None:
        assert result['reason'] == existing_reason
    elif status == 'cancelled':
        assert result['reason'] == (
            'Archiving disabled; unfinished replacement discarded, original history preserved')
    else:
        assert result['reason'] == (
            'Archiving disabled; cancellation requested but not yet complete')
        assert 'discarded' not in result['reason']
    cancel.assert_awaited_once_with(conn, job, config={})
    runner.assert_not_called()
    indexes.assert_not_called()
    conn.fetchval.assert_awaited_once()


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
    conn = SimpleNamespace(fetchval=AsyncMock(side_effect=['storage_archive_jobs', reason != 'extension']),
                           fetchrow=AsyncMock(return_value=None))
    runner = AsyncMock()
    indexes = AsyncMock()
    monkeypatch.setattr(archive_control, 'run_archive_cycle', runner)
    monkeypatch.setattr(archive_control, 'prepare_archive_indexes', indexes)
    result = asyncio.run(archive_control.run_archive_work(conn, {'storage': {'archive': {'enabled': True}}}))
    assert result['status'] == 'blocked'
    runner.assert_not_called()
    indexes.assert_not_called()


def test_partial_index_preparation_defers_compression(monkeypatch):
    conn = SimpleNamespace(fetchval=AsyncMock(return_value=True), fetchrow=AsyncMock(return_value=None))
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


@pytest.mark.parametrize('arguments', [
    ['run', '--allow-estimate-shortfall', '--apply'],
    ['run', '--allow-estimate-shortfall', '--partition', 'month'],
    ['plan', '--allow-estimate-shortfall', '--partition', 'month', '--apply'],
    ['restore', '--allow-estimate-shortfall', '--partition', 'month', '--apply'],
    ['run', '--partition', 'month', '--apply'],
    ['run', '--allow-estimate-shortfall', '--partition', 'month', '--apply', '--job', str(uuid.uuid4())],
])
def test_guarded_cli_rejects_incomplete_or_ambiguous_authorization_before_connect(monkeypatch, arguments):
    connect = AsyncMock()
    monkeypatch.setattr(manage_archive, 'maintenance_connection', connect)
    with pytest.raises(ValueError):
        asyncio.run(manage_archive.run(manage_archive.parser().parse_args(arguments)))
    connect.assert_not_called()


def test_guarded_cli_argument_failure_is_actionable_without_opening_configuration(monkeypatch, capsys):
    load = AsyncMock()
    monkeypatch.setattr(manage_archive, 'load_config', load)
    monkeypatch.setattr(manage_archive.sys, 'argv', ['manage_archive.py', 'run', '--allow-estimate-shortfall'])
    assert manage_archive.main() == 2
    assert 'requires run, --partition NAME and --apply' in capsys.readouterr().err
    load.assert_not_called()


def cli_setup(monkeypatch):
    conn = object()
    @asynccontextmanager
    async def connect(*args, **kwargs):
        yield conn
    monkeypatch.setattr(manage_archive, 'load_config', lambda _: {'storage': {'archive': {'enabled': True}}})
    monkeypatch.setattr(manage_archive, 'maintenance_connection', connect)
    monkeypatch.setattr(manage_archive, 'save_status', AsyncMock())
    monkeypatch.setattr(manage_archive.asyncio, 'sleep', AsyncMock())
    return conn


def test_guarded_cli_advances_only_the_admitted_job_and_stops_at_completion(monkeypatch, capsys):
    conn = cli_setup(monkeypatch)
    job = str(uuid.uuid4())
    start = AsyncMock(return_value={'status': 'copying', 'job_id': job, 'partition': 'month',
                                   'admission_mode': 'guarded', 'admission_free_bytes': 12_000_000_000})
    advance = AsyncMock(side_effect=[{'status': 'busy'}, {'status': 'verifying', 'job_id': job},
                                    {'status': 'complete', 'job_id': job}])
    monkeypatch.setattr(manage_archive, 'start_guarded_archive_work', start)
    monkeypatch.setattr(manage_archive, 'run_archive_work', advance)
    args = manage_archive.parser().parse_args(['run', '--partition', 'month', '--allow-estimate-shortfall',
                                               '--apply', '--until-idle', '--max-cycles', '8'])
    assert asyncio.run(manage_archive.run(args)) == 0
    start.assert_awaited_once()
    assert advance.await_count == 3
    assert all(call.kwargs == {'target_job_id': job} for call in advance.await_args_list)
    rows = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert rows[0]['admission_free_bytes'] == 12_000_000_000
    assert rows[-1]['status'] == 'complete'


@pytest.mark.parametrize('status', ['failed', 'cancelled', 'blocked'])
def test_targeted_cli_stops_after_failure_and_never_readmits(monkeypatch, status):
    cli_setup(monkeypatch)
    job = str(uuid.uuid4())
    start = AsyncMock()
    advance = AsyncMock(return_value={'status': status, 'job_id': job, 'cleanup_requested': status == 'blocked'})
    monkeypatch.setattr(manage_archive, 'start_guarded_archive_work', start)
    monkeypatch.setattr(manage_archive, 'run_archive_work', advance)
    args = manage_archive.parser().parse_args(['run', '--job', job, '--apply', '--until-idle'])
    assert asyncio.run(manage_archive.run(args)) == 2
    start.assert_not_called()
    advance.assert_awaited_once()


def test_targeted_cli_refuses_a_mismatched_progress_identity(monkeypatch):
    cli_setup(monkeypatch)
    advance = AsyncMock(return_value={'status': 'complete', 'job_id': str(uuid.uuid4())})
    monkeypatch.setattr(manage_archive, 'run_archive_work', advance)
    args = manage_archive.parser().parse_args(['run', '--job', str(uuid.uuid4()), '--apply', '--until-idle'])
    with pytest.raises(ValueError, match='unexpected target'):
        asyncio.run(manage_archive.run(args))
    advance.assert_awaited_once()


def test_targeted_cli_finishes_durable_cleanup_without_readmitting(monkeypatch):
    cli_setup(monkeypatch)
    job = str(uuid.uuid4())
    start = AsyncMock()
    advance = AsyncMock(side_effect=[
        {'status': 'cleanup_pending', 'job_id': job, 'cleanup_stage_dropped': True},
        {'status': 'failed', 'job_id': job, 'cleanup_requested': False}])
    monkeypatch.setattr(manage_archive, 'start_guarded_archive_work', start)
    monkeypatch.setattr(manage_archive, 'run_archive_work', advance)
    args = manage_archive.parser().parse_args(['run', '--job', job, '--apply', '--until-idle'])
    assert asyncio.run(manage_archive.run(args)) == 2
    start.assert_not_called()
    assert advance.await_count == 2


def test_cleanup_default_matches_engine_and_public_status_keeps_audit_private():
    from config import DEFAULT_CONFIG
    from storage.db.archive import ArchivePolicy
    from storage.db.maintenance_status import public_status
    assert DEFAULT_CONFIG['storage']['archive']['cleanup_seconds'] == ArchivePolicy().cleanup_seconds == 300
    report = public_status({'archive': {'updated_at': 'now', 'result': {
        'status': 'cleanup_pending', 'admission_free_bytes': 12345,
        'cleanup_reason': 'private capacity reason', 'cleanup_stage_dropped': True}}})
    assert report == {'archive': {'updated_at': 'now', 'status': 'cleanup_pending', 'scopes': {}}}


def test_guarded_cli_busy_start_exhausts_bounded_cycles_without_ordinary_admission(monkeypatch):
    cli_setup(monkeypatch)
    start = AsyncMock(return_value={'status': 'busy'})
    advance = AsyncMock()
    monkeypatch.setattr(manage_archive, 'start_guarded_archive_work', start)
    monkeypatch.setattr(manage_archive, 'run_archive_work', advance)
    args = manage_archive.parser().parse_args(['run', '--partition', 'month', '--allow-estimate-shortfall',
                                               '--apply', '--until-idle', '--max-cycles', '2'])
    assert asyncio.run(manage_archive.run(args)) == 2
    assert start.await_count == 2
    advance.assert_not_called()


def test_target_control_never_prepares_indexes_or_selects_another_job(monkeypatch):
    job = uuid.uuid4()
    conn = SimpleNamespace(fetchval=AsyncMock(return_value=True),
                           fetchrow=AsyncMock(return_value={'job_id': job, 'target_method': 'columnar', 'phase': 'copying'}))
    runner = AsyncMock(return_value={'status': 'copying', 'job_id': str(job)})
    prepare = AsyncMock()
    monkeypatch.setattr(archive_control, 'run_archive_cycle', runner)
    monkeypatch.setattr(archive_control, 'prepare_archive_indexes', prepare)
    config = {'storage': {'archive': {'enabled': True}}}
    free = lambda: 10**10
    asyncio.run(archive_control.run_archive_work(conn, config, free_bytes=free, target_job_id=str(job)))
    runner.assert_awaited_once_with(conn, config, free_bytes=free, target_job_id=job)
    prepare.assert_not_called()
    assert conn.fetchrow.await_args.args[1] == job


def test_disabled_target_control_cancels_only_the_requested_job(monkeypatch):
    job = uuid.uuid4()
    conn = SimpleNamespace(fetchval=AsyncMock(return_value=True),
                           fetchrow=AsyncMock(return_value={'job_id': job, 'target_method': 'columnar', 'phase': 'copying'}))
    cancel = AsyncMock(return_value={'status': 'cancelled', 'job_id': str(job)})
    runner = AsyncMock()
    monkeypatch.setattr(archive_control, 'cancel_archive', cancel)
    monkeypatch.setattr(archive_control, 'run_archive_cycle', runner)
    asyncio.run(archive_control.run_archive_work(conn, {}, target_job_id=job))
    cancel.assert_awaited_once_with(conn, job, config={})
    runner.assert_not_called()


def test_terminal_target_reports_without_filesystem_admission_or_schema_mutation(monkeypatch):
    job = uuid.uuid4()
    conn = SimpleNamespace(fetchval=AsyncMock(return_value=True),
                           fetchrow=AsyncMock(return_value={'job_id': job, 'target_method': 'columnar', 'phase': 'complete'}))
    runner = AsyncMock(return_value={'status': 'complete', 'job_id': str(job)})
    create = AsyncMock()
    monkeypatch.setattr(archive_control, 'run_archive_cycle', runner)
    monkeypatch.setattr(archive_control, 'ensure_archive_schema', create)
    result = asyncio.run(archive_control.run_archive_work(conn, {}, target_job_id=job))
    assert result['status'] == 'complete'
    create.assert_not_called()
    runner.assert_awaited_once_with(conn, {}, free_bytes=None, target_job_id=job)


@pytest.mark.parametrize('enabled,targeted', [(False, False), (True, False), (False, True), (True, True)])
def test_durable_cleanup_precedes_policy_capacity_and_index_checks(monkeypatch, enabled, targeted):
    job = uuid.uuid4()
    conn = SimpleNamespace(fetchval=AsyncMock(return_value=True), fetchrow=AsyncMock(return_value={
        'job_id': job, 'target_method': 'columnar', 'phase': 'copying', 'cleanup_requested': True}))
    cleanup = AsyncMock(return_value={'status': 'failed', 'job_id': str(job), 'cleanup_requested': False})
    prepare, cancel = AsyncMock(), AsyncMock()
    monkeypatch.setattr(archive_control, 'run_archive_cycle', cleanup)
    monkeypatch.setattr(archive_control, 'prepare_archive_indexes', prepare)
    monkeypatch.setattr(archive_control, 'cancel_archive', cancel)
    config = {'storage': {'archive': {'enabled': enabled, 'database_filesystem': '/missing/probe'}}}
    result = asyncio.run(archive_control.run_archive_work(conn, config, target_job_id=job if targeted else None))
    assert result['status'] == 'failed'
    cleanup.assert_awaited_once_with(conn, config, free_bytes=None, target_job_id=job)
    prepare.assert_not_called()
    cancel.assert_not_called()
    # Only journal existence was queried: extension/capacity are irrelevant to cleanup.
    conn.fetchval.assert_awaited_once()


@pytest.mark.parametrize('eligible,method', [(False, 'heap'), (True, 'columnar')])
def test_guarded_control_rejects_ineligible_target_before_index_work(monkeypatch, eligible, method):
    conn = SimpleNamespace(fetchval=AsyncMock(return_value=True))
    monkeypatch.setattr(archive_control, 'ensure_archive_schema', AsyncMock())
    monkeypatch.setattr(archive_control, 'plan_archive', AsyncMock(return_value={
        'partitions': [{'name': 'month', 'eligible': eligible, 'method': method}]}))
    start, prepare = AsyncMock(), AsyncMock()
    monkeypatch.setattr(archive_control, 'start_guarded_archive', start)
    monkeypatch.setattr(archive_control, 'prepare_archive_indexes', prepare)
    result = asyncio.run(archive_control.start_guarded_archive_work(conn, 'month',
        {'storage': {'archive': {'enabled': True}}}, free_bytes=lambda: 10**10))
    assert result['status'] == 'blocked'
    start.assert_not_called()
    prepare.assert_not_called()


def test_guarded_control_passes_named_request_only_after_normal_index_preparation(monkeypatch):
    conn = SimpleNamespace(fetchval=AsyncMock(return_value=True))
    monkeypatch.setattr(archive_control, 'ensure_archive_schema', AsyncMock())
    monkeypatch.setattr(archive_control, 'plan_archive', AsyncMock(return_value={
        'partitions': [{'name': 'month', 'eligible': True, 'method': 'heap'}]}))
    prepare = AsyncMock(side_effect=[{'status': 'partial'}, {'status': 'complete'}])
    start = AsyncMock(return_value={'status': 'copying'})
    monkeypatch.setattr(archive_control, 'start_guarded_archive', start)
    monkeypatch.setattr(archive_control, 'prepare_archive_indexes', prepare)
    cfg = {'storage': {'archive': {'enabled': True}}}
    free = lambda: 10**10
    assert asyncio.run(archive_control.start_guarded_archive_work(conn, 'month', cfg, free_bytes=free)) == {'status': 'partial'}
    start.assert_not_called()
    assert asyncio.run(archive_control.start_guarded_archive_work(conn, 'month', cfg, free_bytes=free)) == {'status': 'copying'}
    start.assert_awaited_once_with(conn, 'month', cfg, free_bytes=free)
