"""Explicit backups use the controller's connection without creating an idle pool."""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from scripts import manage_storage
from storage import backup_controller


def test_backup_database_override_is_passed_without_pool_or_cli_connection(monkeypatch):
    cfg = {'storage': {'postgres': {'host':'example','database':'source','username':'user'}},
           'backups': {'schedule':'off'}}
    monkeypatch.setattr(manage_storage,'load_config',lambda _: cfg)
    pool = AsyncMock(side_effect=AssertionError('CLI must not create a pool'))
    direct = AsyncMock(side_effect=AssertionError('controller owns its connection'))
    monkeypatch.setattr(manage_storage.asyncpg,'create_pool',pool)
    monkeypatch.setattr(manage_storage.asyncpg,'connect',direct)
    runner = AsyncMock(return_value={'status':'complete','bytes':123})
    monkeypatch.setattr(backup_controller,'run_backup',runner)
    args = manage_storage.parser().parse_args(['backup','--apply','--database','isolated_storage_test'])
    result = asyncio.run(manage_storage.report(args))
    assert result == {'status':'complete','bytes':123}
    assert runner.await_args.args[0]['storage']['postgres']['database'] == 'isolated_storage_test'
    assert runner.await_args.args[1] is None
    assert isinstance(runner.await_args.args[2],Path)
    assert runner.await_args.kwargs == {'manual':True}
    pool.assert_not_awaited()
    direct.assert_not_awaited()


def test_backup_without_apply_cannot_open_connections(monkeypatch):
    monkeypatch.setattr(manage_storage,'load_config',lambda _: {'storage':{'postgres':{'database':'test'}}})
    runner = AsyncMock()
    monkeypatch.setattr(backup_controller,'run_backup',runner)
    args = manage_storage.parser().parse_args(['backup'])
    with pytest.raises(ValueError,match='requires --apply'):
        asyncio.run(manage_storage.report(args))
    runner.assert_not_awaited()
