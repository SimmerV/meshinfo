"""Operator CLI honors policy defaults and makes execution failures observable."""

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripts import compress_storage as cli
from storage.db.compression import CompressionRefused, MIB


def config(path=''):
    return {'storage': {'postgres': {'host':'example','database':'meshinfo','username':'postgres'},
                        'compression': {'enabled':False,'after_months':12,'index_fillfactor':95,
                                        'max_index_mib':32,'reserve_mib':2048,'max_seconds':3.5,
                                        'database_filesystem':str(path)}}}


def test_omitted_flags_preserve_configured_policy_and_filesystem():
    args = cli.parser().parse_args(['plan'])
    limits, filesystem = cli.resolved_packing_settings(args, config('/database'))
    assert (limits.after_months, limits.fillfactor, limits.max_index_bytes,
            limits.reserve_bytes, limits.max_seconds) == (12,95,32*MIB,2048*MIB,3.5)
    assert filesystem == Path('/database')


def test_explicit_zero_age_and_other_flags_override_config():
    args = cli.parser().parse_args(['plan','--after-months','0','--fillfactor','100','--max-index-mib','8',
                                    '--reserve-mib','1024','--max-seconds','1.5','--database-filesystem','/override'])
    limits, filesystem = cli.resolved_packing_settings(args, config('/database'))
    assert (limits.after_months, limits.fillfactor, limits.max_index_bytes,
            limits.reserve_bytes, limits.max_seconds) == (0,100,8*MIB,1024*MIB,1.5)
    assert filesystem == Path('/override')


@pytest.mark.parametrize('command', ['plan','benchmark','benchmark-index'])
def test_apply_is_rejected_before_config_or_database_access(command, monkeypatch):
    monkeypatch.setattr(cli, 'load_config', lambda _: pytest.fail('configuration should not be read'))
    args = cli.parser().parse_args([command,'--apply'])
    with pytest.raises(ValueError, match='only valid with pack-index'):
        asyncio.run(cli.run(args))


def setup_apply(monkeypatch, tmp_path, *, result=None, error=None):
    monkeypatch.setattr(cli,'load_config',lambda _: config(tmp_path))
    connection = SimpleNamespace(close=AsyncMock())
    connector = AsyncMock(return_value=connection)
    packer = AsyncMock(return_value=result or {'status':'packed','bytes_saved':16384}, side_effect=error)
    recorder = AsyncMock()
    monkeypatch.setattr(cli,'connect',connector)
    monkeypatch.setattr(cli,'pack_index',packer)
    monkeypatch.setattr(cli,'save_status',recorder)
    args = cli.parser().parse_args(['pack-index','example_index','--apply'])
    return args, connection, connector, packer, recorder


def test_explicit_apply_works_when_automation_disabled_and_persists_actual_outcome(monkeypatch,tmp_path):
    args,connection,connector,packer,recorder = setup_apply(monkeypatch,tmp_path)
    result = asyncio.run(cli.run(args))
    assert result['status'] == 'packed'
    assert cli.exit_code(result) == 0
    assert connector.await_args.kwargs['readonly'] is False
    assert packer.await_args.kwargs['limits'].after_months == 12
    recorder.assert_awaited_once_with(connection,'compression',result)
    connection.close.assert_awaited_once()


@pytest.mark.parametrize('status',['busy','blocked','error'])
def test_unsuccessful_outcome_is_saved_and_returns_failure(status,monkeypatch,tmp_path):
    args,connection,_,_,recorder = setup_apply(monkeypatch,tmp_path,result={'status':status})
    result = asyncio.run(cli.run(args))
    recorder.assert_awaited_once_with(connection,'compression',result)
    assert cli.exit_code(result) == 1


@pytest.mark.parametrize('error,status', [(CompressionRefused('not enough disk'),'blocked'),(TimeoutError(),'error')])
def test_packing_exception_is_recorded_as_failed_attempt(error,status,monkeypatch,tmp_path):
    args,connection,_,_,recorder = setup_apply(monkeypatch,tmp_path,error=error)
    result = asyncio.run(cli.run(args))
    assert result['status'] == status and cli.exit_code(result) == 1
    recorder.assert_awaited_once_with(connection,'compression',result)


def test_failed_audit_write_does_not_hide_already_completed_packing(monkeypatch,tmp_path):
    args,connection,_,_,recorder = setup_apply(monkeypatch,tmp_path)
    recorder.side_effect = TimeoutError()
    result = asyncio.run(cli.run(args))
    assert result['status'] == 'error' and result['status_persisted'] is False
    assert result['operation_status'] == 'packed' and result['bytes_saved'] == 16384
    assert cli.exit_code(result) == 1
    connection.close.assert_awaited_once()


def test_plan_never_packs_or_persists_status(monkeypatch,tmp_path):
    args,connection,connector,packer,recorder = setup_apply(monkeypatch,tmp_path)
    args.apply = False
    planner = AsyncMock(return_value={'indexes':[{'index':'example_index'}]})
    monkeypatch.setattr(cli,'plan_index_packing',planner)
    result = asyncio.run(cli.run(args))
    assert result['indexes'] == [{'index':'example_index'}]
    assert connector.await_args.kwargs['readonly'] is True
    packer.assert_not_awaited()
    recorder.assert_not_awaited()
