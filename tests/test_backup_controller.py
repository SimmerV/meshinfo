"""Durable scheduling, monitored lock ownership and honest publication outcomes."""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from storage import backup_controller as controller
from storage.backup import BackupError, BackupResult

NOW = datetime(2026, 10, 27, 12, tzinfo=timezone.utc)
IDENTITY = '12345678-1234-1234-1234-123456789abc'
OLD_SUCCESS = (NOW - timedelta(days=3)).isoformat()


def config(**settings):
    return {'storage': {'postgres': {'host':'db','database':'test','username':'operator','password':'secret'}},
            'backups': {'schedule':'daily','mode':'local','dir':'backups','remote_target':'backup@example:/snapshots',**settings}}


def snapshot(*, prune_error=None):
    return BackupResult(destination='/private/path/secret.dump',filename='private-file.dump',bytes=12345,
                        sha256='a'*64,completed_at=NOW.timestamp(),verification='archive_decode',pruned=(),
                        prune_error=prune_error)


class Connection:
    def __init__(self, *, previous=None, busy=False):
        self.previous = previous or {}
        self.busy = busy
        self.records = []
        self.events = []
        self.locked = False
        self.heartbeat_failure = False
        self.fail_save_status = None
        self.listeners = []
        self.active_query = False
        self.final_clock_failure = False
        self.clock_calls = 0
        self.identity = IDENTITY

    def add_termination_listener(self, callback):
        self.listeners.append(callback)

    def remove_termination_listener(self, callback):
        self.listeners.remove(callback)

    async def fetchval(self, query, *args, **kwargs):
        assert not self.active_query, 'concurrent asyncpg operation'
        self.active_query = True
        try:
            if 'pg_try_advisory_lock' in query:
                self.locked = not self.busy
                self.events.append('lock' if self.locked else 'busy')
                return self.locked
            if 'pg_advisory_unlock' in query:
                self.events.append('unlock')
                self.locked = False
                return True
            if 'clock_timestamp' in query:
                self.clock_calls += 1
                if self.final_clock_failure and self.clock_calls > 1:
                    raise ConnectionError('private failure')
                return NOW
            if 'to_regclass' in query:
                return 'storage_maintenance_status'
            if 'instance_id' in query:
                return self.identity
            if query == 'SELECT 1':
                self.events.append('heartbeat')
                await asyncio.sleep(0)
                if self.heartbeat_failure:
                    raise ConnectionError('secret hostname')
                return 1
            raise AssertionError(query)
        finally:
            self.active_query = False

    async def fetch(self, query, *args, **kwargs):
        assert not self.active_query
        if not self.previous:
            return []
        return [{'operation':'backup','updated_at':self.previous.get('updated_at',NOW),
                 'result':self.previous.get('result',{})}]

    async def execute(self, query, *args, **kwargs):
        assert not self.active_query, 'status write raced heartbeat'
        if 'storage_maintenance_status' in query:
            detail = json.loads(args[1])
            self.events.append('save:'+detail['status'])
            assert self.locked, 'status written outside maintenance lock'
            if self.fail_save_status == detail['status']:
                raise TimeoutError('private database host')
            self.records.append(detail)
            return 'INSERT 0 1'
        raise AssertionError(query)


class Pool:
    def __init__(self, connection, *, fail=False):
        self.connection, self.fail = connection, fail
        self.acquisitions = 0

    @asynccontextmanager
    async def acquire(self, *, timeout):
        assert timeout == 3
        self.acquisitions += 1
        if self.fail:
            raise OSError('secret connection path')
        try:
            yield self.connection
        finally:
            self.connection.events.append('release')


def storage(connection=None, **kwargs):
    return SimpleNamespace(pool=Pool(connection or Connection(), **kwargs))


@pytest.fixture(autouse=True)
def reset_backoff():
    controller._BACKOFF_UNTIL.clear()
    yield
    controller._BACKOFF_UNTIL.clear()


def test_off_never_reads_database_or_filesystem_or_starts_snapshot(monkeypatch):
    monkeypatch.setattr(controller,'create_snapshot',AsyncMock())
    cfg = config(schedule='off')
    assert not asyncio.run(controller.scheduled_backup_due(cfg,None,now=NOW))
    assert asyncio.run(controller.run_backup(cfg,None,Path('/unused'))) == {'status':'disabled'}
    controller.create_snapshot.assert_not_awaited()


@pytest.mark.parametrize('schedule,age,due',[('daily',23,False),('daily',24,True),('weekly',24*6,False),('weekly',24*7,True),('monthly',24*29,False),('monthly',24*30,True)])
def test_durable_success_controls_schedules_for_ssh(schedule,age,due):
    cfg = config(schedule=schedule,mode='ssh')
    conn = Connection(previous={'result':{'status':'complete','last_success_at':(NOW-timedelta(hours=age)).isoformat(),
                                          'last_success_destination':controller.backup_destination_id(cfg)}})
    assert asyncio.run(controller.scheduled_backup_due(cfg,storage(conn),now=NOW)) is due


def test_local_legacy_mtime_fallback_only_when_no_durable_success(tmp_path):
    archive = tmp_path/'meshinfo-pg-legacy.dump'
    archive.write_bytes(b'legacy')
    os.utime(archive,(NOW.timestamp()-30,NOW.timestamp()-30))
    assert not asyncio.run(controller.scheduled_backup_due(config(dir=str(tmp_path)),storage(),now=NOW))
    assert asyncio.run(controller.scheduled_backup_due(config(dir=str(tmp_path),mode='ssh'),storage(),now=NOW))
    conn = Connection(previous={'result':{'status':'error','last_success_at':OLD_SUCCESS},'updated_at':NOW-timedelta(hours=2)})
    assert asyncio.run(controller.scheduled_backup_due(config(dir=str(tmp_path)),storage(conn),now=NOW))


@pytest.mark.parametrize('state',['error','blocked','running'])
def test_durable_failed_or_interrupted_attempt_has_one_hour_backoff(state):
    conn = Connection(previous={'result':{'status':state,'last_success_at':OLD_SUCCESS},'updated_at':NOW-timedelta(minutes=59)})
    assert not asyncio.run(controller.scheduled_backup_due(config(mode='ssh'),storage(conn),now=NOW))
    assert asyncio.run(controller.scheduled_backup_due(config(mode='ssh'),storage(conn),now=NOW+timedelta(minutes=2)))


def test_database_schedule_failure_is_not_retried_every_tick():
    target = storage(fail=True)
    assert not asyncio.run(controller.scheduled_backup_due(config(),target,now=NOW))
    assert not asyncio.run(controller.scheduled_backup_due(config(),target,now=NOW))
    assert target.pool.acquisitions == 1


def test_invalid_now_is_rejected():
    with pytest.raises(ValueError,match='timezone-aware'):
        asyncio.run(controller.scheduled_backup_due(config(),storage(),now=datetime(2026,1,1)))


def test_success_holds_lock_through_snapshot_and_status_and_hides_destinations(monkeypatch):
    conn = Connection(previous={'result':{'last_success_at':OLD_SUCCESS}})
    async def perform(*args,**kwargs):
        assert conn.locked and conn.records[-1]['status'] == 'running'
        assert kwargs['owner_id'] == hashlib.sha256(IDENTITY.encode()).hexdigest()
        conn.events.append('snapshot')
        return snapshot()
    monkeypatch.setattr(controller,'create_snapshot',perform)
    result = asyncio.run(controller.run_backup(config(),storage(conn),Path('/repo')))
    assert result['status'] == 'complete' and result['status_persisted']
    assert result['last_success_at'] == controller._iso(NOW)
    assert conn.events.index('save:complete') < conn.events.index('unlock') < conn.events.index('release')
    assert conn.records[0]['last_success_at'] == controller._iso(datetime.fromisoformat(OLD_SUCCESS))
    encoded = json.dumps(result)
    assert not any(secret in encoded for secret in ('private-file','/private','secret'))
    assert conn.listeners == []


def test_verified_snapshot_with_prune_failure_is_partial_and_advances_success(monkeypatch):
    monkeypatch.setattr(controller,'create_snapshot',AsyncMock(return_value=snapshot(prune_error='private path')))
    conn = Connection(previous={'result':{'last_success_at':OLD_SUCCESS}})
    result = asyncio.run(controller.run_backup(config(),storage(conn),Path('/repo')))
    assert result['status'] == 'partial' and result['snapshot_completed']
    assert result['last_success_at'] == controller._iso(NOW)
    assert result['reason'] == 'BackupRetentionFailed' and 'private path' not in json.dumps(result)


def test_dump_failure_preserves_prior_success_and_backoff(monkeypatch):
    monkeypatch.setattr(controller,'create_snapshot',AsyncMock(side_effect=BackupError('secret path')))
    conn = Connection(previous={'result':{'last_success_at':OLD_SUCCESS}})
    result = asyncio.run(controller.run_backup(config(),storage(conn),Path('/repo')))
    assert result['status'] == 'error' and result['reason'] == 'BackupError'
    assert result['last_success_at'] == controller._iso(datetime.fromisoformat(OLD_SUCCESS))
    assert not asyncio.run(controller.scheduled_backup_due(config(),storage(conn),now=NOW))
    assert 'secret' not in json.dumps(result)


def test_busy_does_not_overwrite_another_running_job(monkeypatch):
    producer = AsyncMock()
    monkeypatch.setattr(controller,'create_snapshot',producer)
    conn = Connection(busy=True)
    result = asyncio.run(controller.run_backup(config(),storage(conn),Path('/repo')))
    assert result['status'] == 'busy' and not conn.records
    producer.assert_not_awaited()


@pytest.mark.parametrize('bad_identity',[None,'not-a-uuid'])
def test_missing_identity_prevents_snapshot_creation(bad_identity,monkeypatch):
    producer = AsyncMock()
    monkeypatch.setattr(controller,'create_snapshot',producer)
    conn = Connection()
    conn.identity = bad_identity
    result = asyncio.run(controller.run_backup(config(),storage(conn),Path('/repo')))
    assert result['status'] == 'error' and result['reason'] == 'StorageIdentityMissing'
    producer.assert_not_awaited()


def test_failed_running_status_prevents_dump(monkeypatch):
    producer = AsyncMock()
    monkeypatch.setattr(controller,'create_snapshot',producer)
    conn = Connection()
    conn.fail_save_status = 'running'
    result = asyncio.run(controller.run_backup(config(),storage(conn),Path('/repo')))
    assert result['status'] == 'error'
    producer.assert_not_awaited()


def test_failed_final_status_reports_already_published_snapshot(monkeypatch):
    monkeypatch.setattr(controller,'create_snapshot',AsyncMock(return_value=snapshot()))
    conn = Connection()
    conn.fail_save_status = 'complete'
    result = asyncio.run(controller.run_backup(config(),storage(conn),Path('/repo')))
    assert result['status'] == 'error' and result['operation_status'] == 'complete'
    assert result['snapshot_completed'] and result['bytes'] == 12345
    assert result['status_persisted'] is False


def test_final_clock_failure_preserves_acknowledged_publication(monkeypatch):
    monkeypatch.setattr(controller,'create_snapshot',AsyncMock(return_value=snapshot()))
    conn = Connection()
    conn.final_clock_failure = True
    result = asyncio.run(controller.run_backup(config(),storage(conn),Path('/repo')))
    assert result['status'] == 'error' and result['operation_status'] == 'complete'
    assert result['snapshot_completed'] and result['last_success_at'] == result['started_at']


@pytest.mark.parametrize('failure',['heartbeat','termination','cancel'])
def test_loss_or_cancellation_waits_for_snapshot_cleanup_before_unlock(failure,monkeypatch):
    monkeypatch.setattr(controller,'HEARTBEAT_SECONDS',.01)
    async def scenario():
        conn = Connection()
        started = asyncio.Event()
        async def producer(*args,**kwargs):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(.02)
                assert conn.locked
                conn.events.append('snapshot-cleanup')
        monkeypatch.setattr(controller,'create_snapshot',producer)
        task = asyncio.create_task(controller.run_backup(config(),storage(conn),Path('/repo')))
        await started.wait()
        if failure == 'heartbeat':
            conn.heartbeat_failure = True
            result = await task
            assert result['reason'] == 'BackupControlLost'
        elif failure == 'termination':
            for callback in list(conn.listeners):
                callback(conn)
            result = await task
            assert result['reason'] == 'BackupControlLost'
        else:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert conn.events.index('snapshot-cleanup') < conn.events.index('unlock') < conn.events.index('release')
        assert conn.records[-1]['status'] == 'error'
        assert conn.listeners == []
    asyncio.run(scenario())


def test_after_task_finishes_before_lock_attempt(monkeypatch):
    monkeypatch.setattr(controller,'create_snapshot',AsyncMock(return_value=snapshot()))
    async def scenario():
        conn = Connection()
        async def prior():
            await asyncio.sleep(.01)
            conn.events.append('retention-done')
        after = asyncio.create_task(prior())
        result = await controller.run_backup(config(),storage(conn),Path('/repo'),after=after)
        assert result['status'] == 'complete'
        assert conn.events.index('retention-done') < conn.events.index('lock')
    asyncio.run(scenario())


def test_explicit_manual_backup_can_run_with_automatic_schedule_off(monkeypatch):
    producer = AsyncMock(return_value=snapshot())
    monkeypatch.setattr(controller,'create_snapshot',producer)
    conn = Connection()
    result = asyncio.run(controller.run_backup(config(schedule='off'),storage(conn),Path('/repo'),manual=True))
    assert result['status'] == 'complete' and result['snapshot_completed']
    producer.assert_awaited_once()
    assert conn.records[-1]['status'] == 'complete'


def test_repeated_cancellation_cannot_release_lock_during_snapshot_cleanup(monkeypatch):
    async def scenario():
        conn = Connection()
        started, cleaning, finish = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def producer(*args,**kwargs):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaning.set()
                await finish.wait()
                assert conn.locked
                conn.events.append('snapshot-cleanup')
        monkeypatch.setattr(controller,'create_snapshot',producer)
        task = asyncio.create_task(controller.run_backup(config(),storage(conn),Path('/repo')))
        await started.wait()
        task.cancel()
        await cleaning.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert conn.locked and not task.done()
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert conn.events.index('snapshot-cleanup') < conn.events.index('unlock')
    asyncio.run(scenario())


def test_finishing_snapshot_drains_inflight_heartbeat_before_status_write(monkeypatch):
    monkeypatch.setattr(controller,'HEARTBEAT_SECONDS',.001)
    async def scenario():
        heartbeat_started = asyncio.Event()
        class SlowHeartbeat(Connection):
            async def fetchval(self,query,*args,**kwargs):
                if query == 'SELECT 1':
                    assert not self.active_query
                    self.active_query = True
                    heartbeat_started.set()
                    await asyncio.sleep(.02)
                    self.active_query = False
                    self.events.append('heartbeat-finished')
                    return 1
                return await super().fetchval(query,*args,**kwargs)
        conn = SlowHeartbeat()
        async def producer(*args,**kwargs):
            await heartbeat_started.wait()
            return snapshot()
        monkeypatch.setattr(controller,'create_snapshot',producer)
        result = await controller.run_backup(config(),storage(conn),Path('/repo'))
        assert result['status'] == 'complete'
        assert conn.events.index('heartbeat-finished') < conn.events.index('save:complete')
    asyncio.run(scenario())


@pytest.mark.parametrize('change',[{'mode':'ssh'},{'dir':'a-new-local-directory'},
                                  {'mode':'ssh','remote_target':'backup@elsewhere:/different'}])
def test_destination_change_is_due_immediately_after_previous_success(change):
    old_config = config(mode='ssh') if 'remote_target' in change else config()
    cfg = config(**change)
    previous = {'status':'complete','mode':old_config['backups']['mode'],
                'last_success_at':NOW.isoformat(),
                'last_success_destination':controller.backup_destination_id(old_config),
                'destination_id':controller.backup_destination_id(old_config)}
    assert asyncio.run(controller.scheduled_backup_due(cfg,storage(Connection(previous={'result':previous})),now=NOW))


def test_reconfigured_destination_bypasses_other_destinations_error_backoff():
    old, new = config(mode='ssh'), config(mode='ssh',remote_target='backup@new:/snapshots')
    controller._backoff(old)
    previous = {'status':'error','mode':'ssh','last_success_at':OLD_SUCCESS,
                'destination_id':controller.backup_destination_id(old),
                'last_success_destination':controller.backup_destination_id(old),
                'finished_at':NOW.isoformat()}
    assert asyncio.run(controller.scheduled_backup_due(new,storage(Connection(previous={'result':previous})),now=NOW))


def test_failed_new_destination_preserves_old_success_identity_and_retries_after_backoff(monkeypatch):
    old, new = config(), config(mode='ssh')
    old_id, new_id = controller.backup_destination_id(old), controller.backup_destination_id(new)
    previous = {'status':'complete','mode':'local','last_success_at':NOW.isoformat(),'last_success_destination':old_id}
    monkeypatch.setattr(controller,'create_snapshot',AsyncMock(side_effect=BackupError('private remote')))
    conn = Connection(previous={'result':previous})
    result = asyncio.run(controller.run_backup(new,storage(conn),Path('/repo')))
    assert result['status'] == 'error' and result['destination_id'] == new_id
    assert result['last_success_destination'] == old_id
    controller._BACKOFF_UNTIL.clear()
    saved = Connection(previous={'result':conn.records[-1],'updated_at':NOW})
    assert not asyncio.run(controller.scheduled_backup_due(new,storage(saved),now=NOW))
    assert asyncio.run(controller.scheduled_backup_due(new,storage(saved),now=NOW+timedelta(hours=1,seconds=1)))


def test_verified_publication_updates_success_destination(monkeypatch):
    cfg = config(mode='ssh')
    monkeypatch.setattr(controller,'create_snapshot',AsyncMock(return_value=snapshot()))
    result = asyncio.run(controller.run_backup(cfg,storage(),Path('/repo')))
    assert result['last_success_destination'] == result['destination_id'] == controller.backup_destination_id(cfg)


def test_legacy_remote_success_needs_first_tracked_destination_snapshot():
    previous = {'status':'complete','mode':'ssh','last_success_at':NOW.isoformat()}
    assert asyncio.run(controller.scheduled_backup_due(config(mode='ssh'),storage(Connection(previous={'result':previous})),now=NOW))


def test_legacy_local_success_time_still_honored():
    previous = {'status':'complete','mode':'local','last_success_at':NOW.isoformat()}
    assert not asyncio.run(controller.scheduled_backup_due(config(),storage(Connection(previous={'result':previous})),now=NOW))


def test_failed_backup_connection_defers_subsequent_schedule_checks(monkeypatch):
    producer = AsyncMock()
    monkeypatch.setattr(controller,'create_snapshot',producer)
    target = storage(fail=True)
    result = asyncio.run(controller.run_backup(config(),target,Path('/repo')))
    assert result['status'] == 'error' and result['status_persisted'] is False
    assert not asyncio.run(controller.scheduled_backup_due(config(),target,now=NOW))
    assert target.pool.acquisitions == 1
    producer.assert_not_awaited()
