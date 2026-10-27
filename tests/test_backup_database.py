"""Host controller lock lifecycle and container-client termination tests."""

import asyncio
import copy
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import backup_database as controller
from storage.backup import BackupError, BackupResult, backup_destination_id

OWNER = hashlib.sha256(b"isolated-database-uuid").hexdigest()


def cfg():
    return {"backups": {"mode": "local", "min_free_mb": 0}, "storage": {"postgres": {"password": "secret-password"}}}


def test_legacy_environment_overrides_are_data_not_shell_code(tmp_path):
    source = tmp_path / "config.toml"
    source.write_text('[backups]\nkeep_days=4\n[storage.postgres]\nusername="old"\ndatabase="old"\n')
    result = controller.load_config(source, environ={
        "BACKUP_DIR": "/tmp/a 'quote' $(touch nope)", "KEEP_DAYS": "10",
        "REMOTE_TARGET": "backup@host:/remote folder", "DB_USER": "new-user", "DB_NAME": "new-db",
    })
    assert result["backups"] == {
        "dir": "/tmp/a 'quote' $(touch nope)", "keep_days": 10.0,
        "remote_target": "backup@host:/remote folder", "mode": "ssh",
    }
    assert result["storage"]["postgres"] == {"username": "new-user", "database": "new-db"}
    assert not (tmp_path / "nope").exists()


def test_explicit_local_mode_does_not_silently_ignore_remote_target(tmp_path):
    source = tmp_path / "config.toml"
    source.write_text('[backups]\nmode="local"\nremote_target="backup@host:/backup"\n')
    with pytest.raises(ValueError, match="select mode=ssh"):
        controller.load_config(source, environ={})
    assert controller.load_config(source, environ={}, mode="ssh")["backups"]["mode"] == "ssh"


@pytest.mark.parametrize("container", ["--privileged", "name;whoami", "with space", ""])
def test_container_is_not_an_option_or_command(container):
    with pytest.raises(ValueError):
        controller.DockerControlSession(cfg(), container)


class FakeControl:
    def __init__(self, events):
        self.events = events
        self.statuses = []
        self.exited = None
        self.fail_saves = False

    async def __aenter__(self):
        self.events.append("lock_acquired")
        self.exited = asyncio.get_running_loop().create_future()
        return self

    async def __aexit__(self, *_args):
        self.events.append("lock_released")
        self.exited.cancel()

    async def owner_id(self):
        return OWNER

    async def clock(self):
        return datetime(2026, 10, 27, tzinfo=timezone.utc).isoformat()

    async def previous_success(self):
        return "2026-10-26T00:00:00+00:00"

    async def previous_success_destination(self):
        return "b" * 64

    async def save_status(self, value):
        if self.fail_saves:
            raise RuntimeError("status unavailable")
        self.events.append("saved_" + value["status"])
        self.statuses.append(copy.deepcopy(value))

    async def stop_database_clients(self, _application):
        self.events.append("database_clients_stopped")


def install_control(monkeypatch, events):
    control = FakeControl(events)
    monkeypatch.setattr(controller, "DockerControlSession", lambda *_args: control)

    class Clients:
        application = "meshinfo_backup_" + "a" * 32
        prefix = ("fake-docker-clients",)

        def __init__(self, *_args):
            pass

        async def cleanup(self):
            await asyncio.sleep(0)
            events.append("container_clients_stopped")

    monkeypatch.setattr(controller, "DockerClients", Clients)
    return control


def snapshot_result(*, prune_error=None):
    return BackupResult(
        destination="private@host:/private/archive.dump", filename="archive.dump",
        bytes=123, sha256="a" * 64, completed_at=1, verification="archive_decode",
        pruned=(), prune_error=prune_error,
    )


def test_lock_covers_snapshot_container_cleanup_and_durable_success(tmp_path, monkeypatch):
    events = []
    control = install_control(monkeypatch, events)

    async def snapshot(_config, _root, *, owner_id, pg_command_prefix):
        assert owner_id == OWNER and pg_command_prefix == ("fake-docker-clients",)
        assert events == ["lock_acquired", "saved_running"]
        events.append("snapshot_complete")
        return snapshot_result()

    monkeypatch.setattr(controller, "create_snapshot", snapshot)
    result = asyncio.run(controller.run_backup(cfg(), tmp_path, "postgres"))
    assert result.successful
    assert events == [
        "lock_acquired", "saved_running", "snapshot_complete", "container_clients_stopped",
        "database_clients_stopped", "saved_complete", "lock_released",
    ]
    assert control.statuses[-1]["last_success_at"] == "2026-10-27T00:00:00+00:00"
    assert control.statuses[-1]["last_success_destination"] == backup_destination_id(cfg())
    assert control.statuses[0]["destination_id"] == backup_destination_id(cfg())
    assert control.statuses[0]["last_success_destination"] == "b" * 64
    assert "private" not in str(control.statuses)


def test_pruning_failure_is_partial_but_advances_last_verified_snapshot(tmp_path, monkeypatch):
    events = []
    control = install_control(monkeypatch, events)

    async def snapshot(*_args, **_kwargs):
        return snapshot_result(prune_error="private filesystem error")

    monkeypatch.setattr(controller, "create_snapshot", snapshot)
    result = asyncio.run(controller.run_backup(cfg(), tmp_path, "postgres"))
    assert not result.successful
    assert control.statuses[-1]["status"] == "partial"
    assert control.statuses[-1]["last_success_at"] == "2026-10-27T00:00:00+00:00"
    assert "private" not in str(control.statuses)


def test_lost_lock_connection_cancels_snapshot_before_releasing_controller(tmp_path, monkeypatch):
    events = []
    control = install_control(monkeypatch, events)

    async def snapshot(*_args, **_kwargs):
        control.exited.set_result(1)
        try:
            await asyncio.Event().wait()
        finally:
            events.append("snapshot_cancelled")

    monkeypatch.setattr(controller, "create_snapshot", snapshot)
    with pytest.raises(BackupError, match="lock connection was lost"):
        asyncio.run(controller.run_backup(cfg(), tmp_path, "postgres"))
    assert events == [
        "lock_acquired", "saved_running", "snapshot_cancelled", "container_clients_stopped", "lock_released",
    ]


def test_unwritable_status_store_prevents_snapshot_start(tmp_path, monkeypatch):
    events = []
    control = install_control(monkeypatch, events)
    control.fail_saves = True

    async def snapshot(*_args, **_kwargs):
        pytest.fail("snapshot must not start without writable durable status")

    monkeypatch.setattr(controller, "create_snapshot", snapshot)
    with pytest.raises(RuntimeError, match="status unavailable"):
        asyncio.run(controller.run_backup(cfg(), tmp_path, "postgres"))
    assert events == ["lock_acquired", "lock_released"]


def test_snapshot_failure_preserves_previous_success_without_saving_secret_errors(tmp_path, monkeypatch):
    events = []
    control = install_control(monkeypatch, events)

    async def snapshot(*_args, **_kwargs):
        raise RuntimeError("password=private")

    monkeypatch.setattr(controller, "create_snapshot", snapshot)
    with pytest.raises(RuntimeError):
        asyncio.run(controller.run_backup(cfg(), tmp_path, "postgres"))
    failed = control.statuses[-1]
    assert failed["status"] == "error"
    assert failed["reason"] == "RuntimeError"
    assert failed["last_success_at"] == "2026-10-26T00:00:00+00:00"
    assert failed["last_success_destination"] == "b" * 64
    assert failed["destination_id"] == backup_destination_id(cfg())
    assert "private" not in str(control.statuses)
    assert events.index("container_clients_stopped") < events.index("lock_released")


def test_user_cancellation_waits_for_cleanup_before_unlock(tmp_path, monkeypatch):
    events = []
    control = install_control(monkeypatch, events)

    async def run():
        started = asyncio.Event()

        async def snapshot(*_args, **_kwargs):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                events.append("snapshot_cancelled")

        monkeypatch.setattr(controller, "create_snapshot", snapshot)
        task = asyncio.create_task(controller.run_backup(cfg(), tmp_path, "postgres"))
        await asyncio.wait_for(started.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)

    asyncio.run(run())
    assert events.index("snapshot_cancelled") < events.index("container_clients_stopped")
    assert events.index("database_clients_stopped") < events.index("lock_released")
    assert control.statuses[-1]["reason"] == "CancelledError"


@pytest.mark.parametrize("prune_error,operation_status", [(None, "complete"), ("pruning failed", "partial")])
def test_final_status_write_failure_preserves_verified_snapshot_details(tmp_path, monkeypatch, prune_error, operation_status):
    events = []
    control = install_control(monkeypatch, events)
    original_save = control.save_status

    async def save_status(value):
        if value["status"] in {"complete", "partial"}:
            raise RuntimeError("private status storage diagnostic")
        await original_save(value)

    async def snapshot(*_args, **_kwargs):
        return snapshot_result(prune_error=prune_error)

    control.save_status = save_status
    monkeypatch.setattr(controller, "create_snapshot", snapshot)
    with pytest.raises(controller.BackupStatusPersistenceError) as caught:
        asyncio.run(controller.run_backup(cfg(), tmp_path, "postgres"))
    status = caught.value.status
    assert status["status"] == "error"
    assert status["operation_status"] == operation_status
    assert status["snapshot_completed"] is True
    assert status["status_persisted"] is False
    assert status["last_success_at"] == "2026-10-27T00:00:00+00:00"
    assert status["last_success_destination"] == backup_destination_id(cfg())
    assert status["bytes"] == 123
    assert "private" not in str(status)
    assert [item["status"] for item in control.statuses] == ["running"]
    assert events[-1] == "lock_released"


@pytest.mark.parametrize("matching_token", [True, False])
def test_container_cleanup_kills_actual_token_process_but_preserves_reused_pid(tmp_path, monkeypatch, matching_token):
    clients = controller.DockerClients("postgres", 60)
    clients.pid_path = str(tmp_path / "client.pid")
    environment = {**os.environ, "MESHINFO_BACKUP_JOB": clients.token if matching_token else "another-job"}
    child = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(30)"], env=environment)
    Path(clients.pid_path).write_text(str(child.pid) + "\n")
    original_spawn = asyncio.create_subprocess_exec

    async def local_container(*argv, **kwargs):
        assert argv[:3] == ("docker", "exec", "postgres")
        return await original_spawn(*argv[3:], **kwargs)

    monkeypatch.setattr(controller.asyncio, "create_subprocess_exec", local_container)
    try:
        asyncio.run(clients.cleanup())
        if matching_token:
            assert child.wait(timeout=2) < 0
        else:
            assert child.poll() is None
        assert not Path(clients.pid_path).exists()
    finally:
        if child.poll() is None:
            child.kill()
        child.wait()


def test_owner_identity_is_uuid_not_identical_docker_endpoint_defaults():
    session = controller.DockerControlSession(cfg(), "postgres")
    first = "00000000-0000-4000-8000-000000000001"

    async def query(_sql):
        return [first]

    session.query = query
    assert asyncio.run(session.owner_id()) == hashlib.sha256(first.encode()).hexdigest()


@pytest.mark.parametrize("failure", [None, "busy", "missing_schema"])
def test_control_session_acknowledgments_lock_preflight_and_cleanup(tmp_path, monkeypatch, failure):
    program = tmp_path / "psql_protocol.py"
    transcript = tmp_path / "sql.txt"
    program.write_text(
        """import os, re, sys
with open(os.environ['TEST_SQL_LOG'], 'w') as log:
    for line in sys.stdin:
        log.write(line); log.flush()
        if 'pg_try_advisory_lock' in line:
            print('f' if os.getenv('TEST_CONTROL_FAILURE') == 'busy' else 't', flush=True)
        elif 'to_regclass' in line:
            print('missing' if os.getenv('TEST_CONTROL_FAILURE') == 'missing_schema' else 'ready', flush=True)
        elif 'instance_id::text' in line:
            print('00000000-0000-4000-8000-000000000001', flush=True)
        elif 'to_char(clock_timestamp()' in line:
            print('2026-10-27T00:00:00.000000+00:00', flush=True)
        elif "result->>'last_success_at'" in line:
            print('2026-10-26T00:00:00+00:00', flush=True)
        elif "result->>'last_success_destination'" in line:
            print('b' * 64, flush=True)
        elif 'pg_advisory_unlock' in line:
            print('t', flush=True)
        elif line.startswith("SELECT 'meshinfo_ack_"):
            print(re.search("'([^']+)'", line).group(1), flush=True)
"""
    )
    monkeypatch.setenv("TEST_SQL_LOG", str(transcript))
    monkeypatch.setenv("TEST_CONTROL_FAILURE", failure or "")
    cleanups = []

    class Clients:
        def __init__(self, *_args):
            self.prefix = (sys.executable, str(program))

        async def cleanup(self):
            cleanups.append(True)

    monkeypatch.setattr(controller, "DockerClients", Clients)
    session = controller.DockerControlSession(cfg(), "postgres")

    async def run():
        async with session:
            assert await session.clock() == "2026-10-27T00:00:00.000000+00:00"
            assert await session.previous_success() == "2026-10-26T00:00:00+00:00"
            assert await session.previous_success_destination() == "b" * 64
            assert len(await session.owner_id()) == 64
            await session.save_status({"status": "complete", "value": "quote'; DROP DATABASE nope;--"})

    if failure:
        expected = controller.MaintenanceBusy if failure == "busy" else BackupError
        with pytest.raises(expected):
            asyncio.run(run())
    else:
        asyncio.run(run())
    assert session.proc.returncode is not None
    assert cleanups == [True]
    text = transcript.read_text()
    assert "pg_try_advisory_lock" in text
    assert "DROP DATABASE nope" not in text  # Persisted values are hex-encoded data.
    assert "secret-password" not in text
    if failure == "busy":
        assert "pg_advisory_unlock" not in text
    else:
        assert "pg_advisory_unlock" in text


def test_heartbeat_failure_cancels_work_even_if_psql_process_is_still_alive(tmp_path, monkeypatch):
    events = []
    control = install_control(monkeypatch, events)

    async def snapshot(*_args, **_kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            events.append("snapshot_cancelled")

    async def failed_heartbeat(_control, _stopped):
        await asyncio.sleep(0)
        raise BackupError("maintenance lock connection heartbeat failed")

    monkeypatch.setattr(controller, "create_snapshot", snapshot)
    monkeypatch.setattr(controller, "_heartbeat", failed_heartbeat)
    with pytest.raises(BackupError, match="heartbeat failed"):
        asyncio.run(controller.run_backup(cfg(), tmp_path, "postgres"))
    assert events.index("snapshot_cancelled") < events.index("lock_released")
    assert events.index("database_clients_stopped") < events.index("lock_released")
    assert control.statuses[-1]["status"] == "error"
