"""Streaming/retention failure tests using tiny isolated fake PostgreSQL clients.

Real PostgreSQL restore validation is an integration check; these tests exercise
process lifecycle, framing, publication, ownership and transport invariants.
"""

import asyncio
import hashlib
import io
import json
import os
from pathlib import Path
import shlex
import struct
import sys
import threading
import time

import pytest

from storage import backup, backup_remote
from storage.backup import BackupPolicy, backup_destination_id, create_snapshot as _create_snapshot, parse_remote_target
from storage.backup_remote import (
    BackupError,
    CHUNK_BYTES,
    MANIFEST_SUFFIX,
    SnapshotTarget,
    owned_snapshots,
    prune_snapshots,
    receive_stream,
)

OWNER = hashlib.sha256(b"isolated-test-database-identity").hexdigest()


def create_snapshot(cfg, repo_root, **kwargs):
    return _create_snapshot(cfg, repo_root, owner_id=OWNER, **kwargs)


def config(directory, **settings):
    return {
        "backups": {"dir": str(directory), "min_free_mb": 0, "keep_days": 4, **settings},
        "storage": {"postgres": {"database": "test database", "password": "not-in-argv"}},
    }


def name(number):
    return f"meshinfo-pg-20260101-000000-{number:012x}.dump"


def owned(directory, number, completed, data=b"PGDMPexampleDONE"):
    with SnapshotTarget(directory, name(number), 0, owner=OWNER) as target:
        target.write(data)
        target.seal()
        target.publish(now=completed)
        return target.path


@pytest.fixture
def clients(tmp_path, monkeypatch):
    program = tmp_path / "clients.py"
    program.write_text(
        """import os, pathlib, sys, time, json
command = sys.argv[1]
if command == 'psql':
    if os.getenv('TEST_METADATA_PID_PATH'):
        pathlib.Path(os.environ['TEST_METADATA_PID_PATH']).write_text(str(os.getpid()))
    query = ''
    for line in sys.stdin:
        if line.startswith("SELECT 'meshinfo_metadata_"):
            if 'pg_export_snapshot' in query:
                relations = json.loads(os.getenv('TEST_COLUMNAR_RELATIONS', '[]'))
                print(json.dumps({'snapshot': os.getenv('TEST_SNAPSHOT_TOKEN', '00000001-00000002-1'),
                                  'postgres_major': 18, 'extension': '14.1-1' if relations else None,
                                  'relation_count': len(relations)}), flush=True)
            elif 'columnar.options' in query:
                print(os.getenv('TEST_COLUMNAR_RELATIONS', '[]'), flush=True)
            print(line.split("'")[1], flush=True)
            query = ''
        else:
            query += line
    raise SystemExit(0)
if command == 'pg_dump':
    assert sys.argv[sys.argv.index('--snapshot') + 1] == '00000001-00000002-1'
    if os.getenv('TEST_PID_PATH'):
        pathlib.Path(os.environ['TEST_PID_PATH']).write_text(str(os.getpid()))
    time.sleep(float(os.getenv('TEST_DUMP_DELAY', '0')))
    data = b'PGDMP' + b'x' * int(os.getenv('TEST_DUMP_BYTES', '700000')) + b'DONE'
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()
    sys.stderr.write('e' * int(os.getenv('TEST_STDERR_BYTES', '0')))
    sys.exit(int(os.getenv('TEST_DUMP_RC', '0')))
if command == 'pg_restore':
    data = sys.stdin.buffer.read()
    valid = data.startswith(b'PGDMP') and data.endswith(b'DONE')
    sys.exit(int(os.getenv('TEST_RESTORE_RC', '0')) if valid else 17)
raise SystemExit(99)
"""
    )
    remote_restore = tmp_path / "pg_restore"
    remote_restore.write_text(
        f"#!{sys.executable}\n"
        "import os, pathlib, sys\n"
        "data = pathlib.Path(sys.argv[-1]).read_bytes()\n"
        "valid = data.startswith(b'PGDMP') and data.endswith(b'DONE')\n"
        "raise SystemExit(int(os.getenv('TEST_RESTORE_RC', '0')) if valid else 17)\n"
    )
    remote_restore.chmod(0o700)
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ["PATH"])
    return (sys.executable, str(program))


def fake_ssh(monkeypatch):
    def command(policy, filename, owner_id):
        return (
            sys.executable, str(Path(backup_remote.__file__)),
            policy.remote_directory, filename, str(policy.min_free_bytes),
            str(policy.remote_keep_days), str(policy.timeout_seconds),
            owner_id,
        )
    monkeypatch.setattr(backup, "ssh_command", command)


def test_backup_retention_is_independent_of_live_history(tmp_path):
    cfg = config(tmp_path, keep_days=4, remote_keep_days=35)
    cfg["storage"]["retention"] = {"enabled": True, "months": 1}
    policy = BackupPolicy.from_config(cfg, tmp_path)
    assert policy.keep_days == 4
    assert policy.remote_keep_days == 35
    assert policy.mode == "local"
    assert BackupPolicy.from_config(config(tmp_path, keep_days=12), tmp_path).remote_keep_days == 12


def test_destination_identity_is_lexical_and_independent_of_host_checkout(tmp_path, monkeypatch):
    default = backup_destination_id({})
    equivalent = {"backups": {"mode": "local", "dir": "./temporary/../backups//", "keep_days": 10}}
    assert backup_destination_id(equivalent) == default
    monkeypatch.chdir(tmp_path)
    assert backup_destination_id(equivalent) == default
    assert backup_destination_id({"backups": {"dir": "other"}}) != default
    assert len(default) == 64 and "backups" not in default


def test_destination_identity_tracks_remote_host_path_mode_but_not_retention_or_password():
    first = {"backups": {"mode": "ssh", "remote_target": "User@HOST:/backups//daily/./", "keep_days": 4}}
    second = {"backups": {"mode": "ssh", "remote_target": "User@host:/backups/daily", "keep_days": 30},
              "storage": {"postgres": {"password": "rotated"}}}
    assert backup_destination_id(first) == backup_destination_id(second)
    for target in ("User@host:/backups/weekly", "User@elsewhere:/backups/daily", "user@host:/backups/daily"):
        changed = {"backups": {"mode": "ssh", "remote_target": target}}
        assert backup_destination_id(changed) != backup_destination_id(first)
    assert backup_destination_id({"backups": {"mode": "local", "dir": "/backups/daily"}}) != backup_destination_id(first)


@pytest.mark.parametrize("setting,value", [
    ("keep_days", True), ("keep_days", 0), ("remote_keep_days", float("nan")),
    ("timeout_seconds", 0.5), ("timeout_seconds", float("inf")),
    ("min_free_mb", -1), ("min_free_mb", True), ("min_free_mb", 0.1), ("mode", "other"),
])
def test_invalid_limits_rejected_before_creating_files(tmp_path, setting, value):
    with pytest.raises(ValueError):
        BackupPolicy.from_config(config(tmp_path, **{setting: value}), tmp_path)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("target", [
    "-oProxyCommand=touch:/tmp/x", "host;touch:/tmp/x", "host:/tmp/x\nwhoami",
    "ssh://host/tmp/x", "host:relative", "user@host:/", "host:/tmp/../elsewhere", "",
])
def test_remote_spec_rejects_options_and_ambiguous_paths(target):
    with pytest.raises(ValueError):
        parse_remote_target(target)


def test_remote_path_is_a_single_safely_quoted_argument(tmp_path):
    directory = "/backups/space and 'quote';$(touch NOPE)"
    policy = BackupPolicy.from_config(config(tmp_path, mode="ssh", remote_target="backup@host:" + directory), tmp_path)
    command = backup.ssh_command(policy, name(1), OWNER)
    assert command[:3] == ("ssh", "-T", "-oBatchMode=yes")
    assert command[-2] == "backup@host"
    arguments = shlex.split(command[-1])
    assert arguments[:2] == ["python3", "-c"]
    assert arguments[3] == directory
    assert arguments[4] == name(1)
    assert parse_remote_target("user@[2001:db8::1]:/backups") == ("user@[2001:db8::1]", "/backups")


def test_pruning_owns_verified_files_only_and_keeps_newest(tmp_path):
    now = time.time()
    old = owned(tmp_path, 1, now - 10 * 86400)
    newest = owned(tmp_path, 2, now - 8 * 86400)
    legacy = tmp_path / "meshinfo-pg-20260101-000000.dump"
    legacy.write_bytes(b"legacy")
    unknown = tmp_path / name(3)
    unknown.write_bytes(b"no verification marker")
    symlink = tmp_path / name(4)
    symlink.symlink_to(newest)
    symlink.with_name(symlink.name + MANIFEST_SUFFIX).write_text(
        newest.with_name(newest.name + MANIFEST_SUFFIX).read_text()
    )
    assert prune_snapshots(tmp_path, 4, now=now, owner=OWNER) == [old.name]
    assert not old.exists()
    assert not old.with_name(old.name + MANIFEST_SUFFIX).exists()
    assert newest.exists() and legacy.exists() and unknown.exists() and symlink.is_symlink()


def test_modified_or_invalid_manifest_is_never_pruned(tmp_path):
    path = owned(tmp_path, 1, time.time() - 10 * 86400)
    marker = path.with_name(path.name + MANIFEST_SUFFIX)
    metadata = json.loads(marker.read_text())
    metadata["bytes"] += 1
    marker.write_text(json.dumps(metadata))
    owned(tmp_path, 2, time.time())
    assert prune_snapshots(tmp_path, 1, owner=OWNER) == []
    assert path.exists()


def test_free_space_reserve_stops_before_writing(tmp_path, monkeypatch):
    class Stat:
        f_bavail = 1
        f_frsize = 4096
    monkeypatch.setattr(backup_remote.os, "statvfs", lambda _path: Stat())
    with pytest.raises(BackupError, match="free-space reserve"):
        with SnapshotTarget(tmp_path, name(1), 0):
            pass
    assert not list(tmp_path.glob("*.part"))
    assert not list(tmp_path.glob("*.dump"))


def framed(data, *, checksum=None, commit=True):
    stream = io.BytesIO()
    for offset in range(0, len(data), CHUNK_BYTES):
        chunk = data[offset:offset + CHUNK_BYTES]
        stream.write(struct.pack("!I", len(chunk)) + chunk)
    if commit:
        metadata = json.dumps({"bytes": len(data), "sha256": checksum or hashlib.sha256(data).hexdigest()}).encode()
        stream.write(struct.pack("!II", 0, len(metadata)) + metadata)
    stream.seek(0)
    return stream


@pytest.mark.parametrize("case", ["missing_commit", "wrong_checksum", "oversized_frame", "trailing_data"])
def test_receiver_never_promotes_incomplete_or_corrupt_stream(tmp_path, case):
    data = b"PGDMPxDONE"
    stream = framed(data, commit=case != "missing_commit", checksum="0" * 64 if case == "wrong_checksum" else None)
    if case == "oversized_frame":
        stream = io.BytesIO(struct.pack("!I", CHUNK_BYTES + 1))
    if case == "trailing_data":
        stream = io.BytesIO(stream.getvalue() + b"unexpected")
    with pytest.raises(BackupError):
        with SnapshotTarget(tmp_path, name(1), 0) as target:
            receive_stream(stream, target)
    assert not list(tmp_path.glob("*.dump"))
    assert not list(tmp_path.glob("*.part"))


def test_destination_lock_prevents_concurrent_snapshot(tmp_path):
    with SnapshotTarget(tmp_path, name(1), 0):
        with pytest.raises(BlockingIOError):
            with SnapshotTarget(tmp_path, name(2), 0):
                pass
    assert not list(tmp_path.glob("*.part"))


def test_publication_never_overwrites_an_existing_snapshot(tmp_path):
    existing = tmp_path / name(1)
    existing.write_bytes(b"keep this")
    with pytest.raises(FileExistsError):
        with SnapshotTarget(tmp_path, name(1), 0) as target:
            target.write(b"new content")
            target.seal()
            target.publish()
    assert existing.read_bytes() == b"keep this"
    assert not existing.with_name(existing.name + MANIFEST_SUFFIX).exists()


def test_local_snapshot_verifies_checksum_and_prunes_only_after_success(tmp_path, clients):
    directory = tmp_path / "backups"
    old = owned(directory, 1, time.time() - 10 * 86400)
    result = asyncio.run(create_snapshot(config(directory), tmp_path, pg_command_prefix=clients))
    archive = Path(result.destination)
    assert result.successful and result.verification == "archive_decode"
    assert result.bytes > CHUNK_BYTES
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == result.sha256
    assert result.pruned == (old.name,)
    assert not old.exists()
    assert len(owned_snapshots(directory)) == 1
    assert not list(directory.glob("*.part"))
    public = result.public_summary()
    assert public["status"] == "complete"
    assert str(directory) not in json.dumps(public)
    assert result.filename not in json.dumps(public)


@pytest.mark.parametrize("failure", ["dump", "verify"])
def test_failed_local_backup_preserves_prior_copy_and_removes_partial(tmp_path, clients, monkeypatch, failure):
    directory = tmp_path / "backups"
    old = owned(directory, 1, time.time() - 10 * 86400)
    monkeypatch.setenv("TEST_DUMP_RC" if failure == "dump" else "TEST_RESTORE_RC", "19")
    with pytest.raises(BackupError):
        asyncio.run(create_snapshot(config(directory), tmp_path, pg_command_prefix=clients))
    assert [path.name for path in directory.glob("*.dump")] == [old.name]
    assert not list(directory.glob("*.part"))


def test_stderr_is_drained_but_failure_message_is_bounded(tmp_path, clients, monkeypatch):
    monkeypatch.setenv("TEST_STDERR_BYTES", "1000000")
    monkeypatch.setenv("TEST_DUMP_RC", "3")
    with pytest.raises(BackupError) as error:
        asyncio.run(create_snapshot(config(tmp_path / "backups"), tmp_path, pg_command_prefix=clients))
    assert len(str(error.value)) < backup_remote.STDERR_BYTES + 100


def test_file_checksum_mismatch_blocks_local_publication(tmp_path, clients, monkeypatch):
    verify = backup._verify_local

    async def altered(target, prefix):
        data = target.temporary.read_bytes()
        target.temporary.write_bytes(data.replace(b"x", b"y", 1))
        await verify(target, prefix)

    monkeypatch.setattr(backup, "_verify_local", altered)
    directory = tmp_path / "backups"
    with pytest.raises(BackupError, match="checksum"):
        asyncio.run(create_snapshot(config(directory), tmp_path, pg_command_prefix=clients))
    assert not list(directory.glob("*.dump"))
    assert not list(directory.glob("*.part"))


def test_cancellation_stops_producer_and_cleans_partial(tmp_path, clients, monkeypatch):
    directory = tmp_path / "backups"
    pid_path = tmp_path / "producer.pid"
    monkeypatch.setenv("TEST_DUMP_DELAY", "30")
    monkeypatch.setenv("TEST_PID_PATH", str(pid_path))

    async def run():
        task = asyncio.create_task(create_snapshot(config(directory), tmp_path, pg_command_prefix=clients))
        for _attempt in range(100):
            if pid_path.exists():
                break
            await asyncio.sleep(0.01)
        assert pid_path.exists()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)

    asyncio.run(run())
    with pytest.raises(ProcessLookupError):
        os.kill(int(pid_path.read_text()), 0)
    assert not list(directory.glob("*.part"))
    assert not list(directory.glob("*.dump"))


def test_timeout_cleans_partial(tmp_path, clients, monkeypatch):
    monkeypatch.setenv("TEST_DUMP_DELAY", "30")
    directory = tmp_path / "backups"
    with pytest.raises(TimeoutError):
        asyncio.run(create_snapshot(config(directory, timeout_seconds=1), tmp_path, pg_command_prefix=clients))
    assert not list(directory.glob("*.part"))
    assert not list(directory.glob("*.dump"))


def test_slow_disk_keeps_event_loop_responsive_and_cancellation_waits_for_io(tmp_path, clients, monkeypatch):
    directory = tmp_path / "backups"
    entered, release = threading.Event(), threading.Event()
    order = []
    real_seal, real_exit = SnapshotTarget.seal, SnapshotTarget.__exit__

    def slow_seal(target):
        entered.set()
        assert release.wait(3), "event loop did not release the simulated slow disk operation"
        real_seal(target)
        order.append("disk_finished")

    def checked_exit(target, *args):
        assert order == ["disk_finished"], "cleanup raced a running filesystem worker"
        order.append("cleanup")
        return real_exit(target, *args)

    monkeypatch.setattr(SnapshotTarget, "seal", slow_seal)
    monkeypatch.setattr(SnapshotTarget, "__exit__", checked_exit)

    async def run():
        task = asyncio.create_task(create_snapshot(config(directory), tmp_path, pg_command_prefix=clients))
        ticks = 0
        try:
            while not entered.is_set():
                await asyncio.sleep(0.005)
            # An API/SSE-style coroutine can keep running while fsync is slow.
            for _ in range(5):
                await asyncio.sleep(0.005)
                ticks += 1
            task.cancel()
            await asyncio.sleep(0.01)
            task.cancel()  # Repeated cancellation must not detach the IO worker.
            await asyncio.sleep(0.01)
            assert not task.done()
            assert order == []
            assert list(directory.glob("*.part")), "partial archive was removed during its disk operation"
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        assert ticks == 5

    asyncio.run(run())
    assert order == ["disk_finished", "cleanup"]
    assert not list(directory.glob("*.part"))
    assert not list(directory.glob("*.dump"))


def test_ssh_stream_has_no_local_dump_and_receiver_owns_retention(tmp_path, clients, monkeypatch):
    fake_ssh(monkeypatch)
    local, remote = tmp_path / "local", tmp_path / "remote"
    old = owned(remote, 1, time.time() - 10 * 86400)
    cfg = config(local, mode="ssh", remote_target=f"backup@host:{remote}", keep_days=30, remote_keep_days=4)
    result = asyncio.run(create_snapshot(cfg, tmp_path, pg_command_prefix=clients))
    assert not local.exists()
    assert result.successful
    assert result.destination.startswith("backup@host:")
    assert result.pruned == (old.name,)
    assert len(owned_snapshots(remote)) == 1
    assert hashlib.sha256((remote / result.filename).read_bytes()).hexdigest() == result.sha256


@pytest.mark.parametrize("failure", ["dump", "verify"])
def test_ssh_failure_preserves_previous_backup_and_does_not_publish(tmp_path, clients, monkeypatch, failure):
    fake_ssh(monkeypatch)
    local, remote = tmp_path / "local", tmp_path / "remote"
    old = owned(remote, 1, time.time() - 10 * 86400)
    monkeypatch.setenv("TEST_DUMP_RC" if failure == "dump" else "TEST_RESTORE_RC", "23")
    cfg = config(local, mode="ssh", remote_target=f"backup@host:{remote}")
    with pytest.raises(BackupError):
        asyncio.run(create_snapshot(cfg, tmp_path, pg_command_prefix=clients))
    assert not local.exists()
    assert [path.name for path in remote.glob("*.dump")] == [old.name]
    assert not list(remote.glob("*.part"))


def test_ssh_cancellation_cleans_both_sides_without_a_local_archive(tmp_path, clients, monkeypatch):
    fake_ssh(monkeypatch)
    local, remote = tmp_path / "local", tmp_path / "remote"
    pid_path = tmp_path / "producer.pid"
    monkeypatch.setenv("TEST_PID_PATH", str(pid_path))
    monkeypatch.setenv("TEST_DUMP_DELAY", "30")
    cfg = config(local, mode="ssh", remote_target=f"backup@host:{remote}")

    async def run():
        task = asyncio.create_task(create_snapshot(cfg, tmp_path, pg_command_prefix=clients))
        for _attempt in range(100):
            if pid_path.exists() and remote.exists():
                break
            await asyncio.sleep(0.01)
        assert pid_path.exists() and remote.exists()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)

    asyncio.run(run())
    assert not local.exists()
    assert not list(remote.glob("*.part"))
    assert not list(remote.glob("*.dump"))


def test_ssh_acknowledgment_must_match_producer_checksum(tmp_path, clients, monkeypatch):
    def corrupt_ack(policy, filename, _owner_id):
        program = (
            "import json, sys; sys.stdin.buffer.read(); "
            "print(json.dumps({'schema': 1, 'filename': sys.argv[1], "
            "'bytes': 700009, 'sha256': '0' * 64, 'completed_at': 1, "
            "'verification': 'archive_decode', 'pruned': []}))"
        )
        return (sys.executable, "-c", program, filename)

    monkeypatch.setattr(backup, "ssh_command", corrupt_ack)
    local = tmp_path / "local"
    cfg = config(local, mode="ssh", remote_target="backup@host:/remote")
    with pytest.raises(BackupError, match="did not acknowledge"):
        asyncio.run(create_snapshot(cfg, tmp_path, pg_command_prefix=clients))
    assert not local.exists()


def test_pruning_failure_is_visible_without_losing_verified_backup(tmp_path, clients, monkeypatch):
    def fail_prune(*_args, **_kwargs):
        raise PermissionError("cannot delete expired snapshot")
    monkeypatch.setattr(backup_remote, "prune_snapshots", fail_prune)
    result = asyncio.run(create_snapshot(config(tmp_path / "backups"), tmp_path, pg_command_prefix=clients))
    assert not result.successful
    assert result.public_summary()["status"] == "partial"
    assert result.prune_error == "cannot delete expired snapshot"
    assert Path(result.destination).exists()


@pytest.mark.parametrize("mode", ["local", "ssh"])
def test_columnar_options_survive_normal_snapshot_transport(tmp_path, clients, monkeypatch, mode):
    relations = [{"schema": "archive_stage", "name": "cold_2025_01",
                  "chunk_group_row_limit": 1000, "stripe_row_limit": 150000,
                  "compression_level": 7, "compression": "zstd",
                  "geometry": {"row_count": 384, "max_stripe_rows": 128, "max_chunk_group_rows": 128}}]
    monkeypatch.setenv("TEST_COLUMNAR_RELATIONS", json.dumps(relations))
    destination = tmp_path / "destination"
    settings = {}
    if mode == "ssh":
        fake_ssh(monkeypatch)
        settings = {"mode": "ssh", "remote_target": f"backup@host:{destination}"}
    result = asyncio.run(create_snapshot(config(destination, **settings), tmp_path, pg_command_prefix=clients))
    manifest = json.loads((destination / (result.filename + MANIFEST_SUFFIX)).read_text())
    assert manifest["restore_metadata"]["schema"] == 2
    assert manifest["restore_metadata"]["relations"] == relations
    assert manifest["restore_metadata"]["extension"] == "14.1-1"
    assert manifest["restore_metadata_sha256"] == backup_remote.restore_metadata_checksum(manifest["restore_metadata"])
    assert len(owned_snapshots(destination, owner=OWNER)) == 1


def test_invalid_exported_snapshot_refuses_backup_before_destination_creation(tmp_path, clients, monkeypatch):
    monkeypatch.setenv("TEST_SNAPSHOT_TOKEN", "invalid --snapshot token")
    destination = tmp_path / "destination"
    with pytest.raises(BackupError, match="snapshot metadata"):
        asyncio.run(create_snapshot(config(destination), tmp_path, pg_command_prefix=clients))
    assert not destination.exists()


def test_columnar_metadata_larger_than_old_manifest_limit_survives_ssh(tmp_path, clients, monkeypatch):
    fake_ssh(monkeypatch)
    relations = [{"schema": "archive_stage", "name": f"history_{i}",
                  "chunk_group_row_limit": 1000, "stripe_row_limit": 150000,
                  "compression_level": 3, "compression": "zstd",
                  "geometry": {"row_count": 384, "max_stripe_rows": 128, "max_chunk_group_rows": 128}} for i in range(450)]
    monkeypatch.setenv("TEST_COLUMNAR_RELATIONS", json.dumps(relations))
    remote = tmp_path / "remote"
    result = asyncio.run(create_snapshot(config(tmp_path / "local", mode="ssh", remote_target=f"backup@host:{remote}"),
                                         tmp_path, pg_command_prefix=clients))
    manifest = remote / (result.filename + MANIFEST_SUFFIX)
    assert manifest.stat().st_size > 65536
    assert len(owned_snapshots(remote, owner=OWNER)) == 1
