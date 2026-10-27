#!/usr/bin/env python3
"""Host backup controller using Docker's PostgreSQL clients and host SSH keys.

Requires Python 3.11+ and Docker, not host PostgreSQL clients or asyncpg. The
database's maintenance lock is acquired on a monitored psql session, and held
until snapshot subprocesses are stopped and durable status has been saved.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tomllib
import uuid

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from storage.backup import (
    BackupError, BackupPolicy, BackupResult, _stop, _tail,
    backup_destination_id, create_snapshot,
)

# Keep this dependency-free host script independent of asyncpg imports.
MAINTENANCE_LOCK_KEY = 0x4D455348494E464F

_CLIENT_SCRIPT = r'''
set -eu
pid_path=$1
token=$2
application=$3
shift 3
umask 077
export MESHINFO_BACKUP_JOB="$token" PGAPPNAME="$application"
printf '%s\n' "$$" > "$pid_path"
exec "$@"
'''

_CLEANUP_SCRIPT = r'''
set -eu
pid_path=$1
token=$2
if [ ! -f "$pid_path" ]; then exit 0; fi
read -r pid < "$pid_path"
case "$pid" in ''|*[!0-9]*) exit 2;; esac
same_job() {
    [ -r "/proc/$pid/environ" ] &&
    tr '\000' '\n' < "/proc/$pid/environ" 2>/dev/null |
        grep -F -x "MESHINFO_BACKUP_JOB=$token" >/dev/null
}
if same_job; then
    kill -TERM "$pid" 2>/dev/null || true
    tries=0
    while same_job && [ "$tries" -lt 20 ]; do
        sleep 0.1
        tries=$((tries + 1))
    done
    if same_job; then
        kill -KILL "$pid" 2>/dev/null || true
        sleep 0.1
        if same_job; then exit 3; fi
    fi
fi
rm -f "$pid_path"
'''


class MaintenanceBusy(BackupError):
    pass


class BackupStatusPersistenceError(BackupError):
    """A verified snapshot exists, but its durable status could not be saved."""

    def __init__(self, status: dict):
        self.status = status
        super().__init__("snapshot completed and verified, but its maintenance status could not be persisted")


def load_config(path: Path, *, environ=None, mode: str | None = None) -> dict:
    """Read TOML and legacy environment overrides without eval or secret output."""
    environ = os.environ if environ is None else environ
    with path.open("rb") as source:
        config = copy.deepcopy(tomllib.load(source))
    settings = config.setdefault("backups", {})
    pg = config.setdefault("storage", {}).setdefault("postgres", {})
    had_explicit_mode = "mode" in settings
    for environment_key, key in (("BACKUP_DIR", "dir"), ("REMOTE_TARGET", "remote_target")):
        if environ.get(environment_key):
            settings[key] = environ[environment_key]
    if environ.get("KEEP_DAYS"):
        settings["keep_days"] = float(environ["KEEP_DAYS"])
    for environment_key, fallback, key in (
        ("DB_USER", "POSTGRES_USER", "username"), ("DB_NAME", "POSTGRES_DB", "database"),
    ):
        if environ.get(environment_key) or environ.get(fallback):
            pg[key] = environ.get(environment_key) or environ[fallback]
    selected_mode = mode or environ.get("BACKUP_MODE")
    if selected_mode:
        settings["mode"] = selected_mode
    elif settings.get("remote_target") and (environ.get("REMOTE_TARGET") or not had_explicit_mode):
        # Historical host scripts copied remote_target off-box. Preserve that
        # intention by streaming there, avoiding a full local staging archive.
        settings["mode"] = "ssh"
    if settings.get("mode", "local") == "local" and settings.get("remote_target"):
        raise ValueError("remote_target is set with mode=local; select mode=ssh or remove remote_target")
    BackupPolicy.from_config(config, REPO_ROOT)
    return config


class DockerControlSession:
    """One psql backend, command acknowledgments, and a monitored lock owner."""

    def __init__(self, config: dict, container: str):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", container):
            raise ValueError("invalid PostgreSQL container name")
        self.config, self.container = config, container
        self.proc = None
        self.exited = None
        self.errors = None
        self.locked = False
        timeout = BackupPolicy.from_config(config, REPO_ROOT).timeout_seconds
        self.clients = DockerClients(container, timeout + 60)

    async def __aenter__(self):
        pg = self.config.get("storage", {}).get("postgres", {})
        environment = {**os.environ, "PGPASSWORD": str(pg.get("password", "")), "PGCONNECT_TIMEOUT": "15"}
        self.proc = await asyncio.create_subprocess_exec(
            *self.clients.prefix,
            "psql", "-X", "-qAt", "--set=ON_ERROR_STOP=1",
            "--host", str(pg.get("host", "postgres")),
            "--port", str(pg.get("port", 5432)),
            "--username", str(pg.get("username", "postgres")),
            "--dbname", str(pg.get("database", "meshinfo")),
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, env=environment, limit=65536,
        )
        self.exited = asyncio.create_task(self.proc.wait())
        self.errors = asyncio.create_task(_tail(self.proc.stderr))
        try:
            await self.query("SET statement_timeout='5s'; SET lock_timeout='1s'; SET idle_session_timeout=0")
            answer = await self.query(f"SELECT pg_try_advisory_lock({MAINTENANCE_LOCK_KEY}::bigint)")
            if answer != ["t"]:
                raise MaintenanceBusy("another storage maintenance or snapshot operation holds the lock")
            self.locked = True
            ready = await self.query(
                "SELECT CASE WHEN to_regclass('public.storage_maintenance_status') IS NOT NULL "
                "AND to_regclass('public.storage_identity') IS NOT NULL THEN 'ready' ELSE 'missing' END"
            )
            if ready != ["ready"]:
                raise BackupError("storage maintenance schema is missing; start the upgraded backend before backing up")
            return self
        except BaseException:
            await self.__aexit__(*sys.exc_info())
            raise

    async def query(self, sql: str) -> list[str]:
        marker = "meshinfo_ack_" + uuid.uuid4().hex
        if self.proc is None or self.proc.returncode is not None:
            raise BackupError("PostgreSQL maintenance control session is unavailable")
        async with asyncio.timeout(8):
            self.proc.stdin.write((sql + ";\nSELECT '" + marker + "';\n").encode())
            await self.proc.stdin.drain()
            lines, size = [], 0
            while True:
                line = await self.proc.stdout.readline()
                if not line:
                    raise BackupError("PostgreSQL maintenance control session ended unexpectedly")
                size += len(line)
                if size > 65536:
                    raise BackupError("PostgreSQL maintenance response exceeds its limit")
                text = line.decode("utf-8").strip()
                if text == marker:
                    return lines
                if text:
                    lines.append(text)

    async def clock(self) -> str:
        rows = await self.query(
            "SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"+00:00\"')"
        )
        if len(rows) != 1:
            raise BackupError("database clock response is unavailable")
        datetime.fromisoformat(rows[0])
        return rows[0]

    async def owner_id(self) -> str:
        rows = await self.query("SELECT instance_id::text FROM public.storage_identity WHERE singleton")
        if len(rows) != 1:
            raise BackupError("database storage identity is missing")
        value = str(uuid.UUID(rows[0]))
        return hashlib.sha256(value.encode()).hexdigest()

    async def previous_success(self) -> str | None:
        rows = await self.query(
            "SELECT result->>'last_success_at' FROM public.storage_maintenance_status WHERE operation='backup'"
        )
        if not rows:
            return None
        try:
            value = datetime.fromisoformat(rows[0])
            return rows[0] if value.tzinfo is not None else None
        except ValueError:
            return None

    async def previous_success_destination(self) -> str | None:
        rows = await self.query(
            "SELECT result->>'last_success_destination' FROM public.storage_maintenance_status WHERE operation='backup'"
        )
        return rows[0] if len(rows) == 1 and re.fullmatch(r"[a-f0-9]{64}", rows[0]) else None

    async def save_status(self, detail: dict):
        # The only interpolated value is an ASCII hexadecimal literal, not SQL,
        # paths, credentials, or arbitrary strings supplied by the operator.
        encoded = json.dumps(detail, separators=(",", ":")).encode().hex()
        await self.query(
            "INSERT INTO public.storage_maintenance_status(operation,updated_at,result) "
            f"VALUES ('backup',clock_timestamp(),convert_from(decode('{encoded}','hex'),'UTF8')::jsonb) "
            "ON CONFLICT(operation) DO UPDATE SET updated_at=EXCLUDED.updated_at,result=EXCLUDED.result"
        )

    async def stop_database_clients(self, application: str):
        if not re.fullmatch(r"meshinfo_backup_[a-f0-9]{32}", application):
            raise ValueError("invalid backup application identity")
        await self.query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            f"WHERE application_name='{application}' AND pid<>pg_backend_pid()"
        )

    async def __aexit__(self, *_exception):
        try:
            if self.locked and self.proc is not None and self.proc.returncode is None:
                try:
                    await self.query(f"SELECT pg_advisory_unlock({MAINTENANCE_LOCK_KEY}::bigint)")
                except Exception:
                    pass  # Closing the PostgreSQL connection releases its locks.
        finally:
            self.locked = False
            if self.proc is not None:
                self.proc.stdin.close()
                await _stop(self.proc, drain_stdout=True)
            try:
                await self.clients.cleanup()
            finally:
                pending = [task for task in (self.exited, self.errors) if task is not None]
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)


class DockerClients:
    """Track actual container processes; terminating docker exec alone is insufficient."""

    def __init__(self, container: str, timeout: float):
        self.container = container
        self.token = uuid.uuid4().hex
        self.application = "meshinfo_backup_" + self.token
        self.pid_path = "/tmp/meshinfo-backup-" + self.token + ".pid"
        self.timeout = timeout

    @property
    def prefix(self) -> tuple[str, ...]:
        return (
            "docker", "exec", "-i", "-e", "PGPASSWORD", "-e", "PGCONNECT_TIMEOUT", self.container,
            "timeout", "--signal=TERM", "--kill-after=3", str(self.timeout),
            "sh", "-c", _CLIENT_SCRIPT, "meshinfo-backup-client",
            self.pid_path, self.token, self.application,
        )

    async def cleanup(self):
        proc = await asyncio.create_subprocess_exec(
            "docker", "exec", self.container, "sh", "-c", _CLEANUP_SCRIPT,
            "meshinfo-backup-cleanup", self.pid_path, self.token,
            stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        errors = asyncio.create_task(_tail(proc.stderr))
        try:
            async with asyncio.timeout(8):
                code = await proc.wait()
            if code != 0:
                raise BackupError("could not confirm container backup subprocess cleanup")
        finally:
            await _stop(proc)
            await asyncio.gather(errors, return_exceptions=True)


async def _heartbeat(control, stopped: asyncio.Event):
    """psql can remain idle after its backend dies; actively verify the session."""
    while not stopped.is_set():
        try:
            await asyncio.wait_for(stopped.wait(), 15)
        except asyncio.TimeoutError:
            if await control.query("SELECT 1") != ["1"]:
                raise BackupError("maintenance lock connection heartbeat failed")


async def run_backup(config: dict, repo_root: Path, container: str) -> BackupResult:
    policy = BackupPolicy.from_config(config, repo_root)
    async with DockerControlSession(config, container) as control:
        owner_id = await control.owner_id()
        status = {
            "status": "running", "mode": policy.mode,
            "destination_id": backup_destination_id(config),
            "started_at": await control.clock(), "finished_at": None,
            "last_success_at": await control.previous_success(),
            "last_success_destination": await control.previous_success_destination(),
        }
        # Prove writable durable status BEFORE pg_dump or remote file creation.
        await control.save_status(status)
        clients = DockerClients(container, policy.timeout_seconds)
        task = asyncio.create_task(create_snapshot(config, repo_root, owner_id=owner_id, pg_command_prefix=clients.prefix))
        heartbeat_stopped = asyncio.Event()
        heartbeat = asyncio.create_task(_heartbeat(control, heartbeat_stopped))
        result = None
        try:
            try:
                done, _pending = await asyncio.wait((task, control.exited, heartbeat), return_when=asyncio.FIRST_COMPLETED)
                if control.exited in done:
                    raise BackupError("maintenance lock connection was lost during the snapshot")
                if heartbeat in done:
                    await heartbeat
                    raise BackupError("maintenance lock heartbeat stopped unexpectedly")
                result = await task
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                # Finish any in-flight acknowledgment before issuing cleanup
                # SQL on the same psql protocol stream. Its query timeout is 8s.
                heartbeat_stopped.set()
                await asyncio.gather(heartbeat, return_exceptions=True)
                # Retain the lock until real container clients and matching PG
                # backends have stopped, even if docker exec itself already died.
                try:
                    await clients.cleanup()
                finally:
                    if not control.exited.done():
                        await control.stop_database_clients(clients.application)
            status.update(
                status="complete" if result.successful else "partial",
                finished_at=await control.clock(), bytes=result.bytes,
                verification=result.verification, expired_backups=len(result.pruned),
                orphan_files_removed=result.orphan_files_removed,
                orphan_bytes_removed=result.orphan_bytes_removed,
                snapshot_completed=True,
            )
            status["last_success_at"] = status["finished_at"]
            status["last_success_destination"] = status["destination_id"]
            if result.prune_error:
                status["reason"] = "BackupRetentionFailed"
            try:
                await control.save_status(status)
            except Exception as exc:
                failed_status = {
                    **status, "operation_status": status["status"], "status": "error",
                    "status_persisted": False, "status_error": type(exc).__name__,
                }
                raise BackupStatusPersistenceError(failed_status) from exc
            return result
        except BaseException as exc:
            if isinstance(exc, BackupStatusPersistenceError):
                # Never replace verified completion with a generic failed-dump
                # record, nor hide which destination now has the verified copy.
                raise
            if not control.exited.done():
                try:
                    status.update(status="error", reason=type(exc).__name__, finished_at=await control.clock())
                    await control.save_status(status)
                except Exception:
                    pass
            raise


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path(os.environ.get("CONFIG_TOML", REPO_ROOT / "config.toml")))
    parser.add_argument("--container", default=os.environ.get("CONTAINER", "meshinfo-postgres-1"))
    parser.add_argument("--mode", choices=("local", "ssh"), help="Legacy nonempty remote_target defaults to SSH streaming")
    args = parser.parse_args(argv)
    config = None
    try:
        config = load_config(args.config, mode=args.mode)
        result = asyncio.run(run_backup(config, REPO_ROOT, args.container))
        print(json.dumps(result.public_summary(), sort_keys=True))
        return 0 if result.successful else 1
    except MaintenanceBusy:
        print("Backup deferred: another storage maintenance operation is running.", file=sys.stderr)
        return 2
    except BackupStatusPersistenceError as exc:
        print(json.dumps(exc.status, sort_keys=True))
        print(f"Backup status failed: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        message = str(exc)
        if config:
            password = str(config.get("storage", {}).get("postgres", {}).get("password", ""))
            if password:
                message = message.replace(password, "[redacted]")
        print(f"Backup failed: {message}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
