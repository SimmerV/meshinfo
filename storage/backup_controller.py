"""Durable in-process backup scheduling and maintenance-lock coordination.

The snapshot transport owns archive/subprocess cleanup. This controller owns a
PostgreSQL session for the entire snapshot, monitors that session, and records
safe outcome metadata. It never emits destinations, credentials or filenames.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import fnmatch
import hashlib
import logging
import os
from pathlib import Path
import time
import uuid

from storage.backup import BackupPolicy, BackupResult, backup_destination_id, create_snapshot
from storage.db.maintenance_status import read_status, save_status
from storage.db.retention_executor import maintenance_lock

logger = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[1]
INTERVAL_SECONDS = {"daily": 86400, "weekly": 7 * 86400, "monthly": 30 * 86400}
RETRY_SECONDS = 3600
BUSY_RETRY_SECONDS = 60
HEARTBEAT_SECONDS = 5
HEARTBEAT_TIMEOUT_SECONDS = 3
_BACKOFF_UNTIL: dict[tuple, float] = {}
UTC = timezone.utc


class BackupControlLost(RuntimeError):
    def __init__(self, snapshot: BackupResult | None = None):
        super().__init__("PostgreSQL maintenance-lock session was lost")
        self.snapshot = snapshot


class StorageIdentityMissing(RuntimeError):
    pass


def _key(config) -> tuple:
    pg = config.get("storage", {}).get("postgres", {})
    try:
        destination = backup_destination_id(config)
    except (TypeError, ValueError, AttributeError):
        destination = "invalid-configuration"
    return (pg.get("host"), pg.get("port", 5432), pg.get("database"), pg.get("username"), destination)


def _backoff(config, seconds=RETRY_SECONDS):
    now = time.monotonic()
    for key, deadline in list(_BACKOFF_UNTIL.items()):
        if deadline <= now:
            _BACKOFF_UNTIL.pop(key, None)
    if len(_BACKOFF_UNTIL) >= 128:
        _BACKOFF_UNTIL.pop(next(iter(_BACKOFF_UNTIL)))
    _BACKOFF_UNTIL[_key(config)] = now + seconds


def _timestamp(value) -> datetime | None:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        return None
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _local_now() -> str:
    return _iso(datetime.now(UTC))


def _previous(statuses) -> tuple[dict, dict]:
    entry = statuses.get("backup", {})
    if not isinstance(entry, dict):
        return {}, {}
    detail = entry.get("result", {})
    return entry, detail if isinstance(detail, dict) else {}


def _last_success_destination(detail) -> str | None:
    value = detail.get("last_success_destination")
    return value if isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value) else None


def _last_success(detail) -> str | None:
    previous = _timestamp(detail.get("last_success_at"))
    return _iso(previous) if previous else None


def _legacy_local_latest(config) -> datetime | None:
    """Migration fallback only: bounded, non-symlink completed-dump mtime scan."""
    directory = Path(config.get("backups", {}).get("dir", "backups"))
    if not directory.is_absolute():
        directory = REPO_ROOT / directory
    latest = None
    try:
        with os.scandir(directory) as entries:
            for count, entry in enumerate(entries):
                if count >= 4096:
                    raise OSError("legacy backup directory exceeds bounded inventory")
                if fnmatch.fnmatchcase(entry.name, "meshinfo-pg-*.dump") and entry.is_file(follow_symlinks=False):
                    modified = entry.stat(follow_symlinks=False).st_mtime
                    latest = max(latest, modified) if latest is not None else modified
    except FileNotFoundError:
        return None
    return datetime.fromtimestamp(latest, UTC) if latest is not None else None


async def scheduled_backup_due(config, storage, *, now=None) -> bool:
    """Whether a new scheduled snapshot is due, without creating any files.

    A connection/read failure defers one hour in this process. Durable error or
    interrupted-running records provide the same backoff across process restarts.
    ``now`` is an optional aware datetime for deterministic scheduling checks.
    """
    settings = config.get("backups", {})
    interval = INTERVAL_SECONDS.get(settings.get("schedule", "daily"))
    if interval is None:
        return False
    if time.monotonic() < _BACKOFF_UNTIL.get(_key(config), 0):
        return False
    supplied_now = _timestamp(now) if now is not None else None
    if now is not None and supplied_now is None:
        raise ValueError("backup scheduling now must be a timezone-aware datetime")
    try:
        async with storage.pool.acquire(timeout=3) as conn:
            current = supplied_now or _timestamp(await conn.fetchval("SELECT clock_timestamp()", timeout=3))
            if current is None:
                raise RuntimeError("database clock unavailable")
            entry, detail = _previous(await read_status(conn))
        destination = backup_destination_id(config)
        mode = settings.get("mode", "local")
        attempt_destination = detail.get("destination_id")
        same_attempt = (attempt_destination == destination or
                        (attempt_destination is None and detail.get("mode", mode) == mode))
        if same_attempt and detail.get("status") in {"error", "blocked", "running"}:
            attempt = _timestamp(entry.get("updated_at")) or _timestamp(detail.get("finished_at")) or _timestamp(detail.get("started_at"))
            if attempt is not None and (current - attempt).total_seconds() < RETRY_SECONDS:
                return False
        successful_destination = _last_success_destination(detail)
        if successful_destination is not None and successful_destination != destination:
            return True
        if successful_destination is None:
            # An old local snapshot remains useful across upgrade. A remote
            # destination without identity needs one verified, tracked snapshot;
            # another mode's previous success never protects this destination.
            if mode != "local" or detail.get("mode", mode) != mode:
                return True
        last = _timestamp(detail.get("last_success_at"))
        if last is None and mode == "local":
            last = await asyncio.to_thread(_legacy_local_latest, config)
        return last is None or (current - last).total_seconds() >= interval
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _backoff(config)
        logger.warning("Backup schedule check deferred after %s", type(exc).__name__)
        return False


async def _heartbeat(conn, stopped: asyncio.Event, terminated: asyncio.Event):
    while not stopped.is_set():
        stopping = asyncio.create_task(stopped.wait())
        ending = asyncio.create_task(terminated.wait())
        try:
            done, _ = await asyncio.wait((stopping, ending), timeout=HEARTBEAT_SECONDS,
                                         return_when=asyncio.FIRST_COMPLETED)
            if ending in done:
                raise BackupControlLost()
            if stopping in done:
                return
            try:
                if await conn.fetchval("SELECT 1", timeout=HEARTBEAT_TIMEOUT_SECONDS) != 1:
                    raise BackupControlLost()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise BackupControlLost() from exc
        finally:
            for task in (stopping, ending):
                if not task.done():
                    task.cancel()
            await asyncio.gather(stopping, ending, return_exceptions=True)


async def _drain(tasks):
    """Wait through repeated cancellation until subprocess/query cleanup finishes."""
    waiter = asyncio.ensure_future(asyncio.gather(*tasks, return_exceptions=True))
    cancellation = None
    while not waiter.done():
        try:
            await asyncio.shield(waiter)
        except asyncio.CancelledError as exc:
            cancellation = exc
    waiter.result()
    if cancellation is not None:
        raise cancellation


async def _guarded_snapshot(conn, config, repo_root, owner_id):
    stopped, terminated = asyncio.Event(), asyncio.Event()
    def session_ended(_connection):
        terminated.set()
    add_listener = getattr(conn, "add_termination_listener", None)
    if callable(add_listener):
        add_listener(session_ended)
    snapshot = asyncio.create_task(create_snapshot(config, repo_root, owner_id=owner_id))
    heartbeat = asyncio.create_task(_heartbeat(conn, stopped, terminated))
    completed = None
    try:
        done, _ = await asyncio.wait((snapshot, heartbeat), return_when=asyncio.FIRST_COMPLETED)
        if snapshot in done:
            completed = await snapshot
        if heartbeat in done:
            # If both finished together, retain any acknowledged publication in
            # the raised error so the caller does not misreport it as absent.
            try:
                await heartbeat
            except Exception as exc:
                raise BackupControlLost(completed) from exc
            raise BackupControlLost(completed)
        return completed
    finally:
        stopped.set()
        if not snapshot.done():
            snapshot.cancel()
        # Do not cancel an in-flight heartbeat query: finish it before status SQL
        # uses this same asyncpg session. Its query timeout is three seconds.
        try:
            await _drain((snapshot, heartbeat))
        finally:
            if callable(add_listener):
                try:
                    conn.remove_termination_listener(session_ended)
                except Exception:
                    pass  # A dead session already released its advisory lock.


def _snapshot_status(status, snapshot, finished_at):
    detail = {
        **status, "status": "complete" if snapshot.successful else "partial",
        "finished_at": finished_at, "last_success_at": finished_at,
        "last_success_destination": status["destination_id"],
        "bytes": snapshot.bytes, "verification": snapshot.verification,
        "expired_backups": len(snapshot.pruned),
        "orphan_files_removed": snapshot.orphan_files_removed,
        "orphan_bytes_removed": snapshot.orphan_bytes_removed,
        "snapshot_completed": True,
    }
    if snapshot.prune_error:
        detail["reason"] = "BackupRetentionFailed"
    return detail


async def _record(conn, result):
    try:
        await save_status(conn, "backup", result)
        return {**result, "status_persisted": True}
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning("Backup outcome could not be persisted after %s", type(exc).__name__)
        return {**result, "operation_status": result.get("operation_status", result.get("status")), "status": "error",
                "status_persisted": False, "status_error": type(exc).__name__}


async def run_backup(config, storage, repo_root, *, after=None, manual=False) -> dict:
    """Run a configured backup while retaining the shared lock through cleanup.

    Off schedules remain off unless an explicitly invoked operator command sets
    ``manual=True``. The host CLI also has its own explicit-action controller.
    ``after`` can be the current retention task, preventing a local scheduling
    race from repeatedly denying backups the shared maintenance lock.
    """
    settings = config.get("backups", {})
    if type(manual) is not bool:
        raise ValueError("manual must be boolean")
    if not manual and settings.get("schedule", "daily") not in INTERVAL_SECONDS:
        return {"status": "disabled"}
    if after is not None:
        try:
            await asyncio.shield(after)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Prior maintenance ended with %s; backup will acquire its own lock", type(exc).__name__)
    status = {"status": "error", "mode": settings.get("mode", "local"),
              "started_at": _local_now(), "finished_at": None, "last_success_at": None,
              "last_success_destination": None}
    snapshot = None
    try:
        policy = BackupPolicy.from_config(config, Path(repo_root))
        status["mode"] = policy.mode
        status["destination_id"] = backup_destination_id(config)
        async with storage.pool.acquire(timeout=3) as conn:
            async with maintenance_lock(conn) as acquired:
                if not acquired:
                    _backoff(config, BUSY_RETRY_SECONDS)
                    return {**status, "status": "busy", "finished_at": _local_now()}
                _, previous = _previous(await read_status(conn))
                status["last_success_at"] = _last_success(previous)
                status["last_success_destination"] = _last_success_destination(previous)
                clock = _timestamp(await conn.fetchval("SELECT clock_timestamp()", timeout=3))
                if clock is None:
                    raise RuntimeError("database clock unavailable")
                status["started_at"] = _iso(clock)
                try:
                    identity = await conn.fetchval("SELECT instance_id::text FROM public.storage_identity WHERE singleton", timeout=3)
                    try:
                        identity = str(uuid.UUID(str(identity)))
                    except (TypeError, ValueError, AttributeError) as exc:
                        raise StorageIdentityMissing("database storage identity is missing or invalid") from exc
                    owner_id = hashlib.sha256(identity.encode()).hexdigest()
                    status["status"] = "running"
                    # Prove durable status is writable before creating a dump.
                    await save_status(conn, "backup", status)
                    snapshot = await _guarded_snapshot(conn, config, Path(repo_root), owner_id)
                    finished = _timestamp(await conn.fetchval("SELECT clock_timestamp()", timeout=3))
                    if finished is None:
                        raise RuntimeError("database clock unavailable")
                    status = _snapshot_status(status, snapshot, _iso(finished))
                    status = await _record(conn, status)
                    if status["status"] == "error":
                        _backoff(config)
                    else:
                        _BACKOFF_UNTIL.pop(_key(config), None)
                    return status
                except asyncio.CancelledError:
                    _backoff(config)
                    cancelled = {**status, "status": "error", "reason": "CancelledError", "finished_at": _local_now()}
                    # _guarded_snapshot has finished cleanup/heartbeat before we
                    # write this result or let maintenance_lock release the lock.
                    try:
                        await _record(conn, cancelled)
                    finally:
                        raise
                except Exception as exc:
                    _backoff(config)
                    if isinstance(exc, BackupControlLost) and exc.snapshot is not None:
                        snapshot = exc.snapshot
                    if snapshot is not None:
                        # Publication was acknowledged before a control/status
                        # failure. Starting DB time is a conservative schedule
                        # anchor when the finishing DB clock cannot be read.
                        status = {**_snapshot_status(status, snapshot, _local_now()), "last_success_at": status["started_at"]}
                        status = {**status, "operation_status": status["status"], "status": "error"}
                    else:
                        status = {**status, "status": "error", "finished_at": _local_now()}
                    status["reason"] = type(exc).__name__
                    logger.warning("Backup attempt failed after %s", type(exc).__name__)
                    return await _record(conn, status)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _backoff(config)
        if snapshot is not None and not status.get("snapshot_completed"):
            status = {**_snapshot_status(status, snapshot, _local_now()), "last_success_at": status["started_at"]}
        logger.warning("Backup control unavailable after %s", type(exc).__name__)
        return {**status, "operation_status": status.get("operation_status", status.get("status")), "status": "error",
                "reason": type(exc).__name__, "finished_at": status.get("finished_at") or _local_now(),
                "status_persisted": False}
