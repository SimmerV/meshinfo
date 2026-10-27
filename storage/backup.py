"""Bounded PostgreSQL snapshots with independently retained local/SSH copies.

The scheduler/CLI caller must hold the shared PostgreSQL maintenance advisory
lock for the entire operation, preventing retention DDL during pg_dump. This
module also locks the destination against overlapping snapshot/prune jobs.
Secrets are passed through the subprocess environment, never command strings.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import datetime
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import posixpath
import re
import shlex
import struct
from typing import Any, Mapping, Sequence
import uuid

from storage import backup_remote
from storage.backup_remote import BackupError, CHUNK_BYTES, STDERR_BYTES, SnapshotTarget


@dataclass(frozen=True)
class BackupPolicy:
    mode: str
    directory: Path
    keep_days: float
    remote_keep_days: float
    remote_host: str | None
    remote_directory: str | None
    min_free_bytes: int
    timeout_seconds: float

    @classmethod
    def from_config(cls, config: Mapping[str, Any], repo_root: Path) -> "BackupPolicy":
        settings = config.get("backups", {})
        if not isinstance(settings, Mapping):
            raise ValueError("backups must be a table")
        mode = settings.get("mode", "local")
        if mode not in ("local", "ssh"):
            raise ValueError("backups.mode must be local or ssh")

        def positive(name: str, default: float, maximum: float) -> float:
            value = settings.get(name, default)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 < value <= maximum:
                raise ValueError(f"backups.{name} must be positive and at most {maximum:g}")
            return float(value)

        keep_days = positive("keep_days", 4, 365000)
        remote_keep_days = positive("remote_keep_days", keep_days, 365000)
        reserve = settings.get("min_free_mb", 256)
        if type(reserve) is not int or not 0 <= reserve <= 1048576:
            raise ValueError("backups.min_free_mb must be an integer between 0 and 1048576")
        timeout = positive("timeout_seconds", 21600, 604800)
        if timeout < 1:
            raise ValueError("backups.timeout_seconds must be at least 1")
        directory_text = settings.get("dir", "backups")
        if not isinstance(directory_text, str) or not directory_text or "\x00" in directory_text:
            raise ValueError("backups.dir must be a nonempty path")
        directory = Path(directory_text)
        if not directory.is_absolute():
            directory = repo_root.absolute() / directory
        host, remote_directory = None, None
        if mode == "ssh":
            host, remote_directory = parse_remote_target(settings.get("remote_target", ""))
        return cls(mode, directory, keep_days, remote_keep_days, host, remote_directory, reserve * 1024 * 1024, timeout)


def parse_remote_target(target: str) -> tuple[str, str]:
    """Accept a fixed SSH destination and absolute path, never SSH options/URLs."""
    if not isinstance(target, str) or len(target) > 4096 or any(ord(char) < 32 for char in target):
        raise ValueError("backups.remote_target must be user@host:/absolute/path")
    match = re.fullmatch(
        r"((?:[A-Za-z0-9_][A-Za-z0-9_.-]*@)?(?:[A-Za-z0-9][A-Za-z0-9._-]*|\[[A-Fa-f0-9:]+\])):(/.*)",
        target,
    )
    if match is None:
        raise ValueError("backups.remote_target must be user@host:/absolute/path")
    host, directory = match.groups()
    if ".." in PurePosixPath(directory).parts or directory == "/" or directory.startswith("//"):
        raise ValueError("backup remote directory must be a dedicated absolute path without parent traversal")
    return host, directory


def backup_destination_id(config: Mapping[str, Any]) -> str:
    """Opaque destination identity shared by host and container schedulers.

    Relative local directories stay relative: resolving against this process's
    checkout would produce different identities for the same mounted location.
    Only lexical normalization is used; no remote IO or filesystem probes.
    """
    settings = config.get("backups", {})
    mode = settings.get("mode", "local")
    if mode == "local":
        directory = settings.get("dir", "backups")
        if not isinstance(directory, str) or not directory or "\x00" in directory:
            raise ValueError("backups.dir must be a nonempty path")
        destination = {"mode": "local", "directory": posixpath.normpath(directory)}
    elif mode == "ssh":
        host, directory = parse_remote_target(settings.get("remote_target", ""))
        user, separator, hostname = host.rpartition("@")
        host = user + separator + hostname.lower() if separator else host.lower()
        destination = {"mode": "ssh", "host": host, "directory": posixpath.normpath(directory)}
    else:
        raise ValueError("backups.mode must be local or ssh")
    encoded = json.dumps(destination, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class BackupResult:
    destination: str
    filename: str
    bytes: int
    sha256: str
    completed_at: float
    verification: str
    pruned: tuple[str, ...]
    prune_error: str | None = None
    orphan_files_removed: int = 0
    orphan_bytes_removed: int = 0

    @property
    def successful(self) -> bool:
        return self.prune_error is None

    def public_summary(self) -> dict[str, Any]:
        """No destination paths, remote accounts, filenames or raw errors."""
        return {
            "status": "complete" if self.successful else "partial",
            "completed_at": self.completed_at,
            "bytes": self.bytes,
            "verification": self.verification,
            "expired_backups": len(self.pruned),
            "orphan_files_removed": self.orphan_files_removed,
            "orphan_bytes_removed": self.orphan_bytes_removed,
        }


async def _filesystem(function, /, *args, **kwargs):
    """Keep slow disk work off the event loop without racing cleanup on cancel.

    Only one bounded chunk is submitted at a time. A running filesystem call
    cannot safely be abandoned: cancellation waits for it before file handles,
    temporary archives, or the destination lock can be closed by the caller.
    """
    worker = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError as cancellation:
        while not worker.done():
            try:
                await asyncio.shield(worker)
            except asyncio.CancelledError:
                continue  # A second shutdown cancellation still cannot detach IO.
            except Exception:
                break
        if not worker.cancelled():
            worker.exception()  # Retrieve any IO error; cancellation remains primary.
        raise cancellation


async def _tail(stream: asyncio.StreamReader | None) -> bytes:
    tail = bytearray()
    if stream is not None:
        while chunk := await stream.read(STDERR_BYTES):
            tail.extend(chunk)
            del tail[:-STDERR_BYTES]
    return bytes(tail)


async def _bounded_output(stream: asyncio.StreamReader | None, limit: int = 65536) -> bytes:
    if stream is None:
        return b""
    output = bytearray()
    oversized = False
    while chunk := await stream.read(min(CHUNK_BYTES, limit + 1)):
        if len(output) + len(chunk) > limit:
            oversized = True
        output.extend(chunk[:max(0, limit - len(output))])
    if oversized:
        raise BackupError("backup transport returned an oversized acknowledgment")
    return bytes(output)


async def _stop(proc: asyncio.subprocess.Process | None, *, drain_stdout: bool = False):
    if proc is None:
        return
    reader = asyncio.create_task(_tail(proc.stdout)) if drain_stdout else None
    try:
        if proc.returncode is None:
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(proc.wait(), 3)
            except asyncio.TimeoutError:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
        await proc.wait()
    finally:
        if reader is not None:
            await reader


async def _close_stdin(proc: asyncio.subprocess.Process):
    if proc.stdin is not None:
        proc.stdin.close()
        try:
            await asyncio.wait_for(proc.stdin.wait_closed(), 1)
        except asyncio.TimeoutError:
            proc.stdin.transport.abort()
        except (BrokenPipeError, ConnectionResetError):
            pass


def _dump_command(pg: Mapping[str, Any], prefix: Sequence[str]) -> tuple[str, ...]:
    return (
        *prefix,
        "pg_dump", "--format=custom", "--no-owner", "--no-privileges",
        "--host", str(pg.get("host", "postgres")),
        "--port", str(pg.get("port", 5432)),
        "--username", str(pg.get("username", "postgres")),
        "--dbname", str(pg.get("database", "meshinfo")),
    )


def ssh_command(policy: BackupPolicy, filename: str, owner_id: str) -> tuple[str, ...]:
    # SSH executes a remote shell even with local exec argv. Quote EVERY remote
    # argument, including source and directory, using POSIX shlex.join.
    source = Path(backup_remote.__file__).read_text(encoding="utf-8")
    remote_argv = (
        "python3", "-c", source,
        policy.remote_directory or "", filename,
        str(policy.min_free_bytes), str(policy.remote_keep_days), str(policy.timeout_seconds),
        owner_id,
    )
    return (
        "ssh", "-T", "-oBatchMode=yes", "-oStrictHostKeyChecking=yes",
        "-oConnectTimeout=10", "-oServerAliveInterval=30", "-oServerAliveCountMax=3",
        "--", policy.remote_host or "", shlex.join(remote_argv),
    )


async def _verify_local(target: SnapshotTarget, prefix: Sequence[str]):
    proc = await asyncio.create_subprocess_exec(
        *prefix, "pg_restore", "--no-owner", "--no-privileges", "--file=/dev/null",
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    errors = asyncio.create_task(_tail(proc.stderr))
    digest = hashlib.sha256()
    try:
        archive = None

        def open_archive():
            nonlocal archive
            archive = target.temporary.open("rb")

        try:
            await _filesystem(open_archive)
            while chunk := await _filesystem(archive.read, CHUNK_BYTES):
                digest.update(chunk)
                proc.stdin.write(chunk)
                await proc.stdin.drain()
        finally:
            if archive is not None:
                await _filesystem(archive.close)
        await _close_stdin(proc)
        code = await proc.wait()
        detail = (await errors).decode(errors="replace").strip()
        if code != 0:
            raise BackupError(f"pg_restore archive verification failed (exit {code}): {detail}")
        if digest.hexdigest() != target.digest.hexdigest():
            raise BackupError("backup checksum changed before verification")
    finally:
        await _stop(proc)
        await asyncio.gather(errors, return_exceptions=True)


def _result(destination: str, metadata: Mapping[str, Any]) -> BackupResult:
    return BackupResult(
        destination=destination,
        filename=metadata["filename"],
        bytes=metadata["bytes"],
        sha256=metadata["sha256"],
        completed_at=metadata["completed_at"],
        verification=metadata["verification"],
        pruned=tuple(metadata.get("pruned", ())),
        prune_error=metadata.get("prune_error"),
        orphan_files_removed=metadata.get("orphan_files_removed", 0),
        orphan_bytes_removed=metadata.get("orphan_bytes_removed", 0),
    )


async def create_snapshot(
    config: Mapping[str, Any],
    repo_root: Path,
    *,
    owner_id: str,
    pg_command_prefix: Sequence[str] = (),
) -> BackupResult:
    """Stream, decode-verify, atomically publish, then prune expired snapshots.

    Raises on dump/verification/transport/time-limit failure; propagates task
    cancellation after stopping child processes and cleaning our temporary file.
    A pruning failure returns a verified snapshot with successful=False so the
    caller can report that useful distinction. The caller holds the database's
    shared maintenance advisory lock throughout this coroutine.
    """
    if not isinstance(owner_id, str) or not re.fullmatch(r"[a-f0-9]{64}", owner_id):
        raise ValueError("owner_id must be SHA256 of the database's durable storage identity")
    policy = BackupPolicy.from_config(config, repo_root)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d-%H%M%S")
    filename = f"meshinfo-pg-{stamp}-{uuid.uuid4().hex[:12]}.dump"
    pg = config.get("storage", {}).get("postgres", {})
    environment = {**os.environ, "PGPASSWORD": str(pg.get("password", "")), "PGCONNECT_TIMEOUT": "15"}
    dump: asyncio.subprocess.Process | None = None
    transport: asyncio.subprocess.Process | None = None
    background: list[asyncio.Task] = []
    target: SnapshotTarget | None = None
    try:
        async with asyncio.timeout(policy.timeout_seconds):
            if policy.mode == "local":
                target = SnapshotTarget(policy.directory, filename, policy.min_free_bytes, owner=owner_id)
                await _filesystem(target.__enter__)
            else:
                transport = await asyncio.create_subprocess_exec(
                    *ssh_command(policy, filename, owner_id),
                    stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                remote_errors = asyncio.create_task(_tail(transport.stderr))
                remote_output = asyncio.create_task(_bounded_output(transport.stdout))
                background.extend([remote_errors, remote_output])
            dump = await asyncio.create_subprocess_exec(
                *_dump_command(pg, pg_command_prefix), env=environment,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            dump_errors = asyncio.create_task(_tail(dump.stderr))
            background.append(dump_errors)
            digest = hashlib.sha256()
            size = 0
            while chunk := await dump.stdout.read(CHUNK_BYTES):
                digest.update(chunk)
                size += len(chunk)
                if target is not None:
                    await _filesystem(target.write, chunk)
                else:
                    transport.stdin.write(struct.pack("!I", len(chunk)))
                    transport.stdin.write(chunk)
                    await transport.stdin.drain()
            code = await dump.wait()
            detail = (await dump_errors).decode(errors="replace").strip()
            if code != 0:
                raise BackupError(f"pg_dump failed (exit {code}): {detail}")
            if size == 0:
                raise BackupError("pg_dump produced an empty snapshot")
            if target is not None:
                await _filesystem(target.seal)
                await _verify_local(target, pg_command_prefix)
                metadata = await _filesystem(target.publish)
                try:
                    metadata["pruned"] = await _filesystem(backup_remote.prune_snapshots, policy.directory, policy.keep_days, owner=owner_id)
                except OSError as exc:
                    metadata["prune_error"] = str(exc)
                    metadata["pruned"] = []
                return _result(str(target.path), metadata)

            # Only a SUCCESSFUL pg_dump permits the receiver to verify/publish.
            commit = json.dumps({"sha256": digest.hexdigest(), "bytes": size}).encode()
            transport.stdin.write(struct.pack("!II", 0, len(commit)) + commit)
            await transport.stdin.drain()
            await _close_stdin(transport)
            code = await transport.wait()
            detail = (await remote_errors).decode(errors="replace").strip()
            if code != 0:
                raise BackupError(f"SSH backup failed (exit {code}): {detail}")
            try:
                metadata = json.loads(await remote_output)
                if (
                    metadata.get("schema") != 1
                    or metadata.get("owner") != owner_id
                    or metadata.get("filename") != filename
                    or metadata.get("sha256") != digest.hexdigest()
                    or type(metadata.get("bytes")) is not int
                    or metadata.get("bytes") != size
                    or metadata.get("verification") != "archive_decode"
                    or type(metadata.get("completed_at")) not in (int, float)
                    or not math.isfinite(metadata["completed_at"])
                    or metadata["completed_at"] <= 0
                    or not isinstance(metadata.get("pruned"), list)
                    or not all(isinstance(name, str) and backup_remote.OWNED_NAME.fullmatch(name) for name in metadata["pruned"])
                ):
                    raise ValueError("acknowledgment fields do not match the snapshot")
            except (ValueError, TypeError, AttributeError, KeyError) as exc:
                raise BackupError("SSH receiver did not acknowledge the verified snapshot") from exc
            return _result(f"{policy.remote_host}:{policy.remote_directory}/{filename}", metadata)
    finally:
        # Close the remote input before shutdown: a failed producer cannot send
        # a commit, and the receiver removes its incomplete temporary archive.
        if transport is not None:
            await _close_stdin(transport)
            if transport.returncode is None:
                try:
                    await asyncio.wait_for(transport.wait(), 3)
                except asyncio.TimeoutError:
                    pass
        await _stop(dump, drain_stdout=True)
        await _stop(transport)
        if background:
            await asyncio.gather(*background, return_exceptions=True)
        if target is not None:
            await _filesystem(target.__exit__, None, None, None)


if __name__ == "__main__":
    raise SystemExit("Use scripts/backup_database.py; it acquires and monitors the maintenance lock.")
