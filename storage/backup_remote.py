"""Dependency-free backup destination and SSH receiver.

The receiver source is sent over SSH rather than installed on the target. Its
stdin is a length-framed stream: data chunks, zero-length terminator, then a
small JSON commit record. A failed pg_dump never sends that commit record.
Only manifests marked archive_decode are eligible for automatic expiration;
this is archive readability verification, not a successful database restore.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import selectors
import signal
import stat
import struct
import subprocess
import sys
import time
import uuid
from typing import BinaryIO

CHUNK_BYTES = 256 * 1024
STDERR_BYTES = 16 * 1024
RESTORE_METADATA_BYTES = 1024 * 1024
MANIFEST_BYTES = RESTORE_METADATA_BYTES + 65536
OWNED_NAME = re.compile(r"meshinfo-pg-\d{8}-\d{6}-[a-f0-9]{12}\.dump\Z")
MANIFEST_SUFFIX = ".manifest.json"
OWNER_MARKER = ".meshinfo-backup-owner.json"
OWNER_RE = re.compile(r"[a-f0-9]{64}\Z")
OWNER_TEMP_RE = re.compile(r"\.meshinfo-backup-owner-[a-f0-9]{32}\.json\.part\Z")
# Compatibility for direct low-level tests/callers. Production create_snapshot
# requires an explicit fingerprint of the durable database instance identity.
DEFAULT_OWNER = hashlib.sha256(b"meshinfo-backup-compatibility-owner").hexdigest()


def _validate_owner(owner: str) -> str:
    if not isinstance(owner, str) or not OWNER_RE.fullmatch(owner):
        raise ValueError("backup owner must be a 64-character lowercase SHA-256 fingerprint")
    return owner


def _sync_directory(directory: Path):
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class BackupError(RuntimeError):
    """An unsuccessful snapshot, never a signal to prune older snapshots."""


def validate_restore_metadata(metadata: dict) -> dict:
    """Validate the bounded, versioned metadata accepted across SSH and restore."""
    if not isinstance(metadata, dict) or set(metadata) != {"schema", "postgres_major", "extension", "relations"}:
        raise BackupError("invalid Columnar restore metadata fields")
    if type(metadata["schema"]) is not int or metadata["schema"] not in (1, 2) or type(metadata["postgres_major"]) is not int or not 10 <= metadata["postgres_major"] <= 99:
        raise BackupError("unsupported Columnar restore metadata version")
    extension = metadata["extension"]
    if extension is not None and (not isinstance(extension, str) or not re.fullmatch(r"[0-9]+(?:\.[0-9]+)*-[0-9]+", extension) or len(extension) > 64):
        raise BackupError("invalid Columnar extension version")
    relations = metadata["relations"]
    if not isinstance(relations, list) or len(relations) > 4096 or (relations and extension is None):
        raise BackupError("invalid Columnar relation inventory")
    names = set()
    expected = {"schema", "name", "chunk_group_row_limit", "stripe_row_limit", "compression_level", "compression"}
    if metadata["schema"] == 2:
        expected.add("geometry")
    for relation in relations:
        if not isinstance(relation, dict) or set(relation) != expected:
            raise BackupError("invalid Columnar relation options")
        for key in ("schema", "name"):
            value = relation[key]
            if not isinstance(value, str) or not 1 <= len(value.encode("utf-8")) <= 63 or "\x00" in value:
                raise BackupError("invalid Columnar relation identifier")
        name = relation["schema"], relation["name"]
        if name in names:
            raise BackupError("duplicate Columnar relation metadata")
        names.add(name)
        for key in ("chunk_group_row_limit", "stripe_row_limit"):
            if type(relation[key]) is not int or not 1 <= relation[key] <= 1000000000:
                raise BackupError("invalid Columnar row limit")
        if type(relation["compression_level"]) is not int or not -131072 <= relation["compression_level"] <= 131072:
            raise BackupError("invalid Columnar compression level")
        if relation["compression"] not in ("none", "pglz", "lz4", "zstd"):
            raise BackupError("unsupported Columnar compression algorithm")
        if metadata["schema"] == 2:
            geometry = relation["geometry"]
            fields = {"row_count", "max_stripe_rows", "max_chunk_group_rows"}
            if not isinstance(geometry, dict) or set(geometry) != fields:
                raise BackupError("invalid Columnar physical geometry")
            if any(type(geometry[key]) is not int or not 0 <= geometry[key] <= 2**63-1 for key in fields):
                raise BackupError("invalid Columnar geometry row count")
            rows, stripe, group = (geometry[key] for key in ("row_count", "max_stripe_rows", "max_chunk_group_rows"))
            if (rows == 0 and (stripe or group)) or (rows > 0 and not 1 <= group <= stripe <= min(rows, 1000000000)):
                raise BackupError("inconsistent Columnar physical geometry")
    encoded = json.dumps(metadata, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    if len(encoded) > RESTORE_METADATA_BYTES:
        raise BackupError("Columnar restore metadata exceeds its bounded budget")
    return metadata


def restore_metadata_checksum(metadata: dict) -> str:
    validate_restore_metadata(metadata)
    return hashlib.sha256(json.dumps(metadata, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()


def validate_manifest_restore_metadata(manifest: dict):
    if "restore_metadata" not in manifest:
        if "restore_metadata_sha256" in manifest:
            raise BackupError("backup restore metadata is missing")
        return None  # Existing heap-only backup manifests remain valid.
    metadata = validate_restore_metadata(manifest["restore_metadata"])
    if manifest.get("restore_metadata_sha256") != restore_metadata_checksum(metadata):
        raise BackupError("backup restore metadata checksum does not match")
    return metadata


def owned_snapshots(directory: Path, *, owner: str | None = None) -> list[tuple[Path, dict]]:
    """Return only regular files with our complete, valid verification marker."""
    if owner is not None:
        _validate_owner(owner)
    snapshots = []
    if not directory.exists():
        return snapshots
    for path in directory.glob("meshinfo-pg-*.dump"):
        if not OWNED_NAME.fullmatch(path.name):
            continue
        marker = path.with_name(path.name + MANIFEST_SUFFIX)
        try:
            info, marker_info = path.lstat(), marker.lstat()
            if not stat.S_ISREG(info.st_mode) or not stat.S_ISREG(marker_info.st_mode):
                continue
            if marker_info.st_size > MANIFEST_BYTES:
                continue
            metadata = json.loads(marker.read_text(encoding="utf-8"))
            completed = metadata.get("completed_at")
            size = metadata.get("bytes")
            if (
                metadata.get("schema") != 1
                or metadata.get("filename") != path.name
                or metadata.get("verification") != "archive_decode"
                or ("owner" in metadata and not OWNER_RE.fullmatch(str(metadata["owner"])))
                or (owner is not None and metadata.get("owner") != owner)
                or not isinstance(completed, (int, float))
                or isinstance(completed, bool)
                or not math.isfinite(completed)
                or completed <= 0
                or not isinstance(size, int)
                or isinstance(size, bool)
                or size <= 0
                or size != info.st_size
                or not re.fullmatch(r"[a-f0-9]{64}", str(metadata.get("sha256", "")))
            ):
                continue
            validate_manifest_restore_metadata(metadata)
            snapshots.append((path, metadata))
        except (OSError, ValueError, TypeError, AttributeError, BackupError):
            continue
    return snapshots


def prune_snapshots(directory: Path, keep_days: float, *, now: float | None = None, owner: str = DEFAULT_OWNER) -> list[str]:
    """Prune our verified snapshots only, always preserving the newest one.

    The caller must hold the destination lock and have just published a verified
    snapshot. Legacy dumps without manifests are deliberately left untouched.
    """
    if not math.isfinite(keep_days) or keep_days <= 0:
        raise ValueError("backup retention must be positive")
    snapshots = owned_snapshots(directory, owner=_validate_owner(owner))
    if not snapshots:
        return []
    newest = max(snapshots, key=lambda item: (item[1]["completed_at"], item[0].name))[0]
    cutoff = (time.time() if now is None else now) - keep_days * 86400
    removed = []
    for path, metadata in snapshots:
        if path == newest or metadata["completed_at"] >= cutoff:
            continue
        path.unlink()
        path.with_name(path.name + MANIFEST_SUFFIX).unlink(missing_ok=True)
        removed.append(path.name)
    return removed


class SnapshotTarget:
    """A locked destination, private temporary archive, and atomic completion."""

    def __init__(self, directory: Path, filename: str, min_free_bytes: int, *, owner: str = DEFAULT_OWNER):
        if not directory.is_absolute():
            raise ValueError("backup destination must be absolute")
        if not OWNED_NAME.fullmatch(filename):
            raise ValueError("invalid snapshot filename")
        if min_free_bytes < 0:
            raise ValueError("backup free-space reserve must not be negative")
        self.owner = _validate_owner(owner)
        self.directory = directory
        self.path = directory / filename
        self.temporary = directory / (filename + ".part")
        self.marker = directory / (filename + MANIFEST_SUFFIX)
        self.marker_temporary = directory / (filename + MANIFEST_SUFFIX + ".part")
        self.min_free_bytes = min_free_bytes
        self.digest = hashlib.sha256()
        self.size = 0
        self.file: BinaryIO | None = None
        self.lock: BinaryIO | None = None
        self.published = False
        self._created_temporary = False
        self._created_marker_temporary = False
        self._created_marker = False
        self._created_final = False
        self.orphan_files_removed = 0
        self.orphan_bytes_removed = 0

    def __enter__(self):
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            lock_fd = os.open(self.directory / ".meshinfo-backup.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            self.lock = os.fdopen(lock_fd, "r+b")
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._claim_owner()
            self._reap_orphan_partials()
            self._check_space(CHUNK_BYTES)
            file_fd = os.open(self.temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            self.file = os.fdopen(file_fd, "wb")
            self._created_temporary = True
            return self
        except BaseException:
            self.__exit__(*sys.exc_info())
            raise

    def _claim_owner(self):
        """Durably establish one database identity before touching archives.

        All callers hold the destination flock. Publication of the owner marker
        is atomic; a crash before its link leaves only a small private temporary
        marker, never a partly written authoritative ownership record.
        """
        marker = self.directory / OWNER_MARKER
        try:
            fd = os.open(marker, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except FileNotFoundError:
            # A missing marker must not let another instance adopt previously
            # verified archives whose ownership metadata is still available.
            if any(metadata.get("owner") not in (None, self.owner)
                   for _, metadata in owned_snapshots(self.directory)):
                raise BackupError("backup directory contains snapshots belonging to another database instance")
            temporary = self.directory / f".meshinfo-backup-owner-{uuid.uuid4().hex}.json.part"
            created = False
            try:
                fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                created = True
                with os.fdopen(fd, "w", encoding="utf-8") as output:
                    json.dump({"schema": 1, "owner": self.owner}, output, sort_keys=True)
                    output.write("\n")
                    output.flush()
                    os.fsync(output.fileno())
                os.link(temporary, marker)
                _sync_directory(self.directory)
            finally:
                if created:
                    temporary.unlink(missing_ok=True)
            return
        except OSError as exc:
            raise BackupError("backup directory ownership marker cannot be opened safely") from exc
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > 4096:
            os.close(fd)
            raise BackupError("backup directory ownership marker is not a small regular file")
        with os.fdopen(fd, "rb") as existing:
            try:
                metadata = json.loads(existing.read(4097))
            except (ValueError, UnicodeError) as exc:
                raise BackupError("backup directory ownership marker is invalid") from exc
        if not isinstance(metadata, dict) or metadata.get("schema") != 1 or metadata.get("owner") != self.owner:
            raise BackupError("backup directory belongs to a different database instance; use a separate destination")

    def _reap_orphan_partials(self):
        """Remove interrupted files only after identity and exclusivity checks.

        Every active writer holds this same destination flock. Thus recognized
        partials left at this point belong to interrupted attempts. Symlinks,
        directories, legacy names, final archives and verification manifests are
        preserved. Unlinking a partial hard link cannot remove its final link.
        """
        for path in self.directory.iterdir():
            name = path.name
            recognized = bool(OWNER_TEMP_RE.fullmatch(name))
            for suffix in (".part", MANIFEST_SUFFIX + ".part"):
                if name.endswith(suffix) and OWNED_NAME.fullmatch(name[:-len(suffix)]):
                    recognized = True
                    break
            if not recognized:
                continue
            try:
                info = path.lstat()
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(info.st_mode):
                continue
            path.unlink()
            self.orphan_files_removed += 1
            # Hard-linked published archives do not reclaim blocks when their
            # temporary name is removed; do not advertise those bytes as freed.
            if info.st_nlink == 1:
                self.orphan_bytes_removed += info.st_size
        if self.orphan_files_removed:
            _sync_directory(self.directory)

    def _check_space(self, incoming: int):
        info = os.statvfs(self.directory)
        available = info.f_bavail * info.f_frsize
        if available - incoming < self.min_free_bytes:
            raise BackupError("backup destination would cross its free-space reserve")

    def write(self, chunk: bytes):
        if len(chunk) > CHUNK_BYTES:
            raise BackupError("backup chunk exceeds the bounded stream size")
        self._check_space(len(chunk))
        if self.file is None:
            raise BackupError("backup destination is not open")
        self.file.write(chunk)
        self.digest.update(chunk)
        self.size += len(chunk)

    def seal(self):
        if self.file is None or self.size == 0:
            raise BackupError("backup stream is empty")
        self.file.flush()
        os.fsync(self.file.fileno())
        self.file.close()
        self.file = None

    def publish(self, *, now: float | None = None, restore_metadata: dict | None = None) -> dict:
        """Publish after full archive decoding and checksum verification only."""
        if self.file is not None or self.size == 0:
            raise BackupError("backup must be sealed and verified before publication")
        metadata = {
            "schema": 1,
            "owner": self.owner,
            "filename": self.path.name,
            "sha256": self.digest.hexdigest(),
            "bytes": self.size,
            "completed_at": time.time() if now is None else now,
            "verification": "archive_decode",
            "orphan_files_removed": self.orphan_files_removed,
            "orphan_bytes_removed": self.orphan_bytes_removed,
        }
        if restore_metadata is not None:
            metadata["restore_metadata"] = validate_restore_metadata(restore_metadata)
            metadata["restore_metadata_sha256"] = restore_metadata_checksum(restore_metadata)
        encoded_size = len(json.dumps(metadata, sort_keys=True).encode("utf-8")) + 1
        if encoded_size > MANIFEST_BYTES:
            raise BackupError("backup manifest exceeds its bounded budget")
        self._check_space(encoded_size + 8192)
        fd = os.open(self.marker_temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        self._created_marker_temporary = True
        with os.fdopen(fd, "w", encoding="utf-8") as marker:
            json.dump(metadata, marker, sort_keys=True)
            marker.write("\n")
            marker.flush()
            os.fsync(marker.fileno())
        # Exclusive hard links give atomic visibility without ever replacing a
        # pre-existing snapshot. The archive becomes visible after its marker.
        os.link(self.marker_temporary, self.marker)
        self._created_marker = True
        os.link(self.temporary, self.path)
        self._created_final = True
        directory_fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        self.published = True
        return metadata

    def __exit__(self, *_exc):
        try:
            if self.file is not None:
                self.file.close()
                self.file = None
            if self._created_temporary:
                self.temporary.unlink(missing_ok=True)
            if self._created_marker_temporary:
                self.marker_temporary.unlink(missing_ok=True)
            if not self.published:
                if self._created_final:
                    self.path.unlink(missing_ok=True)
                if self._created_marker:
                    self.marker.unlink(missing_ok=True)
        finally:
            if self.lock is not None:
                self.lock.close()
                self.lock = None


def _read_exact(stream: BinaryIO, length: int) -> bytes:
    parts = bytearray()
    while len(parts) < length:
        part = stream.read(length - len(parts))
        if not part:
            raise BackupError("backup stream ended before its commit record")
        parts.extend(part)
    return bytes(parts)


def receive_stream(stream: BinaryIO, target: SnapshotTarget) -> dict | None:
    while True:
        length = struct.unpack("!I", _read_exact(stream, 4))[0]
        if length == 0:
            break
        if length > CHUNK_BYTES:
            raise BackupError("invalid backup frame size")
        target.write(_read_exact(stream, length))
    length = struct.unpack("!I", _read_exact(stream, 4))[0]
    if not 1 <= length <= MANIFEST_BYTES:
        raise BackupError("invalid backup commit record size")
    commit = json.loads(_read_exact(stream, length))
    if (
        not isinstance(commit, dict)
        or commit.get("sha256") != target.digest.hexdigest()
        or type(commit.get("bytes")) is not int
        or commit.get("bytes") != target.size
    ):
        raise BackupError("backup stream checksum or byte count does not match")
    if stream.read(1):
        raise BackupError("unexpected data after backup commit")
    metadata = validate_manifest_restore_metadata(commit)
    target.seal()
    return metadata


def verify_archive(path: Path, *, timeout: float):
    """Decode every archived data block; no database is created or modified."""
    proc = subprocess.Popen(
        ["pg_restore", "--no-owner", "--no-privileges", "--file=/dev/null", str(path)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    tail = bytearray()
    deadline = time.monotonic() + timeout
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(proc.stderr, selectors.EVENT_READ)
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BackupError("backup archive verification timed out")
                for key, _events in selector.select(min(remaining, 1.0)):
                    chunk = os.read(key.fileobj.fileno(), STDERR_BYTES)
                    if not chunk:
                        selector.unregister(key.fileobj)
                    else:
                        tail.extend(chunk)
                        del tail[:-STDERR_BYTES]
            code = proc.wait(timeout=max(0.01, deadline - time.monotonic()))
            if code != 0:
                raise BackupError(f"pg_restore archive verification failed (exit {code}): {tail.decode(errors='replace')}")
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.wait()
        if proc.stderr:
            proc.stderr.close()


def verify_checksum(target: SnapshotTarget):
    digest = hashlib.sha256()
    with target.temporary.open("rb") as archive:
        while chunk := archive.read(CHUNK_BYTES):
            digest.update(chunk)
    if digest.hexdigest() != target.digest.hexdigest():
        raise BackupError("backup file checksum does not match the received stream")


def receiver_main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    try:
        directory, filename, reserve_text, days_text, timeout_text, owner = args
        _validate_owner(owner)
        reserve, keep_days, timeout = int(reserve_text), float(days_text), float(timeout_text)
        if reserve < 0 or not math.isfinite(keep_days) or keep_days <= 0 or not 1 <= timeout <= 604800:
            raise ValueError("invalid backup receiver limits")
        deadline = time.monotonic() + timeout

        def timed_out(_signal, _frame):
            raise BackupError("backup receiver timed out")

        signal.signal(signal.SIGALRM, timed_out)
        signal.signal(signal.SIGTERM, timed_out)
        signal.signal(signal.SIGHUP, timed_out)
        signal.setitimer(signal.ITIMER_REAL, timeout)
        try:
            with SnapshotTarget(Path(directory), filename, reserve, owner=owner) as target:
                restore_metadata = receive_stream(sys.stdin.buffer, target)
                verify_archive(target.temporary, timeout=max(0.01, deadline - time.monotonic()))
                verify_checksum(target)
                result = target.publish(restore_metadata=restore_metadata)
                try:
                    result["pruned"] = prune_snapshots(target.directory, keep_days, owner=owner)
                except OSError as exc:
                    # The verified snapshot survives a retention failure; report
                    # that failure instead of claiming the whole job succeeded.
                    result["prune_error"] = str(exc)
                    result["pruned"] = []
                print(json.dumps(result, sort_keys=True), flush=True)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
        return 0
    except Exception as exc:
        print(f"Backup receiver failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(receiver_main())
