#!/usr/bin/env python3
"""Verify a MeshInfo custom dump and restore it to an explicitly named empty DB.

Run without --apply to verify the archive, manifest, and destination. PostgreSQL
client binaries must be available (they are included in the MeshInfo backend
image). Restore uses one transaction: pre-data, exact Columnar options, data,
post-data. Failure rolls back the entire operation. Keep the verified manifest
beside every Columnar archive; ordinary legacy heap-only dumps remain usable.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from storage.backup import _bounded_output, _close_stdin, _filesystem, _stop, _tail
from storage.backup_remote import (
    BackupError, CHUNK_BYTES, MANIFEST_BYTES, MANIFEST_SUFFIX,
    validate_manifest_restore_metadata,
)
from storage.columnar_backup import (
    connection_arguments, geometry_assertion_sql, inventory_assertion_sql, options_assertion_sql,
    options_sql, quote_literal,
)

SCHEMA_BYTES = 64 * 1024 * 1024
COLUMNAR_SQL = re.compile(rb"(?:default_table_access_method\s*=\s*'?\"?columnar\b|\bUSING\s+\"?columnar\b)", re.I)
EMPTY_QUERY = """SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname !~ '^pg_toast'
      AND n.nspname !~ '^pg_temp_' AND c.relkind IN ('r','p','v','m','S','f')
      AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid='pg_class'::regclass
                      AND d.objid=c.oid AND d.deptype='e')"""
TARGET_SQL = """SELECT json_build_object(
    'postgres_major',current_setting('server_version_num')::int/10000,
    'objects',(""" + EMPTY_QUERY + """),
    'preload',current_setting('shared_preload_libraries'),
    'installed',(SELECT extversion FROM pg_extension WHERE extname='citus_columnar'),
    'available',coalesce((SELECT json_agg(version) FROM pg_available_extension_versions
                         WHERE name='citus_columnar'),'[]'::json))"""


class RestoreRefused(BackupError):
    """A failed precondition; no archive data should be restored."""


def read_manifest(path, archive_name):
    if path is None:
        return None, None
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > MANIFEST_BYTES:
            raise RestoreRefused("backup manifest is not a bounded regular file")
        raw = stream.read(MANIFEST_BYTES + 1)
    if len(raw) > MANIFEST_BYTES:
        raise RestoreRefused("backup manifest exceeds its size limit")
    value = json.loads(raw)
    if (not isinstance(value, dict) or type(value.get("schema")) is not int or value.get("schema") != 1
            or value.get("filename") != archive_name
            or value.get("verification") != "archive_decode"
            or type(value.get("bytes")) is not int or value["bytes"] <= 0
            or not isinstance(value.get("sha256"), str)
            or not re.fullmatch(r"[a-f0-9]{64}", value["sha256"])):
        raise RestoreRefused("backup manifest does not identify a verified archive")
    return value, validate_manifest_restore_metadata(value)


async def archive_digest(fd):
    digest = hashlib.sha256()
    size = 0
    await _filesystem(os.lseek, fd, 0, os.SEEK_SET)
    while chunk := await _filesystem(os.read, fd, CHUNK_BYTES):
        digest.update(chunk)
        size += len(chunk)
    await _filesystem(os.lseek, fd, 0, os.SEEK_SET)
    return size, digest.hexdigest()


async def query_target(pg, environment):
    proc = await asyncio.create_subprocess_exec(
        "psql", "-X", "-qAt", "--set=ON_ERROR_STOP=1", *connection_arguments(pg),
        env={**environment, "PGOPTIONS": "-c statement_timeout=5000 -c lock_timeout=1000"},
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    output = asyncio.create_task(_bounded_output(proc.stdout, 65536))
    errors = asyncio.create_task(_tail(proc.stderr))
    try:
        async with asyncio.timeout(10):
            proc.stdin.write((TARGET_SQL + ";\n").encode())
            await proc.stdin.drain()
            await _close_stdin(proc)
            code = await proc.wait()
            if code != 0:
                raise RestoreRefused("destination preflight query failed")
            result = json.loads(await output)
            if not isinstance(result, dict):
                raise RestoreRefused("invalid destination preflight response")
            return result
    finally:
        await _stop(proc, drain_stdout=False)
        await asyncio.gather(output, errors, return_exceptions=True)


def validate_target(target, metadata, has_columnar):
    if target.get("objects") != 0:
        raise RestoreRefused("restore requires an empty destination database")
    if metadata is not None and target.get("postgres_major") != metadata["postgres_major"]:
        raise RestoreRefused("source and destination PostgreSQL major versions differ")
    if has_columnar and (metadata is None or not metadata["relations"]):
        raise RestoreRefused("Columnar archive requires its matching restore metadata manifest")
    if metadata is None or metadata["extension"] is None:
        return
    version = metadata["extension"]
    if version not in target.get("available", []):
        raise RestoreRefused("the exact backed-up citus_columnar extension version is unavailable")
    if target.get("installed") not in (None, version):
        raise RestoreRefused("destination citus_columnar version differs from the backup")
    preloaded = [entry.strip().strip('"').split('/')[-1] for entry in target.get("preload", "").split(',')]
    if not any(entry in ("citus_columnar", "citus") for entry in preloaded):
        raise RestoreRefused("destination must preload citus_columnar before restore")


async def archive_process(fd, *arguments):
    # An already-open, no-follow descriptor prevents path replacement between
    # checksum verification and restore. pg_restore sees a seekable archive.
    os.lseek(fd, 0, os.SEEK_SET)
    return await asyncio.create_subprocess_exec(
        "pg_restore", "--no-owner", "--no-privileges", *arguments,
        f"/proc/self/fd/{fd}", pass_fds=(fd,), stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)


async def inspect_archive(fd):
    # Decode all data blocks even for a validation-only invocation.
    proc = await archive_process(fd, "--file=/dev/null")
    errors = asyncio.create_task(_tail(proc.stderr))
    try:
        await _tail(proc.stdout)
        if await proc.wait() != 0:
            raise RestoreRefused("custom archive decoding failed")
    finally:
        await _stop(proc)
        await asyncio.gather(errors, return_exceptions=True)
    proc = await archive_process(fd, "--section=pre-data", "--file=-")
    errors = asyncio.create_task(_tail(proc.stderr))
    seen, tail, has_columnar = 0, b"", False
    try:
        while chunk := await proc.stdout.read(CHUNK_BYTES):
            seen += len(chunk)
            if seen > SCHEMA_BYTES:
                raise RestoreRefused("archive schema exceeds its bounded inspection budget")
            combined = tail + chunk
            has_columnar = has_columnar or bool(COLUMNAR_SQL.search(combined))
            tail = combined[-4096:]
        if await proc.wait() != 0:
            raise RestoreRefused("archive schema inspection failed")
        return has_columnar
    finally:
        await _stop(proc, drain_stdout=True)
        await asyncio.gather(errors, return_exceptions=True)


async def send_sql(proc, sql):
    proc.stdin.write(sql.encode("utf-8"))
    await proc.stdin.drain()


async def stream_section(fd, section, target, metadata=None):
    proc = await archive_process(fd, "--section=" + section, "--file=-")
    errors = asyncio.create_task(_tail(proc.stderr))
    async def write(chunk):
        target.stdin.write(chunk)
        await target.stdin.drain()
    splitter = None
    if section == "data" and metadata is not None and metadata["schema"] == 2 and metadata["relations"]:
        from storage.columnar_copy import CopyBatchStream
        splitter = CopyBatchStream(metadata, write)
    try:
        while chunk := await proc.stdout.read(CHUNK_BYTES):
            if splitter is None:
                await write(chunk)
            else:
                await splitter.feed(chunk)
        if splitter is not None:
            await splitter.finish()
        if await proc.wait() != 0:
            raise RestoreRefused("archive section decoding failed; restore rolled back")
    finally:
        await _stop(proc, drain_stdout=True)
        await asyncio.gather(errors, return_exceptions=True)


def file_identity(fd):
    info = os.fstat(fd)
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def reset_archive_analysis_sql():
    """Invalidate copied statistics checkpoints without relying on changed OIDs.

    Logical dumps preserve application flags, not the planner statistics those
    flags describe. Fresh clusters may coincidentally reuse every recorded OID.
    Older journals have only ``analyzed``; unrelated legacy heap dumps may have
    no journal at all. Reset only existing boolean flags on active MeshInfo jobs.
    """
    return """DO $meshinfo_archive_statistics$
    DECLARE journal regclass := to_regclass('public.storage_archive_jobs');
            assignments text;
    BEGIN
      IF journal IS NULL OR NOT EXISTS (
          SELECT 1 FROM pg_class WHERE oid=journal AND relkind='r') THEN RETURN; END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid=journal
          AND attname='job_id' AND atttypid='uuid'::regtype AND attnum>0 AND NOT attisdropped)
         OR NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid=journal
          AND attname='phase' AND atttypid IN ('text'::regtype,'varchar'::regtype)
          AND attnum>0 AND NOT attisdropped) THEN RETURN; END IF;
      SELECT string_agg(format('%I = false',attname),', ' ORDER BY attname)
        INTO assignments FROM pg_attribute WHERE attrelid=journal
          AND attname IN ('analyzed','stage_analyzed','topic_analyzed')
          AND atttypid='bool'::regtype AND attnum>0 AND NOT attisdropped
          AND attgenerated='' AND attidentity='';
      IF assignments IS NOT NULL THEN
        EXECUTE 'UPDATE public.storage_archive_jobs SET ' || assignments ||
          ' WHERE phase IN (''copying'',''verifying'',''ready'')';
      END IF;
    END $meshinfo_archive_statistics$;\n"""


async def apply_archive(fd, pg, environment, metadata, identity):
    # A metadata-less legacy heap dump still gets an empty Columnar inventory
    # assertion before COPY. Detection never relies solely on SQL text matching.
    effective = metadata or {"schema": 1, "postgres_major": 18, "extension": None, "relations": []}
    proc = await asyncio.create_subprocess_exec(
        "psql", "-X", "-q", "--set=ON_ERROR_STOP=1", "--single-transaction", "--file=-",
        *connection_arguments(pg), env=environment, stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
    errors = asyncio.create_task(_tail(proc.stderr))
    committed = False
    try:
        # Recheck under the restore transaction immediately before schema DDL.
        await send_sql(proc, "SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout=0;\n"
                       "DO $meshinfo_empty$ BEGIN IF (" + EMPTY_QUERY + ") <> 0 "
                       "THEN RAISE EXCEPTION 'restore destination is no longer empty'; END IF; END $meshinfo_empty$;\n")
        if effective["extension"] is not None:
            await send_sql(proc, "CREATE EXTENSION IF NOT EXISTS citus_columnar VERSION "
                           + quote_literal(effective["extension"]) + ";\n")
        await stream_section(fd, "pre-data", proc)
        # Generated identifiers are UTF-8. Legacy pg_restore sections can set a
        # different source encoding; each following section reinstates its own.
        await send_sql(proc, "SET client_encoding='UTF8';\n")
        await send_sql(proc, inventory_assertion_sql(effective))
        await send_sql(proc, options_sql(effective))
        await send_sql(proc, options_assertion_sql(effective))
        await stream_section(fd, "data", proc, metadata=effective)
        await stream_section(fd, "post-data", proc)
        await send_sql(proc, inventory_assertion_sql(effective))
        await send_sql(proc, options_assertion_sql(effective))
        await send_sql(proc, geometry_assertion_sql(effective))
        await send_sql(proc, reset_archive_analysis_sql())
        if file_identity(fd) != identity:
            raise RestoreRefused("backup archive changed during restore; transaction was not committed")
        await _close_stdin(proc)
        if await proc.wait() != 0:
            raise RestoreRefused("restore failed and its transaction rolled back")
        committed = True
    except (BrokenPipeError, ConnectionResetError) as exc:
        raise RestoreRefused("restore rejected schema, settings or data; transaction rolled back") from exc
    finally:
        if not committed and proc.returncode is None:
            # Never close a successful stdin on producer failure: psql -1
            # commits on clean EOF. Terminating its connection rolls back.
            await _stop(proc)
        else:
            await _stop(proc)
        await asyncio.gather(errors, return_exceptions=True)


async def run(args):
    if not Path("/proc/self/fd").is_dir():
        raise RestoreRefused("restore requires Linux; run it inside the MeshInfo backend container")
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,62}", args.database):
        raise RestoreRefused("destination database must be an explicit simple database name")
    if not 1 <= args.timeout_seconds <= 604800:
        raise RestoreRefused("restore timeout must be between 1 and 604800 seconds")
    with args.config.open("rb") as source:
        config = tomllib.load(source)
    pg = dict(config.get("storage", {}).get("postgres", {}))
    pg["database"] = args.database
    if args.host:
        pg["host"] = args.host
    environment = {**os.environ, "PGPASSWORD": str(pg.get("password", "")),
                   "PGCONNECT_TIMEOUT": "15", "PGAPPNAME": "meshinfo-restore"}
    manifest_path = args.manifest
    if manifest_path is None:
        candidate = args.archive.with_name(args.archive.name + MANIFEST_SUFFIX)
        if candidate.exists() or candidate.is_symlink():
            manifest_path = candidate
    manifest, metadata = read_manifest(manifest_path, args.archive.name)
    fd = os.open(args.archive, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise RestoreRefused("backup archive must be a regular file")
        identity = file_identity(fd)
        async with asyncio.timeout(args.timeout_seconds):
            size, checksum = await archive_digest(fd)
            if manifest is not None and (manifest["bytes"] != size or manifest["sha256"] != checksum):
                raise RestoreRefused("backup archive checksum or byte count does not match its manifest")
            has_columnar = await inspect_archive(fd)
            if has_columnar and metadata is None:
                raise RestoreRefused("Columnar archive is missing its verified restore metadata manifest")
            target = await query_target(pg, environment)
            validate_target(target, metadata, has_columnar)
            if file_identity(fd) != identity:
                raise RestoreRefused("backup archive changed during validation")
            if args.apply:
                await apply_archive(fd, pg, environment, metadata, identity)
            return {"status": "restored" if args.apply else "verified", "bytes": size,
                    "sha256": checksum, "manifest_verified": manifest is not None,
                    "columnar_relations": len(metadata["relations"]) if metadata else 0,
                    "physical_geometry_preserved": bool(metadata is not None and metadata["schema"] == 2),
                    "geometry_note": ("Snapshot physical stripe/chunk upper bounds preserved"
                        if metadata is not None and metadata["schema"] == 2 else
                        "Legacy metadata: data/options restored; physical geometry and query latency may change"),
                    "single_transaction": True, "data_loaded": bool(args.apply)}
    finally:
        os.close(fd)


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("archive", type=Path)
    result.add_argument("--manifest", type=Path, help="Verified manifest (defaults to archive.manifest.json)")
    result.add_argument("--config", type=Path, default=Path("config.toml"))
    result.add_argument("--host", help="Destination hostname override")
    result.add_argument("--database", required=True, help="Explicit existing EMPTY destination database")
    result.add_argument("--timeout-seconds", type=float, default=21600)
    result.add_argument("--apply", action="store_true", help="Restore after validation; default only verifies")
    return result


def main():
    try:
        result = asyncio.run(run(parser().parse_args()))
    except (OSError, ValueError, BackupError, TimeoutError) as exc:
        # PostgreSQL errors can contain payloads, SQL context or credentials.
        message = str(exc) if isinstance(exc, RestoreRefused) else type(exc).__name__
        print("Database restore failed: " + message, file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
