#!/usr/bin/env python3
"""Snapshot, restore and verify the bounded, isolated archive workload.

Only a completed benchmark_archive_workload report and its dedicated
*_columnar_test database are accepted. This program creates one unique empty
test database; it never changes source tables. The explicit filesystem path
must be the PostgreSQL data filesystem, mounted into this runner if necessary.
PostgreSQL client binaries must be on PATH. Credentials and rows are never
written to the report. A successful target can be retained for read benchmarks.

Logical restore preserves Columnar options and observed stripe/chunk upper
bounds inside one atomic transaction. Record actual geometry on both sides;
preserving upper bounds does not imply identical physical layout or query speed.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import time
from urllib.parse import unquote, urlsplit, urlunsplit
import uuid

import asyncpg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import restore_database
from scripts.benchmark_columnar_history import (
    MQTT_COLUMNS, RECEPTION_COLUMNS, TOPIC_COLUMNS, metadata_size, normalize,
    qi, rows_digest, schema_size,
)
from storage.backup import create_snapshot
from storage.columnar_backup import OPTIONS_SQL
from storage.db.retention_executor import maintenance_lock

MIB = 1024**2
TARGET_PATTERN = r"meshinfo_workload_restore_[a-f0-9]{10}_columnar_test"


class WorkloadRestoreRefused(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise WorkloadRestoreRefused(message)


def load_report(path):
    require(path.stat().st_size <= 2*MIB, "Workload report exceeds its bounded budget")
    report = json.loads(path.read_text())
    require(report.get("all_checks_passed") is True and report.get("dedicated_target") is True,
            "A completed, dedicated workload report is required")
    schemas = report.get("packet_schemas", {})
    require(schemas.get("columnar") == "public" and
            re.fullmatch(r"benchmark_heap_full_[a-f0-9]{10}", schemas.get("heap", "")) and
            re.fullmatch(r"benchmark_shared_[a-f0-9]{10}", report.get("shared_schema", "")),
            "Report does not identify the isolated workload schemas")
    fixture = report.get("fixture", {})
    require(type(fixture.get("canonical_rows")) is int and 100000 <= fixture["canonical_rows"] <= 250000,
            "Unexpected canonical workload size")
    require(type(fixture.get("reception_rows")) is int and 0 < fixture["reception_rows"] <= 2000000,
            "Unexpected reception workload size")
    digests = [fixture.get("expanded_packet_digest"), fixture.get("expanded_reception_digest"),
               fixture.get("original_digests", {}).get("topics")]
    if "archived_topic_lookup_digest" in fixture:
        digests.append(fixture["archived_topic_lookup_digest"])
    for digest in digests:
        require(isinstance(digest, dict) and type(digest.get("rows")) is int and digest["rows"] > 0
                and all(re.fullmatch(r"[a-f0-9]{64}", digest.get(key, ""))
                        for key in ("sha256_sum", "sha256_xor")), "Invalid expected workload digest")
    require(fixture["expanded_packet_digest"]["rows"] == fixture["canonical_rows"] and
            fixture["expanded_reception_digest"]["rows"] == fixture["reception_rows"],
            "Reported row counts disagree with expected digests")
    return report


def source_configuration(dsn, report):
    parsed = urlsplit(dsn)
    database = unquote(parsed.path.lstrip("/"))
    require(parsed.scheme in ("postgres", "postgresql") and parsed.hostname and
            not parsed.query and not parsed.fragment, "An explicit PostgreSQL DSN without query options is required")
    require(re.fullmatch(r"[A-Za-z0-9_]+_columnar_test", database) and
            report["target_database"] == database, "DSN does not identify the report's isolated source")
    pg = {"host": parsed.hostname, "port": parsed.port or 5432, "database": database,
          "username": unquote(parsed.username or "postgres"), "password": unquote(parsed.password or "")}
    return parsed, pg


def option_settings(options):
    """Physical maxima may improve on restore; effective settings must match."""
    return [{key:value for key,value in relation.items() if key != "geometry"} for relation in options]


def verify_geometry(source,restored):
    """Independently enforce exact rows and no larger per-relation row groups."""
    originals = {(row["schema"],row["name"]):row for row in source}
    replacements = {(row["schema"],row["name"]):row for row in restored}
    require(len(originals) == len(source) and len(replacements) == len(restored)
            and originals.keys() == replacements.keys(),"Restored geometry relation inventory differs")
    for identity,old in originals.items():
        before,after = old.get("geometry"),replacements[identity].get("geometry")
        require(isinstance(before,dict) and isinstance(after,dict),"Physical geometry metadata is required")
        require(after.get("row_count") == before.get("row_count"),"Restored physical relation row count differs")
        for name in ("max_stripe_rows","max_chunk_group_rows"):
            require(type(before.get(name)) is int and type(after.get(name)) is int and
                    0 <= after[name] <= before[name],"Restored physical geometry exceeds source upper bounds")


async def verify_data(conn, workload):
    fixture = workload["fixture"]
    expected = {
        "heap_packets": fixture["expanded_packet_digest"],
        "columnar_packets": fixture["expanded_packet_digest"],
        "receptions": fixture["expanded_reception_digest"],
        "topics": fixture["original_digests"]["topics"],
    }
    actual = {}
    cases = [
        ("heap_packets", workload["packet_schemas"]["heap"], "mqtt_messages", MQTT_COLUMNS),
        ("columnar_packets", "public", "mqtt_messages", MQTT_COLUMNS),
        ("receptions", workload["shared_schema"], "packet_receptions", RECEPTION_COLUMNS),
        ("topics", workload["shared_schema"], "topics", TOPIC_COLUMNS),
    ]
    helper_exists = await conn.fetchval("SELECT to_regclass('public.mqtt_archive_topics') IS NOT NULL")
    if helper_exists and await conn.fetchval("SELECT EXISTS(SELECT 1 FROM public.mqtt_archive_topics)"):
        require("archived_topic_lookup_digest" in fixture,"Workload report lacks expected canonical-topic lookup digest")
    if "archived_topic_lookup_digest" in fixture:
        require(helper_exists,"Verified canonical-topic lookup is missing")
        expected["canonical_topic_lookup"] = fixture["archived_topic_lookup_digest"]
        cases.append(("canonical_topic_lookup","public","mqtt_archive_topics",("id","created_at","topic")))
    for label, schema, table, columns in cases:
        actual[label] = await rows_digest(conn, schema, table, columns)
        require(actual[label] == expected[label], "Workload digest mismatch: " + label)
    return actual


async def physical_state(conn, workload):
    options = json.loads(await conn.fetchval(OPTIONS_SQL))
    require(len(options) == sum(row["amname"] == "columnar" for row in workload["partition_methods"]),
            "Unexpected Columnar relation inventory")
    stripes = dict(await conn.fetchrow("""SELECT count(*)::bigint AS stripes,
        coalesce(sum(row_count),0)::bigint AS rows,
        coalesce(sum(chunk_group_count),0)::bigint AS chunk_groups,
        coalesce(min(row_count),0)::bigint AS smallest_stripe_rows,
        coalesce(max(row_count),0)::bigint AS largest_stripe_rows
        FROM columnar.stripe"""))
    methods = [dict(row) for row in await conn.fetch("""SELECT c.relname,a.amname,
        pg_get_expr(c.relpartbound,c.oid) AS bound FROM pg_inherits i
        JOIN pg_class c ON c.oid=i.inhrelid JOIN pg_am a ON a.oid=c.relam
        WHERE i.inhparent='public.mqtt_messages'::regclass ORDER BY c.relname""")]
    require(methods == workload["partition_methods"], "Workload partition methods/bounds changed")
    schemas = {label: await schema_size(conn, schema) for label, schema in workload["packet_schemas"].items()}
    schemas["shared"] = await schema_size(conn, workload["shared_schema"])
    return {"options": options, "physical_stripes": stripes, "partition_methods": methods,
            "schema_storage": schemas, "columnar_metadata": await metadata_size(conn),
            "database_bytes": await conn.fetchval("SELECT pg_database_size(current_database())")}


async def run(args):
    require(re.fullmatch(r"[A-Z][A-Z0-9_]*", args.dsn_env), "Invalid DSN environment-variable name")
    require(60 <= args.timeout_seconds <= 600, "Timeout must be 60..600 seconds per backup/restore step")
    require(args.database_filesystem.is_dir() and args.scratch_dir.is_dir(), "Existing database and scratch filesystems are required")
    require(1024 <= args.reserve_mib <= 4096 and 512 <= args.max_growth_mib <= 1024,
            "Restore requires at least 1 GiB reserve and at most 1 GiB growth")
    require(all(shutil.which(tool) for tool in ("pg_dump", "pg_restore", "psql")),
            "Native PostgreSQL clients are required")
    workload = load_report(args.workload_report)
    dsn = os.environ.get(args.dsn_env, "")
    parsed, pg = source_configuration(dsn, workload)
    free_bytes = lambda: shutil.disk_usage(args.database_filesystem).free
    initial_free = free_bytes()
    require(initial_free >= max(3072, args.reserve_mib + args.max_growth_mib)*MIB,
            "At least 3 GiB of database-filesystem headroom is required")
    require(shutil.disk_usage(args.scratch_dir).free >= 64*MIB, "Backup scratch needs at least 64 MiB free")
    target = "meshinfo_workload_restore_" + uuid.uuid4().hex[:10] + "_columnar_test"
    require(re.fullmatch(TARGET_PATTERN, target), "Invalid generated restore target")
    report = {"all_checks_passed": False, "source_database": pg["database"],
              "target_database": target, "target_retained": False,
              "packet_schemas": workload["packet_schemas"], "shared_schema": workload["shared_schema"],
              "hot_month": workload["hot_month"], "free_bytes_before": initial_free,
              "limits": {"reserve_bytes": args.reserve_mib*MIB, "maximum_growth_bytes": args.max_growth_mib*MIB,
                         "filesystem_probe": "Operator-supplied path must identify the PostgreSQL data filesystem",
                         "geometry": "Exact rows/settings and observed stripe/chunk upper bounds must survive; stripe sequence/distribution and query speed are measured separately"}}
    settings = {"application_name": "meshinfo-workload-restore-proof", "timezone": "UTC", "work_mem": "4MB",
                "maintenance_work_mem": "32MB", "max_parallel_workers_per_gather": "0",
                "max_parallel_maintenance_workers": "0", "lock_timeout": "500ms", "jit": "off"}
    conn = restored = None
    created = False
    phase = "source_preflight"
    monitor = None
    minimum_free = initial_free

    async def watch_space():
        nonlocal minimum_free
        while True:
            current = free_bytes()
            minimum_free = min(minimum_free, current)
            require(current >= args.reserve_mib*MIB, "Database free space fell below the reserved floor")
            require(initial_free-current <= args.max_growth_mib*MIB, "Database/WAL growth exceeded the bounded restore budget")
            await asyncio.sleep(0.25)

    async def bounded(operation):
        task = asyncio.create_task(operation)
        try:
            done, _ = await asyncio.wait((task, monitor), return_when=asyncio.FIRST_COMPLETED)
            if monitor in done:
                await monitor
            return await task
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    try:
        conn = await asyncpg.connect(dsn, timeout=5, command_timeout=args.timeout_seconds, server_settings=settings)
        require(await conn.fetchval("SELECT current_database()") == pg["database"], "Connected source identity differs")
        require(await conn.fetchval("SHOW default_tablespace") in ("", "pg_default") and
                await conn.fetchval("SHOW temp_tablespaces") in ("", "pg_default"), "Custom session tablespaces are unsupported")
        require(await conn.fetchval("""SELECT t.spcname FROM pg_database d JOIN pg_tablespace t ON t.oid=d.dattablespace
            WHERE d.datname=current_database()""") == "pg_default", "Custom source database tablespace is unsupported")
        require(not await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM pg_class c
                JOIN pg_tablespace t ON t.oid=c.reltablespace WHERE t.spcname NOT IN ('pg_default','pg_global'))
            OR EXISTS(SELECT 1 FROM pg_replication_slots)
            OR EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database()
                AND pid<>pg_backend_pid() AND backend_type='client backend')"""),
            "Source has custom tablespaces, replication retention or another client")
        require(await conn.fetchval("SHOW archive_mode") == "off", "WAL archiving needs a separate capacity budget")
        require(not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM public.storage_archive_jobs WHERE phase IN ('copying','verifying','ready'))"),
                "Workload must have no unfinished archive job")
        monitor = asyncio.create_task(watch_space())
        phase = "verify_source"
        report["source_digests"] = await bounded(verify_data(conn, workload))
        report["source"] = await bounded(physical_state(conn, workload))
        require(report["source"]["database_bytes"] <= 512*MIB,
                "Source database exceeds the 512 MiB workload restore budget")
        with tempfile.TemporaryDirectory(prefix="meshinfo-workload-restore-", dir=args.scratch_dir) as temporary:
            private = Path(temporary)
            private.chmod(0o700)
            config_path = private/"restore.toml"
            config_path.write_text("[storage.postgres]\n"+"".join(f"{key} = {json.dumps(value)}\n" for key,value in pg.items()))
            config_path.chmod(0o600)
            config = {"storage": {"postgres": pg}, "backups": {"mode": "local", "dir": str(private/"snapshots"),
                      "keep_days": 1, "min_free_mb": 32, "timeout_seconds": args.timeout_seconds}}
            phase = "create_actual_snapshot"
            started = time.monotonic()
            async with maintenance_lock(conn) as acquired:
                require(acquired, "Another maintenance operation holds the source lock")
                owner = hashlib.sha256((pg["database"]+target).encode()).hexdigest()
                snapshot = await bounded(create_snapshot(config, ROOT, owner_id=owner))
            require(snapshot.successful, "Snapshot was not fully successful")
            report["snapshot"] = {"bytes": snapshot.bytes, "sha256": snapshot.sha256,
                                  "verification": snapshot.verification, "seconds": time.monotonic()-started}
            phase = "create_restore_target"
            await conn.execute(f"CREATE DATABASE {qi(target)} TEMPLATE template0")
            created = True
            phase = "actual_restore"
            restore_args = restore_database.parser().parse_args([snapshot.destination, "--config", str(config_path),
                "--database", target, "--timeout-seconds", str(args.timeout_seconds), "--apply"])
            started = time.monotonic()
            report["restore"] = await bounded(restore_database.run(restore_args))
            report["restore"]["seconds"] = time.monotonic()-started
            require(report["restore"]["status"] == "restored", "Restore did not complete")
            require(report["restore"].get("physical_geometry_preserved") is True,
                    "The restore did not enforce physical stripe/chunk upper bounds")
        target_dsn = urlunsplit((parsed.scheme, parsed.netloc, "/"+target, "", ""))
        restored = await asyncpg.connect(target_dsn, timeout=5, command_timeout=args.timeout_seconds, server_settings=settings)
        require(await restored.fetchval("SELECT current_database()") == target, "Connected target identity differs")
        phase = "analyze_restored_tables"
        started = time.monotonic()
        # pg_dump does not preserve planner statistics. Analyze only the new
        # target before any post-restore query performance comparison.
        await bounded(restored.execute("ANALYZE"))
        report["analyze_seconds"] = time.monotonic()-started
        phase = "verify_restored_rows_and_layout"
        report["restored_digests"] = await bounded(verify_data(restored, workload))
        report["restored"] = await bounded(physical_state(restored, workload))
        require(option_settings(report["restored"]["options"]) == option_settings(report["source"]["options"]),
                "Restored Columnar options differ")
        verify_geometry(report["source"]["options"],report["restored"]["options"])
        require(report["restored"]["physical_stripes"]["rows"] == report["source"]["physical_stripes"]["rows"],
                "Columnar physical row inventory differs")
        require(report["restored"]["physical_stripes"]["largest_stripe_rows"] <=
                report["source"]["physical_stripes"]["largest_stripe_rows"],
                "Restored global stripe maximum exceeds the source bound")
        report["source_options_unchanged"] = json.loads(await conn.fetchval(OPTIONS_SQL)) == report["source"]["options"]
        require(report["source_options_unchanged"], "Source Columnar options changed during proof")
        report["all_checks_passed"] = True
        report["target_retained"] = bool(args.keep_target)
    except (Exception, asyncio.CancelledError) as exc:
        report["error"] = {"phase": phase, "category": type(exc).__name__, "sqlstate": getattr(exc, "sqlstate", None)}
        if isinstance(exc, WorkloadRestoreRefused):
            report["error"]["reason"] = str(exc)
    finally:
        if monitor is not None:
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
        if restored is not None:
            await restored.close()
        if created and not report["target_retained"]:
            try:
                require(re.fullmatch(TARGET_PATTERN, target) and target != pg["database"], "Cleanup target identity differs")
                await conn.execute(f"DROP DATABASE {qi(target)} WITH (FORCE)")
                report["target_removed"] = True
            except Exception as exc:
                report["all_checks_passed"] = False
                report["cleanup_error"] = {"category": type(exc).__name__, "target_database": target}
        if conn is not None:
            await conn.close()
        report["minimum_free_bytes"] = minimum_free
        report["free_bytes_after"] = free_bytes()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn-env", required=True)
    parser.add_argument("--workload-report", type=Path, required=True)
    parser.add_argument("--database-filesystem", type=Path, required=True)
    parser.add_argument("--scratch-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--keep-target", action="store_true")
    parser.add_argument("--reserve-mib", type=int, default=1024)
    parser.add_argument("--max-growth-mib", type=int, default=1024)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    args = parser.parse_args()
    try:
        report = asyncio.run(run(args))
    except Exception as exc:
        print("Workload restore refused: " + (str(exc) if isinstance(exc, WorkloadRestoreRefused) else type(exc).__name__), file=sys.stderr)
        return 1
    args.output.write_text(json.dumps(report, indent=2, default=normalize)+"\n")
    print(json.dumps({"output": str(args.output), "all_checks_passed": report["all_checks_passed"],
                      "target_database": report["target_database"], "target_retained": report["target_retained"]}))
    return 0 if report["all_checks_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
