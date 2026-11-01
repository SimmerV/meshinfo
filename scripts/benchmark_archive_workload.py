#!/usr/bin/env python3
"""Build a bounded large fixture and measure the actual chunked archiver.

Only a NEW, dedicated *_columnar_test database is accepted. This program never
opens source storage. A previously exported private fixture is deterministically
replicated in memory/streams; it is not copied into enlarged fixture files.
The heap baseline and converted public history are retained for the separate
read-only benchmark_history_read_plans.py runner.
"""

import argparse
import asyncio
from collections import Counter
from datetime import timedelta
import json
import math
import os
from pathlib import Path
import re
import shutil
import statistics
import sys
import time
from unittest.mock import patch
from urllib.parse import unquote, urlsplit
import uuid

import asyncpg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.benchmark_columnar_history import (
    MIB, MQTT_COLUMNS, RECEPTION_COLUMNS, TOPIC_COLUMNS, MultisetDigest,
    create_packets, fixture_rows, index_specifications, load_fixture, logical_row,
    metadata_size, month, next_month, normalize, qi, qualified, rows_digest, schema_size,
)
from storage.db import archive
from storage.db.archive_catalog import prepare_archive_indexes
from storage.retention import complete_month_cutoff


def shifted_time(stamp, replica):
    """Keep every canonical replica inside its original monthly partition."""
    offset = timedelta(microseconds=replica)
    shifted = stamp + offset
    return stamp - offset if shifted >= next_month(month(stamp)) else shifted


def expanded_packets(packets, replicas, stride, time_offsets=None):
    time_offsets = time_offsets or {}
    expanded = []
    for replica in range(replicas):
        for source in packets:
            row = list(source)
            row[0] += replica * stride
            if not -(1 << 63) <= row[0] < (1 << 63):
                raise ValueError("Replicated packet IDs exceed PostgreSQL bigint")
            row[6] = shifted_time(row[6] + time_offsets.get(source[0], timedelta()), replica)
            expanded.append(tuple(row))
    expanded.sort(key=lambda row: (row[6], row[0]))
    return expanded


def expanded_receptions(directory, canonical_times, replicas, stride, time_offsets=None):
    time_offsets = time_offsets or {}
    for replica in range(replicas):
        for source in fixture_rows(directory / "receptions.jsonl", RECEPTION_COLUMNS, 250_000):
            if source[0] not in canonical_times:
                raise ValueError("Reception fixture contains an unlinked canonical ID")
            row = list(source)
            stamp = canonical_times[source[0]]
            row[0] += replica * stride
            shifted_base = stamp + time_offsets.get(source[0], timedelta())
            row[1] += shifted_time(shifted_base, replica) - stamp
            yield tuple(row)


def digest_records(records, columns):
    digest = MultisetDigest()
    for row in records:
        digest.add(logical_row(row, columns))
    return digest.result()


async def create_shared(conn, schema, directory, canonical_times, replicas, stride, time_offsets):
    await conn.execute(f"CREATE SCHEMA {qi(schema)}")
    await conn.execute(f"""CREATE TABLE {qualified(schema,'topics')} (
        id SERIAL PRIMARY KEY,topic TEXT NOT NULL UNIQUE);
        CREATE VIEW {qualified(schema,'mqtt_topics')} AS SELECT * FROM {qualified(schema,'topics')};
        CREATE TABLE {qualified(schema,'packet_receptions')} (
        mqtt_row_id BIGINT NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        gateway VARCHAR(8),topic_id INTEGER,rx_rssi SMALLINT,rx_snr DOUBLE PRECISION,
        rx_time BIGINT,hop_limit SMALLINT,hops_away SMALLINT,relay_node INTEGER,transport SMALLINT,
        extras JSONB) PARTITION BY RANGE(created_at)""")
    receipt_digest = MultisetDigest()
    receipt_months = set()
    for row in expanded_receptions(directory, canonical_times, replicas, stride, time_offsets):
        receipt_months.add(month(row[1]))
        receipt_digest.add(logical_row(row, RECEPTION_COLUMNS))
    for stamp in sorted(receipt_months):
        await conn.execute(
            f"CREATE TABLE {qualified(schema,f'packet_receptions_{stamp:%Y_%m}')} "
            f"PARTITION OF {qualified(schema,'packet_receptions')} "
            f"FOR VALUES FROM ('{stamp.isoformat()}') TO ('{next_month(stamp).isoformat()}')")
    await conn.execute(f"CREATE INDEX reception_row ON {qualified(schema,'packet_receptions')}(mqtt_row_id,created_at)")
    await conn.execute(f"CREATE INDEX reception_gateway ON {qualified(schema,'packet_receptions')}(gateway,created_at DESC)")
    await conn.copy_records_to_table("topics", schema_name=schema, columns=TOPIC_COLUMNS,
        records=fixture_rows(directory / "topics.jsonl", TOPIC_COLUMNS, 20_000), timeout=60)
    await conn.copy_records_to_table("packet_receptions", schema_name=schema, columns=RECEPTION_COLUMNS,
        records=expanded_receptions(directory, canonical_times, replicas, stride, time_offsets), timeout=120)
    await conn.execute(f"ANALYZE {qualified(schema,'packet_receptions')}", timeout=60)
    await conn.execute(f"ANALYZE {qualified(schema,'topics')}", timeout=30)
    return receipt_digest.result()


async def move_source_to_public(conn, temporary, partitions):
    # PostgreSQL SET SCHEMA moves a table's owned sequence/indexes/constraints.
    # Partition relations and the extraction function need explicit moves.
    for partition in partitions:
        await conn.execute(f"ALTER TABLE {qualified(temporary,partition['name'])} SET SCHEMA public")
    await conn.execute(f"ALTER TABLE {qualified(temporary,'mqtt_messages')} SET SCHEMA public")
    await conn.execute(f"ALTER FUNCTION {qualified(temporary,'mqtt_messages_extract_node_ids')}() SET SCHEMA public")
    await conn.execute(f"DROP SCHEMA {qi(temporary)}")


def distribution(values):
    return {"count": len(values), "minimum": min(values) if values else None,
            "maximum": max(values) if values else None,
            "median": statistics.median(values) if values else None,
            "total": sum(values)}


def copy_samples(report):
    """Keep payload-conversion stripes distinct from metadata-only backfills."""
    groups = report.setdefault("copy_batch_samples_by_target", {})
    groups.setdefault("columnar", report.setdefault("copy_batch_samples", {
        "rows": [], "wal_bytes": [], "milliseconds": []}))
    return groups


def topic_lookup_digest(packets, cutoff):
    return digest_records(((row[0],row[6],row[1]) for row in packets if row[6] < cutoff),
                          ("id","created_at","topic"))


def resume_report(path, original_digests, replicas, database):
    """A report authorizes no SQL until its fixture and owned names agree."""
    if path.stat().st_size > 16 * MIB:
        raise ValueError("Resume report exceeds its metadata budget")
    report = json.loads(path.read_text())
    if (not report.get("dedicated_target")
            or report.get("target_database", database) != database
            or report.get("fixture", {}).get("original_digests") != original_digests
            or report.get("fixture", {}).get("replicas") != replicas):
        raise ValueError("Resume target or fixture identity differs from the original run")
    schemas = report.get("packet_schemas", {})
    if (schemas.get("columnar") != "public"
            or not re.fullmatch(r"benchmark_heap_full_[a-f0-9]{10}", schemas.get("heap", ""))
            or not re.fullmatch(r"benchmark_shared_[a-f0-9]{10}", report.get("shared_schema", ""))):
        raise ValueError("Resume report does not identify owned workload schemas")
    if not all(key in report for key in ("source_load", "shared_storage", "hot_month")):
        raise ValueError("Resume requires completed fixture loading")
    failure_phase = report.get("error", {}).get("phase")
    if failure_phase and failure_phase not in {
            "target_preflight", "verify_resume_fixture", "prepare_leaf_indexes",
            "actual_archive_cycles", "verify_full_output", "measure_final_storage"}:
        raise ValueError("This failed phase cannot safely resume without a fresh isolated target")
    return report


async def run(args):
    if not re.fullmatch(r"[A-Z][A-Z0-9_]*", args.dsn_env):
        raise ValueError("Invalid DSN environment-variable name")
    dsn = os.environ.get(args.dsn_env, "")
    database = unquote(urlsplit(dsn).path.lstrip("/"))
    if not re.fullmatch(r"[A-Za-z0-9_]+_columnar_test", database):
        raise ValueError("A NEW dedicated *_columnar_test database is required")
    if not 100_000 <= args.minimum_packets <= 200_000:
        raise ValueError("The workload must request 100000..200000 canonical rows")
    if not 1 <= args.max_logical_mib <= 256 or not 1024 <= args.reserve_mib <= 8192:
        raise ValueError("Invalid workload byte/reserve budget")
    if not args.database_filesystem.is_dir():
        raise ValueError("An explicit existing database-filesystem path is required")
    manifest, packets, original_digests, sizes, fixture_hot, trigger = load_fixture(args.fixture_dir)
    replicas = math.ceil(args.minimum_packets / len(packets))
    if replicas > 32 or len(packets) * replicas > 250_000:
        raise ValueError("Replication exceeds the bounded canonical row budget")
    if original_digests["receptions"]["rows"] * replicas > 2_000_000:
        raise ValueError("Replication exceeds two million linked receptions")
    logical_budget = (sizes["packets"] + sizes["receptions"]) * replicas + sizes["topics"]
    logical_budget += 64 * replicas * (len(packets) + original_digests["receptions"]["rows"])
    if logical_budget > args.max_logical_mib * MIB:
        raise ValueError("Expanded fixture exceeds the logical workload byte budget")
    free_bytes = lambda: shutil.disk_usage(args.database_filesystem).free
    initial_free = free_bytes()
    resuming = getattr(args, "resume_report", None)
    required_extra = 128 * MIB if resuming else 4 * logical_budget + 256 * MIB
    if initial_free < args.reserve_mib * MIB + required_extra:
        raise ValueError("Insufficient database-filesystem headroom for both copies, staging and WAL")
    stride = max(row[0] for row in packets) - min(row[0] for row in packets) + 1
    canonical_times = {row[0]: row[6] for row in packets}
    specs = index_specifications(manifest)
    token = uuid.uuid4().hex[:10]
    baseline = "benchmark_heap_full_" + token
    incoming = "benchmark_heap_full_" + uuid.uuid4().hex[:10]
    shared = "benchmark_shared_" + token
    report = {"all_checks_passed": False, "dedicated_target": True,
        "target_database": database,
        "fixture": {"original_digests": original_digests, "replicas": replicas,
                    "canonical_rows": len(packets) * replicas, "logical_budget_bytes": logical_budget,
                    "sampling_note": manifest.get("sampling_note"),
                    "replication_note": "Deterministic copies with unique ID offsets and month-preserving timestamp shifts; these are not independent traffic samples."},
        "packet_schemas": {"heap": baseline, "columnar": "public"}, "shared_schema": shared,
        "free_bytes_before": initial_free, "cycles": [], "index_preparation": [],
        "limits": ["Only generated isolated data is converted; source storage is never opened.",
                   "Both expanded heap copies and all linked receptions are retained for query comparison.",
                   "Replication interleaves distant IDs at nearby timestamps, a random-lookup stress case rather than typical monotonic ingestion.",
                   "The explicit filesystem argument must identify the PostgreSQL data filesystem.",
                   "WAL counters are cluster-wide; measurements are not a production capacity guarantee."]}
    if resuming:
        report = resume_report(resuming, original_digests, replicas, database)
        report.setdefault("measurement_history", []).append({
            key: report.get(key) for key in ("all_checks_passed","archive_elapsed_seconds",
                "complete_deployment_bytes","copy_batches","topic_lookup_storage")})
        baseline, shared = report["packet_schemas"]["heap"], report["shared_schema"]
        report["all_checks_passed"] = False
        report["target_database"] = database
        report["resume_count"] = report.get("resume_count", 0) + 1
        if "error" in report:
            report.setdefault("previous_errors", []).append(report.pop("error"))
    conn = await asyncpg.connect(dsn, timeout=5, command_timeout=120, server_settings={
        "application_name": "meshinfo-large-archive-workload", "timezone": "UTC",
        "work_mem": "4MB", "maintenance_work_mem": "32MB", "max_parallel_workers_per_gather": "0",
        "max_parallel_maintenance_workers": "0", "lock_timeout": "500ms", "statement_timeout": "120000", "jit": "off"})
    phase = "target_preflight"
    try:
        if await conn.fetchval("SELECT current_database()") != database:
            raise ValueError("Connected database differs from guarded target")
        if await conn.fetchval("SHOW default_tablespace") not in ("", "pg_default"):
            raise ValueError("Custom default tablespaces do not share the declared staging-space budget")
        if await conn.fetchval("SHOW temp_tablespaces") not in ("", "pg_default"):
            raise ValueError("Custom temporary tablespaces require their own space budget")
        if await conn.fetchval("""SELECT t.spcname FROM pg_database d JOIN pg_tablespace t ON t.oid=d.dattablespace
            WHERE d.datname=current_database()""") != "pg_default":
            raise ValueError("Custom database tablespaces require their own space budget")
        existing = await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE c.relkind IN ('r','p','m','S') AND (n.nspname='public' OR n.nspname LIKE 'benchmark_%' OR n.nspname LIKE 'history_%'))""")
        if existing and not resuming:
            raise ValueError("Workload target must be new and contain no public or prior fixture tables")
        extensions = {row["extname"] for row in await conn.fetch("SELECT extname FROM pg_extension")}
        if not {"citus_columnar", "pg_trgm"} <= extensions:
            raise ValueError("Target requires preinstalled citus_columnar and pg_trgm")
        hot = complete_month_cutoff(await conn.fetchval("SELECT clock_timestamp()"), 1)
        if resuming and report["hot_month"] != hot.date().isoformat():
            raise ValueError("Archive cutoff changed since the original workload run")
        # The exporter anchors edge witnesses to its latest real month, while
        # the real archive worker retains one full month. Move only generated
        # witness timestamps so their cold/hot roles match this actual cutoff.
        # Real sampled rows and all raw payload text remain unchanged.
        synthetic_offset = hot - fixture_hot
        time_offsets = {identity: synthetic_offset
                        for identity in manifest.get("synthetic_packet_ids", [])}
        expanded = expanded_packets(packets, replicas, stride, time_offsets)
        packet_digest = digest_records(expanded, MQTT_COLUMNS)
        report["fixture"]["synthetic_timestamp_alignment_seconds"] = synthetic_offset.total_seconds()
        report["fixture"]["synthetic_timestamp_alignment_rows"] = len(time_offsets) * replicas
        report["fixture"]["packets_by_month"] = dict(Counter(f"{row[6]:%Y-%m}" for row in expanded))
        if not any(row[6] < hot for row in expanded) or not any(row[6] >= hot for row in expanded):
            raise ValueError("Fixture must straddle the actual one-month archiver cutoff")
        report["hot_month"] = hot.date().isoformat()
        if resuming:
            phase = "verify_resume_fixture"
            print("Verifying retained workload before resume", file=sys.stderr, flush=True)
            receipt_digest = digest_records(
                expanded_receptions(args.fixture_dir, canonical_times, replicas, stride, time_offsets), RECEPTION_COLUMNS)
            for schema in (baseline, "public"):
                if await rows_digest(conn, schema, "mqtt_messages", MQTT_COLUMNS) != packet_digest:
                    raise AssertionError("Retained canonical data differs from the expected workload")
            if await rows_digest(conn, shared, "packet_receptions", RECEPTION_COLUMNS) != receipt_digest:
                raise AssertionError("Retained reception data differs from the expected workload")
            if await rows_digest(conn, shared, "topics", TOPIC_COLUMNS) != original_digests["topics"]:
                raise AssertionError("Retained topic dictionary differs from the expected workload")
        else:
            report["columnar_metadata_before"] = await metadata_size(conn)
            phase = "load_shared_receptions"
            receipt_digest = await create_shared(conn, shared, args.fixture_dir, canonical_times, replicas, stride, time_offsets)
            if await rows_digest(conn, shared, "packet_receptions", RECEPTION_COLUMNS) != receipt_digest:
                raise AssertionError("Expanded reception roundtrip failed")
            if await rows_digest(conn, shared, "topics", TOPIC_COLUMNS) != original_digests["topics"]:
                raise AssertionError("Topic dictionary roundtrip failed")
            report["fixture"]["reception_rows"] = receipt_digest["rows"]
            report["shared_storage"] = await schema_size(conn, shared)
            for label, schema in (("heap", baseline), ("source", incoming)):
                phase = "load_" + label
                print(f"Loading indexed {label} fixture", file=sys.stderr, flush=True)
                built = await create_packets(conn, schema, shared, "heap_full", expanded, hot, specs, trigger, 1000)
                if await rows_digest(conn, schema, "mqtt_messages", MQTT_COLUMNS) != packet_digest:
                    raise AssertionError("Expanded canonical roundtrip failed")
                report[label + "_load"] = built
                if label == "source":
                    phase = "move_generated_source_to_public"
                    await move_source_to_public(conn, incoming, built["partitions"])
        report["fixture"]["expanded_packet_digest"] = packet_digest
        report["fixture"]["expanded_reception_digest"] = receipt_digest
        await conn.execute(f"SET search_path=public,{qi(shared)}")
        await archive.ensure_archive_schema(conn)
        policy = {"storage": {"archive": {"enabled": True, "after_months": 1,
            "batch_rows": 1000, "batch_mib": 8, "cycle_seconds": 10, "statement_seconds": 3,
            "reserve_mib": args.reserve_mib, "max_wal_mib": 64, "chunk_group_rows": 1000,
            "database_filesystem": str(args.database_filesystem)}}}
        report["archive_policy"] = policy["storage"]["archive"]
        phase = "prepare_leaf_indexes"
        for _ in range(40):
            outcome = await prepare_archive_indexes(conn, policy, free_bytes=free_bytes)
            report["index_preparation"].append(outcome)
            if outcome["status"] == "complete":
                break
            if outcome["status"] != "partial":
                raise RuntimeError("Archive index preparation did not progress")
        else:
            raise RuntimeError("Archive index preparation exceeded its bounded step count")
        report["heap_storage"] = await schema_size(conn, baseline)
        if "source_storage_before_archive" not in report:
            report["source_storage_before_archive"] = await schema_size(conn, "public")
        samples_by_target = copy_samples(report)
        original_copy = archive._copy_chunk

        async def measured_copy(connection, job, policy_value, filesystem_probe):
            lsn = await connection.fetchval("SELECT pg_current_wal_insert_lsn()::text")
            started = time.perf_counter()
            await original_copy(connection, job, policy_value, filesystem_probe)
            copied = await connection.fetchval("SELECT rows_copied FROM public.storage_archive_jobs WHERE job_id=$1", job["job_id"])
            if copied > job["rows_copied"]:
                samples = samples_by_target.setdefault(job.get("target_method","columnar"),
                    {"rows": [], "wal_bytes": [], "milliseconds": []})
                samples["rows"].append(copied - job["rows_copied"])
                samples["milliseconds"].append(round((time.perf_counter()-started)*1000, 3))
                samples["wal_bytes"].append(int(await connection.fetchval(
                    "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::text::pg_lsn)", lsn)))

        started = time.perf_counter()
        prior_elapsed = report.get("archive_elapsed_seconds", 0)
        phase = "actual_archive_cycles"
        with patch.object(archive, "_copy_chunk", side_effect=measured_copy):
            async with asyncio.timeout(args.max_seconds):
                for cycle in range(2000):
                    outcome = await archive.run_archive_cycle(conn, policy, free_bytes=free_bytes)
                    report["cycles"].append(outcome)
                    report["archive_elapsed_seconds"] = round(prior_elapsed + time.perf_counter()-started, 3)
                    print(f"Archive cycle {cycle+1}: {outcome['status']}", file=sys.stderr, flush=True)
                    if outcome["status"] == "idle":
                        break
                    if outcome["status"] not in {"copying", "verifying", "ready", "complete"}:
                        raise RuntimeError("Actual archive worker did not progress")
                else:
                    raise RuntimeError("Actual archive worker exceeded cycle budget")
        report["archive_elapsed_seconds"] = round(prior_elapsed + time.perf_counter()-started, 3)
        report["copy_batches_by_target"] = {method: {key: distribution(values) for key,values in samples.items()}
                                             for method,samples in samples_by_target.items()}
        report["copy_batches"] = report["copy_batches_by_target"]["columnar"]
        phase = "verify_full_output"
        if await rows_digest(conn, "public", "mqtt_messages", MQTT_COLUMNS) != packet_digest:
            raise AssertionError("Actual chunked archive changed canonical rows")
        if await rows_digest(conn, shared, "packet_receptions", RECEPTION_COLUMNS) != receipt_digest:
            raise AssertionError("Actual chunked archive changed linked receptions")
        expected_topics = topic_lookup_digest(expanded,hot)
        actual_topics = await rows_digest(conn,"public","mqtt_archive_topics",("id","created_at","topic"))
        if actual_topics != expected_topics:
            raise AssertionError("Archived canonical topic lookup differs from complete original topic/key triples")
        report["fixture"]["archived_topic_lookup_digest"] = expected_topics
        leaves = await conn.fetch("""SELECT c.relname,a.amname,pg_get_expr(c.relpartbound,c.oid) AS bound
            FROM pg_class c JOIN pg_inherits i ON i.inhrelid=c.oid JOIN pg_am a ON a.oid=c.relam
            WHERE i.inhparent='public.mqtt_messages'::regclass ORDER BY c.relname""")
        from storage.db.storage_report import monthly_partition_bounds
        for leaf in leaves:
            bounds = monthly_partition_bounds(leaf["bound"])
            if bounds is None or leaf["amname"] != ("columnar" if bounds[1] <= hot else "heap"):
                raise AssertionError("Archive did not convert exactly the eligible closed months")
        report["partition_methods"] = [dict(row) for row in leaves]
        topic_parts = []
        for leaf in leaves:
            if leaf["amname"] == "columnar":
                helper = await archive.archive_topics.attached_partition(conn,*monthly_partition_bounds(leaf["bound"]))
                if helper is None:
                    raise AssertionError("Archived canonical month lacks verified topic lookup coverage")
                topic_parts.append({"name":helper["name"],"bound":helper["bound"],"total_bytes":helper["total_bytes"]})
        report["topic_lookup_storage"] = {"total_bytes":sum(row["total_bytes"] for row in topic_parts),
            "partitions":topic_parts,"note":"Already included in public columnar_storage; never add twice to deployment totals"}
        phase = "measure_final_storage"
        report["columnar_storage"] = await schema_size(conn, "public")
        report["shared_storage"] = await schema_size(conn,shared)
        report["columnar_metadata_after"] = await metadata_size(conn)
        heap_total = report["heap_storage"]["total_bytes"] + report["shared_storage"]["total_bytes"]
        columnar_total = (report["columnar_storage"]["total_bytes"] + report["shared_storage"]["total_bytes"]
                          + report["columnar_metadata_after"]["total_bytes"])
        report["complete_deployment_bytes"] = {"heap": heap_total, "columnar": columnar_total,
            "saved": heap_total-columnar_total, "saved_percent": round(100*(1-columnar_total/heap_total), 2)}
        report["fixture"]["expanded_packet_digest"] = packet_digest
        report["fixture"]["expanded_reception_digest"] = receipt_digest
        report["free_bytes_after"] = free_bytes()
        report["all_checks_passed"] = True
    except (Exception, asyncio.CancelledError) as exc:
        report["error"] = {"category": type(exc).__name__, "phase": phase,
                           "sqlstate": getattr(exc, "sqlstate", None),
                           "note": "Generated isolated state is retained; source storage was never opened."}
    finally:
        await conn.close()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn-env", required=True)
    parser.add_argument("--fixture-dir", type=Path, required=True)
    parser.add_argument("--database-filesystem", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--resume-report", type=Path,
                        help="Verify and resume an already loaded workload; never reload its rows")
    parser.add_argument("--minimum-packets", type=int, default=100000)
    parser.add_argument("--max-logical-mib", type=int, default=256)
    parser.add_argument("--reserve-mib", type=int, default=1024)
    parser.add_argument("--max-seconds", type=int, choices=(300, 600, 1200, 1800), default=1200)
    args = parser.parse_args()
    try:
        report = asyncio.run(run(args))
    except Exception as exc:
        print(f"Archive workload refused: {type(exc).__name__}", file=sys.stderr)
        return 1
    args.output.write_text(json.dumps(report, indent=2, default=normalize) + "\n")
    print(json.dumps({"output": str(args.output), "all_checks_passed": report["all_checks_passed"],
                      "complete_deployment_bytes": report.get("complete_deployment_bytes")}))
    return 0 if report["all_checks_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
