#!/usr/bin/env python3
"""Exercise archived readers while bounded hot fixture writes commit.

Only an existing, explicitly isolated *_columnar_test database is accepted.
Both packet layouts receive the same owned rows in one transaction; shared
receptions are inserted once. Cold query windows exclude those writes, allowing
complete response comparison while ingestion proceeds. Owned rows are removed
in finally, with a durable ownership record for interrupted-run recovery.
"""

import argparse
import asyncio
from datetime import datetime, timedelta
import json
import math
import os
from pathlib import Path
import re
import secrets
import signal
import sys
import time
import uuid
from urllib.parse import unquote, urlsplit

import asyncpg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.benchmark_columnar_history import qi, qualified, plan_summary
from scripts.benchmark_history_read_plans import open_storage, result_digest, schema_name
from storage.db.history_queries import TOPIC_CANDIDATE_LIMIT


def percentile(samples, fraction):
    """Nearest-rank percentiles preserve observed worst-tail latency."""
    return sorted(samples)[max(0, math.ceil(len(samples) * fraction) - 1)] if samples else None


def summarize(samples):
    return {"count": len(samples), "p50_ms": percentile(samples, .50),
            "p95_ms": percentile(samples, .95), "max_ms": max(samples) if samples else None}


def bounded_candidates(captured):
    match = re.search(r"\bid=ANY\(\$(\d+)::bigint\[\]\)", captured.get("sql", ""))
    if match is None:
        return None
    parameters = captured.get("parameters", ())
    index = int(match.group(1))-1
    if index >= len(parameters) or not isinstance(parameters[index], (list, tuple)):
        return None
    return len(parameters[index])


def classify_execution(case, paths, successful_reads, candidates, evidence):
    """Only proved bounded execution earns the indexed-work acceptance rule."""
    indexed_cases = {"archive_page", "archive_node", "archive_literal_topic", "archive_copies",
                     "archive_topic_payload_common", "archive_topic_payload_absent"}
    scan_cases = {"archive_common_payload", "archive_absent_payload", "archive_receipt_topic"}
    if case in indexed_cases:
        return "indexed lookup"
    if case not in scan_cases:
        return "unclassified"
    if (successful_reads > 0 and paths == {"bounded_payload": successful_reads}
            and candidates.get("bounded_samples") == successful_reads
            and candidates.get("unknown_samples", 0) == 0
            and 0 < (candidates.get("max_candidates") or 0) <= TOPIC_CANDIDATE_LIMIT
            and evidence.get("canonical_index_scan") is True):
        return "bounded indexed search"
    return "unindexed text search"


async def plan_evidence(storage):
    captured = storage.pool.captured
    sql, parameters = captured.get("sql"), captured.get("parameters", ())
    if not sql:
        return {"canonical_index_scan": False}
    async with storage.pool.pool.acquire(timeout=3) as connection:
        async with connection.transaction(readonly=True):
            for name, value in captured.get("statement_settings", {}).items():
                await connection.execute("SELECT set_config($1,$2,true)", name, value)
            plan = json.loads(await connection.fetchval("EXPLAIN(FORMAT JSON) " + sql, *parameters))[0]["Plan"]
    canonical = []
    def visit(node):
        if node.get("Relation Name", "").startswith("mqtt_messages"):
            canonical.append(node.get("Node Type"))
        for child in node.get("Plans", []):
            visit(child)
    visit(plan)
    return {"canonical_index_scan": bool(canonical) and all(value in {"Index Scan", "Index Only Scan"}
                                                           for value in canonical),
            "candidate_count": bounded_candidates(captured),
            "statement_settings": captured.get("statement_settings", {}), "plan": plan_summary(plan)}


def persist_report(path, report, *, create=False):
    """Persist ownership privately before writes, without overwriting old runs."""
    target = path if create else path.with_name("." + path.name + "." + uuid.uuid4().hex + ".tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(target, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w") as output:
            output.write(json.dumps(report, indent=2) + "\n")
            output.flush()
            os.fsync(output.fileno())
        if not create:
            os.replace(target, path)
        parent = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    finally:
        if not create:
            target.unlink(missing_ok=True)


async def require_heap(connection, table_oid):
    method = await connection.fetchval("SELECT am.amname FROM pg_class c "
        "JOIN pg_am am ON am.oid=c.relam WHERE c.oid=$1::oid", table_oid)
    if method != "heap":
        raise ValueError("Test inserts must route to an existing heap partition")


def parameters(args):
    if not re.fullmatch(r"[A-Z][A-Z0-9_]*", args.dsn_env):
        raise ValueError("Invalid DSN environment-variable name")
    dsn = os.environ.get(args.dsn_env, "")
    database = unquote(urlsplit(dsn).path.lstrip("/"))
    if not re.fullmatch(r"[A-Za-z0-9_]+_columnar_test", database):
        raise ValueError("Only dedicated *_columnar_test databases are allowed")
    schemas = {}
    for item in args.packet_schema:
        label, separator, schema = item.partition("=")
        if not separator or label in schemas or not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", label):
            raise ValueError("Packet schemas require unique label=schema values")
        schemas[label] = schema_name(schema, allow_public=True)
    if set(schemas) != {"heap", "columnar"} or len(set(schemas.values())) != 2:
        raise ValueError("Exactly distinct heap= and columnar= fixture schemas are required")
    shared = schema_name(args.shared_schema)
    hot = datetime.fromisoformat(args.hot_month + "T00:00:00+00:00")
    if hot.day != 1:
        raise ValueError("Hot month must be a month boundary")
    if not 10 <= args.duration_seconds <= 120 or not 1 <= args.ingest_per_second <= 10:
        raise ValueError("Duration must be 10..120 seconds and ingest rate 1..10 per second")
    if not 1 <= args.readers <= 4 or not 1 <= args.reads_per_second <= 10:
        raise ValueError("Read workers must be 1..4 with 1..10 queries per second each")
    if not 1 <= args.relative_limit <= 100 or not 10 <= args.read_p95_ms <= 10_000:
        raise ValueError("Read thresholds must be explicit bounded positive values")
    if not 10 <= args.write_p95_ms <= 10_000:
        raise ValueError("Write threshold must be between 10 and 10000 milliseconds")
    if not 1 <= args.reception_lookback_seconds <= 604800:
        raise ValueError("Reception lookback must be between one second and seven days")
    return dsn, database, schemas, shared, hot


async def remove_owned(connection, schemas, shared, ownership):
    """Exact owned ID/time bounds prune immutable cold partitions on DELETE."""
    low, high = ownership["first_id"], ownership["exclusive_last_id"]
    stamp = datetime.fromisoformat(ownership["created_at"])
    args = (low, high, stamp, stamp + timedelta(seconds=1))
    tables = [(shared, "packet_receptions", "mqtt_row_id")]
    tables.extend((schema, "mqtt_messages", "id") for schema in schemas.values())
    async with connection.transaction():
        for schema, table, column in tables:
            await connection.execute(f"DELETE FROM {qualified(schema,table)} "
                f"WHERE {qi(column)}>=$1 AND {qi(column)}<$2 AND created_at>=$3 AND created_at<$4", *args)
        for schema, table, column in tables:
            if await connection.fetchval(f"SELECT count(*) FROM {qualified(schema,table)} "
                    f"WHERE {qi(column)}>=$1 AND {qi(column)}<$2 AND created_at>=$3 AND created_at<$4", *args):
                raise AssertionError("Owned concurrency-test rows remain after cleanup")


async def run(args):
    dsn, database, schemas, shared, hot = parameters(args)
    report = {"all_checks_passed": False, "target_database": database, "packet_schemas": schemas,
              "shared_schema": shared, "duration_seconds": args.duration_seconds,
              "ingest_per_second": args.ingest_per_second, "reader_workers": args.readers,
              "reader_connections_per_layout": args.readers,
              "total_read_connections": args.readers*len(schemas), "writer_connections": 1,
              "reads_per_second_per_worker": args.reads_per_second,
              "thresholds": {"read_p95_ms": args.read_p95_ms, "write_p95_ms": args.write_p95_ms,
                             "columnar_to_heap_p95_ratio": args.relative_limit},
              "operational_thresholds": {"indexed_p95_ms": 250, "indexed_ratio": 3,
                  "indexed_additional_p95_ms": 100, "unindexed_p95_ms": 10000,
                  "unindexed_ratio": 3, "writer_p95_ms": 250,
                  "first_postgres_buffer_reset_page_ms": "1000; verified by separate read-plan report"},
              "limits": ["Bounded isolated fixture; this is not production-scale capacity proof.",
                         "Hot writes use an existing fixture month; archived query windows exclude them.",
                         "PostgreSQL and OS caches may be warm. No host caches are dropped.",
                         "Relative gates use the same query on each layout under concurrent load."]}
    connection = None
    opened = []
    ownership = None
    tasks = []
    persist_report(args.output, report, create=True)
    try:
        connection = await asyncpg.connect(dsn, command_timeout=10,
            server_settings={"application_name": "meshinfo-history-concurrency-writer", "timezone": "UTC",
                             "work_mem": "4MB", "statement_timeout": "10000", "lock_timeout": "500ms"})
        if await connection.fetchval("SELECT current_database()") != database:
            raise ValueError("Connected database differs from guarded target")
        stamp = hot + timedelta(days=14, seconds=secrets.randbelow(3600))
        first_id = 8_000_000_000_000_000_000 + secrets.randbelow(1_000_000_000) * 2048
        ownership = {"first_id": first_id, "exclusive_last_id": first_id + 2048,
                     "created_at": stamp.isoformat()}
        for schema in schemas.values():
            if await connection.fetchval(f"SELECT EXISTS(SELECT 1 FROM {qualified(schema,'mqtt_messages')} "
                                          "WHERE id >= $1 AND id < $2)", first_id, first_id+2048):
                raise ValueError("Owned ID range collides with existing fixture rows")
            # Read-only discovery establishes an existing heap leaf at the write timestamp.
            method = await connection.fetchval(f"SELECT am.amname FROM {qualified(schema,'mqtt_messages')} m "
                "JOIN pg_class c ON c.oid=m.tableoid JOIN pg_am am ON am.oid=c.relam "
                "WHERE created_at >= $1 AND created_at < $2 LIMIT 1", hot, hot+timedelta(days=28))
            if method != "heap":
                raise ValueError("An existing nonempty heap hot partition is required")
        if await connection.fetchval(f"SELECT EXISTS(SELECT 1 FROM {qualified(shared,'packet_receptions')} "
                                      "WHERE mqtt_row_id >= $1 AND mqtt_row_id < $2)", first_id, first_id+2048):
            raise ValueError("Owned ID range collides with existing receptions")
        topic = await connection.fetchrow(f"SELECT id,topic FROM {qualified(shared,'mqtt_topics')} ORDER BY id LIMIT 1")
        if topic is None:
            raise ValueError("A populated topic dictionary is required")
        report["ownership"] = ownership
        report["cleanup_complete"] = False
        persist_report(args.output, report)
        cold = {"end": hot-timedelta(microseconds=1), "limit": 200}
        cases = [
            ("archive_page", "query_mqtt_messages", cold),
            ("archive_common_payload", "query_mqtt_messages", {**cold, "search": "from"}),
            ("archive_absent_payload", "query_mqtt_messages", {**cold, "search": "meshinfo-concurrency-absent-0ac71f"}),
            ("archive_receipt_topic", "query_mqtt_messages", {**cold, "search": "columnar-reception-only-witness"}),
            ("archive_node", "query_node_mqtt_messages", {**cold, "node_id": "c0decafe"}),
            ("archive_literal_topic", "query_mqtt_messages", {**cold, "topic": "msh/prototype/%_\\literal"}),
            ("archive_topic_payload_common", "query_mqtt_messages",
             {**cold, "topic": "msh/prototype/%_\\literal", "search": "from"}),
            ("archive_topic_payload_absent", "query_mqtt_messages",
             {**cold, "topic": "msh/prototype/%_\\literal", "search": "meshinfo-topic-payload-absent-2ce9a8"}),
            ("archive_copies", "query_mqtt_message_by_id", {"row_id": args.copy_row_id, "include_copies": True}),
        ]
        # Each worker owns a single-connection pool for each layout. The writer
        # has a separate connection; no monkeypatching or query overrides occur.
        readers = []
        for _ in range(args.readers):
            layouts = {}
            for label, schema in schemas.items():
                storage, pool, captured = await open_storage(dsn, schema, shared, 5)
                storage._reception_lookback = args.reception_lookback_seconds
                storage._reception_count_sql = (
                    "(SELECT count(*)::int FROM packet_receptions pr WHERE pr.mqtt_row_id=mqtt_messages.id "
                    "AND pr.created_at>=mqtt_messages.created_at "
                    f"AND pr.created_at<mqtt_messages.created_at+interval '{args.reception_lookback_seconds} seconds')")
                layouts[label] = storage
                opened.append(pool)
            readers.append(layouts)
        expected = {}
        evidence = {label: {} for label in schemas}
        for name, method, arguments in cases:
            for label in schemas:
                value = await getattr(readers[0][label], method)(**arguments)
                digest = result_digest(value)
                if name in expected and expected[name] != digest:
                    raise AssertionError("Baseline reader responses differ before ingestion")
                nonempty = bool(value) if method == "query_mqtt_message_by_id" else bool(value["messages"])
                if name not in {"archive_absent_payload", "archive_topic_payload_absent"} and not nonempty:
                    raise AssertionError("A required positive archived-query witness is missing")
                expected[name] = digest
                evidence[label][name] = await plan_evidence(readers[0][label])
        read_times = {label: {name: [] for name, _, _ in cases} for label in schemas}
        read_paths = {label: {name: {} for name, _, _ in cases} for label in schemas}
        candidates_seen = {label: {name: {"bounded_samples": 0, "unknown_samples": 0, "max_candidates": None}
                                   for name, _, _ in cases} for label in schemas}
        read_errors, write_errors, write_times = [], [], []
        end = time.monotonic() + args.duration_seconds

        async def writer():
            sequence = 0
            while time.monotonic() < end:
                started = time.monotonic()
                row_id = first_id + sequence
                created = stamp + timedelta(microseconds=sequence)
                payload = json.dumps({"from": 3735928559, "to": 4294967295, "id": sequence,
                                      "text": "isolated concurrent hot ingestion"}, separators=(",", ":"))
                try:
                    async with connection.transaction():
                        for schema in schemas.values():
                            table_oid = await connection.fetchval(f"INSERT INTO {qualified(schema,'mqtt_messages')} "
                                "(id,topic,payload,qos,retain,timestamp,created_at,from_node_id,to_node_id,packet_id) "
                                "VALUES($1,$2,$3,0,false,$4,$5,'deadbeef','ffffffff',$6) RETURNING tableoid",
                                row_id, topic["topic"], payload, int(created.timestamp()), created, sequence)
                            await require_heap(connection, table_oid)
                        table_oid = await connection.fetchval(f"INSERT INTO {qualified(shared,'packet_receptions')} "
                            "(mqtt_row_id,created_at,gateway,topic_id) VALUES($1,$2,'deadbeef',$3) RETURNING tableoid",
                            row_id, created, topic["id"])
                        await require_heap(connection, table_oid)
                    write_times.append(round((time.monotonic()-started)*1000, 3))
                except (asyncpg.PostgresError, TimeoutError) as exc:
                    write_errors.append({"error": type(exc).__name__, "sequence": sequence})
                sequence += 1
                if sequence >= 2048:
                    raise AssertionError("Bounded owned ID range exhausted")
                await asyncio.sleep(max(0, 1/args.ingest_per_second - (time.monotonic()-started)))

        async def reader(worker, layouts):
            sequence = worker
            while time.monotonic() < end:
                cycle_start = time.monotonic()
                name, method, arguments = cases[sequence % len(cases)]
                labels = list(schemas)
                if sequence % 2:
                    labels.reverse()
                for label in labels:
                    started = time.monotonic()
                    try:
                        value = await getattr(layouts[label], method)(**arguments)
                        if result_digest(value) != expected[name]:
                            raise AssertionError("Archived response changed during hot ingestion")
                        read_times[label][name].append(round((time.monotonic()-started)*1000, 3))
                        sql = layouts[label].pool.captured.get("sql", "")
                        path = "bounded_payload" if "id=ANY(" in sql else "native_reader"
                        read_paths[label][name][path] = read_paths[label][name].get(path, 0)+1
                        if path == "bounded_payload":
                            candidate_count = bounded_candidates(layouts[label].pool.captured)
                            observed = candidates_seen[label][name]
                            if candidate_count is None:
                                observed["unknown_samples"] += 1
                            else:
                                observed["bounded_samples"] += 1
                                observed["max_candidates"] = max(observed["max_candidates"] or 0, candidate_count)
                    except Exception as exc:
                        read_errors.append({"layout": label, "case": name, "error": type(exc).__name__,
                                            "timed_out": getattr(exc, "timed_out", isinstance(exc, TimeoutError))})
                sequence += 1
                await asyncio.sleep(max(0, 1/args.reads_per_second - (time.monotonic()-cycle_start)))

        tasks = [asyncio.create_task(writer())]
        tasks.extend(asyncio.create_task(reader(index, layouts)) for index, layouts in enumerate(readers))
        await asyncio.gather(*tasks)
        report["write_latency"] = summarize(write_times)
        report["write_errors"] = write_errors
        report["read_errors"] = read_errors
        report["read_latency"] = {label: {name: summarize(values) for name, values in cases_.items()}
                                  for label, cases_ in read_times.items()}
        report["read_execution_paths"] = read_paths
        report["read_candidate_counts"] = candidates_seen
        report["read_plan_evidence"] = evidence
        gates, operational = {}, {}
        for name, _, _ in cases:
            heap = report["read_latency"]["heap"][name]
            columnar = report["read_latency"]["columnar"][name]
            ratio = columnar["p95_ms"]/heap["p95_ms"] if heap["p95_ms"] and columnar["p95_ms"] else None
            gates[name] = {"columnar_to_heap_p95_ratio": ratio,
                          "columnar_p95_under_100ms": columnar["p95_ms"] is not None and columnar["p95_ms"] < 100,
                          "columnar_p95_under_250ms": columnar["p95_ms"] is not None and columnar["p95_ms"] < 250,
                          "columnar_minus_heap_p95_ms": (round(columnar["p95_ms"]-heap["p95_ms"], 3)
                              if columnar["p95_ms"] is not None and heap["p95_ms"] is not None else None),
                          "passed": bool(min(heap["count"], columnar["count"]) >= 5 and ratio is not None
                                         and ratio <= args.relative_limit
                                         and max(heap["p95_ms"], columnar["p95_ms"]) <= args.read_p95_ms)}
            classification = classify_execution(name, read_paths["columnar"][name], columnar["count"],
                candidates_seen["columnar"][name], evidence["columnar"][name])
            unindexed = classification == "unindexed text search"
            enough = min(heap["count"], columnar["count"]) >= 5 and ratio is not None
            practical = bool(enough and classification != "unclassified" and
                (columnar["p95_ms"] <= 10000 and ratio <= 3 if unindexed else
                columnar["p95_ms"] <= 250 and (ratio <= 3 or columnar["p95_ms"]-heap["p95_ms"] <= 100)))
            operational[name] = {"classification": classification,
                                 "passed": practical}
        report["read_gates"] = gates
        report["operational_read_gates"] = operational
        report["response_consistency_passed"] = bool(not read_errors and all(
            summary["count"] > 0 for cases_ in report["read_latency"].values() for summary in cases_.values()))
        report["ingest_checks_passed"] = not write_errors and len(write_times) >= 5
        report["latency_gates_passed"] = bool(report["write_latency"]["p95_ms"] is not None
            and report["write_latency"]["p95_ms"] <= args.write_p95_ms and all(g["passed"] for g in gates.values()))
        report["operational_acceptance_passed"] = bool(report["response_consistency_passed"]
            and report["ingest_checks_passed"] and report["write_latency"]["p95_ms"] <= 250
            and all(g["passed"] for g in operational.values()))
        report["all_checks_passed"] = bool(not read_errors and not write_errors and len(write_times) >= 5
            and report["write_latency"]["p95_ms"] <= args.write_p95_ms and all(g["passed"] for g in gates.values()))
    except BaseException as exc:
        report["error"] = {"type": type(exc).__name__, "phase": "concurrent_history_validation"}
        if exc.__cause__ is not None:
            report["error"]["cause_type"] = type(exc.__cause__).__name__
            report["error"]["sqlstate"] = getattr(exc.__cause__, "sqlstate", None)
        if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)):
            report["interrupted"] = True
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for pool in opened:
            try:
                await asyncio.wait_for(pool.close(), timeout=5)
            except Exception as exc:
                pool.terminate()
                report.setdefault("pool_cleanup_errors", []).append(type(exc).__name__)
                report["all_checks_passed"] = False
        if connection is not None:
            if ownership is not None and "ownership" in report:
                try:
                    await remove_owned(connection, schemas, shared, ownership)
                    report["cleanup_complete"] = True
                except Exception as exc:
                    report["cleanup_complete"] = False
                    report["cleanup_error"] = type(exc).__name__
                    report["all_checks_passed"] = False
                    report["operational_acceptance_passed"] = False
            await connection.close()
        persist_report(args.output, report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn-env", required=True)
    parser.add_argument("--packet-schema", action="append", required=True)
    parser.add_argument("--shared-schema", required=True)
    parser.add_argument("--hot-month", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration-seconds", type=int, default=30)
    parser.add_argument("--ingest-per-second", type=int, default=5)
    parser.add_argument("--readers", type=int, default=2)
    parser.add_argument("--reads-per-second", type=int, default=5)
    parser.add_argument("--reception-lookback-seconds", type=int, default=86400)
    parser.add_argument("--read-p95-ms", type=float, default=1000)
    parser.add_argument("--write-p95-ms", type=float, default=1000)
    parser.add_argument("--relative-limit", type=float, default=3)
    parser.add_argument("--copy-row-id", type=int, default=8000000000000009,
                        help="A known cold fixture packet with reconstructible reception copies")
    args = parser.parse_args()

    async def guarded():
        task = asyncio.current_task()
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, task.cancel)
        return await run(args)
    report = asyncio.run(guarded())
    print(json.dumps({"output": str(args.output), "all_checks_passed": report["all_checks_passed"],
                      "cleanup_complete": report.get("cleanup_complete", False)}))
    return 0 if report["all_checks_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
