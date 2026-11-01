#!/usr/bin/env python3
"""Compare a patched and pristine native Columnar planner on synthetic data.

Only two distinct isolated clusters are accepted. The verifier creates and
removes one unique schema on each; it never converts or inspects user tables.
No latency threshold substitutes for exact estimated-plan, cost and row parity.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import itertools
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import unquote, urlsplit
import uuid

import asyncpg


EPOCH = datetime(2020, 1, 1, tzinfo=timezone.utc)
COLUMNS = ("id", "stamp", "sender", "recipient", "payload", "packet_id")
TYPE_NAMES = {20: "bigint", 23: "integer", 25: "text", 1184: "timestamptz", 1016: "bigint[]"}
PLAN_KEYS = {
    "Node Type", "Parent Relationship", "Parallel Aware", "Async Capable", "Disabled",
    "Join Type", "Strategy", "Partial Mode", "Custom Plan Provider",
    "Relation Name", "Alias", "Schema", "Index Name", "Scan Direction",
    "Startup Cost", "Total Cost", "Plan Rows", "Plan Width", "Output",
    "Index Cond", "Recheck Cond", "Filter", "Join Filter", "Hash Cond",
    "Merge Cond", "Sort Key", "Presorted Key", "Group Key", "Inner Unique",
    "Subplan Name", "Function Name", "Function Call", "Cache Key", "Cache Mode",
    "Columnar Chunk Group Filters", "Columnar Projected Columns",
}
SETTINGS = {
    "application_name": "meshinfo-native-planner-parity",
    "statement_timeout": "5000", "lock_timeout": "500ms",
    "work_mem": "2MB", "maintenance_work_mem": "16MB",
    "max_parallel_workers_per_gather": "0", "jit": "off", "timezone": "UTC",
    "default_statistics_target": "1000", "random_page_cost": "4",
    "seq_page_cost": "1", "cpu_tuple_cost": "0.01", "cpu_index_tuple_cost": "0.005",
    "cpu_operator_cost": "0.0025", "effective_cache_size": "128MB",
}


def guarded_database(dsn: str) -> str:
    parts = urlsplit(dsn)
    database = unquote(parts.path.lstrip("/"))
    if (parts.scheme not in {"postgres", "postgresql"}
            or not re.fullmatch(r"(?:meshinfo_planner_test|[A-Za-z0-9_]+_columnar_test)", database)):
        raise ValueError("A dedicated meshinfo_planner_test or *_columnar_test database is required")
    return database


def fingerprint(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, default=lambda v: v.isoformat() if isinstance(v, datetime) else str(v)).encode()).hexdigest()


def plan_signature(plan: dict) -> dict:
    """Retain estimates/expressions/structure exactly; exclude runtime noise."""
    result = {key: value for key, value in plan.items() if key in PLAN_KEYS}
    if "Plans" in plan:
        result["Plans"] = [plan_signature(child) for child in plan["Plans"]]
    return result


def plan_differences(left, right, path="Plan", limit=20):
    differences = []
    if type(left) is not type(right):
        return [path + ": type differs"]
    if isinstance(left, dict):
        for key in sorted(left.keys() | right.keys()):
            if key not in left or key not in right:
                differences.append(path + "." + key + ": missing")
            else:
                differences.extend(plan_differences(left[key], right[key], path + "." + key, limit))
            if len(differences) >= limit:
                break
    elif isinstance(left, list):
        if len(left) != len(right):
            differences.append(path + ": length differs")
        for i, (a, b) in enumerate(zip(left, right)):
            differences.extend(plan_differences(a, b, f"{path}[{i}]", limit))
            if len(differences) >= limit:
                break
    elif left != right:
        differences.append(path + ": value differs")
    return differences[:limit]


def fixture_row(identity: int):
    return (identity, EPOCH + timedelta(microseconds=identity // 3),
        None if identity % 19 == 0 else f"node-{identity % 17:02d}",
        None if identity % 23 == 0 else f"node-{identity % 11:02d}",
        None if identity % 29 == 0 else json.dumps({"n": identity, "text": "café雪" * (identity % 9 + 1)}, ensure_ascii=False),
        None if identity % 7 == 0 else identity % 113)


async def copy_rows(connection, schema, table, rows):
    await connection.copy_records_to_table(table, schema_name=schema, columns=COLUMNS, records=rows, timeout=15)


async def fixture(connection, schema):
    await connection.execute(f'SET search_path="{schema}",public')
    definition = "(id bigint PRIMARY KEY,stamp timestamptz NOT NULL,sender text,recipient text,payload text,packet_id bigint)"
    for table in ("empty", "one", "many", "evolving"):
        await connection.execute(f'CREATE TABLE {table}{definition} USING columnar')
        await connection.execute(f"ALTER TABLE {table} SET(columnar.compression='zstd',"
            "columnar.compression_level=3,columnar.stripe_row_limit=1000,columnar.chunk_group_row_limit=1000)")
        await connection.execute(f"CREATE INDEX {table}_sender ON {table}(sender,stamp DESC,id DESC);"
            f"CREATE INDEX {table}_recipient ON {table}(recipient,stamp DESC,id DESC);"
            f"CREATE INDEX {table}_stamp ON {table}(stamp DESC,id DESC);"
            f"CREATE INDEX {table}_packet ON {table}(packet_id,id) WHERE packet_id IS NOT NULL")
    records = [fixture_row(i) for i in range(1, 6401)]
    await copy_rows(connection, schema, "one", records[:500])
    for offset in range(0, len(records), 64):
        await copy_rows(connection, schema, "many", records[offset:offset + 64])
    for offset in range(0, 640, 64):
        await copy_rows(connection, schema, "evolving", records[offset:offset + 64])
    await connection.execute("ALTER TABLE evolving ADD COLUMN extra text")
    for offset in range(640, 1280, 64):
        rows = [(*row, "added-column-雪") for row in records[offset:offset + 64]]
        await connection.copy_records_to_table("evolving", schema_name=schema,
            columns=(*COLUMNS, "extra"), records=rows, timeout=15)
    await connection.execute("CREATE TABLE dimension(node text PRIMARY KEY,label text);"
        "INSERT INTO dimension SELECT 'node-'||lpad(i::text,2,'0'),'label-'||i FROM generate_series(0,16) i")
    for table in ("empty", "one", "many", "evolving", "dimension"):
        await connection.execute(f"ANALYZE {table}", timeout=15)
    geometry = {}
    for table, expected in (("empty", []), ("one", records[:500]), ("many", records)):
        actual = [tuple(row) for row in await connection.fetch(f"SELECT * FROM {table} ORDER BY id")]
        if actual != expected:
            raise AssertionError("Synthetic fixture raw row mismatch")
    evolved = [(*row, None if i < 640 else "added-column-雪") for i, row in enumerate(records[:1280])]
    if [tuple(row) for row in await connection.fetch("SELECT * FROM evolving ORDER BY id")] != evolved:
        raise AssertionError("Historical stripe column values changed")
    for table in ("empty", "one", "many", "evolving"):
        geometry[table] = dict(await connection.fetchrow("SELECT count(*)::int AS stripes,"
            "coalesce(sum(row_count),0)::bigint AS rows,coalesce(sum(data_length),0)::bigint AS bytes,"
            "min(column_count)::int AS min_columns,max(column_count)::int AS max_columns "
            "FROM columnar.stripe WHERE relation=$1::regclass", table))
    if geometry["empty"]["stripes"] != 0 or geometry["one"]["stripes"] != 1 or geometry["many"]["stripes"] < 100:
        raise AssertionError("Fixture does not exercise empty/one/many stripes")
    if geometry["evolving"]["min_columns"] == geometry["evolving"]["max_columns"]:
        raise AssertionError("Fixture lacks different historical stripe column counts")
    stats = [tuple(row) for row in await connection.fetch("SELECT tablename,attname,null_frac,avg_width,n_distinct,"
        "most_common_vals::text,most_common_freqs::text,histogram_bounds::text,correlation "
        "FROM pg_stats WHERE schemaname=$1 ORDER BY tablename,attname", schema)]
    physical = [dict(row) for row in await connection.fetch("SELECT c.relname,c.relkind::text AS relkind,c.relpages,c.reltuples,"
        "c.relallvisible,c.relallfrozen,pg_relation_size(c.oid) AS bytes FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind IN ('r','i') ORDER BY c.relname", schema)]
    return {"geometry": geometry, "statistics_sha256": fingerprint(stats), "relations": physical}


def query_cases():
    return [
        ("empty_count", "SELECT count(*) FROM empty", ()),
        ("empty_point", "SELECT id,payload FROM empty WHERE id=$1", (17,)),
        ("one_point", "SELECT id,payload FROM one WHERE id=$1", (17,)),
        ("one_projection", "SELECT sender,count(*) FROM one GROUP BY sender ORDER BY sender NULLS FIRST", ()),
        ("many_point", "SELECT id,payload FROM many WHERE id=$1", (17,)),
        ("many_array", "SELECT id,payload FROM many WHERE id=ANY($1::bigint[]) ORDER BY id", ([1, 17, 1700, 6399, 99999],)),
        ("many_range", "SELECT id,payload FROM many WHERE id BETWEEN $1 AND $2 ORDER BY id", (120, 301)),
        ("many_node", "SELECT id,payload FROM many WHERE sender=$1 ORDER BY stamp DESC,id DESC LIMIT $2", ("node-03", 31)),
        ("many_or", "SELECT id,payload FROM many WHERE sender=$1 OR recipient=$1 ORDER BY stamp DESC,id DESC LIMIT $2", ("node-03", 31)),
        ("many_search", "SELECT id FROM many WHERE payload ILIKE $1 ORDER BY id", ("%雪%",)),
        ("many_partial", "SELECT id,packet_id FROM many WHERE packet_id=$1 ORDER BY id", (7,)),
        ("many_alias", "SELECT a.id,a.sender FROM many a WHERE a.stamp>=$1 AND a.id<=$2 ORDER BY a.id", (EPOCH, 80)),
        ("many_self_join", "SELECT a.id,b.id FROM many a JOIN many b ON a.sender=b.sender "
            "WHERE a.id<=$1 AND b.id<=$2 ORDER BY a.id,b.id", (8, 80)),
        ("dimension_join", "SELECT a.id,d.label FROM many a JOIN dimension d ON a.sender=d.node "
            "WHERE d.node=$1 ORDER BY a.stamp DESC,a.id DESC LIMIT $2", ("node-03", 31)),
        ("lateral_alias", "SELECT d.node,a.id FROM dimension d CROSS JOIN LATERAL "
            "(SELECT id FROM many a WHERE a.sender=d.node ORDER BY a.stamp DESC,a.id DESC LIMIT 2) a "
            "WHERE d.node IN ($1,$2) ORDER BY d.node,a.id", ("node-03", "node-04")),
        ("evolving_columns", "SELECT id,extra FROM evolving WHERE id BETWEEN $1 AND $2 ORDER BY id", (630, 650)),
        ("evolving_projection", "SELECT extra,count(*),sum(id) FROM evolving GROUP BY extra ORDER BY extra NULLS FIRST", ()),
    ]


async def measured(connection, sql, parameters, flags, mode):
    custom, seq, index = flags
    async with connection.transaction():
        await connection.execute(f"SET LOCAL columnar.enable_custom_scan={custom}; SET LOCAL enable_seqscan={seq};"
            f"SET LOCAL enable_indexscan={index}; SET LOCAL plan_cache_mode={mode}")
        prepared = await connection.prepare(sql)
        name = prepared.get_name()
        if not re.fullmatch(r"[A-Za-z0-9_]+", name):
            raise RuntimeError("Invalid generated statement name")
        literals = []
        for datatype, parameter in zip(prepared.get_parameters(), parameters):
            typename = TYPE_NAMES[datatype.oid]
            literals.append(await connection.fetchval(f"SELECT quote_nullable($1::{typename})", parameter) + "::" + typename)
        arguments = "(" + ",".join(literals) + ")" if literals else ""
        explain = "EXPLAIN (ANALYZE,BUFFERS,MEMORY,VERBOSE,FORMAT JSON) EXECUTE " + name + arguments
        first = json.loads(await connection.fetchval(explain))[0]
        rows = [tuple(row) for row in await prepared.fetch(*parameters)]
        warm = json.loads(await connection.fetchval(explain))[0]
        counts = dict(await connection.fetchrow("SELECT generic_plans,custom_plans FROM pg_prepared_statements WHERE name=$1", name))
        if parameters and mode == "force_generic_plan" and counts != {"generic_plans": 3, "custom_plans": 0}:
            raise AssertionError("Generic prepared-plan reuse was not observed")
        return {"plan": plan_signature(first["Plan"]), "warm_plan": plan_signature(warm["Plan"]),
            "rows": len(rows), "response_sha256": fingerprint(rows), "prepared_counts": counts,
            "first_planning_ms": first["Planning Time"], "first_execution_ms": first["Execution Time"],
            "first_planner_memory": first.get("Planning", {}),
            "warm_planning_ms": warm["Planning Time"], "warm_execution_ms": warm["Execution Time"],
            "warm_planner_memory": warm.get("Planning", {})}


async def compare_case(connections, report, name, sql, parameters, flags, mode, state="initial"):
    case = {"name": name, "state": state, "custom_scan": flags[0], "seqscan": flags[1],
        "indexscan": flags[2], "plan_cache_mode": mode, "passed": False}
    report["cases"].append(case)
    values = []
    for label, connection in zip(("baseline", "patched"), connections):
        try:
            values.append(await measured(connection, sql, parameters, flags, mode))
        except Exception as error:
            case[label + "_error"] = {"type": type(error).__name__, "sqlstate": getattr(error, "sqlstate", None)}
            values.append(None)
    if None in values:
        return
    baseline, patched = values
    differences = plan_differences(baseline["plan"], patched["plan"])
    warm_differences = plan_differences(baseline["warm_plan"], patched["warm_plan"])
    case["plan_differences"] = differences
    case["warm_plan_differences"] = warm_differences
    case["response_equal"] = (baseline["rows"], baseline["response_sha256"]) == (patched["rows"], patched["response_sha256"])
    case["passed"] = not differences and not warm_differences and case["response_equal"]
    for label, value in zip(("baseline", "patched"), values):
        case[label] = {k: v for k, v in value.items() if k not in {"plan", "warm_plan"}}
    if differences or warm_differences:
        case["baseline_plan"] = baseline["plan"]
        case["patched_plan"] = patched["plan"]
    else:
        case["equal_plan"] = baseline["plan"]


async def subtransactions(connections, schema, report):
    outer = [connection.transaction() for connection in connections]
    active = []
    committed = [fixture_row(i) for i in range(7001, 7065)]
    aborted = [fixture_row(i) for i in range(8001, 8065)]
    try:
        for transaction in outer:
            await transaction.start()
            active.append(transaction)
        for connection in connections:
            async with connection.transaction():
                await copy_rows(connection, schema, "many", committed)
        for state in ("committed_savepoint", "aborted_savepoint"):
            if state == "aborted_savepoint":
                for connection in connections:
                    child = connection.transaction()
                    await child.start()
                    await copy_rows(connection, schema, "many", aborted)
                    await child.rollback()
            for flags in itertools.product(("on", "off"), repeat=3):
                await compare_case(connections, report, "visible_rows_after_savepoint",
                    "SELECT id,payload FROM many WHERE id>=$1 ORDER BY id", (7000,), flags,
                    "force_custom_plan", state)
            for connection in connections:
                actual = [tuple(row) for row in await connection.fetch("SELECT * FROM many WHERE id>=7000 ORDER BY id")]
                if actual != committed:
                    raise AssertionError("Savepoint visibility differs from independent fixture rows")
        for transaction in outer:
            await transaction.commit()
            active.remove(transaction)
        for connection in connections:
            if await connection.fetchval("SELECT count(*) FROM many") != 6464:
                raise AssertionError("Outer commit lost committed or exposed aborted rows")
        await compare_case(connections, report, "visible_rows_after_outer_commit",
            "SELECT id,payload FROM many WHERE id>=$1 ORDER BY id", (7000,), ("off", "on", "on"),
            "force_generic_plan", "outer_committed")
    finally:
        for transaction in reversed(active):
            await transaction.rollback()


async def repeatable_snapshot(connections, dsns, schema, report):
    """A separate committed writer must not leak into an established snapshot."""
    active = []
    writers = []
    additions = [fixture_row(i) for i in range(9001, 9065)]
    sql = "SELECT id,payload FROM many WHERE id>=$1 ORDER BY id"
    try:
        for connection in connections:
            transaction = connection.transaction(isolation="repeatable_read", readonly=True)
            await transaction.start()
            active.append(transaction)
            if await connection.fetchval("SELECT count(*) FROM many") != 6464:
                raise AssertionError("Unexpected pre-writer fixture count")
        await compare_case(connections, report, "concurrent_writer_snapshot", sql, (9000,),
            ("off", "on", "on"), "force_custom_plan", "snapshot_before_writer")
        for dsn in dsns:
            writer = await asyncpg.connect(dsn, timeout=5, command_timeout=5, server_settings=SETTINGS)
            writers.append(writer)
            await copy_rows(writer, schema, "many", additions)
            if await writer.fetchval(f'SELECT count(*) FROM "{schema}".many') != 6528:
                raise AssertionError("Separate writer did not commit all synthetic rows")
        await compare_case(connections, report, "concurrent_writer_snapshot", sql, (9000,),
            ("off", "on", "on"), "force_custom_plan", "snapshot_after_writer_commit")
        for connection in connections:
            if await connection.fetchval("SELECT count(*) FROM many") != 6464:
                raise AssertionError("Committed writer leaked into repeatable-read visibility")
            if await connection.fetch("SELECT id FROM many WHERE id>=9000"):
                raise AssertionError("Repeatable snapshot exposed newer canonical identities")
        for transaction in list(active):
            await transaction.commit()
            active.remove(transaction)
        await compare_case(connections, report, "concurrent_writer_snapshot", sql, (9000,),
            ("off", "on", "on"), "force_custom_plan", "snapshot_released")
        for connection in connections:
            actual = [tuple(row) for row in await connection.fetch("SELECT * FROM many WHERE id>=9000 ORDER BY id")]
            if actual != additions:
                raise AssertionError("Fresh snapshot failed to expose exact committed writer rows")
    finally:
        cleanup_errors = []
        for transaction in reversed(active):
            try:
                await transaction.rollback()
            except Exception as error:
                cleanup_errors.append(type(error).__name__)
        for writer in writers:
            try:
                await writer.close(timeout=3)
            except Exception as error:
                writer.terminate()
                cleanup_errors.append(type(error).__name__)
        if cleanup_errors:
            report["snapshot_cleanup_errors"] = cleanup_errors
            raise RuntimeError("Snapshot proof cleanup failed")


async def run(args):
    dsns = [os.environ[args.baseline_dsn_env], os.environ[args.patched_dsn_env]]
    databases = [guarded_database(dsn) for dsn in dsns]
    if dsns[0] == dsns[1]:
        raise ValueError("Two distinct isolated clusters are required")
    schema = "native_cost_test_" + uuid.uuid4().hex[:12]
    report = {"synthetic_only": True, "schema": schema, "cases": [], "all_checks_passed": False,
        "limits": SETTINGS, "comparison": "Exact EXPLAIN estimated plans/costs and ordered row digests; timings are descriptive only."}
    connections = []
    created = []
    try:
        identities = []
        versions = []
        for dsn, database in zip(dsns, databases):
            connection = await asyncpg.connect(dsn, timeout=5, command_timeout=5, server_settings=SETTINGS)
            connections.append(connection)
            if await connection.fetchval("SELECT current_database()") != database:
                raise ValueError("Connected database differs from guarded target")
            identities.append(await connection.fetchval("SELECT system_identifier::text FROM pg_control_system()"))
            version = dict(await connection.fetchrow("SELECT current_setting('server_version_num') AS postgres,"
                "(SELECT extversion FROM pg_extension WHERE extname='citus_columnar') AS columnar"))
            if not version["postgres"].startswith("18") or not (version["columnar"] or "").startswith("14.1"):
                raise ValueError("The paired proof requires PostgreSQL18 and Columnar14.1 installed")
            versions.append(version)
        if identities[0] == identities[1] or versions[0] != versions[1]:
            raise ValueError("Distinct clusters with identical PostgreSQL/Columnar versions are required")
        report["versions"] = versions
        fixtures = []
        for connection in connections:
            await connection.execute(f'CREATE SCHEMA "{schema}"')
            created.append(connection)
            fixtures.append(await fixture(connection, schema))
        report["fixtures"] = fixtures
        if fixtures[0] != fixtures[1]:
            raise AssertionError("Fixture geometry/statistics differ; cost comparison would be misleading")
        for mode in ("force_custom_plan", "force_generic_plan"):
            for flags in itertools.product(("on", "off"), repeat=3):
                for name, sql, parameters in query_cases():
                    await compare_case(connections, report, name, sql, parameters, flags, mode)
        await subtransactions(connections, schema, report)
        await repeatable_snapshot(connections, dsns, schema, report)
        report["all_checks_passed"] = bool(report["cases"]) and all(case["passed"] for case in report["cases"])
    except Exception as error:
        report["error"] = {"type": type(error).__name__, "sqlstate": getattr(error, "sqlstate", None)}
    finally:
        cleanup = []
        for connection in created:
            try:
                # A failed second transaction start or connection error must
                # never let DROP appear successful inside a doomed outer TX.
                if connection.is_in_transaction():
                    await connection.execute("ROLLBACK", timeout=5)
                await connection.execute(f'DROP SCHEMA "{schema}" CASCADE', timeout=20)
                cleanup.append(True)
            except Exception as error:
                cleanup.append(False)
                report.setdefault("cleanup_errors", []).append(type(error).__name__)
                report["all_checks_passed"] = False
        report["owned_schemas_removed"] = cleanup
        for connection in connections:
            try:
                await connection.close(timeout=3)
            except Exception as error:
                connection.terminate()
                report.setdefault("close_errors", []).append(type(error).__name__)
                report["all_checks_passed"] = False
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dsn-env", required=True)
    parser.add_argument("--patched-dsn-env", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    # Reserve the report before any database writes; never follow or replace a file.
    descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as output:
        try:
            report = asyncio.run(run(args))
        except Exception as error:
            report = {"all_checks_passed": False, "error": {"type": type(error).__name__}}
        # Render before writing so an unexpected codec value cannot leave a
        # deceptively complete-looking, truncated proof artifact.
        try:
            rendered = json.dumps(report, indent=2)
        except (TypeError, ValueError) as error:
            report = {"all_checks_passed": False, "error": {"type": "ReportSerializationError"},
                "completed_cases": len(report.get("cases", [])), "schema": report.get("schema"),
                "owned_schemas_removed": report.get("owned_schemas_removed", [])}
            rendered = json.dumps(report, indent=2)
        output.write(rendered)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    print(json.dumps({"all_checks_passed": report["all_checks_passed"],
        "cases": len(report.get("cases", [])), "failed_cases": sum(not c["passed"] for c in report.get("cases", []))}))
    return 0 if report["all_checks_passed"] else 2


if __name__ == "__main__":
    sys.exit(main())
