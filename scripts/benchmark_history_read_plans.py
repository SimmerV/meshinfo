#!/usr/bin/env python3
"""Compare real packet readers' bounded plans on existing isolated fixtures.

This tool is strictly read-only and refuses database names without the suffix
_columnar_test. It never creates tables, loads fixtures, or connects to source
storage. Supply identically populated packet schemas and their shared receipt
schema. Query arguments stay private; reports contain counts, hashes and plans.
"""

import argparse
import asyncio
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import sys
import time
from unittest.mock import patch
from urllib.parse import unquote, urlsplit

import asyncpg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.benchmark_columnar_history import normalize, plan_summary, qi
from storage.db.history_queries import (
    HistoryReadError, packet_page_sql, packet_payload_sql, ordered_packet_rows,
    literal_topic_pattern, search_condition, fetch_topic_packet_page,
)
from storage.db.postgres import PostgresStorage

PLANNER_PROFILES = {
    "default": {},
    "custom": {"plan_cache_mode": "force_custom_plan"},
    "columnar_off": {"plan_cache_mode": "force_custom_plan", "columnar.enable_custom_scan": "off"},
    "custom_no_seq": {"plan_cache_mode": "force_custom_plan", "enable_seqscan": "off"},
    "index": {"plan_cache_mode": "force_custom_plan", "columnar.enable_custom_scan": "off", "enable_seqscan": "off"},
}


async def apply_profile(connection, profile):
    applied = {}
    for name, value in PLANNER_PROFILES[profile].items():
        if name.startswith("columnar."):
            if await connection.fetchval("SELECT current_setting($1,true)", name) is None:
                continue
        await connection.execute("SELECT set_config($1,$2,true)", name, value)
        applied[name] = value
    return applied


def result_digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), default=normalize).encode()).hexdigest()


def schema_name(value, *, allow_public=False):
    if allow_public and value == "public":
        return value
    if not re.fullmatch(r"(?:benchmark|history)_[a-z0-9_]{1,48}", value):
        raise ValueError("Only explicitly isolated benchmark/history schemas are allowed")
    return value


class RecordedAcquisition:
    def __init__(self, pool, captured, **kwargs):
        self.acquisition = pool.acquire(**kwargs)
        self.captured = captured

    async def __aenter__(self):
        self.connection = await self.acquisition.__aenter__()
        self._profile_transaction = None
        self._recorded_settings = {}
        transaction_entered = False
        profile = self.captured.get("planner_profile", "default")
        try:
            if profile != "default":
                self._profile_transaction = self.connection.transaction(isolation="repeatable_read", readonly=True)
                await self._profile_transaction.__aenter__()
                transaction_entered = True
                self._recorded_settings.update(await apply_profile(self.connection, profile))
        except BaseException:
            details = sys.exc_info()
            try:
                if transaction_entered:
                    await self._profile_transaction.__aexit__(*details)
            finally:
                await self.acquisition.__aexit__(*details)
            raise
        return self

    async def __aexit__(self, *exc):
        try:
            if self._profile_transaction is not None:
                await self._profile_transaction.__aexit__(*exc)
        finally:
            await self.acquisition.__aexit__(*exc)
        return False

    async def fetch(self, query, *parameters, **kwargs):
        query += " /* history planner profile: " + self.captured.get("planner_profile", "default") + " */"
        self.captured.update(sql=query, parameters=parameters, statement_settings=dict(self._recorded_settings))
        return await self.connection.fetch(query, *parameters, **kwargs)

    async def execute(self, query, *parameters, **kwargs):
        result = await self.connection.execute(query, *parameters, **kwargs)
        setting = re.fullmatch(r"SET LOCAL ([a-z_.]+)\s*=\s*([a-z_]+)", query)
        if setting:
            self._recorded_settings[setting.group(1)] = setting.group(2)
        return result

    def __getattr__(self, name):
        return getattr(self.connection, name)


class RecordedPool:
    def __init__(self, pool, captured):
        self.pool, self.captured = pool, captured

    def acquire(self, **kwargs):
        return RecordedAcquisition(self.pool, self.captured, **kwargs)


async def open_storage(dsn, schema, shared, timeout):
    async def setup(connection):
        await connection.execute("SELECT set_config('search_path',$1,false)",
                                 f"{qi(schema)},{qi(shared)},public")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=1, setup=setup,
        command_timeout=timeout, server_settings={"default_transaction_read_only": "on",
            "application_name": "meshinfo-history-read-plans", "timezone": "UTC",
            "work_mem": "4MB", "statement_timeout": str(timeout * 1000),
            "lock_timeout": "250ms", "max_parallel_workers_per_gather": "0", "jit": "off"})
    storage = PostgresStorage({"server": {"timezone": "UTC"},
                              "storage": {"postgres": {"enabled": True}}})
    captured = {}
    storage.pool = RecordedPool(pool, captured)
    return storage, pool, captured


def envelope_payload_sql(keys, reception_count_sql):
    """Retain the rejected range-pruning experiment only in the benchmark."""
    query, parameters = packet_payload_sql(keys, reception_count_sql)
    ordered_ids = sorted({row["id"] for row in keys})
    gaps = [(ordered_ids[index+1]-value, index+1) for index, value in enumerate(ordered_ids[:-1])
            if ordered_ids[index+1] > value+1]
    edges = [0, *sorted(index for _, index in sorted(gaps, reverse=True)[:15]), len(ordered_ids)]
    clauses = []
    for start, end in zip(edges, edges[1:]):
        parameter = len(parameters)+1
        clauses.append(f"(id>=${parameter}::bigint AND id<=${parameter+1}::bigint)")
        parameters.extend((ordered_ids[start], ordered_ids[end-1]))
    return query.replace("ORDER BY id,created_at", "AND (" + " OR ".join(clauses) + ") ORDER BY id,created_at"), parameters


async def bound_key_page(storage, pool, captured, method, arguments, *, profile, lookup=False, envelopes=False):
    """Experimental two-statement page, compared with the real reader response."""
    conditions, parameters, search_parameter = [], [], None
    node = method == "query_node_mqtt_messages"
    if node:
        conditions.append("(from_node_id = $1 OR to_node_id = $1)")
        parameters.append(storage._normalize_node_id(arguments["node_id"]) or arguments["node_id"])
    if arguments.get("search"):
        search_parameter = len(parameters)+1
        conditions.append(search_condition(search_parameter, storage._reception_lookback))
        parameters.append(arguments["search"])
    if arguments.get("topic"):
        conditions.append(f"topic ILIKE '%' || ${len(parameters)+1} || '%' ESCAPE '\\'")
        parameters.append(literal_topic_pattern(arguments["topic"]))
    limit_parameter = storage._append_window_conditions(conditions, parameters, len(parameters)+1,
        arguments.get("start"), arguments.get("end"), arguments.get("before"))
    parameters.append(arguments["limit"]+1)
    if lookup:
        # This function locks both parents and establishes a repeatable snapshot.
        # The wrapper captures the final payload plan, including bound arrays.
        async with storage.pool.acquire(timeout=3) as connection:
            if envelopes:
                with patch("storage.db.history_queries.packet_payload_sql", side_effect=envelope_payload_sql):
                    rows = await fetch_topic_packet_page(connection, conditions, parameters,
                        limit_parameter, storage._reception_count_sql, indexed_payload=False)
            else:
                rows = await fetch_topic_packet_page(connection, conditions, parameters,
                    limit_parameter, storage._reception_count_sql, indexed_payload=False)
        return storage._build_mqtt_message_page(rows, arguments["limit"])
    async with pool.acquire(timeout=3) as connection:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            await connection.execute("SET LOCAL plan_cache_mode = force_custom_plan")
            applied = await apply_profile(connection, profile)
            captured["statement_settings"] = {"plan_cache_mode": "force_custom_plan", **applied}
            await connection.execute("LOCK TABLE ONLY mqtt_messages IN ACCESS SHARE MODE")
            sql = packet_page_sql(conditions, limit_parameter, storage._reception_count_sql,
                                  strategy="node_rows" if node else "direct",
                                  search_parameter=search_parameter, keys_only=True)
            keys = await connection.fetch(sql, *parameters)
            captured.update(sql=sql, parameters=parameters)
            if not keys:
                return {"messages": [], "next_cursor": None}
            sql, parameters = (envelope_payload_sql if envelopes else packet_payload_sql)(keys, storage._reception_count_sql)
            rows = await connection.fetch(sql, *parameters)
            captured.update(sql=sql, parameters=parameters)
            return storage._build_mqtt_message_page(ordered_packet_rows(keys, rows), arguments["limit"])


async def run(args):
    if not re.fullmatch(r"[A-Z][A-Z0-9_]*", args.dsn_env):
        raise ValueError("Invalid DSN environment-variable name")
    dsn = os.environ.get(args.dsn_env, "")
    database = unquote(urlsplit(dsn).path.lstrip("/"))
    if not re.fullmatch(r"[A-Za-z0-9_]+_columnar_test", database):
        raise ValueError("Only dedicated *_columnar_test databases are allowed")
    schemas = {}
    for item in args.packet_schema:
        label, separator, schema = item.partition("=")
        if not separator or not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", label) or label in schemas:
            raise ValueError("Packet schemas must have unique label=schema arguments")
        # Public is permitted only after the dedicated database-name guard.
        # The real archiver uses this canonical schema in its isolated target.
        schemas[label] = schema_name(schema, allow_public=True)
    if not 1 <= len(schemas) <= 3:
        raise ValueError("One to three isolated packet schemas are required")
    shared = schema_name(args.shared_schema)
    if not 1 <= args.reception_lookback_seconds <= 604800:
        raise ValueError("Reception lookback must be between one second and seven days")
    hot = datetime.fromisoformat(args.hot_month + "T00:00:00+00:00")
    if hot.day != 1:
        raise ValueError("Hot month must be a UTC month boundary")
    deadline = args.query_timeout_seconds
    opened = {}
    try:
        counts = {}
        seeds = None
        for label, schema in schemas.items():
            opened[label] = await open_storage(dsn, schema, shared, deadline)
            storage, pool, _ = opened[label]
            storage._reception_lookback = args.reception_lookback_seconds
            storage._reception_count_sql = (
                "(SELECT count(*)::int FROM packet_receptions pr WHERE pr.mqtt_row_id=mqtt_messages.id "
                "AND pr.created_at>=mqtt_messages.created_at "
                f"AND pr.created_at<mqtt_messages.created_at+interval '{args.reception_lookback_seconds} seconds')")
            async with pool.acquire() as connection:
                if await connection.fetchval("SELECT current_database()") != database:
                    raise ValueError("Connected database differs from guarded target")
                if not args.skip_data_probes:
                    counts[label] = dict(await connection.fetchrow("""SELECT count(*) AS packets,
                        count(*) FILTER (WHERE created_at<$1) AS archived FROM mqtt_messages""", hot))
                if seeds is None and not args.skip_data_probes:
                    seeds = await connection.fetch("""SELECT from_node_id,to_node_id,left(topic,256) AS topic
                        FROM mqtt_messages WHERE created_at<$1
                        ORDER BY created_at DESC,id DESC LIMIT 1000""", hot)
        if counts and len({tuple(value.values()) for value in counts.values()}) != 1:
            raise ValueError("Packet schema row counts differ")
        if not seeds and not args.skip_data_probes:
            raise ValueError("An archived fixture is required")
        seeds = seeds or []
        nodes = Counter(value for row in seeds for value in (row["from_node_id"], row["to_node_id"])
                        if value and value != "ffffffff")
        topics = Counter(row["topic"] for row in seeds if row["topic"])
        node = args.node_id or (nodes.most_common(1)[0][0] if nodes else None)
        topic = args.topic_term or (topics.most_common(1)[0][0] if topics else None)
        archived = {"end": hot - timedelta(microseconds=1), "limit": args.limit}
        cases = [
            ("recent_page", "query_mqtt_messages", {"start": hot, "limit": args.limit}),
            ("archive_page", "query_mqtt_messages", archived),
            ("archive_payload_common", "query_mqtt_messages", {**archived, "search": args.payload_term}),
            ("archive_payload_selective", "query_mqtt_messages", {**archived, "search": args.selective_term}),
            ("archive_search_absent", "query_mqtt_messages", {**archived, "search": "meshinfo-read-plan-absent-73cd9125"}),
            ("archive_reception_search", "query_mqtt_messages", {**archived, "search": args.receipt_search}),
        ]
        if node:
            cases.append(("archive_node", "query_node_mqtt_messages", {**archived, "node_id": node}))
        if args.selective_node_id:
            cases.append(("archive_node_selective", "query_node_mqtt_messages",
                          {**archived, "node_id": args.selective_node_id}))
        if topic:
            cases.append(("archive_literal_topic", "query_mqtt_messages", {**archived, "topic": topic}))
        if args.selective_topic:
            cases.append(("archive_literal_topic_selective", "query_mqtt_messages",
                          {**archived, "topic": args.selective_topic}))
            cases.append(("archive_topic_payload_common", "query_mqtt_messages",
                          {**archived, "topic": args.selective_topic, "search": args.payload_term}))
            cases.append(("archive_topic_payload_absent", "query_mqtt_messages",
                          {**archived, "topic": args.selective_topic, "search": "meshinfo-topic-payload-absent-2ce9a8"}))
        results = {}
        if args.case:
            cases = [case for case in cases if case[0] in args.case]
        profiles = args.planner_profile or ["default"]
        for case_number, (case_name, method, arguments) in enumerate(cases):
            targets = [(label, strategy, profile) for label in schemas
                       for strategy in (("production", "bound_keys", "bound_envelopes", "direct", "candidates", "candidate_array", "node_branches", "node_branch_array", "node_rows")
                                        if method == "query_node_mqtt_messages"
                                        else (("production", "bound_keys", "bound_envelopes", "lookup_keys", "lookup_envelopes", "direct", "candidates", "candidate_array")
                                              if not arguments.get("search") else
                                              ("production", "bound_keys", "direct", "candidates", "candidate_array")))
                       if not args.strategy or strategy in args.strategy
                       for profile in profiles]
            if not targets:
                continue
            records = {(label, strategy, profile): {"times_ms": [], "status": "passed", "planner_profile": profile}
                       for label, strategy, profile in targets}
            reference = None
            for iteration in range(args.repeats + 1):
                offset = (case_number + iteration) % len(targets)
                for label, strategy, profile in targets[offset:] + targets[:offset]:
                    record = records[label, strategy, profile]
                    if record["status"] != "passed":
                        continue
                    storage, pool, captured = opened[label]
                    captured["planner_profile"] = profile
                    def build(conditions, limit_parameter, reception_count_sql, **_production_options):
                        if strategy == "production":
                            return packet_page_sql(conditions, limit_parameter, reception_count_sql,
                                                   **_production_options)
                        _production_options.pop("strategy", None)
                        return packet_page_sql(conditions, limit_parameter, reception_count_sql,
                                               strategy=strategy, **_production_options)
                    started = time.perf_counter()
                    try:
                        if strategy in {"bound_keys", "lookup_keys", "bound_envelopes", "lookup_envelopes"}:
                            value = await bound_key_page(storage, pool, captured, method, arguments,
                                                         profile=profile, lookup=strategy.startswith("lookup_"),
                                                         envelopes=strategy.endswith("_envelopes"))
                        else:
                            with patch("storage.db.postgres.packet_page_sql", side_effect=build):
                                value = await getattr(storage, method)(**arguments)
                        elapsed = (time.perf_counter() - started) * 1000
                        fingerprint = result_digest(value)
                        if reference is None:
                            reference = fingerprint
                        if fingerprint != reference:
                            raise AssertionError("Reader responses differ across storage or plans")
                        if case_name in {"recent_page", "archive_page", "archive_node", "archive_literal_topic",
                                         "archive_node_selective", "archive_literal_topic_selective", "archive_topic_payload_common"}:
                            if not value["messages"]:
                                raise AssertionError("Required positive query witness is empty")
                        record.update(sha256=fingerprint, rows=len(value["messages"]))
                        record["times_ms"].append(round(elapsed, 3))
                        if "plan" not in record:
                            async with pool.acquire() as connection:
                                async with connection.transaction(readonly=True):
                                    await apply_profile(connection, profile)
                                    for name, setting in captured.get("statement_settings", {}).items():
                                        await connection.execute("SELECT set_config($1,$2,true)", name, setting)
                                    plan = json.loads(await connection.fetchval(
                                        "EXPLAIN (FORMAT JSON) " + captured["sql"], *captured["parameters"]))[0]["Plan"]
                            record["plan"] = plan_summary(plan)
                            record["statement_settings"] = captured.get("statement_settings", {})
                    except (HistoryReadError, asyncpg.PostgresError, TimeoutError, AssertionError) as exc:
                        record.update(status="failed", error=type(exc).__name__,
                                      timed_out=isinstance(exc, TimeoutError) or getattr(exc, "timed_out", False),
                                      elapsed_ms=round((time.perf_counter()-started)*1000, 3))
            for (label, strategy, profile), record in records.items():
                samples = record.pop("times_ms")
                record["first_ms"] = samples[0] if samples else None
                record["warm_median_ms"] = round(statistics.median(samples[1:]), 3) if len(samples) > 1 else None
                record["successful_executions"] = len(samples)
                record["samples_ms"] = samples
                record["max_ms"] = max(samples) if samples else None
                key = strategy if profile == "default" else strategy + "/" + profile
                results.setdefault(label, {}).setdefault(case_name, {})[key] = record
            print(f"Measured {case_name}", file=sys.stderr, flush=True)
        passed = all(result["status"] == "passed" for cases in results.values()
                     for strategies in cases.values() for result in strategies.values())
        return {"all_checks_passed": passed, "read_only": True, "fixture_counts": counts,
                "cache_state": args.cache_state, "preflight_data_probes": not args.skip_data_probes,
                "planner_profiles": profiles,
                "query_timeout_seconds": deadline, "page_limit": args.limit, "repeats": args.repeats,
                "results": results, "limits": [
                    "Existing isolated fixtures only; storage loading and migration are not measured.",
                    "First and repeated queries use shared local caches; disk-scale latency is not established.",
                    "Bound-key experiments force custom planning and fetch one bounded key page before payloads.",
                    "Response hashes compare all completed plans; timed-out plans remain failed gates."]}
    finally:
        for _, pool, _ in opened.values():
            await pool.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn-env", required=True)
    parser.add_argument("--packet-schema", action="append", required=True, help="label=isolated_schema")
    parser.add_argument("--shared-schema", required=True)
    parser.add_argument("--hot-month", required=True, help="YYYY-MM-01")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--query-timeout-seconds", type=int, choices=(2, 5, 10), default=5)
    parser.add_argument("--limit", type=int, choices=(50, 200, 1000), default=200)
    parser.add_argument("--repeats", type=int, choices=(2, 3, 5), default=3)
    parser.add_argument("--skip-data-probes", action="store_true",
                        help="Skip row-count and seed reads for an independently verified first-query fixture")
    parser.add_argument("--cache-state", choices=("shared-local-caches", "postgres-restarted-os-cache-warm"),
                        default="shared-local-caches",
                        help="An externally established cache state; this script never restarts PostgreSQL")
    parser.add_argument("--planner-profile", action="append", choices=tuple(PLANNER_PROFILES),
                        help="Repeat to compare transaction-local planner settings")
    parser.add_argument("--strategy", action="append",
                        choices=("production", "bound_keys", "bound_envelopes", "lookup_keys", "lookup_envelopes", "direct", "candidates", "candidate_array", "node_branches", "node_branch_array", "node_rows"),
                        help="Restrict the plan matrix; repeat as needed")
    parser.add_argument("--case", action="append", choices=(
        "recent_page", "archive_page", "archive_payload_common", "archive_payload_selective",
        "archive_search_absent", "archive_reception_search", "archive_node", "archive_node_selective",
        "archive_literal_topic", "archive_literal_topic_selective", "archive_topic_payload_common",
        "archive_topic_payload_absent"))
    parser.add_argument("--reception-lookback-seconds", type=int, default=86400)
    parser.add_argument("--node-id")
    parser.add_argument("--selective-node-id", default="c0decafe")
    parser.add_argument("--topic-term")
    parser.add_argument("--selective-topic", default="msh/prototype/%_\\literal")
    parser.add_argument("--payload-term", default="from")
    parser.add_argument("--selective-term", default="Unicode")
    parser.add_argument("--receipt-search", default="columnar-reception-only-witness")
    args = parser.parse_args()
    try:
        report = asyncio.run(run(args))
    except Exception as exc:
        print(f"Read-plan benchmark failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"output": str(args.output), "all_checks_passed": report["all_checks_passed"]}))
    return 0 if report["all_checks_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
