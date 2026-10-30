#!/usr/bin/env python3
"""Isolated PostgreSQL columnar-history feasibility benchmark.

This is an operator prototype, not an application migration. It only writes to
a database whose name ends in ``_columnar_test`` and creates unique benchmark
schemas. The source fixture is a bounded, read-only export prepared separately.
No connection to a live source database is opened by this program.

Fixture directory:
  manifest.json: version=1, packet_columns, reception_columns, topic_columns;
                 source_packet_indexes; optional hot_month, query_terms
  packets.jsonl / receptions.jsonl / topics.jsonl: ordered JSON arrays
  trigger.sql: the application's exact BEFORE INSERT function/trigger SQL

Datetime fields must be timezone-aware ISO8601 strings. Payload and extras must
remain strings; in particular, do not parse and reserialize the payload JSON.
The three packet variants share one heap reception/topic dataset. Storage is
reported as separate components and hypothetical single-deployment totals, not
as the size of the entire database containing all three copies.
"""

from __future__ import annotations

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
from urllib.parse import unquote, urlsplit
import uuid

import asyncpg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from storage.db.compression import MQTT_COLUMNS

UTC = timezone.utc
MIB = 1024 ** 2
MAX_FIXTURE_BYTES = 128 * MIB
RECEPTION_COLUMNS = (
    "mqtt_row_id", "created_at", "gateway", "topic_id", "rx_rssi", "rx_snr",
    "rx_time", "hop_limit", "hops_away", "relay_node", "transport", "extras",
)
TOPIC_COLUMNS = ("id", "topic")
VARIANTS = ("heap_full", "heap_compatible", "columnar")


def qi(name):
    return '"' + name.replace('"', '""') + '"'


def qualified(schema, relation):
    return qi(schema) + "." + qi(relation)


def normalize(value):
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat(timespec="microseconds")
    raise TypeError(type(value).__name__)


def canonical(row):
    return json.dumps(tuple(row), ensure_ascii=False, separators=(",", ":"),
                      default=normalize, allow_nan=False).encode()


def logical_row(row, columns):
    """Compare JSONB as stored JSON values, while preserving raw payload TEXT."""
    values = list(row)
    if "extras" in columns:
        index = columns.index("extras")
        if values[index] is not None:
            values[index] = json.loads(values[index])
    return values


class MultisetDigest:
    """Bounded-memory row multiset check, including duplicate multiplicity."""

    def __init__(self):
        self.count, self.total, self.xor = 0, 0, 0

    def add(self, row):
        encoded = json.dumps(tuple(row), ensure_ascii=False, sort_keys=True,
                             separators=(",", ":"), default=normalize,
                             allow_nan=False).encode()
        hashed = int.from_bytes(hashlib.sha256(encoded).digest(), "big")
        self.count += 1
        self.total = (self.total + hashed) % (1 << 256)
        self.xor ^= hashed

    def result(self):
        return {"rows": self.count, "sha256_sum": f"{self.total:064x}", "sha256_xor": f"{self.xor:064x}"}


def ordered_digest(rows):
    state = hashlib.sha256()
    count = 0
    for row in rows:
        encoded = canonical(row)
        state.update(len(encoded).to_bytes(8, "big"))
        state.update(encoded)
        count += 1
    return {"rows": count, "sha256": state.hexdigest()}


def parse_time(value):
    if not isinstance(value, str):
        raise ValueError("Fixture timestamps must be ISO8601 strings")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Fixture timestamps must include a timezone")
    return parsed.astimezone(UTC)


def fixture_rows(path, columns, maximum):
    count = 0
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if len(line.encode()) > 2 * MIB:
                raise ValueError("One fixture record exceeds the 2 MiB budget")
            row = json.loads(line)
            if not isinstance(row, list) or len(row) != len(columns):
                raise ValueError(f"Invalid column shape in {path.name}")
            if "created_at" in columns:
                index = columns.index("created_at")
                row[index] = parse_time(row[index])
            for name in ("payload", "extras"):
                if name in columns:
                    value = row[columns.index(name)]
                    if value is not None and not isinstance(value, str):
                        raise ValueError(f"{name} must preserve its exact source text")
            count += 1
            if count > maximum:
                raise ValueError(f"Row budget exceeded in {path.name}")
            yield tuple(row)


def load_fixture(directory):
    if (directory / "manifest.json").stat().st_size > MIB:
        raise ValueError("Fixture manifest exceeds the 1 MiB metadata budget")
    manifest = json.loads((directory / "manifest.json").read_text())
    if manifest.get("version") != 1:
        raise ValueError("Unsupported fixture version")
    shapes = (("packets", MQTT_COLUMNS, 100_000),
              ("receptions", RECEPTION_COLUMNS, 250_000),
              ("topics", TOPIC_COLUMNS, 20_000))
    sizes = {name: (directory / f"{name}.jsonl").stat().st_size for name, _, _ in shapes}
    if sum(sizes.values()) > MAX_FIXTURE_BYTES:
        raise ValueError("Fixture exceeds the 128 MiB input budget")
    expected = {}
    for name, columns, maximum in shapes:
        singular = "packet" if name == "packets" else "reception" if name == "receptions" else "topic"
        if tuple(manifest.get(singular + "_columns", ())) != tuple(columns):
            raise ValueError(f"Fixture {singular}_columns do not match application storage")
        digest = MultisetDigest()
        for row in fixture_rows(directory / f"{name}.jsonl", columns, maximum):
            digest.add(logical_row(row, columns))
        expected[name] = digest.result()
    packets = list(fixture_rows(directory / "packets.jsonl", MQTT_COLUMNS, 100_000))
    packets.sort(key=lambda row: (row[6], row[0]))
    if not packets or len({row[0] for row in packets}) != len(packets):
        raise ValueError("Benchmark requires nonempty packets with distinct deep-link IDs")
    hot = parse_time(manifest["hot_month"] + "T00:00:00Z") if manifest.get("hot_month") else packets[-1][6].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if hot != hot.replace(day=1, hour=0, minute=0, second=0, microsecond=0):
        raise ValueError("hot_month must be a UTC month boundary")
    if not any(row[6] < hot for row in packets) or not any(row[6] >= hot for row in packets):
        raise ValueError("Fixture must contain both archived and hot months")
    trigger = (directory / "trigger.sql").read_text()
    if len(trigger.encode()) > 64 * 1024 or "mqtt_messages_extract_node_ids" not in trigger:
        raise ValueError("Missing bounded application trigger SQL")
    return manifest, packets, expected, sizes, hot, trigger


def month(value):
    return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def next_month(value):
    return value.replace(year=value.year + 1, month=1) if value.month == 12 else value.replace(month=value.month + 1)


async def set_path(conn, schema, shared):
    await conn.execute("SELECT set_config('search_path',$1,false)", f"{qi(schema)},{qi(shared)},public")


async def copy_fixture(conn, directory, name, columns, maximum, schema):
    records = fixture_rows(directory / f"{name}.jsonl", columns, maximum)
    await conn.copy_records_to_table(name if name == "topics" else "packet_receptions",
                                    schema_name=schema, columns=columns, records=records, timeout=60)


async def create_shared(conn, schema, directory):
    await conn.execute(f"CREATE SCHEMA {qi(schema)}")
    await conn.execute(f"""CREATE TABLE {qualified(schema, 'topics')} (
        id SERIAL PRIMARY KEY, topic TEXT NOT NULL UNIQUE);
        CREATE TABLE {qualified(schema, 'packet_receptions')} (
        mqtt_row_id BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        gateway VARCHAR(8),topic_id INTEGER,rx_rssi SMALLINT,rx_snr DOUBLE PRECISION,
        rx_time BIGINT,hop_limit SMALLINT,hops_away SMALLINT,relay_node INTEGER,
        transport SMALLINT,extras JSONB) PARTITION BY RANGE(created_at)""")
    receipt_months = set()
    for row in fixture_rows(directory / "receptions.jsonl", RECEPTION_COLUMNS, 250_000):
        receipt_months.add(month(row[1]))
    for value in sorted(receipt_months):
        name = f"packet_receptions_{value:%Y_%m}"
        await conn.execute(f"CREATE TABLE {qualified(schema,name)} PARTITION OF {qualified(schema,'packet_receptions')} "
                           f"FOR VALUES FROM ('{value.isoformat()}') TO ('{next_month(value).isoformat()}')")
    await conn.execute(f"CREATE INDEX receptions_row ON {qualified(schema,'packet_receptions')}(mqtt_row_id,created_at)")
    await conn.execute(f"CREATE INDEX receptions_gateway ON {qualified(schema,'packet_receptions')}(gateway,created_at DESC)")
    await copy_fixture(conn, directory, "topics", TOPIC_COLUMNS, 20_000, schema)
    await copy_fixture(conn, directory, "receptions", RECEPTION_COLUMNS, 250_000, schema)
    # Application SQL uses mqtt_topics; preserve that relation name with a view
    # over the single shared dictionary rather than replicating fixture storage.
    await conn.execute(f"CREATE VIEW {qualified(schema,'mqtt_topics')} AS SELECT * FROM {qualified(schema,'topics')}")
    await conn.execute(f"ANALYZE {qualified(schema,'packet_receptions')}", timeout=30)
    await conn.execute(f"ANALYZE {qualified(schema,'topics')}")


def index_specifications(manifest):
    definitions = manifest.get("source_packet_indexes")
    if not isinstance(definitions, list) or not definitions:
        raise ValueError("Fixture must provide the actual source_packet_indexes inventory")
    result = []
    pattern = re.compile(r"^CREATE\s+(?P<unique>UNIQUE\s+)?INDEX\s+\S+\s+ON\s+(?:ONLY\s+)?(?:\"?public\"?\.)?\"?mqtt_messages\"?\s+USING\s+(?P<method>\w+)\s+(?P<tail>.+)$", re.I | re.S)
    for entry in definitions:
        if entry.get("primary"):
            continue
        definition = entry.get("definition", "")
        matched = pattern.fullmatch(definition)
        if not matched or any(token in definition for token in (";", "--", "/*")):
            raise ValueError("Unrecognized source index definition")
        method = matched["method"].lower()
        if method not in {"btree", "hash", "gin"}:
            raise ValueError("Unexpected source index method; explicit prototype handling required")
        result.append({"source_name": entry.get("name"), "unique": bool(matched["unique"]),
                       "method": method, "tail": matched["tail"]})
    return result


async def compact_fixture_gin_indexes(conn, schema):
    """Remove bulk-load GIN pending allocation from the measured baseline.

    Only ordinary GIN leaf indexes in this invocation's packet schema are
    rebuilt. Parent partitioned indexes have no storage, and rebuilding Btrees
    would introduce a different comparison unrelated to GIN pending lists.
    """
    if not re.fullmatch(r"benchmark_(heap_full|heap_compatible|columnar)_[a-f0-9]{10}", schema):
        raise ValueError("GIN normalization requires an isolated benchmark packet schema")
    indexes = await conn.fetch("""SELECT c.relname, pg_relation_size(c.oid)::bigint AS bytes
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        JOIN pg_am am ON am.oid=c.relam
        WHERE n.nspname=$1 AND c.relkind='i' AND am.amname='gin'
        ORDER BY c.relname""", schema)
    started = time.perf_counter()
    details = []
    for index in indexes:
        target = qualified(schema, index["relname"])
        await conn.execute(f"REINDEX INDEX {target}", timeout=30)
        after = await conn.fetchval("SELECT pg_relation_size($1::regclass)::bigint", target)
        details.append({"index": index["relname"], "before_bytes": index["bytes"],
                        "after_bytes": after})
    return {
        "count": len(details), "names": [item["index"] for item in details],
        "before_bytes": sum(item["before_bytes"] for item in details),
        "after_bytes": sum(item["after_bytes"] for item in details),
        "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
        "indexes": details,
        "note": "GIN leaf indexes rebuilt after COPY and before measurement; pending-list allocation is not counted as compression savings. Btrees are unchanged.",
    }


async def create_packets(conn, schema, shared, variant, packets, hot, specs, trigger, chunk_rows):
    await conn.execute(f"CREATE SCHEMA {qi(schema)}")
    await set_path(conn, schema, shared)
    await conn.execute("""CREATE TABLE mqtt_messages (
        id BIGSERIAL,topic TEXT,payload TEXT COMPRESSION lz4,qos INTEGER,retain BOOLEAN,
        timestamp BIGINT,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        from_node_id VARCHAR(8),to_node_id VARCHAR(8),packet_id BIGINT,
        PRIMARY KEY(id,created_at)) PARTITION BY RANGE(created_at)""")
    parent = qualified(schema, "mqtt_messages")
    for index, spec in enumerate(specs):
        if spec["method"] == "gin" and variant != "heap_full":
            continue
        unique = "UNIQUE " if spec["unique"] else ""
        await conn.execute(f"CREATE {unique}INDEX {qi('packet_index_'+str(index))} ON {parent} "
                           f"USING {spec['method']} {spec['tail']}")
    partitions = []
    for value in sorted({month(row[6]) for row in packets}):
        name = f"mqtt_messages_{value:%Y_%m}"
        cold = value < hot
        am = "columnar" if variant == "columnar" and cold else "heap"
        await conn.execute(f"CREATE TABLE {qualified(schema,name)} PARTITION OF {parent} "
                           f"FOR VALUES FROM ('{value.isoformat()}') TO ('{next_month(value).isoformat()}') USING {am}")
        if am == "columnar":
            await conn.execute(f"ALTER TABLE {qualified(schema,name)} SET "
                               f"(columnar.compression='zstd',columnar.compression_level=3,"
                               f"columnar.stripe_row_limit=150000,columnar.chunk_group_row_limit={chunk_rows})")
        if not cold and variant != "heap_full":
            for index, spec in enumerate(specs):
                if spec["method"] == "gin":
                    await conn.execute(f"CREATE INDEX {qi(name+'_gin_'+str(index))} ON {qualified(schema,name)} "
                                       f"USING gin {spec['tail']}")
        partitions.append({"name":name,"month":value.date().isoformat(),"cold":cold,"method":am})
    before = time.perf_counter()
    await conn.copy_records_to_table("mqtt_messages", schema_name=schema, columns=MQTT_COLUMNS,
                                    records=packets, timeout=60)
    load_ms = round((time.perf_counter()-before)*1000, 3)
    gin_compaction = await compact_fixture_gin_indexes(conn, schema)
    # Historical NULL-derived fields must remain unchanged during bulk copy.
    # Only subsequent new inserts exercise the application's real trigger.
    await conn.execute(trigger, timeout=10)
    await conn.execute(f"ANALYZE {parent}", timeout=30)
    return {"schema":schema,"partitions":partitions,"load_ms":load_ms,
            "gin_compaction":gin_compaction}


async def rows_digest(conn, schema, table, columns):
    digest = MultisetDigest()
    projection = ",".join(qi(column) for column in columns)
    async with conn.transaction(readonly=True):
        async for row in conn.cursor(f"SELECT {projection} FROM {qualified(schema,table)}", prefetch=256):
            digest.add(logical_row(row, columns))
    return digest.result()


async def schema_size(conn, schema):
    rows = await conn.fetch("""SELECT c.relname,c.relkind::text AS kind,
        pg_table_size(c.oid)::bigint AS table_bytes,pg_indexes_size(c.oid)::bigint AS index_bytes,
        pg_total_relation_size(c.oid)::bigint AS total_bytes,am.amname AS access_method
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        LEFT JOIN pg_am am ON am.oid=c.relam
        WHERE n.nspname=$1 AND c.relkind IN ('r','m','S') ORDER BY c.relname""",schema)
    details = [dict(row) for row in rows]
    return {"table_bytes":sum(row["table_bytes"] for row in rows),
            "index_bytes":sum(row["index_bytes"] for row in rows),
            "total_bytes":sum(row["total_bytes"] for row in rows),"relations":details}


async def metadata_size(conn):
    rows = await conn.fetch("""SELECT n.nspname,c.relname,pg_total_relation_size(c.oid)::bigint AS bytes
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname LIKE 'columnar%' AND c.relkind IN ('r','m','S') ORDER BY n.nspname,c.relname""")
    return {"total_bytes":sum(row["bytes"] for row in rows),"relations":[dict(row) for row in rows]}


def query_cases(packets, hot, manifest, topic_rows, receipt_topics):
    cold = [row for row in packets if row[6] < hot]
    received = [row for row in cold if row[0] in receipt_topics]
    if not received:
        raise ValueError("Fixture needs a cold packet with linked receptions")
    chosen = received[len(received)//2]
    first_hot = next(row for row in packets if row[6] >= hot)
    terms = manifest.get("query_terms", {})
    node = terms.get("node_id") or next((row[7] for row in cold if row[7]), None)
    sender = next((row for row in cold if row[7] and row[9] is not None), None)
    lookback = manifest.get("reception_lookback_seconds", 86400)
    if type(lookback) is not int or not 1 <= lookback <= 604800:
        raise ValueError("Invalid reception lookback")
    count_sql = ("(SELECT count(*) FROM packet_receptions pr WHERE pr.mqtt_row_id=mqtt_messages.id "
                 "AND pr.created_at>=mqtt_messages.created_at "
                 f"AND pr.created_at<mqtt_messages.created_at+interval '{lookback} seconds')")
    projection = f"id,topic,payload,qos,retain,timestamp,created_at,{count_sql} AS reception_count"
    base = f"SELECT {projection} FROM mqtt_messages"
    order = " ORDER BY created_at DESC,id DESC LIMIT 201"
    start = month(chosen[6])
    end = next_month(start) - timedelta(microseconds=1)
    cases = [
        ("recent_page",base+order,()),
        ("archived_month_page",base+" WHERE created_at >= $1 AND created_at <= $2"+order,(start,end)),
        ("hot_cold_boundary_page",base+" WHERE (created_at,id)<($1,$2)"+order,(first_hot[6],first_hot[0]+1)),
        ("archived_keyset_page",base+" WHERE (created_at,id)<($1,$2)"+order,(chosen[6],chosen[0])),
        ("deep_link_without_date",base+" WHERE id=$1",(chosen[0],)),
    ]
    timestamp = next((row[5] for row in cold if row[5] is not None),None)
    if timestamp is not None:
        cases.append(("reported_timestamp_range",base+" WHERE timestamp >= $1"+order,(timestamp,)))
        cases.append(("archived_reported_timestamp_range",base+" WHERE timestamp >= $1 AND created_at < $2"+order,(timestamp,hot)))
    if node:
        cases.append(("node_or_page",base+" WHERE (from_node_id=$1 OR to_node_id=$1)"+order,(node,)))
        cases.append(("node_union_page",f"({base} WHERE from_node_id=$1) UNION ({base} WHERE to_node_id=$1)"+order,(node,)))
        cases.append(("archived_node_or_page",base+" WHERE (from_node_id=$1 OR to_node_id=$1) AND created_at < $2"+order,(node,hot)))
        cases.append(("archived_node_union_page",f"({base} WHERE from_node_id=$1 AND created_at < $2) UNION ({base} WHERE to_node_id=$1 AND created_at < $2)"+order,(node,hot)))
    if sender:
        sender_id = terms.get("sender_node_id",sender[7])
        packet_id = terms.get("sender_packet_id",sender[9])
        cases.append(("sender_packet_lookup",base+" WHERE from_node_id=$1 AND packet_id=$2 ORDER BY created_at DESC LIMIT 1",(sender_id,packet_id)))
    topic_term = terms.get("topic_literal") or next((row[1] for row in cold if row[1]),"")
    if topic_term:
        escaped = topic_term.replace("\\","\\\\").replace("%","\\%").replace("_","\\_")
        cases.append(("literal_topic_filter",base+" WHERE topic ILIKE '%' || $1 || '%' ESCAPE '\\'"+order,(escaped,)))
    receipt_term = terms.get("receipt_topic_search") or next(
        (row[1] for row in topic_rows if row[0] == receipt_topics[chosen[0]]), None)
    search_condition = ("(topic ILIKE '%' || $1 || '%' OR payload ILIKE '%' || $1 || '%' OR EXISTS "
        "(SELECT 1 FROM packet_receptions pr JOIN mqtt_topics t ON t.id=pr.topic_id "
        "WHERE pr.mqtt_row_id=mqtt_messages.id AND pr.created_at>=mqtt_messages.created_at "
        f"AND pr.created_at<mqtt_messages.created_at+interval '{lookback} seconds' "
        "AND t.topic ILIKE '%' || $1 || '%'))")
    preselected_condition = (
        "(topic ILIKE '%' || $1 || '%' OR payload ILIKE '%' || $1 || '%' OR EXISTS "
        "(SELECT 1 FROM packet_receptions pr "
        "WHERE pr.mqtt_row_id=mqtt_messages.id AND pr.created_at>=mqtt_messages.created_at "
        f"AND pr.created_at<mqtt_messages.created_at+interval '{lookback} seconds' "
        "AND pr.topic_id=ANY(ARRAY(SELECT id FROM mqtt_topics "
        "WHERE topic ILIKE '%' || $1 || '%'))))")
    for name, term in (("payload_search_present","from"),
                       ("payload_search_absent","meshinfo-columnar-proof-absent-7df9a4"),
                       ("reception_topic_search",receipt_term)):
        if term:
            cases.append((name,base+" WHERE "+search_condition+order,(term,)))
            cases.append(("archived_"+name,base+" WHERE "+search_condition+" AND created_at < $2"+order,(term,hot)))
            if name in ("payload_search_absent", "reception_topic_search"):
                cases.append((name+"_topic_preselect",base+" WHERE "+preselected_condition+order,(term,)))
                cases.append(("archived_"+name+"_topic_preselect",
                              base+" WHERE "+preselected_condition+" AND created_at < $2"+order,(term,hot)))
    cases.append(("reception_details", "SELECT pr.gateway,t.topic,pr.rx_rssi,pr.rx_snr,pr.rx_time,pr.hop_limit,"
        "pr.hops_away,pr.relay_node,pr.transport,pr.extras,pr.created_at FROM packet_receptions pr "
        "LEFT JOIN mqtt_topics t ON t.id=pr.topic_id WHERE pr.mqtt_row_id=$1 AND pr.created_at >= $2 "
        f"AND pr.created_at < $2+interval '{lookback} seconds' ORDER BY pr.created_at,pr.gateway LIMIT 5001",(chosen[0],chosen[6])))
    return cases


def plan_summary(plan):
    fields = ("Node Type","Custom Plan Provider","Index Name","Relation Name",
              "Scan Direction","Plan Rows","Total Cost")
    result = {name:plan[name] for name in fields if name in plan}
    if "Plans" in plan:
        result["Plans"] = [plan_summary(child) for child in plan["Plans"]]
    return result


async def measure_queries(conn, schemas, shared, cases, repeats, hot, query_timeout_seconds=5):
    """Alternate variants each round; never flush caches on this shared host."""
    results = {variant: {} for variant in VARIANTS}
    for case_index, (name, query, parameters) in enumerate(cases):
        print(f"Benchmark query: {name}", file=sys.stderr, flush=True)
        expected = None
        elapsed = {variant: [] for variant in VARIANTS}
        fingerprints = {}
        failures = {}
        for iteration in range(repeats + 1):
            offset = (case_index + iteration) % len(VARIANTS)
            order = VARIANTS[offset:] + VARIANTS[:offset]
            for variant in order:
                if variant in failures:
                    continue
                await set_path(conn, schemas[variant], shared)
                started = time.perf_counter()
                try:
                    rows = await conn.fetch(query, *parameters, timeout=query_timeout_seconds)
                except (TimeoutError, asyncpg.PostgresError) as exc:
                    failures[variant] = {
                        "category": type(exc).__name__, "phase": "execute",
                        "attempt": iteration + 1,
                        "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
                        "sqlstate": getattr(exc, "sqlstate", None),
                    }
                    print(f"Benchmark query failed: {variant}/{name}: {type(exc).__name__}",
                          file=sys.stderr, flush=True)
                    continue
                elapsed[variant].append((time.perf_counter() - started) * 1000)
                fingerprint = ordered_digest(rows)
                fingerprints[variant] = fingerprint
                try:
                    if expected is None:
                        expected = fingerprint
                    elif fingerprint != expected:
                        raise AssertionError("Query results differ across variants or repetitions")
                    if name.removesuffix("_topic_preselect").endswith("search_absent"):
                        if rows:
                            raise AssertionError("Absent search marker unexpectedly matched")
                    elif not rows:
                        raise AssertionError("Required positive query witness is empty")
                    if name.startswith("archived_"):
                        if any(row["created_at"] >= hot for row in rows):
                            raise AssertionError("Archive query unexpectedly included hot rows")
                    if name == "hot_cold_boundary_page":
                        if not any(row["created_at"] < hot for row in rows):
                            raise AssertionError("Boundary page did not reach archived data")
                        if not any(row["created_at"] >= hot for row in rows):
                            raise AssertionError("Boundary page did not include hot data")
                except AssertionError as exc:
                    failures[variant] = {
                        "category": "AssertionError", "phase": "result_validation",
                        "attempt": iteration + 1, "reason": str(exc),
                    }
        for variant in VARIANTS:
            await set_path(conn, schemas[variant], shared)
            plan = None
            # EXPLAIN without ANALYZE does not rerun a timed-out query, and can
            # still explain the failed workload. Keep its own failure distinct.
            plan_error = None
            try:
                plan = json.loads(await conn.fetchval(
                    "EXPLAIN (FORMAT JSON) " + query, *parameters,
                    timeout=query_timeout_seconds))[0]["Plan"]
            except (TimeoutError, asyncpg.PostgresError) as exc:
                plan_error = {"category": type(exc).__name__, "phase": "explain",
                              "sqlstate": getattr(exc, "sqlstate", None)}
                failures.setdefault(variant, plan_error)
            timings = elapsed[variant]
            results[variant][name] = {
                "status": "failed" if variant in failures else "passed",
                "result": fingerprints.get(variant),
                "successful_executions": len(timings),
                "first_ms": round(timings[0], 3) if timings else None,
                "warm_median_ms": round(statistics.median(timings[1:]), 3) if len(timings) > 1 else None,
                "warm_max_ms": round(max(timings[1:]), 3) if len(timings) > 1 else None,
                "plan": plan_summary(plan) if plan else None,
                "error": failures.get(variant), "explain_error": plan_error,
            }
    for variant, measured in results.items():
        for prefix in ("", "archived_"):
            if prefix + "node_or_page" in measured:
                original = measured[prefix + "node_or_page"]
                rewritten = measured[prefix + "node_union_page"]
                if original["status"] == rewritten["status"] == "passed":
                    if original["result"] != rewritten["result"]:
                        rewritten["status"] = "failed"
                        rewritten["error"] = {
                            "category": "AssertionError", "phase": "rewrite_parity",
                            "reason": "Node UNION rewrite differs from OR query",
                        }
    for name, _, _ in cases:
        if not name.endswith("_topic_preselect"):
            continue
        original_name = name.removesuffix("_topic_preselect")
        originals = [measured[original_name]["result"] for measured in results.values()
                     if measured[original_name]["status"] == "passed"]
        for measured in results.values():
            candidate = measured[name]
            candidate["original_query_parity"] = "unavailable_original_failed"
            if candidate["status"] == "passed" and originals:
                if any(candidate["result"] != original for original in originals):
                    candidate["status"] = "failed"
                    candidate["error"] = {
                        "category": "AssertionError", "phase": "rewrite_parity",
                        "reason": "Topic preselection differs from completed original query",
                    }
                else:
                    candidate["original_query_parity"] = "verified"
    return results


async def constraint_and_trigger_checks(conn, schema, shared, packets, hot):
    await set_path(conn,schema,shared)
    parent = qualified(schema,"mqtt_messages")
    duplicate = packets[0]
    try:
        async with conn.transaction():
            await conn.execute(f"INSERT INTO {parent}(id,created_at) VALUES($1,$2)",duplicate[0],duplicate[6])
    except asyncpg.UniqueViolationError:
        pass
    else:
        raise AssertionError("Archived primary key did not reject a duplicate")
    outcomes = []
    for offset, stamp in enumerate((month(packets[0][6])+timedelta(days=1),hot+timedelta(days=1))):
        transaction = conn.transaction()
        await transaction.start()
        try:
            row = await conn.fetchrow(f"INSERT INTO {parent}(id,payload,created_at) VALUES($1,$2,$3) "
                "RETURNING from_node_id,to_node_id,packet_id",-8_700_000_000_000_000-offset,
                '{"from":1,"to":2,"id":987654321}',stamp)
            if tuple(row) != ("00000001","00000002",987654321):
                raise AssertionError("Application BEFORE INSERT trigger changed semantics")
            outcomes.append("cold" if stamp<hot else "hot")
        finally:
            await transaction.rollback()
    return {"duplicate_primary_key_rejected":True,"trigger_probed_on":outcomes,
            "synthetic_probe_rows_rolled_back":True}


async def run(args):
    if not re.fullmatch(r"[A-Z][A-Z0-9_]*",args.dsn_env):
        raise ValueError("--dsn-env must name an environment variable")
    dsn = os.environ.get(args.dsn_env,"")
    database = unquote(urlsplit(dsn).path.lstrip("/"))
    if not re.fullmatch(r"[A-Za-z0-9_]+_columnar_test",database):
        raise ValueError("Benchmark requires a dedicated *_columnar_test database")
    manifest, packets, expected, sizes, hot, trigger = load_fixture(args.fixture_dir)
    specs = index_specifications(manifest)
    topics = list(fixture_rows(args.fixture_dir/"topics.jsonl",TOPIC_COLUMNS,20_000))
    receipt_topics = {row[0]: row[3] for row in fixture_rows(
        args.fixture_dir / "receptions.jsonl", RECEPTION_COLUMNS, 250_000)
        if row[3] is not None}
    cases = query_cases(packets,hot,manifest,topics,receipt_topics)
    identifier = uuid.uuid4().hex[:10]
    shared = "benchmark_shared_"+identifier
    schemas = {variant:"benchmark_"+variant+"_"+identifier for variant in VARIANTS}
    created = []
    payload_lengths = [len((row[2] or "").encode("utf-8")) for row in packets]
    report = {"prototype_only":True,"created_at":datetime.now(UTC).isoformat(),
        "fixture": {"logical_file_bytes":sizes,"expected_rows":{name:value["rows"] for name,value in expected.items()},
                    "logical_digests":expected,
                    "real_counts":manifest.get("real_counts"),
                    "synthetic_counts":manifest.get("synthetic_counts"),
                    "sampling_note":manifest.get("sampling_note"),
                    "payload_bytes":{"minimum":min(payload_lengths),"maximum":max(payload_lengths),
                        "mean":round(statistics.mean(payload_lengths),2),
                        "median":statistics.median(payload_lengths)},
                    "hot_month":hot.date().isoformat(),"packets_by_month":dict(Counter(f"{row[6]:%Y-%m}" for row in packets))},
        "shared_schema":shared,"variants":{},"settings":{"compression":"zstd","compression_level":3,
             "chunk_group_row_limit":args.chunk_rows,"stripe_row_limit":150000,"repeats":args.repeats,
             "query_timeout_seconds":args.query_timeout_seconds},
        "limits":["Bounded sample; not a whole-database saving forecast.",
            "First and repeated queries use local data and do not flush caches; same-speed disk-scale behavior is unproven.",
            "This benchmark does not validate the live migration, concurrent ingestion or crash-safe cutover.",
            "Only fixture months are created; default-partition routing and automatic future partition creation are not exercised.",
            "The baseline heap_full retains cold GIN; heap_compatible omits it to isolate compression from index removal."],
        "comparison_semantics": {"payload_text":"exact", "extras_jsonb":"decoded JSON values",
            "query_order":"variant order rotates for each query and repetition",
            "gin_indexes":"All copied leaf GIN indexes rebuilt before measurement to exclude pending-list allocation from compression savings.",
            "storage":"pristine snapshots precede probes; deployment totals include any space retained after rollback probes"}}
    conn = await asyncpg.connect(dsn,timeout=5,command_timeout=15,server_settings={
        "application_name":"meshinfo-isolated-columnar-benchmark","timezone":"UTC",
        "lock_timeout":"250ms","statement_timeout":"15000","work_mem":"4MB",
        "maintenance_work_mem":"32MB","max_parallel_workers_per_gather":"0",
        "max_parallel_maintenance_workers":"0","jit":"off"})
    try:
        if await conn.fetchval("SELECT current_database()") != database:
            raise ValueError("Connected database differs from the explicitly guarded target")
        if not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_am WHERE amname='columnar')"):
            raise ValueError("Target must have the columnar extension installed before benchmarking")
        if not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_trgm')"):
            raise ValueError("Target must have pg_trgm installed for faithful heap comparison")
        report["server"] = {"version":await conn.fetchval("SHOW server_version"),
            "extensions":[dict(row) for row in await conn.fetch("SELECT extname,extversion FROM pg_extension ORDER BY extname")]}
        report["database_bytes_before"] = await conn.fetchval("SELECT pg_database_size(current_database())")
        report["columnar_metadata_before"] = await metadata_size(conn)
        created.append(shared)
        await create_shared(conn,shared,args.fixture_dir)
        for name,table,columns in (("receptions","packet_receptions",RECEPTION_COLUMNS),("topics","topics",TOPIC_COLUMNS)):
            if await rows_digest(conn,shared,table,columns) != expected[name]:
                raise AssertionError(f"Exact {name} source roundtrip failed")
        report["shared_heap_storage"] = await schema_size(conn,shared)
        for variant in VARIANTS:
            schema = schemas[variant]
            created.append(schema)
            data = await create_packets(conn,schema,shared,variant,packets,hot,specs,trigger,args.chunk_rows)
            if await rows_digest(conn,schema,"mqtt_messages",MQTT_COLUMNS) != expected["packets"]:
                raise AssertionError(f"Exact canonical source roundtrip failed for {variant}")
            data["pristine_storage"] = await schema_size(conn,schema)
            data["all_rows_exact"] = True
            report["variants"][variant] = data
        report["columnar_metadata_pristine"] = await metadata_size(conn)
        for variant, data in report["variants"].items():
            schema = schemas[variant]
            data["constraints_and_trigger"] = await constraint_and_trigger_checks(conn,schema,shared,packets,hot)
            if await rows_digest(conn,schema,"mqtt_messages",MQTT_COLUMNS) != expected["packets"]:
                raise AssertionError(f"Compatibility probes changed logical source rows for {variant}")
            # Columnar rollback can leave physical storage allocated. Include it
            # in the final physical totals and expose the pristine snapshot.
            data["storage"] = await schema_size(conn,schema)
        measurements = await measure_queries(conn,schemas,shared,cases,args.repeats,hot,args.query_timeout_seconds)
        for variant, queries in measurements.items():
            report["variants"][variant]["queries"] = queries
        report["columnar_metadata_after"] = await metadata_size(conn)
        metadata_total = report["columnar_metadata_after"]["total_bytes"]
        metadata_growth = metadata_total-report["columnar_metadata_before"]["total_bytes"]
        report["columnar_metadata_growth_bytes"] = metadata_growth
        for variant,data in report["variants"].items():
            data["hypothetical_deployment_bytes"] = (data["storage"]["total_bytes"]+
                report["shared_heap_storage"]["total_bytes"]+(metadata_total if variant=="columnar" else 0))
            data["cold_packet_bytes"] = sum(row["total_bytes"] for row in data["storage"]["relations"]
                if row["relname"] in {part["name"] for part in data["partitions"] if part["cold"]})
        full = report["variants"]["heap_full"]["hypothetical_deployment_bytes"]
        compatible = report["variants"]["heap_compatible"]["hypothetical_deployment_bytes"]
        compressed = report["variants"]["columnar"]["hypothetical_deployment_bytes"]
        report["savings"] = {"versus_heap_full_bytes":full-compressed,
            "versus_heap_full_percent":round(100*(1-compressed/full),2),
            "versus_compatible_heap_bytes":compatible-compressed,
            "versus_compatible_heap_percent":round(100*(1-compressed/compatible),2)}
        report["database_bytes_after"] = await conn.fetchval("SELECT pg_database_size(current_database())")
        report["query_failures"] = [
            {"variant": variant, "case": name, "error": result["error"]}
            for variant, data in report["variants"].items()
            for name, result in data["queries"].items() if result["status"] != "passed"
        ]
        report["all_checks_passed"] = not report["query_failures"]
        report["schemas_retained"] = args.keep_schemas
        return report
    finally:
        if not args.keep_schemas:
            await conn.execute("SET search_path=public")
            for schema in reversed(created):
                await conn.execute(f"DROP SCHEMA IF EXISTS {qi(schema)} CASCADE",timeout=30)
        await conn.close()


def parser():
    result = argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
    result.add_argument("--dsn-env",required=True,help="Environment variable holding only the isolated target DSN")
    result.add_argument("--fixture-dir",type=Path,required=True)
    result.add_argument("--output",type=Path,required=True,help="Source-free aggregate JSON report")
    result.add_argument("--keep-schemas",action="store_true",help="Retain generated target schemas for separate restore/API tests")
    result.add_argument("--repeats",type=int,default=5,choices=range(3,16))
    result.add_argument("--chunk-rows",type=int,default=1000,choices=(1000,10000))
    result.add_argument("--query-timeout-seconds",type=int,default=5,choices=(2,5,10),
                        help="Per-query deadline; failures are retained in the report")
    return result


def main():
    args = parser().parse_args()
    try:
        report = asyncio.run(run(args))
    except Exception as exc:
        # Database exceptions may include source values or credentials. Keep
        # terminal errors categorical; an operator may inspect the target logs.
        print(f"Columnar benchmark failed: {type(exc).__name__}",file=sys.stderr)
        return 1
    args.output.write_text(json.dumps(report,indent=2,default=normalize)+"\n")
    print(json.dumps({"output":str(args.output),"savings":report["savings"],
                      "all_checks_passed":report["all_checks_passed"],
                      "query_failures":report["query_failures"]}))
    return 0 if report["all_checks_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
