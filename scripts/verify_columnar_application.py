#!/usr/bin/env python3
"""Read-only application/API verification for retained isolated benchmark schemas.

Uses the real PostgresStorage readers, including cursor formatting and uplink
reconstruction. Does not connect the application lifecycle, migrate, or write
database rows. Database names must end in _columnar_test. Reports contain
response hashes and counts, never packet payloads or connection strings.

After pg_dump/restore into a second isolated database, supply --restore-report
with the first verification report to recheck its columnar/shared schemas.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import sys
import time
from unittest.mock import patch
from urllib.parse import unquote, urlsplit

import asyncpg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from config import DEFAULT_CONFIG
from scripts.benchmark_columnar_history import (
    MQTT_COLUMNS, RECEPTION_COLUMNS, TOPIC_COLUMNS, fixture_rows, load_fixture,
    month, next_month, normalize, qi,
)
from storage.db.postgres import PostgresStorage

UTC = timezone.utc


class VerificationFailure(RuntimeError):
    """Only safe, source-free context belongs in these messages."""


def digest(value):
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False,
                         separators=(",", ":"), default=normalize).encode()
    return hashlib.sha256(encoded).hexdigest()


def require(condition, message):
    if not condition:
        raise VerificationFailure(message)


class StorageWarnings(logging.Handler):
    def __init__(self):
        super().__init__(logging.WARNING)
        self.levels = []
        self.categories = []

    def emit(self, record):
        self.levels.append(record.levelname)
        exceptions = [item for item in record.args if isinstance(item, Exception)] if isinstance(record.args, tuple) else []
        self.categories.append({"level": record.levelname,
                                "exceptions": [type(item).__name__ for item in exceptions]})


def describe(value):
    if isinstance(value, dict) and "messages" in value:
        rows = value["messages"]
        return {"rows": len(rows), "has_next_cursor": value["next_cursor"] is not None}
    if isinstance(value, dict):
        receptions = value.get("receptions", [])
        return {"rows": 1, "receptions": len(receptions),
                "reconstructed_copies": sum("reconstructed" in row for row in receptions)}
    return {"rows": 0}


def plan_cases(packets, manifest, hot, receptions, topics):
    cold = [row for row in packets if row[6] < hot]
    warm = [row for row in packets if row[6] >= hot]
    terms = manifest.get("query_terms", {})
    synthetic = set(manifest.get("synthetic_packet_ids", []))
    by_id = {row[0]: row for row in packets}
    first = cold[0]
    last = cold[-1]
    cases = []

    def add(name, method, arguments, *, positive=True, expected_ids=None,
            expected_packet=None, reconstruction=False):
        cases.append({"name": name, "method": method, "arguments": arguments,
                      "positive": positive, "expected_ids": expected_ids,
                      "expected_packet": expected_packet, "reconstruction": reconstruction})

    add("recent_page", "query_mqtt_messages", {"limit": 200, "start": hot})
    add("archive_first_month_page", "query_mqtt_messages",
        {"limit": 200, "start": month(first[6]),
         "end": next_month(month(first[6])) - timedelta(microseconds=1)})
    add("archive_latest_month_page", "query_mqtt_messages",
        {"limit": 200, "start": month(last[6]),
         "end": next_month(month(last[6])) - timedelta(microseconds=1)})
    add("archive_search_absent", "query_mqtt_messages",
        {"limit": 200, "end": hot - timedelta(microseconds=1),
         "search": "meshinfo-columnar-verifier-absent-f47bf89ae5"}, positive=False, expected_ids=[])
    add("archive_search_payload", "query_mqtt_messages",
        {"limit": 200, "end": hot - timedelta(microseconds=1), "search": "from"})
    node = terms.get("node_id") or next((row[7] or row[8] for row in cold if row[7] or row[8]), None)
    if node:
        matching = [row for row in reversed(packets) if row[6] < hot and node in (row[7], row[8])]
        add("archive_node_or", "query_node_mqtt_messages",
            {"node_id": node, "limit": 200, "end": hot - timedelta(microseconds=1)},
            expected_ids=[row[0] for row in matching[:200]])
    topic = terms.get("topic_literal") or next((row[1] for row in cold if row[1]), None)
    if topic:
        matching = [row for row in reversed(packets)
                    if row[6] < hot and row[1] and topic.casefold() in row[1].casefold()]
        add("archive_literal_topic", "query_mqtt_messages",
            {"limit": 200, "end": hot - timedelta(microseconds=1), "topic": topic},
            expected_ids=[row[0] for row in matching[:200]])
    if terms.get("receipt_topic_search"):
        marker = terms["receipt_topic_search"]
        require(not any(marker.casefold() in (row[1] or "").casefold()
                        or marker.casefold() in (row[2] or "").casefold() for row in packets),
                "Reception-only marker occurs in canonical topic/payload")
        lookback = manifest.get("reception_lookback_seconds", 86400)
        topic_ids = {row[0] for row in topics if marker.casefold() in row[1].casefold()}
        matches = set()
        for receipt in receptions:
            packet = by_id.get(receipt[0])
            if (packet and packet[6] < hot and receipt[3] in topic_ids
                    and packet[6] <= receipt[1] < packet[6] + timedelta(seconds=lookback)):
                matches.add(packet[0])
        require(matches, "Reception-only witness has no linked archived packet")
        add("archive_reception_only_search", "query_mqtt_messages",
            {"limit": 200, "end": hot - timedelta(microseconds=1),
             "search": marker}, expected_ids=[row[0] for row in reversed(packets) if row[0] in matches][:200])

    # Freeze wall time only around this reader: its threshold uses the MQTT
    # timestamp, which deliberately differs from ingestion created_at.
    timestamps = [row[5] for row in cold if row[5] is not None]
    if timestamps:
        threshold = sorted(timestamps)[len(timestamps)//2]
        matching = [row for row in reversed(packets) if row[5] is not None and row[5] >= threshold]
        add("reported_timestamp_range", "query_mqtt_messages", {"limit": 200, "range_seconds": 1234},
            expected_ids=[row[0] for row in matching[:200]])
        cases[-1]["frozen_time"] = threshold + 1234

    eligible_receipts = Counter()
    lookback = manifest.get("reception_lookback_seconds", 86400)
    for row in receptions:
        packet = by_id.get(row[0])
        if packet and packet[6] <= row[1] < packet[6] + timedelta(seconds=lookback):
            eligible_receipts[row[0]] += 1
    selected = {first[0], last[0], warm[0][0], warm[-1][0]} | synthetic
    selected.update(row[0] for row in cold[::max(1, len(cold)//6)])
    real_receipt_ids = [row[0] for row in packets if row[0] not in synthetic and eligible_receipts[row[0]]]
    selected.update(real_receipt_ids[:8])
    selected.update(real_receipt_ids[-8:])
    require(len(selected) <= 128, "Too many deep-link witnesses for bounded API verification")
    for index, row_id in enumerate(sorted(selected)):
        row = by_id[row_id]
        try:
            object_payload = isinstance(json.loads(row[2] or "null"), dict)
        except (ValueError, TypeError):
            object_payload = False
        add(f"deep_link_{index:03d}", "query_mqtt_message_by_id",
            {"row_id": row_id, "include_copies": True}, expected_packet=row_id,
            reconstruction=bool(eligible_receipts[row_id] and object_payload))
        if row_id in synthetic:
            try:
                expected_base = json.loads(row[2]) if row[2] else {}
            except ValueError:
                expected_base = {"raw": row[2]}
            if not isinstance(expected_base, dict):
                expected_base = {"raw": expected_base}
            expected_base.setdefault("topic", row[1])
            expected_base.setdefault("timestamp", row[5])
            expected_base["mqtt_row_id"] = row_id
            cases[-1]["expected_base"] = expected_base
    if terms.get("sender_node_id") and terms.get("sender_packet_id") is not None:
        matching = [row for row in packets if (row[7], row[9]) ==
                    (terms["sender_node_id"], terms["sender_packet_id"])]
        require(len(matching) >= 2, "Packet-ID reuse witness is missing")
        newest = max(matching, key=lambda row: row[6])
        require(sum(row[6] == newest[6] for row in matching) == 1,
                "Newest reused packet-ID witness has an ambiguous timestamp")
        add("reused_packet_id_newest", "query_mqtt_message_by_packet",
            {"from_id": terms["sender_node_id"], "mesh_packet_id": terms["sender_packet_id"],
             "include_copies": True}, expected_packet=newest[0])
    missing = min(row[0] for row in packets) - 1
    add("missing_deep_link", "query_mqtt_message_by_id", {"row_id": missing}, positive=False)

    # About 440 packets surrounding the exact hot/cold boundary, independently
    # checked against the fixture order rather than merely against another DB.
    start = cold[max(0, len(cold)-220)][6]
    end = warm[min(len(warm)-1, 219)][6]
    expected_boundary = [row[0] for row in reversed(packets) if start <= row[6] <= end]
    require(200 < len(expected_boundary) <= 5000, "Boundary window must cover 201..5000 packets")
    windows = {"boundary_pagination": {"start": start, "end": end, "ids": expected_boundary, "limit": 200}}
    witnesses = manifest.get("synthetic_witnesses", {})
    if "self_addressed_tie_a" in witnesses:
        stamp = by_id[witnesses["self_addressed_tie_a"]][6]
        tied = [row[0] for row in reversed(packets) if row[6] == stamp]
        require(2 <= len(tied) <= 25, "Cursor tie fixture must have 2..25 rows")
        windows["tied_timestamp_pagination"] = {"start": stamp, "end": stamp, "ids": tied, "limit": 1}
    return cases, windows, eligible_receipts


def synthetic_originals(packets, receptions, topics, manifest):
    """Independent expected copies for the two deliberately simple witnesses.

    Their canonical payloads have no per-copy envelope keys, so the expected
    messages can be built directly from fixture constants, without calling
    reception_template, reconstruct_copy, or other implementation helpers.
    """
    witnesses = manifest.get("synthetic_witnesses", {})
    by_id = {row[0]: row for row in packets}
    dictionary = dict(topics)
    originals = {}
    for kind in ("cross_month", "self_addressed_tie_a"):
        if kind not in witnesses:
            continue
        row_id = witnesses[kind]
        packet = by_id[row_id]
        copies = sorted((row for row in receptions if row[0] == row_id), key=lambda row: (row[1], row[2] or ""))
        require(len(copies) == 1, "Synthetic copy witness must have exactly one reception")
        receipt = copies[0]
        message = json.loads(packet[2])
        require(set(message) == {"from", "to", "id", "payload"},
                "Synthetic canonical copy shape changed; review expected reconstruction")
        if kind == "cross_month":
            require(packet[6] < receipt[1] and month(packet[6]) != month(receipt[1]),
                    "Cross-month receipt does not cross a month boundary")
            patch_data = json.loads(receipt[11])
            expected_sets = {"rxRssi": None, "prototype_note": '雪 quoted "value"\n', "nullable": None}
            require(patch_data == {"set": expected_sets, "del": ["to"]},
                    "Synthetic copy patch changed or lacks Unicode/null/deletion witnesses")
            message.pop("to")
            message.update(expected_sets)
            for index, key in ((4, "rx_rssi"), (5, "rx_snr"), (6, "rx_time"),
                               (7, "hop_limit"), (8, "hops_away"),
                               (9, "relay_node"), (10, "transport_mechanism")):
                if receipt[index] is not None:
                    message[key] = receipt[index]
            require("columnar-reception-only-witness" in dictionary[receipt[3]],
                    "Cross-month receipt-only topic witness is missing")
        else:
            require(all(value is None for value in receipt[4:]),
                    "Tie receipt must have an empty RF envelope and no patch")
        # Topic/sender are absent from these original canonical payloads, so
        # reconstruction deliberately does not invent them.
        originals[str(row_id)] = [message]
    return originals


async def make_storage(dsn, schema, shared, lookback):
    async def setup(conn):
        await conn.execute("SELECT set_config('search_path',$1,false)",
                           f"{qi(schema)},{qi(shared)},public")

    config = copy.deepcopy(DEFAULT_CONFIG)
    config["server"]["timezone"] = "UTC"
    config["storage"]["postgres"]["enabled"] = True
    storage = PostgresStorage(config)
    storage._reception_lookback = lookback
    storage._reception_count_sql = (
        "(SELECT count(*)::int FROM packet_receptions pr WHERE pr.mqtt_row_id=mqtt_messages.id "
        "AND pr.created_at>=mqtt_messages.created_at "
        f"AND pr.created_at<mqtt_messages.created_at+interval '{lookback} seconds')")
    storage.pool = await asyncpg.create_pool(dsn, min_size=1, max_size=1, setup=setup,
        command_timeout=10, server_settings={"application_name": "meshinfo-columnar-api-verification",
            "timezone": "UTC", "default_transaction_read_only": "on", "lock_timeout": "250ms",
            "statement_timeout": "10000", "work_mem": "4MB", "max_parallel_workers_per_gather": "0",
            "jit": "off"})
    return storage


async def checked_call(storage, method, arguments, warnings, name, frozen_time=None):
    previous = len(warnings.levels)
    started = time.perf_counter()
    if frozen_time is None:
        result = await getattr(storage, method)(**arguments)
    else:
        with patch("time.time", return_value=frozen_time):
            result = await getattr(storage, method)(**arguments)
    elapsed = round((time.perf_counter()-started)*1000, 3)
    require(len(warnings.levels) == previous, f"Application emitted a warning/error during {name}")
    return result, elapsed


async def verify_case(storage, case, warnings, originals):
    name = case["name"]
    value, elapsed = await checked_call(storage, case["method"], case["arguments"], warnings,
                                       name, case.get("frozen_time"))
    details = describe(value)
    if case["positive"]:
        require(details["rows"] > 0, f"Expected nonempty result for {name}")
    if case["expected_ids"] is not None:
        require([row["mqtt_row_id"] for row in value["messages"]] == case["expected_ids"],
                f"Fixture row order/filter differs for {name}")
    if case["expected_packet"] is not None:
        require(value and value["mqtt_row_id"] == case["expected_packet"],
                f"Incorrect packet selected by {name}")
        if str(case["expected_packet"]) in originals:
            actual = [row.get("reconstructed") for row in value.get("receptions", [])]
            require(actual == originals[str(case["expected_packet"])],
                    f"Synthetic original copy reconstruction differs for {name}")
    if "expected_base" in case:
        base = {key: item for key, item in value.items() if key not in {"receptions", "reception_count"}}
        require(base == case["expected_base"], f"Synthetic payload decoding differs for {name}")
    if case["reconstruction"]:
        require(details["reconstructed_copies"] == details["receptions"] > 0,
                f"Missing reconstructed uplink copies for {name}")
    if name == "missing_deep_link":
        require(value is None, "Missing deep link unexpectedly returned a row")
    return {**details, "sha256": digest(value), "elapsed_ms": elapsed}


async def verify_window(storage, name, window, warnings):
    pages, row_ids, elapsed, before = [], [], 0, None
    for index in range(26):
        value, duration = await checked_call(storage, "query_mqtt_messages",
            {"limit": window["limit"], "start": window["start"], "end": window["end"], "before": before},
            warnings, name)
        elapsed += duration
        pages.append(value)
        row_ids.extend(row["mqtt_row_id"] for row in value["messages"])
        before = value["next_cursor"]
        if before is None:
            break
    require(before is None, f"Pagination exceeded bounded page budget for {name}")
    require(row_ids == window["ids"], f"Pagination lost, duplicated or reordered fixture rows for {name}")
    return {"rows": len(row_ids), "pages": len(pages),
            "sha256": digest(pages), "elapsed_ms": round(elapsed, 3)}


async def verify_variant(storage, cases, windows, warnings, originals, *, keep_going=False):
    results = {}

    async def record(name, awaitable):
        started = time.perf_counter()
        prior = len(warnings.categories)
        try:
            results[name] = await awaitable
        except Exception as exc:
            if not keep_going:
                raise
            results[name] = {"failed": True,
                "reason": str(exc) if isinstance(exc, VerificationFailure) else type(exc).__name__,
                "elapsed_ms": round((time.perf_counter()-started)*1000, 3),
                "application_log_categories": warnings.categories[prior:]}

    for case in cases:
        await record(case["name"], verify_case(storage, case, warnings, originals))
    for name, window in windows.items():
        await record(name, verify_window(storage, name, window, warnings))
    return results


def comparable(results):
    return {name: {key: value for key, value in details.items() if key != "elapsed_ms"}
            for name, details in results.items()}


async def run(args):
    require(re.fullmatch(r"[A-Z][A-Z0-9_]*", args.dsn_env), "Invalid DSN environment-variable name")
    dsn = os.environ.get(args.dsn_env, "")
    database = unquote(urlsplit(dsn).path.lstrip("/"))
    require(re.fullmatch(r"[A-Za-z0-9_]+_columnar_test", database), "Only dedicated *_columnar_test targets are allowed")
    benchmark = json.loads(args.benchmark_report.read_text())
    require(benchmark.get("schemas_retained"), "Benchmark must have retained its isolated schemas")
    for variant in ("heap_full", "heap_compatible", "columnar"):
        data = benchmark.get("variants", {}).get(variant, {})
        probes = data.get("constraints_and_trigger", {})
        require(data.get("all_rows_exact") and probes.get("duplicate_primary_key_rejected")
                and set(probes.get("trigger_probed_on", [])) == {"hot", "cold"}
                and probes.get("synthetic_probe_rows_rolled_back"),
                "Benchmark data/constraint gates must pass before application verification")
    shared = benchmark["shared_schema"]
    require(re.fullmatch(r"benchmark_shared_[a-f0-9]{10}", shared), "Invalid shared benchmark schema")
    manifest, packets, expected, sizes, hot, _trigger = load_fixture(args.fixture_dir)
    require({key: item["rows"] for key, item in expected.items()} == benchmark["fixture"]["expected_rows"],
            "Fixture counts differ from the benchmark report")
    require(expected == benchmark["fixture"].get("logical_digests"),
            "Fixture content differs from the benchmark report")
    lookback = manifest.get("reception_lookback_seconds", 86400)
    require(type(lookback) is int and 1 <= lookback <= 604800, "Invalid reception lookback")
    receptions = list(fixture_rows(args.fixture_dir/"receptions.jsonl", RECEPTION_COLUMNS, 250_000))
    topics = list(fixture_rows(args.fixture_dir/"topics.jsonl", TOPIC_COLUMNS, 20_000))
    cases, windows, receipts = plan_cases(packets, manifest, hot, receptions, topics)
    originals = synthetic_originals(packets, receptions, topics, manifest)
    previous = json.loads(args.restore_report.read_text()) if args.restore_report else None
    if previous:
        require("columnar" in previous.get("variants", {}), "Original API report has no columnar cases")
        require(previous["fixture_digest"] == digest(expected), "Restore fixture differs from the original verification")
    variants = ("columnar",) if previous else ("heap_full", "heap_compatible", "columnar")
    results = {}
    failures, comparisons = [], {}
    warnings = StorageWarnings()
    logger = logging.getLogger("storage.db.postgres")
    old_handlers, old_level, old_propagate = logger.handlers[:], logger.level, logger.propagate
    logger.handlers = [warnings]
    logger.setLevel(logging.WARNING)
    logger.propagate = False
    try:
        for variant in variants:
            schema = benchmark["variants"][variant]["schema"]
            require(re.fullmatch(r"benchmark_"+variant+r"_[a-f0-9]{10}", schema), "Invalid packet benchmark schema")
            storage = await make_storage(dsn, schema, shared, lookback)
            try:
                async with storage.pool.acquire() as conn:
                    require(await conn.fetchval("SELECT current_database()") == database,
                            "Connected database differs from guarded target")
                    require(await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", schema+".mqtt_messages"),
                            "Expected packet benchmark schema is missing")
                    require(await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", shared+".packet_receptions"),
                            "Expected shared benchmark schema is missing")
                current = await verify_variant(storage, cases, windows, warnings, originals, keep_going=True)
                if previous:
                    baseline = previous["variants"]["columnar"]
                elif results:
                    baseline = results["heap_full"]
                else:
                    baseline = current
                matched, unverified = [], []
                for name, details in current.items():
                    original = baseline.get(name)
                    if details.get("failed"):
                        failures.append({"variant": variant, "case": name, "reason": details["reason"]})
                    elif original is None or original.get("failed"):
                        unverified.append(name)
                        failures.append({"variant": variant, "case": name, "reason": "No successful baseline response to compare"})
                    elif comparable({name: details}) != comparable({name: original}):
                        failures.append({"variant": variant, "case": name, "reason": "Application response differs from baseline"})
                    else:
                        matched.append(name)
                comparisons[variant] = {"matched_cases": matched, "unverified_cases": unverified}
                results[variant] = current
            finally:
                await storage.pool.close()
    finally:
        logger.handlers, logger.level, logger.propagate = old_handlers, old_level, old_propagate
    return {"prototype_only": True, "all_checks_passed": not failures,
        "benchmark_all_checks_passed": bool(benchmark.get("all_checks_passed")),
        "created_at": datetime.now(UTC).isoformat(), "restore_verification": bool(previous),
        "fixture_digest": digest(expected), "fixture_rows": {key: value["rows"] for key, value in expected.items()},
        "variants": results, "cases_per_variant": len(next(iter(results.values()))),
        "query_failures": failures, "comparisons": comparisons,
        "application_warnings_or_errors": len(warnings.levels),
        "synthetic_packets": len(manifest.get("synthetic_packet_ids", [])),
        "packets_with_receptions_in_fixture": len(receipts),
        "limits": ["Actual storage/API reader methods; HTTP routing and live ingestion are not exercised.",
                   "Bounded fixture and warm local caches do not establish production-scale latency."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dsn-env", required=True)
    parser.add_argument("--benchmark-report", type=Path, required=True)
    parser.add_argument("--fixture-dir", type=Path, required=True)
    parser.add_argument("--restore-report", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        report = asyncio.run(run(args))
    except Exception as exc:
        context = str(exc) if isinstance(exc, VerificationFailure) else type(exc).__name__
        print(f"Columnar application verification failed: {context}", file=sys.stderr)
        return 1
    args.output.write_text(json.dumps(report, indent=2)+"\n")
    print(json.dumps({"all_checks_passed": report["all_checks_passed"], "cases_per_variant": report["cases_per_variant"],
                      "variants": list(report["variants"]), "output": str(args.output)}))
    return 0 if report["all_checks_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
