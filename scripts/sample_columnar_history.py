#!/usr/bin/env python3
"""Export a private, bounded fixture for the isolated columnar prototype.

The source session is read-only. Eight seeded block samples preserve source
payload text; linked receptions use the application's bounded time window.
Small, explicitly marked synthetic witnesses are added only to the output.
An existing output directory is refused, and incomplete exports are removed.
No source payloads or connection credentials are printed.
"""

from __future__ import annotations

import argparse
import ast
import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import asyncpg

from scripts.manage_storage import load_config
from storage.db.compression import MQTT_COLUMNS, collect_benchmark_sample

UTC = timezone.utc
MIB = 1024 ** 2
SEEDS = (1729, 2718, 3141, 5772, 6931, 8111, 104729, 130363)
RECEPTION_COLUMNS = (
    "mqtt_row_id", "created_at", "gateway", "topic_id", "rx_rssi", "rx_snr",
    "rx_time", "hop_limit", "hops_away", "relay_node", "transport", "extras",
)
TOPIC_COLUMNS = ("id", "topic")
MAX_PACKET_ROWS = 32_000
MAX_PACKET_BYTES = 64 * MIB
MAX_RECEPTION_ROWS = 200_000
MAX_RECEPTION_BYTES = 64 * MIB
MAX_FIXTURE_BYTES = 128 * MIB
MAX_RECORD_BYTES = 2 * MIB
MAX_TOPIC_ROWS = 20_000
MAX_BATCH_RECEPTIONS = 10_000
RECEPTION_BATCH = 128
MAX_SECONDS = 180
FILES = ("packets.jsonl", "receptions.jsonl", "topics.jsonl", "trigger.sql", "manifest.json")


class ExportRefused(ValueError):
    """An explicit safety or fixture-integrity limit was reached."""


def normalize(value):
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ExportRefused("Source timestamp is missing its timezone")
        return value.astimezone(UTC).isoformat(timespec="microseconds")
    raise ExportRefused("Unsupported source value type")


def encode(value):
    return (json.dumps(value, ensure_ascii=False, separators=(",", ":"),
                       allow_nan=False, default=normalize) + "\n").encode("utf-8")


class FixtureWriter:
    def __init__(self, directory):
        self.directory = directory
        self.total = 0
        self.inventory = {}

    def open(self, name):
        # O_EXCL refuses existing paths, including symlinks. The parent is new
        # and private; do not follow any pre-existing fixture paths.
        fd = os.open(self.directory / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.fchmod(fd, 0o600)
        self.inventory[name] = {"bytes": 0, "rows": 0, "sha256": hashlib.sha256()}
        return os.fdopen(fd, "wb")

    def write(self, stream, name, data, *, cap, row=False):
        item = self.inventory[name]
        if row and len(data) > MAX_RECORD_BYTES:
            raise ExportRefused("A fixture record exceeds the 2 MiB record budget")
        if item["bytes"] + len(data) > cap or self.total + len(data) > MAX_FIXTURE_BYTES:
            raise ExportRefused("Fixture byte budget exceeded; export was not truncated")
        stream.write(data)
        item["bytes"] += len(data)
        item["rows"] += int(row)
        item["sha256"].update(data)
        self.total += len(data)

    def write_rows(self, name, rows, *, cap, maximum):
        with self.open(name) as stream:
            for number, row in enumerate(rows, 1):
                if number > maximum:
                    raise ExportRefused("Fixture row budget exceeded; export was not truncated")
                self.write(stream, name, encode(row), cap=cap, row=True)

    def metadata(self):
        return {name: {**{k: v for k, v in values.items() if k != "sha256"},
                       "sha256": values["sha256"].hexdigest()}
                for name, values in self.inventory.items()}


class SamplerConnection:
    """Keep the existing sampler's local lock timeout within this tool's cap."""

    def __init__(self, connection):
        self.connection = connection

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def execute(self, query, *args, **kwargs):
        # The reused sampler otherwise sets 250ms inside its transaction.
        if query.strip().lower().startswith("set local lock_timeout"):
            query = "SET LOCAL lock_timeout='100ms'"
        return await self.connection.execute(query, *args, **kwargs)


def extract_trigger():
    tree = ast.parse((ROOT / "storage/db/postgres.py").read_text(encoding="utf-8"))
    prefixes = (
        "CREATE OR REPLACE FUNCTION mqtt_messages_extract_node_ids()",
        "DROP TRIGGER IF EXISTS trg_mqtt_messages_extract_node_ids ON mqtt_messages;",
        "CREATE TRIGGER trg_mqtt_messages_extract_node_ids",
    )
    found = []
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "execute" and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)):
            value = node.args[0].value
            matches = [index for index, prefix in enumerate(prefixes) if value.lstrip().startswith(prefix)]
            if matches:
                found.append((node.lineno, matches[0], value))
    found.sort()
    if [entry[1] for entry in found] != [0, 1, 2]:
        raise ExportRefused("Application trigger SQL literals changed; explicit review required")
    result = "\n".join(entry[2] for entry in found).encode("utf-8")
    if len(result) > 64 * 1024:
        raise ExportRefused("Application trigger SQL exceeds the fixture budget")
    return result


async def sample_packets(conn, per_partition):
    unique = {}
    encoded_bytes = 0
    sampling = []
    adapter = SamplerConnection(conn)
    for seed in SEEDS:
        rows, metadata = await collect_benchmark_sample(
            adapter, per_partition=per_partition, max_rows=per_partition * 4, max_bytes=16 * MIB, seed=seed)
        if any(entry.get("byte_cap_reached") for entry in metadata):
            raise ExportRefused("A source sample reached its byte cap; export was not truncated")
        sampling.extend(metadata)
        for row in rows:
            if len(row) != len(MQTT_COLUMNS):
                raise ExportRefused("Source packet column shape changed")
            if row[0] in unique:
                if unique[row[0]] != row:
                    raise ExportRefused("A deep-link packet ID identifies multiple source rows")
                continue
            encoded = encode(row)
            if len(encoded) > MAX_RECORD_BYTES:
                raise ExportRefused("A source packet exceeds the fixture record budget")
            encoded_bytes += len(encoded)
            if encoded_bytes > MAX_PACKET_BYTES or len(unique) >= MAX_PACKET_ROWS:
                raise ExportRefused("Source packet budget exceeded; export was not truncated")
            unique[row[0]] = row
    packets = sorted(unique.values(), key=lambda row: (row[6], row[0]))
    if not packets:
        raise ExportRefused("The bounded source sample contains no packets")
    if packets[0][6].strftime("%Y-%m") == packets[-1][6].strftime("%Y-%m"):
        raise ExportRefused("Real source sample must contain at least two months")
    return packets, sampling


async def export_receptions(conn, writer, packets, lookback, synthetic):
    name = "receptions.jsonl"
    topics = set()
    real_count = 0
    delta = timedelta(seconds=lookback)
    # A CASE bounds transfer of pathological JSONB values without silently
    # dropping the row. The extra flag causes an explicit failure instead.
    projection = ",".join('"' + column + '"' for column in RECEPTION_COLUMNS[:-1])
    query = f"""SELECT {projection},
        CASE WHEN octet_length(extras::text) <= $4 THEN extras::text ELSE NULL END AS extras,
        coalesce(octet_length(extras::text),0) > $4 AS oversized_extras
        FROM public.packet_receptions
        WHERE mqtt_row_id = ANY($1::bigint[])
          AND created_at >= $2 AND created_at < $3
        LIMIT {MAX_BATCH_RECEPTIONS + 1}"""
    with writer.open(name) as stream:
        for offset in range(0, len(packets), RECEPTION_BATCH):
            batch = packets[offset:offset + RECEPTION_BATCH]
            times = {row[0]: row[6] for row in batch}
            seen = 0
            async for row in conn.cursor(query, list(times), batch[0][6], batch[-1][6] + delta,
                                         MIB, prefetch=16):
                seen += 1
                if seen > MAX_BATCH_RECEPTIONS:
                    raise ExportRefused("A reception batch exceeded 10000 rows; export was not truncated")
                if row["oversized_extras"]:
                    raise ExportRefused("Source reception extras exceed the bounded transfer budget")
                canonical_time = times[row["mqtt_row_id"]]
                if not canonical_time <= row["created_at"] < canonical_time + delta:
                    continue
                real_count += 1
                if real_count + len(synthetic) > MAX_RECEPTION_ROWS:
                    raise ExportRefused("Reception row budget exceeded; export was not truncated")
                values = tuple(row[column] for column in RECEPTION_COLUMNS)
                writer.write(stream, name, encode(values), cap=MAX_RECEPTION_BYTES, row=True)
                if row["topic_id"] is not None:
                    topics.add(row["topic_id"])
                if len(topics) > MAX_TOPIC_ROWS:
                    raise ExportRefused("Reception topic dictionary exceeds the fixture budget")
        for row in synthetic:
            writer.write(stream, name, encode(row), cap=MAX_RECEPTION_BYTES, row=True)
    return real_count, topics


async def synthetic_witnesses(conn, packets, hot):
    """Construct small witnesses without modifying any source relation."""
    first_id = 8_000_000_000_000_000
    node, other = "c0decafe", "deadbeef"
    packet_id = 4_242_424_242
    literal = "msh/prototype/%_\\literal/!c0decafe"
    receipt_term = "columnar-reception-only-witness"
    topic_ids = (2_000_000_000, 2_000_000_001)
    older = hot - timedelta(days=3)
    newer = hot + timedelta(days=2, hours=12)
    base = {"from": int(node, 16), "to": int(other, 16), "id": packet_id,
            "payload": {"text": "Unicode 雪 🛰️", "nullable": None}}
    pretty = " \n" + json.dumps(base, ensure_ascii=False, indent=2) + "\n "
    self_payload = json.dumps({**base, "to": int(node, 16), "id": packet_id + 1})
    definitions = [
        ("reused_old", older, pretty, node, other, packet_id),
        ("reused_new", newer, pretty, node, other, packet_id),
        ("self_addressed_tie_a", older + timedelta(seconds=1), self_payload, node, node, packet_id + 1),
        ("self_addressed_tie_b", older + timedelta(seconds=1), self_payload, node, node, packet_id + 1),
        ("unicode_whitespace", older + timedelta(seconds=2), pretty, node, other, packet_id),
        ("null_payload", older + timedelta(seconds=3), None, None, None, None),
        ("malformed_payload", older + timedelta(seconds=4), '{"from": malformed', None, None, None),
        ("scalar_payload", older + timedelta(seconds=5), '42', None, None, None),
        ("cross_month", hot - timedelta(microseconds=1), pretty, node, other, packet_id),
    ]
    rows, ids = [], {}
    existing = {row[0] for row in packets}
    for index, (kind, created, payload, sender, recipient, pid) in enumerate(definitions, 1):
        identity = first_id + index
        if identity in existing:
            raise ExportRefused("Synthetic packet ID collides with a sampled source ID")
        ids[kind] = identity
        rows.append((identity, literal, payload, 0, False, int(created.timestamp()), created,
                     sender, recipient, pid))
    # Let PostgreSQL render canonical JSONB text, so its ordering/spacing match
    # a later binary COPY round trip exactly. This SELECT has no source writes.
    extras = await conn.fetchval("SELECT $1::jsonb::text", json.dumps(
        {"set": {"rxRssi": None, "prototype_note": "雪 quoted \"value\"\n", "nullable": None}, "del": ["to"]}, ensure_ascii=False))
    receipts = [
        (ids["cross_month"], hot + timedelta(seconds=1), other, topic_ids[1], -91, 4.25,
         int((hot + timedelta(seconds=1)).timestamp()), 3, 1, None, 0, extras),
        (ids["self_addressed_tie_a"], older + timedelta(seconds=2), other, topic_ids[0],
         None, None, None, None, None, None, None, None),
    ]
    topics = [(topic_ids[0], literal), (topic_ids[1], "msh/prototype/" + receipt_term + "/!deadbeef")]
    terms = {"node_id": node, "sender_node_id": node, "sender_packet_id": packet_id,
             "sender_expected_latest_row_id": ids["reused_new"], "topic_literal": literal,
             "receipt_topic_search": receipt_term}
    return rows, receipts, topics, ids, terms


async def export(args):
    config = load_config(args.config)
    pg = config["storage"]["postgres"]
    lookback = max(86400, 2 * int(config["storage"].get("dedup_window_seconds", 900)))
    if lookback > 604800:
        raise ExportRefused("Reception lookback exceeds the seven-day prototype limit")
    trigger = extract_trigger()
    # mkdir fails even for an empty existing directory. Only files owned by
    # this invocation are ever cleaned up after an incomplete export.
    args.output.mkdir(mode=0o700)
    args.output.chmod(0o700)
    writer = FixtureWriter(args.output)
    conn = None
    complete = False
    try:
        async with asyncio.timeout(MAX_SECONDS):
            conn = await asyncpg.connect(
                host=args.host or pg["host"], port=pg.get("port", 5432),
                user=pg["username"], password=pg.get("password", ""), database=pg["database"],
                timeout=5, command_timeout=5,
                server_settings={"application_name": "meshinfo-columnar-sample",
                                 "default_transaction_read_only": "on", "timezone": "UTC",
                                 "statement_timeout": "4s", "lock_timeout": "100ms",
                                 "idle_in_transaction_session_timeout": "15s", "work_mem": "4MB"})
            async with conn.transaction(isolation="repeatable_read", readonly=True):
                packets, sampling = await sample_packets(conn, args.per_partition)
                await conn.execute("SET LOCAL statement_timeout='4s'")
                hot = packets[-1][6].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                added, receipts, extra_topics, witness_ids, terms = await synthetic_witnesses(conn, packets, hot)
                real_packets = len(packets)
                writer.write_rows("packets.jsonl", sorted(packets + added, key=lambda row: (row[6], row[0])),
                                  cap=MAX_PACKET_BYTES, maximum=MAX_PACKET_ROWS + len(added))
                real_receipts, topic_ids = await export_receptions(conn, writer, packets, lookback, receipts)
                if topic_ids.intersection(row[0] for row in extra_topics):
                    raise ExportRefused("Synthetic topic ID collides with a source dictionary ID")
                topics = await conn.fetch("SELECT id,topic FROM public.mqtt_topics WHERE id=ANY($1::integer[]) ORDER BY id LIMIT 20001",
                                          sorted(topic_ids))
                if len(topics) + len(extra_topics) > MAX_TOPIC_ROWS:
                    raise ExportRefused("Topic dictionary row budget exceeded")
                writer.write_rows("topics.jsonl", [tuple(row) for row in topics] + extra_topics,
                                  cap=16 * MIB, maximum=MAX_TOPIC_ROWS)
                index_rows = await conn.fetch("""SELECT idx.relname AS name,
                    pg_get_indexdef(i.indexrelid) AS definition, i.indisprimary AS primary,
                    am.amname AS method FROM pg_index i
                    JOIN pg_class idx ON idx.oid=i.indexrelid
                    JOIN pg_am am ON am.oid=idx.relam
                    WHERE i.indrelid='public.mqtt_messages'::regclass
                    ORDER BY idx.relname LIMIT 65""")
                if not index_rows or len(index_rows) > 64:
                    raise ExportRefused("Source packet index inventory is empty or exceeds its budget")
                version = await conn.fetchval("SHOW server_version")
            with writer.open("trigger.sql") as stream:
                writer.write(stream, "trigger.sql", trigger, cap=64 * 1024)
            manifest = {
                "version": 1, "created_at": datetime.now(UTC).isoformat(),
                "packet_columns": list(MQTT_COLUMNS), "reception_columns": list(RECEPTION_COLUMNS),
                "topic_columns": list(TOPIC_COLUMNS), "hot_month": hot.date().isoformat(),
                "reception_lookback_seconds": lookback,
                "source_postgres_version": version,
                "source_packet_indexes": [dict(row) for row in index_rows],
                "sampling": sampling, "seeds": list(SEEDS), "per_partition": args.per_partition,
                "sampling_note": "Bounded repeatable-read SYSTEM block samples, not a random population estimate; source text over 1 MiB is excluded by the reused sampler.",
                "real_counts": {"packets": real_packets, "receptions": real_receipts, "topics": len(topics)},
                "synthetic_counts": {"packets": len(added), "receptions": len(receipts), "topics": len(extra_topics)},
                "synthetic_packet_ids": list(witness_ids.values()), "synthetic_witnesses": witness_ids,
                "synthetic_note": "Explicit edge-case witnesses exist only in this fixture; no source rows were inserted or changed.",
                "query_terms": terms, "missing_source_topic_ids": len(topic_ids) - len(topics),
                "files": writer.metadata(),
                "limits": {"source_packet_rows": MAX_PACKET_ROWS, "reception_rows": MAX_RECEPTION_ROWS,
                           "reception_bytes": MAX_RECEPTION_BYTES, "fixture_bytes": MAX_FIXTURE_BYTES,
                           "statement_timeout_ms": 4000, "lock_timeout_ms": 100,
                           "runtime_seconds": MAX_SECONDS},
            }
            with writer.open("manifest.json") as stream:
                writer.write(stream, "manifest.json", encode(manifest), cap=MIB)
            complete = True
            return {"output": str(args.output.resolve()), "real_counts": manifest["real_counts"],
                    "synthetic_counts": manifest["synthetic_counts"], "fixture_bytes": writer.total,
                    "source_read_only": True}
    finally:
        try:
            if conn is not None:
                await conn.close(timeout=5)
        finally:
            if not complete:
                for name in writer.inventory:
                    (args.output / name).unlink(missing_ok=True)
                args.output.rmdir()


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--config", type=Path, default=Path("config.toml"))
    result.add_argument("--host", help="Explicit source database hostname override")
    result.add_argument("--per-partition", type=int, choices=(250, 500, 1000), default=500,
                        help="Rows per monthly partition per seed (default: 500); hard resource caps remain fixed")
    result.add_argument("--output", type=Path, required=True, help="NEW private fixture directory; existing paths are refused")
    return result


def main():
    args = parser().parse_args()
    try:
        result = asyncio.run(export(args))
    except (OSError, ValueError, asyncpg.PostgresError, TimeoutError) as exc:
        # Database exception text may contain SQL context, payloads or a DSN.
        message = str(exc) if isinstance(exc, ExportRefused) else type(exc).__name__
        print("Columnar fixture export failed: " + message, file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
