"""Bounded native PostgreSQL compression and index-packing benchmarks.

Benchmarks only copy a bounded source sample into disposable temporary tables.
They never rewrite source data or claim savings without measured evidence.
"""

import math
import statistics
import time
import uuid

from storage.db.storage_report import monthly_partition_bounds
from datetime import datetime, timedelta, timezone

MIB = 1024 ** 2
GIB = 1024 ** 3
MQTT_COLUMNS = (
    "id", "topic", "payload", "qos", "retain", "timestamp", "created_at",
    "from_node_id", "to_node_id", "packet_id",
)
_COLUMNS_SQL = ", ".join(f'"{name}"' for name in MQTT_COLUMNS)


class CompressionRefused(ValueError):
    """An unsupported layout or resource limit prevented a rewrite."""


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def qualified(name: str) -> str:
    return '"public".' + quote_identifier(name)


def validate_target(target: int) -> int:
    if type(target) is not int or not 128 <= target <= 8160:
        raise ValueError("toast_tuple_target must be an integer from 128 to 8160")
    return target


async def collect_benchmark_sample(conn, *, per_partition: int = 500,
                                   max_bytes: int = 16 * MIB, max_partitions: int = 6,
                                   max_rows: int = 2000, seed: int = 1729) -> tuple[list[tuple], list[dict]]:
    """Read a seeded block sample, capped by rows, bytes and statement time.

    SYSTEM sampling avoids scanning a whole leaf. It samples blocks rather than
    individual rows, so this is a bounded workload sample, not a statistical
    estimate of total archive savings. Huge payloads are explicitly excluded.
    """
    if (not 1 <= per_partition <= 1000 or not 1 <= max_partitions <= 12
            or not MIB <= max_bytes <= 32 * MIB or not 1 <= max_rows <= 4000
            or type(seed) is not int):
        raise ValueError("sample bounds exceeded")
    rows, metadata = [], []
    used = 0
    async with conn.transaction(readonly=True):
        await conn.execute("SET LOCAL statement_timeout='3s'")
        await conn.execute("SET LOCAL lock_timeout='250ms'")
        await conn.execute("SET LOCAL TIME ZONE 'UTC'")
        leaves = await conn.fetch("""SELECT c.relname, pg_get_expr(c.relpartbound,c.oid) AS bound,
                   pg_relation_size(c.oid) AS heap_bytes, c.reltuples
            FROM pg_class c JOIN pg_inherits i ON i.inhrelid=c.oid
            WHERE i.inhparent='public.mqtt_messages'::regclass AND c.relkind='r'
              AND pg_relation_size(c.oid)>0 ORDER BY c.relname DESC""")
        selected = [(r, monthly_partition_bounds(r["bound"])) for r in leaves]
        selected = [(r, b) for r, b in selected if b is not None][:max_partitions]
        for leaf, _ in selected:
            # Oversample by 4 to tolerate block clustering without retry scans.
            estimated_rows = max(float(leaf["reltuples"]), leaf["heap_bytes"] / 1024, 1)
            percent = min(100.0, max(0.0001, per_partition * 4 / estimated_rows * 100))
            candidates = conn.cursor(
                f"SELECT {_COLUMNS_SQL},pg_column_compression(payload) AS stored_compression "
                f"FROM {qualified(leaf['relname'])} TABLESAMPLE SYSTEM ($1) REPEATABLE ($2) "
                "WHERE coalesce(octet_length(payload),0)+coalesce(octet_length(topic),0)<=$3 LIMIT $4",
                percent, seed, MIB, min(per_partition, max_rows - len(rows)), prefetch=8)
            added, lengths, methods = 0, [], {}
            capped = False
            async for row in candidates:
                values = tuple(row[name] for name in MQTT_COLUMNS)
                size = sum(len(v.encode("utf-8")) for v in values if isinstance(v, str)) + 128
                if used + size > max_bytes:
                    capped = True
                    break
                used += size
                rows.append(values)
                added += 1
                lengths.append(len((row["payload"] or "").encode("utf-8")))
                method = row["stored_compression"] or "none"
                methods[method] = methods.get(method, 0) + 1
            metadata.append({"partition": leaf["relname"], "rows": added,
                             "sampling": "SYSTEM block sample", "sampling_percent": percent,
                             "seed": seed, "excludes_text_over_bytes": MIB,
                             "payload_min_bytes": min(lengths) if lengths else None,
                             "payload_max_bytes": max(lengths) if lengths else None,
                             "payload_mean_bytes": round(statistics.mean(lengths), 2) if lengths else None,
                             "stored_payload_compression": methods, "byte_cap_reached": capped})
            if capped or len(rows) >= max_rows:
                break
    return rows, metadata


async def benchmark_compression(conn, rows: list[tuple], *, targets=(2040, 1024, 512), repeats=7) -> dict:
    """Temporary-table benchmark; reports initial and repeated queries, never 'cold cache'."""
    if not rows or len(rows) > 20_000 or not 3 <= repeats <= 25 or len(targets) > 5:
        raise ValueError("benchmark requires 1..20000 rows, 3..25 repetitions, and <=5 targets")
    if any(len(row) != len(MQTT_COLUMNS) for row in rows):
        raise ValueError("benchmark rows must match the MQTT column shape")
    if sum(len(value.encode("utf-8")) for row in rows for value in row if isinstance(value, str)) > 32 * MIB:
        raise ValueError("benchmark sample exceeds 32 MiB")
    targets = tuple(dict.fromkeys(validate_target(t) for t in targets))
    baseline_results, results = {}, []
    node = next((row[7] for row in rows if row[7]), None)
    packet = rows[len(rows) // 2][0]
    queries = {
        "recent_page": (f"SELECT {_COLUMNS_SQL} FROM {{table}} ORDER BY created_at DESC,id DESC LIMIT 100", ()),
        "payload_search": (f"SELECT {_COLUMNS_SQL} FROM {{table}} WHERE payload ILIKE '%from%' ORDER BY created_at DESC,id DESC LIMIT 100", ()),
        "packet_lookup": (f"SELECT {_COLUMNS_SQL} FROM {{table}} WHERE id=$1 ORDER BY created_at,id", (packet,)),
        "node_page": (f"SELECT {_COLUMNS_SQL} FROM {{table}} WHERE from_node_id=$1 ORDER BY created_at DESC,id DESC LIMIT 100", (node,)),
    }
    async with conn.transaction():
        await conn.execute("SET LOCAL statement_timeout='15s'")
        await conn.execute("SET LOCAL lock_timeout='250ms'")
        await conn.execute("SET LOCAL work_mem='8MB'")
        for target in targets:
            table = "compression_benchmark_" + uuid.uuid4().hex[:12]
            qt = quote_identifier(table)
            await conn.execute(f"CREATE TEMP TABLE {qt} (LIKE public.mqtt_messages INCLUDING ALL) ON COMMIT DROP")
            await conn.execute(f"ALTER TABLE {qt} SET (toast_tuple_target={target})")
            await conn.execute(f"ALTER TABLE {qt} ALTER COLUMN payload SET COMPRESSION lz4")
            started = time.perf_counter()
            await conn.copy_records_to_table(table, columns=MQTT_COLUMNS, records=rows)
            insert_ms = (time.perf_counter() - started) * 1000
            await conn.execute(f"ANALYZE {qt}")
            sizes = await conn.fetchrow("""SELECT pg_total_relation_size($1::regclass) AS total_bytes,
                pg_table_size($1::regclass) AS table_bytes, pg_indexes_size($1::regclass) AS index_bytes""", qt)
            timings = {}
            for name, (query, parameters) in queries.items():
                elapsed = []
                for repetition in range(repeats + 1):
                    started = time.perf_counter()
                    found = [tuple(row) for row in await conn.fetch(query.format(table=qt), *parameters)]
                    elapsed.append((time.perf_counter() - started) * 1000)
                    if name not in baseline_results:
                        baseline_results[name] = found
                    elif found != baseline_results[name]:
                        raise CompressionRefused(f"benchmark query changed results: {name}")
                warm = sorted(elapsed[1:])
                timings[name] = {"initial_ms": round(elapsed[0], 3), "warm_median_ms": round(statistics.median(warm), 3),
                                 "warm_p95_ms": round(warm[math.ceil(len(warm) * .95) - 1], 3)}
            stored = [tuple(row) for row in await conn.fetch(f"SELECT {_COLUMNS_SQL} FROM {qt} ORDER BY id,created_at")]
            if stored != sorted(rows, key=lambda row: (row[0], row[6])):
                raise CompressionRefused("benchmark changed stored payloads or row identities")
            compression = await conn.fetch(f"SELECT coalesce(pg_column_compression(payload),'none') AS method,count(*) AS rows FROM {qt} GROUP BY 1")
            results.append({"toast_tuple_target": target, **dict(sizes), "insert_ms": round(insert_ms, 3),
                            "queries": timings, "payload_compression": [dict(r) for r in compression], "rows_verified": len(rows)})
            await conn.execute(f"DROP TABLE {qt}")
    return {"rows": len(rows), "results": results,
            "limits": "Bounded sample; copied data is already cached. Initial queries are not cold-cache measurements. No source rows were rewritten."}


async def benchmark_index_geometry(conn, rows: list[tuple], *, modeled_rows: int = 20000) -> dict:
    """Model append-time index growth using bounded payload samples and synthetic IDs/times.

    Measures identical indexed values under ASC/DESC append order and closed-index
    REINDEX at fillfactor 90/100. This isolates layout behavior; it is not a claim
    about actual whole-archive savings or a substitute for production timing.
    """
    if not rows or not 1000 <= modeled_rows <= 40000:
        raise ValueError("geometry benchmark needs a sample and 1000..40000 modeled rows")
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    records = []
    text_bytes = 0
    for number in range(modeled_rows):
        record = list(rows[number % len(rows)])
        if len(record) != len(MQTT_COLUMNS):
            raise ValueError("benchmark rows must match the MQTT column shape")
        text_bytes += sum(len(value.encode("utf-8")) for value in record if isinstance(value, str))
        if text_bytes > 32 * MIB:
            raise ValueError("modeled rows exceed the 32 MiB text budget")
        record[0] = number + 1
        record[6] = start + timedelta(seconds=number)
        records.append(tuple(record))
    result = []
    baseline = None
    async with conn.transaction():
        await conn.execute("SET LOCAL statement_timeout='15s'")
        await conn.execute("SET LOCAL work_mem='8MB'")
        await conn.execute("SET LOCAL maintenance_work_mem='32MB'")
        for order in ("DESC", "ASC"):
            table = "index_geometry_" + uuid.uuid4().hex[:12]
            index = table + "_time"
            qt, qi = quote_identifier(table), quote_identifier(index)
            await conn.execute(f"CREATE TEMP TABLE {qt} (LIKE public.mqtt_messages INCLUDING DEFAULTS INCLUDING STORAGE INCLUDING COMPRESSION) ON COMMIT DROP")
            await conn.execute(f"CREATE INDEX {qi} ON {qt} (created_at {order}) WITH (fillfactor=90)")
            await conn.copy_records_to_table(table, columns=MQTT_COLUMNS, records=records)
            await conn.execute(f"ANALYZE {qt}")
            phases = {}
            for phase, fillfactor in (("after_append", None), ("rebuilt_90", 90), ("rebuilt_100", 100)):
                if fillfactor is not None:
                    await conn.execute(f"ALTER INDEX {qi} SET (fillfactor={fillfactor})")
                    await conn.execute(f"REINDEX INDEX {qi}")
                sizes = await conn.fetchrow("""SELECT pg_relation_size($1::regclass) AS index_bytes,
                                                pg_total_relation_size($2::regclass) AS total_bytes""", qi, qt)
                query = f"SELECT {_COLUMNS_SQL} FROM {qt} ORDER BY created_at DESC LIMIT 100"
                times = []
                for _ in range(9):
                    before = time.perf_counter()
                    found = [tuple(row) for row in await conn.fetch(query)]
                    times.append((time.perf_counter() - before) * 1000)
                    if baseline is None:
                        baseline = found
                    elif baseline != found:
                        raise CompressionRefused("index geometry changed query results")
                explain = await conn.fetchval("EXPLAIN (FORMAT JSON) " + query)
                phases[phase] = {**dict(sizes), "warm_median_ms": round(statistics.median(times[1:]), 3),
                                 "plan": explain}
            result.append({"index_order": order, "phases": phases})
            await conn.execute(f"DROP TABLE {qt}")
    return {"modeled_rows": modeled_rows, "sample_rows": len(rows), "results": result,
            "limits": "Synthetic increasing IDs/timestamps with sampled payloads; cached data. Models index layout only; no source rows or indexes changed."}
