"""Bounded, read-only storage inventory and retention preview.

``build_storage_report(connection_or_pool, full_config, ...)`` returns a public
history envelope and a separate operator inventory. It reads PostgreSQL catalogs
only: row counts are planner estimates, not COUNT(*) scans. Partition bounds are
enclosing ranges, never claims about the first/last stored packet. No host paths,
credentials, or capacity information enter the public envelope.

Candidate bytes describe whole old packet partitions, including their indexes
and TOAST storage. They are not promised reclaimed space: the executor must
revalidate layout, dependencies, and cross-month receptions under locks before
expiring anything. Row deletion in other scopes normally makes space reusable
inside PostgreSQL; it does not immediately return that space to the filesystem.
"""

from collections.abc import Mapping
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import re

from storage.retention import (
    RETENTION_SCOPES, SCOPE_TABLES, complete_month_cutoff, retention_policy,
    retention_summary, timestamp_json, utc_datetime,
)
from storage.db.maintenance_status import read_status, public_status

# Names select only the app's public history tables. Identifiers are never
# interpolated into SQL, including names returned from the catalogs.
_CATALOG_SQL = """
WITH parents AS (
    SELECT c.oid, c.relname, c.relkind,
           CASE WHEN c.relkind = 'p' THEN pg_get_partkeydef(c.oid) END AS partition_key
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relname = ANY($1::text[])
), relations AS (
    SELECT p.relname AS parent_name, p.partition_key, p.oid AS relation_oid, true AS is_parent
    FROM parents p
    UNION ALL
    SELECT p.relname, p.partition_key, i.inhrelid, false
    FROM parents p JOIN pg_inherits i ON i.inhparent = p.oid
)
SELECT r.parent_name, r.partition_key, r.is_parent, c.oid::bigint AS relation_oid,
       n.nspname AS schema_name, c.relname AS relation_name, c.relkind::text AS relation_kind,
       CASE WHEN c.relispartition THEN pg_get_expr(c.relpartbound, c.oid) END AS partition_bound,
       CASE WHEN c.relkind = 'r' THEN pg_total_relation_size(c.oid) ELSE 0 END AS total_bytes,
       CASE WHEN c.relkind = 'r' THEN pg_table_size(c.oid) ELSE 0 END AS table_bytes,
       CASE WHEN c.relkind = 'r' THEN pg_indexes_size(c.oid) ELSE 0 END AS index_bytes,
       CASE WHEN c.relkind = 'r' THEN pg_relation_size(c.oid) ELSE 0 END AS heap_bytes,
       c.reltuples::double precision AS estimated_rows
FROM relations r JOIN pg_class c ON c.oid = r.relation_oid
JOIN pg_namespace n ON n.oid = c.relnamespace
ORDER BY r.parent_name, r.is_parent DESC, n.nspname, c.relname
"""

_BOUND_RE = re.compile(r"^FOR VALUES FROM \('([^']+)'\) TO \('([^']+)'\)$")


def monthly_partition_bounds(expression: str | None) -> tuple[datetime, datetime] | None:
    """Accept only finite, single-column, complete UTC month ranges.

    Parse catalog expressions, not table-name suffixes. DEFAULT, MINVALUE,
    MAXVALUE, multi-column keys, and non-monthly ranges remain ineligible.
    """
    match = _BOUND_RE.fullmatch(expression or "")
    if not match:
        return None
    try:
        start, end = (utc_datetime(datetime.fromisoformat(value)) for value in match.groups())
    except (ValueError, OverflowError):
        return None
    if start.day != 1 or any((start.hour, start.minute, start.second, start.microsecond)):
        return None
    if start.year == 9999 and start.month == 12:
        return None
    expected_end = start.replace(year=start.year + (start.month == 12), month=start.month % 12 + 1)
    if end != expected_end:
        return None
    return start, end


@asynccontextmanager
async def _connection(source):
    if hasattr(source, "acquire"):
        async with source.acquire(timeout=3.0) as conn:
            yield conn
    else:
        yield source


def _estimate(row: Mapping) -> int | None:
    if row["relation_kind"] != "r":
        return None
    # No allocated heap pages means no stored rows, regardless of ANALYZE age.
    if row["heap_bytes"] == 0:
        return 0
    value = row["estimated_rows"]
    return max(0, round(value)) if value is not None and value >= 0 else None


def _relation_detail(row: Mapping, cutoff: datetime | None) -> dict:
    bounds = monthly_partition_bounds(row["partition_bound"]) if not row["is_parent"] else None
    return {
        "oid": row["relation_oid"],
        "schema": row["schema_name"],
        "name": row["relation_name"],
        "parent": row["parent_name"],
        "kind": row["relation_kind"],
        "is_parent": row["is_parent"],
        "partition_key": row["partition_key"],
        "partition_bound": row["partition_bound"],
        "start": timestamp_json(bounds[0]) if bounds else None,
        "end": timestamp_json(bounds[1]) if bounds else None,
        "total_bytes": row["total_bytes"],
        "table_bytes": row["table_bytes"],
        "index_bytes": row["index_bytes"],
        "heap_bytes": row["heap_bytes"],
        "estimated_rows": _estimate(row),
        "expired_by_age": bool(bounds and cutoff and bounds[1] <= cutoff),
        "eligible": False,
        "eligibility": "keep_forever" if cutoff is None else "requires_row_expiration",
    }


def _packet_candidates(relations: list[dict], cutoff: datetime | None) -> list[str]:
    """Fail closed on layouts not covered by paired monthly packet expiration."""
    parents = {r["name"]: r for r in relations if r["is_parent"]}
    issues = []
    for name in SCOPE_TABLES["packets"]:
        parent = parents.get(name)
        if not parent or parent["kind"] != "p" or parent["partition_key"] != "RANGE (created_at)":
            issues.append(f"{name} is not a supported RANGE (created_at) partitioned table")
    children = [r for r in relations if not r["is_parent"]]
    for row in children:
        if row["kind"] != "r":
            issues.append(f"{row['name']} is not a regular leaf partition")
        elif row["partition_bound"] == "DEFAULT":
            if row["heap_bytes"] > 0:
                issues.append(f"{row['name']} needs an empty-default-partition check")
        elif row["start"] is None:
            issues.append(f"{row['name']} does not cover exactly one UTC calendar month")
    pairs: dict[tuple, set[str]] = {}
    for row in children:
        if row["start"] is not None:
            pairs.setdefault((row["start"], row["end"]), set()).add(row["parent"])
    for row in relations:
        if cutoff is None:
            row["eligibility"] = "keep_forever"
        elif row["is_parent"] or row["start"] is None:
            row["eligibility"] = "unsupported_partition" if not row["is_parent"] else "parent_relation"
        elif issues:
            row["eligibility"] = "unsupported_layout"
        elif not row["expired_by_age"]:
            row["eligibility"] = "retained"
        elif pairs[(row["start"], row["end"])] != set(SCOPE_TABLES["packets"]):
            row["eligibility"] = "unpaired_partition"
        else:
            row["eligible"] = True
            row["eligibility"] = "candidate_pending_execution_checks"
    return issues


def _public_bounds(rows: list[dict]) -> tuple[str | None, str | None, str]:
    """Only describe known enclosing partition ranges; never scan user data."""
    physical = [r for r in rows if r["kind"] == "r" and r["heap_bytes"] > 0]
    if not physical or any(r["start"] is None for r in physical):
        return None, None, "unknown"
    return min(r["start"] for r in physical), max(r["end"] for r in physical), "partition"


async def build_storage_report(connection_or_pool, config: Mapping, *,
                               now: datetime | None = None,
                               enforcement_available: bool = False) -> dict:
    """Return ``{public, details}``; accepts an asyncpg connection or pool.

    Pool acquisition is bounded to three seconds. Each catalog query has a
    three-second statement timeout and a 250ms lock timeout. Cancellation and
    database errors propagate to the caller, which can serve a cached report or
    an explicit unavailable response. The caller should cache public requests.
    """
    policy = retention_policy(config)
    table_names = [name for names in SCOPE_TABLES.values() for name in names]
    async with _connection(connection_or_pool) as conn:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            await conn.execute("SET LOCAL statement_timeout = '3s'")
            await conn.execute("SET LOCAL lock_timeout = '250ms'")
            await conn.execute("SET LOCAL TIME ZONE 'UTC'")
            now = utc_datetime(now or await conn.fetchval("SELECT CURRENT_TIMESTAMP"))
            catalog = [dict(row) for row in await conn.fetch(_CATALOG_SQL, table_names)]
            database_bytes = await conn.fetchval("SELECT pg_database_size(current_database())")
            maintenance = await read_status(conn)
    public = retention_summary(config, now=now, enforcement_available=enforcement_available)
    public["maintenance"] = public_status(maintenance)
    scopes = {}
    for scope in RETENTION_SCOPES:
        cutoff = complete_month_cutoff(now, policy.months_for(scope))
        rows = [_relation_detail(row, cutoff) for row in catalog if row["parent_name"] in SCOPE_TABLES[scope]]
        issues = _packet_candidates(rows, cutoff) if scope == "packets" else []
        present = {row["name"] for row in rows if row["is_parent"]}
        missing = sorted(set(SCOPE_TABLES[scope]) - present)
        if missing:
            issues.append("Missing history tables: " + ", ".join(missing))
        oldest, newest, kind = _public_bounds(rows)
        public["scopes"][scope].update(oldest_available=oldest, newest_available=newest, available_bounds_kind=kind)
        physical = [row for row in rows if row["kind"] == "r"]
        estimates = [row["estimated_rows"] for row in physical]
        candidate_bytes = sum(row["total_bytes"] for row in rows if row["eligible"])
        scopes[scope] = {
            "total_bytes": sum(row["total_bytes"] for row in physical),
            "table_bytes": sum(row["table_bytes"] for row in physical),
            "index_bytes": sum(row["index_bytes"] for row in physical),
            "estimated_rows": sum(estimates) if estimates and all(n is not None for n in estimates) else None,
            "candidate_partition_bytes": candidate_bytes,
            # A preview cannot promise filesystem reclamation or pass the
            # executor's lock-time relationship/dependency checks.
            "reclaimable_bytes": None,
            "layout_issues": issues,
            "relations": rows,
        }
    details = {
        "generated_at": public["generated_at"],
        "database_bytes": database_bytes,
        "history_bytes": sum(s["total_bytes"] for s in scopes.values()),
        "candidate_partition_bytes": sum(s["candidate_partition_bytes"] for s in scopes.values()),
        "row_estimates_source": "pg_class.reltuples (may be stale; no table scans)",
        "candidate_bytes_note": "Whole paired partitions before the UTC cutoff; execution checks may retain them. Row deletes do not promise filesystem space reclamation.",
        "scopes": scopes,
        "maintenance": maintenance,
    }
    return {"public": public, "details": details}


async def public_history_report(connection_or_pool, config: Mapping, **kwargs) -> dict:
    """Convenience wrapper with no operator inventory in its return value."""
    return (await build_storage_report(connection_or_pool, config, **kwargs))["public"]
