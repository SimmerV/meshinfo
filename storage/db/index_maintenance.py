"""Bounded, opt-in packing of B-tree indexes on closed history partitions.

This uses ordinary REINDEX in one short transaction. Queries using that index
can briefly wait; lock acquisition and total execution are bounded. Unlike a
cancelled concurrent rebuild, rollback cannot leave an invalid staging index
consuming disk. No history rows, index keys, uniqueness or partition attachments
are removed. Current/open months are always excluded.
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import time
from typing import Callable

from storage.db.retention_executor import maintenance_lock
from storage.db.storage_report import monthly_partition_bounds
from storage.retention import complete_month_cutoff, timestamp_json
from storage.db.compression import CompressionRefused, GIB, MIB, qualified


@dataclass(frozen=True)
class IndexPackingLimits:
    after_months: int = 6
    fillfactor: int = 100
    max_index_bytes: int = 64 * MIB
    reserve_bytes: int = GIB
    max_seconds: float = 5.0

    def __post_init__(self):
        if type(self.after_months) is not int or not 0 <= self.after_months <= 1200:
            raise ValueError("after_months must be an integer from 0 to 1200")
        if type(self.fillfactor) is not int or not 90 <= self.fillfactor <= 100:
            raise ValueError("fillfactor must be an integer from 90 to 100")
        if type(self.max_index_bytes) is not int or self.max_index_bytes < 32768:
            raise ValueError("max_index_bytes must be at least 32768")
        if type(self.reserve_bytes) is not int or self.reserve_bytes < GIB:
            raise ValueError("reserve_bytes must be at least 1 GiB")
        if type(self.max_seconds) not in (int, float) or not math.isfinite(self.max_seconds) or not 0 < self.max_seconds <= 30:
            raise ValueError("max_seconds must be greater than zero and at most 30")


def required_headroom(index_bytes: int, reserve_bytes: int = GIB) -> int:
    """Replacement/index build work/WAL allowance plus an untouched free-space floor."""
    if type(index_bytes) is not int or index_bytes < 0 or type(reserve_bytes) is not int or reserve_bytes < GIB:
        raise ValueError("invalid size or reserved free-space floor")
    return index_bytes * 4 + reserve_bytes


_INDEX_SQL = """
SELECT idx.oid::bigint AS index_oid, idx.relname AS index_name, idx.reloptions,
       pg_relation_size(idx.oid) AS index_bytes, pg_get_indexdef(idx.oid) AS definition,
       t.oid::bigint AS table_oid,t.relname AS partition,p.relname AS parent,
       pg_get_expr(t.relpartbound,t.oid) AS bound, ts.spcname AS tablespace,
       pg_get_partkeydef(p.oid) AS partition_key, am.amname AS method,
       idx.relkind::text AS index_kind,t.relkind::text AS table_kind,
       t.relpersistence::text AS persistence,
       x.indisvalid,x.indisready,x.indisunique,x.indisprimary,
       x.indkey::text AS keys,x.indclass::text AS classes,x.indcollation::text AS collations,
       x.indoption::text AS options,pg_get_expr(x.indpred,t.oid) AS predicate,
       pg_get_expr(x.indexprs,t.oid) AS expressions,
       (SELECT inhparent::bigint FROM pg_inherits WHERE inhrelid=idx.oid) AS parent_index_oid
FROM pg_index x JOIN pg_class idx ON idx.oid=x.indexrelid
JOIN pg_class t ON t.oid=x.indrelid JOIN pg_namespace n ON n.oid=t.relnamespace
JOIN pg_namespace ni ON ni.oid=idx.relnamespace
JOIN pg_inherits inh ON inh.inhrelid=t.oid JOIN pg_class p ON p.oid=inh.inhparent
JOIN pg_namespace pn ON pn.oid=p.relnamespace JOIN pg_am am ON am.oid=idx.relam
LEFT JOIN pg_tablespace ts ON ts.oid=idx.reltablespace
WHERE n.nspname='public' AND ni.nspname='public' AND pn.nspname='public'
  AND p.relname IN ('mqtt_messages','packet_receptions')
"""


def _fingerprint(row):
    return tuple(row[key] for key in ("index_oid", "table_oid", "index_kind", "method", "keys", "classes",
                                      "collations", "options", "predicate", "expressions", "indisunique",
                                      "indisprimary", "parent_index_oid", "indisvalid", "indisready"))


def _reason(row, now, limits):
    bounds = monthly_partition_bounds(row["bound"])
    cutoff = complete_month_cutoff(now, limits.after_months) if limits.after_months else now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if (row["index_kind"] != "i" or row["table_kind"] != "r" or row["persistence"] != "p"
            or row["partition_key"] != "RANGE (created_at)" or row["method"] != "btree"
            or not row["indisvalid"] or not row["indisready"] or bounds is None):
        return "unsupported_index_or_partition"
    if bounds[1] > cutoff:
        return "too_recent"
    options = dict(option.split("=", 1) for option in row["reloptions"] or [])
    if int(options.get("fillfactor", "90")) >= limits.fillfactor:
        return "already_configured"
    if row["index_bytes"] < 32768:
        return "too_small"
    if row["index_bytes"] > limits.max_index_bytes:
        return "above_size_limit"
    return None


async def plan_index_packing(conn, *, limits: IndexPackingLimits | None = None) -> dict:
    """Catalog-only preview; no index scan or data mutation."""
    limits = limits or IndexPackingLimits()
    async with conn.transaction(readonly=True):
        await conn.execute("SET LOCAL statement_timeout='3s'")
        await conn.execute("SET LOCAL lock_timeout='100ms'")
        await conn.execute("SET LOCAL TIME ZONE 'UTC'")
        now = await conn.fetchval("SELECT clock_timestamp()")
        rows = await conn.fetch(_INDEX_SQL + " ORDER BY pg_relation_size(idx.oid),idx.oid")
    indexes = []
    for row in rows:
        reason = _reason(row, now, limits)
        indexes.append({"index": row["index_name"], "partition": row["partition"],
                        "index_bytes": row["index_bytes"], "method": row["method"],
                        "eligible": reason is None, "reason": reason,
                        "required_free_bytes": required_headroom(row["index_bytes"], limits.reserve_bytes),
                        "tablespace": row["tablespace"]})
    return {"generated_at": timestamp_json(now), "after_months": limits.after_months,
            "fillfactor": limits.fillfactor, "max_seconds": limits.max_seconds,
            "indexes": indexes, "note": "No savings are promised. Closed-index rebuilds may briefly block queries using the index."}


async def pack_index(conn, index_name: str, *, free_bytes: Callable[[], int],
                     limits: IndexPackingLimits | None = None) -> dict:
    """Repack one eligible leaf index, or fail with the entire change rolled back.

    The filesystem probe must describe the index's actual PostgreSQL filesystem
    (including a custom index tablespace). The caller supplies that knowledge;
    the application host's free space is never assumed to be database headroom.
    """
    limits = limits or IndexPackingLimits()
    if conn.is_in_transaction():
        raise ValueError("packing requires a dedicated connection outside a transaction")
    async with maintenance_lock(conn) as acquired:
        if not acquired:
            return {"status": "busy", "index": index_name}
        return await _pack_index_locked(conn, index_name, free_bytes=free_bytes, limits=limits)


async def _pack_index_locked(conn, index_name, *, free_bytes, limits):
    """Rebuild after the caller acquired the shared session maintenance lock."""
    started = time.monotonic()
    async with asyncio.timeout(limits.max_seconds):
        async with conn.transaction():
            await conn.execute("SET LOCAL TIME ZONE 'UTC'")
            await conn.execute("SET LOCAL lock_timeout='100ms'")
            await conn.execute(f"SET LOCAL statement_timeout='{math.ceil(limits.max_seconds * 1000)}ms'")
            await conn.execute("SET LOCAL maintenance_work_mem='32MB'")
            await conn.execute("SET LOCAL max_parallel_maintenance_workers=0")
            row = await conn.fetchrow(_INDEX_SQL + " AND idx.relname=$1", index_name)
            if row is None:
                raise CompressionRefused("index is not attached to a supported packet-history partition")
            now = await conn.fetchval("SELECT clock_timestamp()")
            reason = _reason(row, now, limits)
            if reason:
                raise CompressionRefused(reason)
            required = required_headroom(row["index_bytes"], limits.reserve_bytes)
            if free_bytes() < required:
                raise CompressionRefused(f"index rebuild requires at least {required} free bytes on its PostgreSQL filesystem")
            if await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_replication_slots)"):
                raise CompressionRefused("replication slots require an independently budgeted WAL operation")
            if await conn.fetchval("SHOW archive_mode") != "off":
                raise CompressionRefused("WAL archiving requires an independently budgeted operation")
            await conn.execute(f"LOCK TABLE ONLY {qualified(row['partition'])} IN SHARE MODE NOWAIT")
            locked = await conn.fetchrow(_INDEX_SQL + " AND idx.relname=$1", index_name)
            if locked is None or _fingerprint(locked) != _fingerprint(row):
                raise CompressionRefused("index changed while acquiring the partition lock")
            # Imports or a concurrent writer can grow an old index between
            # the catalog preview and the SHARE lock. Recheck both limits
            # against its locked size before allocating a replacement.
            locked_reason = _reason(locked, now, limits)
            if locked_reason:
                raise CompressionRefused(locked_reason)
            before = locked["index_bytes"]
            if free_bytes() < required_headroom(before, limits.reserve_bytes):
                raise CompressionRefused("insufficient headroom after locking the current index size")
            wal_start = await conn.fetchval("SELECT pg_current_wal_insert_lsn()::text")
            await conn.execute(f"ALTER INDEX {qualified(index_name)} SET (fillfactor={limits.fillfactor})")
            await conn.execute(f"REINDEX INDEX {qualified(index_name)}")
            after = await conn.fetchrow(_INDEX_SQL + " AND idx.relname=$1", index_name)
            if after is None or _fingerprint(after) != _fingerprint(row):
                raise CompressionRefused("index identity, definition, validity or attachment changed")
            if after["index_bytes"] > before:
                raise CompressionRefused("replacement is larger; original index restored by rollback")
            if free_bytes() < limits.reserve_bytes:
                raise CompressionRefused("reserved free-space floor reached; rebuild rolled back")
            wal_bytes = await conn.fetchval("SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::text::pg_lsn)::bigint", wal_start)
            saved = before - after["index_bytes"]
    return {"status": "packed" if saved else "unchanged", "index": index_name,
            "partition": row["partition"], "before_bytes": before, "after_bytes": after["index_bytes"],
            "bytes_saved": saved, "fillfactor": limits.fillfactor,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "wal_bytes_during_operation": wal_bytes,
            "verification": "index OID, keys, predicates, uniqueness, validity and parent attachment preserved; heap data untouched"}


def packing_policy(config):
    """Validate opt-in operator configuration; returns enabled, limits, filesystem."""
    from collections.abc import Mapping

    storage = config.get("storage", {})
    if not isinstance(storage, Mapping):
        raise ValueError("storage must be a table")
    raw = storage.get("compression", {})
    if not isinstance(raw, Mapping):
        raise ValueError("storage.compression must be a table")
    allowed = {"enabled", "after_months", "index_fillfactor", "max_index_mib", "reserve_mib", "max_seconds", "database_filesystem"}
    unknown = set(raw) - allowed
    if unknown:
        raise ValueError("Unknown storage.compression settings: " + ", ".join(sorted(unknown)))
    enabled = raw.get("enabled", False)
    if type(enabled) is not bool:
        raise ValueError("storage.compression.enabled must be boolean")
    filesystem = raw.get("database_filesystem", "")
    if not isinstance(filesystem, str):
        raise ValueError("storage.compression.database_filesystem must be a string")
    max_mib, reserve_mib = raw.get("max_index_mib", 64), raw.get("reserve_mib", 1024)
    if type(max_mib) is not int or type(reserve_mib) is not int:
        raise ValueError("storage.compression size limits must be integers in MiB")
    limits = IndexPackingLimits(after_months=raw.get("after_months", 6), fillfactor=raw.get("index_fillfactor", 100),
                                max_index_bytes=max_mib * MIB, reserve_bytes=reserve_mib * MIB,
                                max_seconds=raw.get("max_seconds", 5))
    return enabled, limits, filesystem


def _next_packing_candidate(candidates, previous):
    """Stable round-robin order; index size changes never reset progress."""
    ordered = sorted(candidates, key=lambda row: row["index"])
    if not ordered:
        return None
    if isinstance(previous, str):
        for candidate in ordered:
            if candidate["index"] > previous:
                return candidate
    return ordered[0]


async def _read_packing_cursor(conn):
    return await conn.fetchval(
        "SELECT result->>'last_index' FROM public.storage_maintenance_status "
        "WHERE operation='compression_cursor'", timeout=3,
    )


async def _save_packing_cursor(conn, index_name):
    # Exactly one private scheduler row, separate from the compression outcome:
    # late/manual/busy status writes must not erase or rewind automatic progress.
    await conn.execute(
        """INSERT INTO public.storage_maintenance_status (operation, updated_at, result)
           VALUES ('compression_cursor', clock_timestamp(), $1::jsonb)
           ON CONFLICT (operation) DO UPDATE
           SET updated_at=EXCLUDED.updated_at, result=EXCLUDED.result""",
        json.dumps({"last_index": index_name}), timeout=3,
    )


async def run_index_packing_cycle(connection_or_pool, config):
    """Pack at most one closed leaf index when explicitly configured.

    A filesystem location is required to avoid confusing application-host free
    space with PostgreSQL-host free space. Scheduling belongs to the caller.
    One durable cursor advances before each attempt, including attempts that
    fail or are interrupted. Candidates rotate by name, with wraparound, so a
    repeatedly blocked index cannot starve other eligible indexes.
    """
    from pathlib import Path
    import shutil
    from storage.db.storage_report import _connection

    enabled, limits, filesystem = packing_policy(config)
    if not enabled:
        return {"status": "disabled"}
    if not filesystem or not Path(filesystem).is_dir():
        return {"status": "blocked", "reason": "Configure the actual database index filesystem before enabling packing"}
    async with _connection(connection_or_pool) as conn:
        if conn.is_in_transaction():
            raise ValueError("automatic packing requires a connection outside a transaction")
        async with maintenance_lock(conn) as acquired:
            if not acquired:
                return {"status": "busy"}
            plan = await plan_index_packing(conn, limits=limits)
            candidates = [row for row in plan["indexes"] if row["eligible"] and row["tablespace"] is None]
            previous = await _read_packing_cursor(conn)
            selected = _next_packing_candidate(candidates, previous)
            if selected is None:
                return {"status": "no_candidates"}
            # Autocommit before any rebuild. Cancellation or a backend restart
            # after this point preserves progress without retaining attempt logs.
            await _save_packing_cursor(conn, selected["index"])
            return await _pack_index_locked(conn, selected["index"], limits=limits,
                                            free_bytes=lambda: shutil.disk_usage(filesystem).free)
