"""Opt-in, bounded history expiration for ordinary PostgreSQL installations.

All destructive cutoffs use the database clock and whole UTC months. A shared
session advisory lock serializes retention, compression, and snapshots. Packet
expiration holds short exclusive locks and commits tail cleanup plus partition
removal atomically. Unsupported/ambiguous layouts are reported, never guessed.
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
import time

from storage.db.storage_report import _CATALOG_SQL, _connection, monthly_partition_bounds
from storage.retention import (
    RETENTION_SCOPES, SCOPE_TABLES, complete_month_cutoff, retention_policy,
    timestamp_json, utc_datetime,
)

logger = logging.getLogger(__name__)
MAINTENANCE_LOCK_KEY = 0x4D455348494E464F  # ASCII MESHINFO; shared by all storage maintenance


class RetentionBlocked(Exception):
    """A safety precondition cannot be established without unbounded work."""


def _quoted(schema: str, name: str) -> str:
    return '"' + schema.replace('"', '""') + '"."' + name.replace('"', '""') + '"'


def _relation(row) -> str:
    return _quoted(row["schema_name"], row["relation_name"])


def _error_reason(exc: Exception) -> str:
    if isinstance(exc, RetentionBlocked):
        return str(exc)
    code = getattr(exc, "sqlstate", None)
    return type(exc).__name__ + (f" (SQLSTATE {code})" if code else "")


@asynccontextmanager
async def maintenance_lock(conn):
    """Yield whether the shared lock was acquired; never wait for another job.

    Keep this same connection acquired throughout the operation. The session
    lock spans transactions and must also protect a complete pg_dump stream.
    """
    async def release():
        task = asyncio.create_task(conn.fetchval("SELECT pg_advisory_unlock($1::bigint)", MAINTENANCE_LOCK_KEY, timeout=3.0))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            # Do not hand a connection back while its unlock is still in flight.
            await task
            raise

    task = asyncio.create_task(conn.fetchval("SELECT pg_try_advisory_lock($1::bigint)", MAINTENANCE_LOCK_KEY, timeout=3.0))
    try:
        acquired = await asyncio.shield(task)
    except asyncio.CancelledError:
        # Session locks survive transaction rollback. If cancellation races the
        # acquire response, first resolve whether this session got the lock.
        try:
            acquired = await task
        except Exception:
            await release()  # result uncertain; release any lock acquired here
        else:
            if acquired:
                await release()
        raise
    except Exception:
        # A client/network timeout can leave an executed acquisition uncertain.
        # This helper requires a connection with no pre-existing maintenance lock.
        await release()
        raise
    try:
        yield acquired
    finally:
        if acquired:
            await release()


async def _transaction_limits(conn):
    await conn.execute("SET LOCAL statement_timeout = '1500ms'")
    await conn.execute("SET LOCAL lock_timeout = '100ms'")
    await conn.execute("SET LOCAL TIME ZONE 'UTC'")


async def _indexed_columns(conn, relation_oids):
    rows = await conn.fetch("""
        SELECT DISTINCT i.indrelid::bigint AS oid, a.attname
        FROM pg_index i
        JOIN pg_class idx ON idx.oid = i.indexrelid
        JOIN pg_am am ON am.oid = idx.relam
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
        WHERE i.indrelid = ANY($1::oid[]) AND i.indisvalid AND i.indisready
          AND i.indpred IS NULL AND i.indexprs IS NULL AND am.amname = 'btree'
    """, relation_oids)
    return {(row["oid"], row["attname"]) for row in rows}


async def _reject_inbound_foreign_keys(conn, relation_oids):
    dependent = await conn.fetchval("""
        SELECT EXISTS(SELECT 1 FROM pg_constraint
                      WHERE contype = 'f' AND confrelid = ANY($1::oid[]))
    """, relation_oids)
    if dependent:
        raise RetentionBlocked("History has inbound foreign keys; expiration needs an explicit dependency policy")


def _packet_layout(catalog):
    parents = {r["parent_name"]: r for r in catalog if r["is_parent"]}
    for name in SCOPE_TABLES["packets"]:
        parent = parents.get(name)
        if not parent or parent["relation_kind"] != "p" or parent["partition_key"] != "RANGE (created_at)":
            raise RetentionBlocked(f"{name} must be partitioned by RANGE (created_at)")
    children = [r for r in catalog if not r["is_parent"]]
    months = {}
    defaults = []
    for row in children:
        if row["relation_kind"] != "r":
            raise RetentionBlocked("Packet history contains a non-regular or nested partition")
        if row["partition_bound"] == "DEFAULT":
            defaults.append(row)
            continue
        bounds = monthly_partition_bounds(row["partition_bound"])
        if bounds is None:
            raise RetentionBlocked("Packet history contains a range other than one complete UTC calendar month")
        group = months.setdefault(bounds, {})
        if row["parent_name"] in group:
            raise RetentionBlocked("Packet history contains overlapping month assignments")
        group[row["parent_name"]] = row
    return parents, children, defaults, months


async def _id_bounds(conn, row, column):
    # The caller verified a valid leading-column btree index. Each endpoint
    # therefore needs an index probe, not an aggregate scan of the partition.
    table = _relation(row)
    result = await conn.fetchrow(
        f'SELECT (SELECT "{column}" FROM ONLY {table} ORDER BY "{column}" ASC LIMIT 1) AS lo, '
        f'(SELECT "{column}" FROM ONLY {table} ORDER BY "{column}" DESC LIMIT 1) AS hi'
    )
    return result["lo"], result["hi"]


def _empty_scope(cutoff, status="complete"):
    return {"status": status, "cutoff": timestamp_json(cutoff), "rows_deleted": 0,
            "partitions_dropped": [], "receptions_deleted": 0,
            "estimated_partition_bytes_removed": 0}


async def _expire_packets(conn, cutoff, *, max_months, tail_limit, interleaved_limit):
    # The caller holds the shared maintenance lock. If a newly shortened
    # retention policy expires a source being copied, discard only its private
    # replacement and release the source guard before normal grouped expiration.
    # Leaving a detached stage behind would otherwise retain expired data.
    if await conn.fetchval("SELECT to_regclass('public.storage_archive_jobs')"):
        from storage.db.archive import cancel_archive
        active = await conn.fetch("SELECT job_id FROM public.storage_archive_jobs "
                                  "WHERE source_frozen AND phase IN ('copying','verifying','ready') "
                                  "AND bound_end <= $1", cutoff)
        for job in active:
            cancelled = await cancel_archive(conn, job['job_id'])
            if cancelled['status'] != 'cancelled':
                raise RetentionBlocked('Archive replacement must be cancelled before expiring its source')
    async with conn.transaction():
        await _transaction_limits(conn)
        # ONLY avoids implicit recursive lock order. Parent locks prevent
        # attach/detach and normal app writes; explicit leaf locks also exclude
        # direct SQL writers before any coherence checks.
        await conn.execute('LOCK TABLE ONLY public.mqtt_messages, ONLY public.packet_receptions IN ACCESS EXCLUSIVE MODE NOWAIT')
        catalog = [dict(r) for r in await conn.fetch(_CATALOG_SQL, list(SCOPE_TABLES["packets"]))]
        parents, children, defaults, months = _packet_layout(catalog)
        if children:
            lock_targets = ", ".join("ONLY " + _relation(r) for r in sorted(children, key=lambda r: r["relation_oid"]))
            await conn.execute(f"LOCK TABLE {lock_targets} IN ACCESS EXCLUSIVE MODE NOWAIT")
        # Guard names/OIDs against a child rename/drop race before leaf locking.
        current = [dict(r) for r in await conn.fetch(_CATALOG_SQL, list(SCOPE_TABLES["packets"]))]
        fingerprint = lambda rows: sorted((r["relation_oid"], r["schema_name"], r["relation_name"], r["partition_bound"] or "") for r in rows)
        if fingerprint(catalog) != fingerprint(current):
            raise RetentionBlocked("Packet layout changed while acquiring locks; retry later")
        for row in defaults:
            if await conn.fetchval(f"SELECT EXISTS(SELECT 1 FROM ONLY {_relation(row)} LIMIT 1)"):
                raise RetentionBlocked("A default packet partition contains rows; repair its placement before expiration")
        expired = sorted(bounds for bounds in months if bounds[1] <= cutoff)
        if any(set(months[bounds]) != set(SCOPE_TABLES["packets"]) for bounds in expired):
            raise RetentionBlocked("Expired packet history contains an unpaired month")
        if not expired:
            return _empty_scope(cutoff)
        selected = expired[:max_months]
        old = [months[bounds][parent] for bounds in selected for parent in SCOPE_TABLES["packets"]]
        old_oids = {r["relation_oid"] for r in old}
        old_canonical = [r for r in old if r["parent_name"] == "mqtt_messages"]
        old_receptions = [r for r in old if r["parent_name"] == "packet_receptions"]
        kept_canonical = [r for r in children if r["parent_name"] == "mqtt_messages" and r["relation_oid"] not in old_oids]
        kept_receptions = [r for r in children if r["parent_name"] == "packet_receptions" and r["relation_oid"] not in old_oids]
        identity_columns = await conn.fetch("""
            SELECT a.attrelid::bigint AS oid, a.attname
            FROM pg_attribute a WHERE a.attrelid = ANY($1::oid[])
              AND a.atttypid = 'bigint'::regtype AND a.attnotnull AND NOT a.attisdropped
              AND a.attname = ANY($2::text[])
        """, [row["relation_oid"] for row in parents.values()], ["id", "mqtt_row_id"])
        identities = {(r["oid"], r["attname"]) for r in identity_columns}
        for name, column in (("mqtt_messages", "id"), ("packet_receptions", "mqtt_row_id")):
            if (parents[name]["relation_oid"], column) not in identities:
                raise RetentionBlocked(f"{name}.{column} must be a non-null bigint")
        indexes = await _indexed_columns(conn, [r["relation_oid"] for r in children])
        for row in children:
            column = "id" if row["parent_name"] == "mqtt_messages" else "mqtt_row_id"
            if (row["relation_oid"], column) not in indexes:
                raise RetentionBlocked(f"{row['relation_name']} needs a valid leading {column} btree index")
        await _reject_inbound_foreign_keys(conn, [r["relation_oid"] for r in catalog])
        endpoints = []
        for row in old:
            column = "id" if row["parent_name"] == "mqtt_messages" else "mqtt_row_id"
            endpoints.extend(value for value in await _id_bounds(conn, row, column) if value is not None)
        tails = []
        if endpoints:
            lo, hi = min(endpoints), max(endpoints)
            # IDs usually increase with ingestion time. A small interleaving at
            # midnight is normal for concurrent transactions: explicitly exclude
            # those kept IDs and prove they have no receptions in dropped leaves.
            kept_ids = set()
            for row in kept_canonical:
                ids = await conn.fetch(
                    f"SELECT DISTINCT id FROM ONLY {_relation(row)} WHERE id BETWEEN $1 AND $2 ORDER BY id LIMIT $3",
                    lo, hi, interleaved_limit + 1,
                )
                kept_ids.update(r["id"] for r in ids)
                if len(kept_ids) > interleaved_limit:
                    raise RetentionBlocked("Too many retained packet IDs overlap the expiration cohort; review imported/non-monotonic IDs")
            if kept_ids:
                for row in old:
                    column = "id" if row["parent_name"] == "mqtt_messages" else "mqtt_row_id"
                    if await conn.fetchval(f'SELECT EXISTS(SELECT 1 FROM ONLY {_relation(row)} WHERE "{column}" = ANY($1::bigint[]) LIMIT 1)', list(kept_ids)):
                        raise RetentionBlocked("An expired partition contains an ID or reception belonging to a retained canonical packet")
            for row in kept_receptions:
                remaining = tail_limit - len(tails)
                found = await conn.fetch(
                    f"SELECT ctid::text AS row_tid, mqtt_row_id FROM ONLY {_relation(row)} "
                    "WHERE mqtt_row_id BETWEEN $1 AND $2 AND NOT (mqtt_row_id = ANY($3::bigint[])) "
                    "LIMIT $4 FOR UPDATE",
                    lo, hi, list(kept_ids), remaining + 1,
                )
                tails.extend((row, dict(r)) for r in found)
                if len(tails) > tail_limit:
                    raise RetentionBlocked("Cross-month reception cleanup exceeds this run's bounded transaction limit")
            # The interval may contain ID gaps. Keep unrelated orphan rows in
            # those gaps: delete only tails with an existing expiring canonical.
            tail_ids = list({r["mqtt_row_id"] for _, r in tails})
            expiring_ids = set()
            if tail_ids:
                for row in old_canonical:
                    found = await conn.fetch(f"SELECT id FROM ONLY {_relation(row)} WHERE id = ANY($1::bigint[])", tail_ids)
                    expiring_ids.update(r["id"] for r in found)
            tails = [(row, r) for row, r in tails if r["mqtt_row_id"] in expiring_ids]
        for row in kept_receptions:
            tids = [r["row_tid"] for target, r in tails if target["relation_oid"] == row["relation_oid"]]
            if tids:
                result = await conn.execute(f"DELETE FROM ONLY {_relation(row)} WHERE ctid = ANY($1::text[]::tid[])", tids)
                if int(result.rsplit(" ", 1)[-1]) != len(tids):
                    raise RetentionBlocked("Reception cleanup affected an unexpected number of rows")
        # No CASCADE: extension/user dependencies must fail and roll back the
        # entire cohort, including any reception tail deletes above.
        targets = ", ".join(_relation(r) for r in old_receptions + old_canonical)
        await conn.execute(f"DROP TABLE {targets}")
        if await conn.fetchval("SELECT to_regclass('public.storage_archive_jobs')"):
            await conn.execute("DELETE FROM public.storage_archive_jobs WHERE phase IN ('complete','cancelled','failed') "
                               "AND source_name=ANY($1::text[])", [r['relation_name'] for r in old_canonical])
        result = _empty_scope(cutoff, "partial" if len(expired) > len(selected) else "complete")
        result.update(partitions_dropped=[r["relation_name"] for r in old],
                      receptions_deleted=len(tails),
                      estimated_partition_bytes_removed=sum(r["total_bytes"] for r in old))
        if result["status"] == "partial":
            result["reason"] = "Additional expired months remain for subsequent bounded runs"
        return result


async def _row_table_ready(conn, table):
    row = await conn.fetchrow("""
        SELECT c.oid::bigint AS oid, c.relkind::text AS kind,
               EXISTS(SELECT 1 FROM pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid) AS inherited,
               EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attname='created_at'
                      AND a.atttypid='timestamptz'::regtype AND NOT a.attisdropped) AS has_timestamp
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND c.relname=$1
    """, table)
    if not row or row["kind"] != "r" or row["inherited"] or not row["has_timestamp"]:
        raise RetentionBlocked(f"{table} needs an ordinary table with a timestamptz created_at column")
    if (row["oid"], "created_at") not in await _indexed_columns(conn, [row["oid"]]):
        raise RetentionBlocked(f"{table} needs a valid leading created_at btree index")
    await _reject_inbound_foreign_keys(conn, [row["oid"]])


async def _expire_rows(conn, table, cutoff, *, deadline, batch_size, max_batches):
    result = _empty_scope(cutoff)
    target = _quoted("public", table)
    for _ in range(max_batches):
        if time.monotonic() >= deadline:
            result.update(status="partial", reason="Run time budget exhausted; additional rows may remain")
            break
        try:
            async with asyncio.timeout(max(0.001, deadline - time.monotonic())):
                async with conn.transaction():
                    await _transaction_limits(conn)
                    await conn.execute(f"LOCK TABLE ONLY {target} IN ROW EXCLUSIVE MODE NOWAIT")
                    await _row_table_ready(conn, table)
                    outcome = await conn.execute(f"""
                        WITH expired AS MATERIALIZED (
                            SELECT ctid FROM ONLY {target} WHERE created_at < $1
                            ORDER BY created_at LIMIT $2 FOR UPDATE SKIP LOCKED
                        )
                        DELETE FROM ONLY {target} AS history USING expired
                        WHERE history.ctid = expired.ctid
                    """, cutoff, batch_size)
                    count = int(outcome.rsplit(" ", 1)[-1])
                    # SKIP LOCKED can make a short batch while older rows remain.
                    pending = await conn.fetchval(f"SELECT EXISTS(SELECT 1 FROM ONLY {target} WHERE created_at < $1 LIMIT 1)", cutoff)
            result["rows_deleted"] += count
            if not pending:
                result["status"] = "complete"
                result.pop("reason", None)
                break
            result.update(status="partial", reason="Additional expired or locked rows remain for subsequent bounded runs")
            if count == 0:
                break
        except Exception as exc:
            result.update(status="partial" if result["rows_deleted"] else "blocked" if isinstance(exc, RetentionBlocked) else "error", reason=_error_reason(exc))
            logger.warning("Retention for %s could not finish: %s", table, _error_reason(exc))
            break
    return result


def _overall_status(scopes):
    statuses = {s["status"] for s in scopes.values()}
    changed = any(s["rows_deleted"] or s["partitions_dropped"] or s["receptions_deleted"] for s in scopes.values())
    if "partial" in statuses or (changed and statuses & {"error", "blocked"}):
        return "partial"
    if "error" in statuses:
        return "error"
    if "blocked" in statuses:
        return "blocked"
    return "complete"


async def enforce_retention(connection_or_pool, config, *, max_seconds=12.0,
                            batch_size=1000, max_batches_per_scope=4,
                            max_packet_months=2, tail_limit=5000,
                            interleaved_limit=256):
    """Apply one bounded pass and return an explicit status/progress summary.

    Packet cohorts commit atomically. Ordinary history commits per bounded row
    batch, so a later failure reports previously committed progress as partial.
    Cancellation propagates after transaction rollback and advisory-lock release.
    This function never creates indexes, VACUUMs, or silently handles legacy
    non-partitioned packet tables by issuing large deletes.
    """
    for name, value in (("batch_size", batch_size), ("max_batches_per_scope", max_batches_per_scope),
                        ("max_packet_months", max_packet_months), ("tail_limit", tail_limit),
                        ("interleaved_limit", interleaved_limit)):
        if type(value) is not int or value < 1:
            raise ValueError(f"{name} must be a positive integer")
    if isinstance(max_seconds, bool) or not isinstance(max_seconds, (int, float)) or not 0 < max_seconds <= 300:
        raise ValueError("max_seconds must be greater than zero and at most 300")
    policy = retention_policy(config)
    result = {"status": "disabled", "started_at": timestamp_json(datetime.now(timezone.utc)), "finished_at": None, "scopes": {}}
    if not policy.enabled:
        result["finished_at"] = result["started_at"]
        return result
    if all(policy.months_for(scope) == 0 for scope in RETENTION_SCOPES):
        result.update(status="forever", finished_at=result["started_at"])
        return result
    deadline = time.monotonic() + max_seconds
    try:
        async with _connection(connection_or_pool) as conn:
            async with maintenance_lock(conn) as acquired:
                if not acquired:
                    result.update(status="busy", reason="Another storage maintenance or snapshot operation holds the lock")
                else:
                    now = utc_datetime(await conn.fetchval("SELECT clock_timestamp()", timeout=3.0))
                    result["started_at"] = timestamp_json(now)
                    for scope in RETENTION_SCOPES:
                        cutoff = complete_month_cutoff(now, policy.months_for(scope))
                        if cutoff is None:
                            result["scopes"][scope] = _empty_scope(None, "forever")
                            continue
                        if time.monotonic() >= deadline:
                            result["scopes"][scope] = _empty_scope(cutoff, "partial")
                            result["scopes"][scope]["reason"] = "Run time budget exhausted before this scope"
                            continue
                        if scope == "packets":
                            try:
                                # Bound exclusive-lock duration separately from
                                # the total run, even when callers raise its budget.
                                async with asyncio.timeout(min(5.0, max(0.001, deadline - time.monotonic()))):
                                    detail = await _expire_packets(conn, cutoff, max_months=max_packet_months,
                                                                   tail_limit=tail_limit, interleaved_limit=interleaved_limit)
                            except Exception as exc:
                                detail = _empty_scope(cutoff, "blocked" if isinstance(exc, RetentionBlocked) else "error")
                                detail["reason"] = _error_reason(exc)
                                logger.warning("Packet retention could not finish: %s", _error_reason(exc))
                        else:
                            detail = await _expire_rows(conn, SCOPE_TABLES[scope][0], cutoff,
                                                        deadline=deadline, batch_size=batch_size,
                                                        max_batches=max_batches_per_scope)
                        result["scopes"][scope] = detail
                    result["status"] = _overall_status(result["scopes"])
    except Exception as exc:
        result.update(status="partial" if any(s["rows_deleted"] or s["partitions_dropped"] for s in result["scopes"].values()) else "error", reason=_error_reason(exc))
        logger.warning("Retention could not finish: %s", _error_reason(exc))
    result["finished_at"] = timestamp_json(datetime.now(timezone.utc))
    return result
