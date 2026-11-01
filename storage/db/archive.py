"""Resumable, lossless conversion of closed packet leaves to Citus Columnar.

The live leaf remains attached and readable while its guarded, immutable contents
are copied and independently verified in bounded transactions. Only the final
metadata switch requires a parent lock. No failed copy or cancellation removes
source data. A durable in-flight marker accounts for columnar storage retained
by an aborted INSERT transaction.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import json
import math
import time
from typing import Callable
import uuid

from storage.db.compression import GIB, MIB, MQTT_COLUMNS, quote_identifier
from storage.db import archive_topics
from storage.db.retention_executor import maintenance_lock
from storage.db.storage_report import monthly_partition_bounds
from storage.retention import complete_month_cutoff, retention_policy


class ArchiveBlocked(ValueError):
    """A precondition failed; the original history remains authoritative."""


@dataclass(frozen=True)
class ArchivePolicy:
    enabled: bool = False
    after_months: int = 6
    batch_rows: int = 1000
    batch_mib: int = 8
    cycle_seconds: float = 10
    statement_seconds: float = 3
    reserve_mib: int = 1024
    max_wal_mib: int = 64
    estimate_multiplier: float = 1.5
    compression_level: int = 3
    chunk_group_rows: int = 1000
    database_filesystem: str = ""
    index_build_seconds: float = 300
    analyze_seconds: float = 8

    def __post_init__(self):
        if type(self.enabled) is not bool or not isinstance(self.database_filesystem, str):
            raise ValueError("archive enabled/database_filesystem have invalid types")
        for name, lower, upper in (("after_months", 1, 1200), ("batch_rows", 1, 5000),
                                   ("batch_mib", 1, 64), ("reserve_mib", 1024, 1048576),
                                   ("max_wal_mib", 16, 1024), ("compression_level", 1, 19),
                                   ("chunk_group_rows", 1000, 10000)):
            value = getattr(self, name)
            if type(value) is not int or not lower <= value <= upper:
                raise ValueError(f"archive {name} must be an integer from {lower} to {upper}")
        for name, lower, upper in (("cycle_seconds", 1, 12), ("statement_seconds", .1, 5),
                                   ("estimate_multiplier", 1.1, 4), ("index_build_seconds", 1, 1800),
                                   ("analyze_seconds", .1, 10)):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or not lower <= value <= upper:
                raise ValueError(f"archive {name} must be between {lower} and {upper}")
        if self.statement_seconds > self.cycle_seconds:
            raise ValueError("archive statement_seconds must not exceed cycle_seconds")
        if self.max_wal_mib < 4*self.batch_mib:
            raise ValueError("archive max_wal_mib must cover at least four batch_mib")

    @property
    def batch_bytes(self):
        return self.batch_mib * MIB

    @property
    def reserve_bytes(self):
        return self.reserve_mib * MIB

    @property
    def max_wal_bytes(self):
        return self.max_wal_mib * MIB


def archive_policy(config) -> ArchivePolicy:
    raw = config.get("storage", {}).get("archive", {})
    if not isinstance(raw, dict):
        raise ValueError("storage.archive must be a table")
    unknown = set(raw) - set(ArchivePolicy.__dataclass_fields__)
    if unknown:
        raise ValueError("Unknown storage.archive settings: " + ", ".join(sorted(unknown)))
    return ArchivePolicy(**raw)


ARCHIVE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS public.storage_archive_jobs (
    job_id UUID PRIMARY KEY,
    source_oid OID NOT NULL, source_name TEXT NOT NULL,
    stage_oid OID, stage_name TEXT NOT NULL,
    topic_stage_oid OID, topic_stage_name TEXT, topic_stage_fingerprint JSONB NOT NULL DEFAULT '{}',
    source_method TEXT NOT NULL DEFAULT 'heap', target_method TEXT NOT NULL DEFAULT 'columnar',
    previous_guard_job UUID, original_comment TEXT, target_index_signatures JSONB NOT NULL DEFAULT '[]',
    target_constraint_signatures JSONB NOT NULL DEFAULT '[]',
    bound_start TIMESTAMPTZ NOT NULL, bound_end TIMESTAMPTZ NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('copying','verifying','ready','complete','cancelled','failed')),
    source_frozen BOOLEAN NOT NULL DEFAULT false,
    blocked_reason TEXT,
    schema_fingerprint JSONB NOT NULL, compression_options JSONB NOT NULL,
    last_id BIGINT, last_created_at TIMESTAMPTZ,
    verify_id BIGINT, verify_created_at TIMESTAMPTZ,
    rows_copied BIGINT NOT NULL DEFAULT 0, rows_verified BIGINT NOT NULL DEFAULT 0,
    copied_logical_bytes BIGINT NOT NULL DEFAULT 0,
    source_bytes BIGINT NOT NULL, estimated_stage_bytes BIGINT NOT NULL,
    inflight_bytes BIGINT, aborted_bytes BIGINT NOT NULL DEFAULT 0,
    abort_count INTEGER NOT NULL DEFAULT 0,
    analyzed BOOLEAN NOT NULL DEFAULT false,
    stage_analyzed BOOLEAN NOT NULL DEFAULT false,
    topic_analyzed BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), finished_at TIMESTAMPTZ
);
ALTER TABLE public.storage_archive_jobs ADD COLUMN IF NOT EXISTS analyzed BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE public.storage_archive_jobs ADD COLUMN IF NOT EXISTS stage_analyzed BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE public.storage_archive_jobs ADD COLUMN IF NOT EXISTS topic_analyzed BOOLEAN NOT NULL DEFAULT false;
UPDATE public.storage_archive_jobs SET stage_analyzed=true,topic_analyzed=true
    WHERE analyzed AND NOT (stage_analyzed AND topic_analyzed);
ALTER TABLE public.storage_archive_jobs ADD COLUMN IF NOT EXISTS source_method TEXT NOT NULL DEFAULT 'heap';
ALTER TABLE public.storage_archive_jobs ADD COLUMN IF NOT EXISTS target_method TEXT NOT NULL DEFAULT 'columnar';
ALTER TABLE public.storage_archive_jobs ADD COLUMN IF NOT EXISTS previous_guard_job UUID;
ALTER TABLE public.storage_archive_jobs ADD COLUMN IF NOT EXISTS original_comment TEXT;
ALTER TABLE public.storage_archive_jobs ADD COLUMN IF NOT EXISTS target_index_signatures JSONB NOT NULL DEFAULT '[]';
ALTER TABLE public.storage_archive_jobs ADD COLUMN IF NOT EXISTS target_constraint_signatures JSONB NOT NULL DEFAULT '[]';
ALTER TABLE public.storage_archive_jobs ADD COLUMN IF NOT EXISTS topic_stage_oid OID;
ALTER TABLE public.storage_archive_jobs ADD COLUMN IF NOT EXISTS topic_stage_name TEXT;
ALTER TABLE public.storage_archive_jobs ADD COLUMN IF NOT EXISTS topic_stage_fingerprint JSONB NOT NULL DEFAULT '{}';
CREATE UNIQUE INDEX IF NOT EXISTS storage_archive_one_active
    ON public.storage_archive_jobs ((true)) WHERE phase IN ('copying','verifying','ready');
CREATE OR REPLACE FUNCTION public.meshinfo_archive_freeze() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $archive$
BEGIN
    RAISE EXCEPTION USING ERRCODE='55000',
      MESSAGE='This history partition is immutable while archived or being archived';
END
$archive$;
CREATE OR REPLACE FUNCTION public.meshinfo_archive_stage_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $archive$
BEGIN
    IF current_setting('meshinfo.archive_job',true) IS DISTINCT FROM TG_ARGV[0]
       OR NOT EXISTS (SELECT 1 FROM public.storage_archive_jobs
                      WHERE job_id=TG_ARGV[0]::uuid AND (stage_oid=TG_RELID OR topic_stage_oid=TG_RELID)
                        AND phase='copying' AND source_frozen) THEN
        RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='Archive staging table is private to its copy job';
    END IF;
    RETURN NULL;
END
$archive$;
"""


async def ensure_archive_schema(conn):
    await conn.execute(ARCHIVE_SCHEMA_SQL)
    await conn.execute(archive_topics.SCHEMA_SQL)


def _q(name):
    return '"public".' + quote_identifier(name)


def _json(value):
    return json.loads(value) if isinstance(value, str) else value


def _columns(alias=""):
    return ",".join((alias + "." if alias else "") + quote_identifier(name) for name in MQTT_COLUMNS)


def _key(alias=""):
    prefix = alias + "." if alias else ""
    return f"({prefix}id,{prefix}created_at)"


def _reason(exc):
    return str(exc) if isinstance(exc, ArchiveBlocked) else type(exc).__name__ + (
        " (SQLSTATE " + str(exc.sqlstate) + ")" if getattr(exc, "sqlstate", None) else "")


async def _settings(conn, policy):
    await conn.execute(f"SET LOCAL statement_timeout='{math.ceil(policy.statement_seconds*1000)}ms'")
    await conn.execute("SET LOCAL lock_timeout='100ms'; SET LOCAL TIME ZONE 'UTC'; "
                       "SET LOCAL work_mem='4MB'; SET LOCAL maintenance_work_mem='32MB'; "
                       "SET LOCAL plan_cache_mode=force_custom_plan; "
                       "SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL max_parallel_maintenance_workers=0")


async def _environment(conn):
    major = int(await conn.fetchval("SHOW server_version_num")) // 10000
    extension = await conn.fetchval("SELECT extversion FROM pg_extension WHERE extname='citus_columnar'")
    if major != 18 or extension != "14.1-1":
        raise ArchiveBlocked(f"Unsupported archive engine: PostgreSQL {major}, citus_columnar {extension or 'missing'}; supported matrix is PostgreSQL 18 and citus_columnar 14.1-1")
    preloaded = {entry.strip().strip('"').split('/')[-1].removesuffix('.so')
                 for entry in (await conn.fetchval("SHOW shared_preload_libraries")).split(',')}
    if not preloaded.intersection({"citus_columnar", "citus"}):
        raise ArchiveBlocked("The archive engine requires citus_columnar or citus in shared_preload_libraries")
    if (await conn.fetchval("SHOW default_tablespace") not in ("", "pg_default")
            or await conn.fetchval("SHOW temp_tablespaces") not in ("", "pg_default")
            or await conn.fetchval("""SELECT t.spcname FROM pg_database d JOIN pg_tablespace t
                ON t.oid=d.dattablespace WHERE d.datname=current_database()""") != "pg_default"):
        raise ArchiveBlocked("Archive requires pg_default database, staging and temporary tablespaces on the verified filesystem")
    if not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='citus_columnar')"):
        raise ArchiveBlocked("citus_columnar must be installed in this database")
    if await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname LIKE 'columnar%' AND c.reltablespace NOT IN
              (0,(SELECT oid FROM pg_tablespace WHERE spcname='pg_default')))"""):
        raise ArchiveBlocked("Columnar metadata uses an unverified custom tablespace")
    if await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_replication_slots)"):
        raise ArchiveBlocked("Replication slots require an independently budgeted archive WAL policy")
    if await conn.fetchval("SHOW archive_mode") != "off":
        raise ArchiveBlocked("WAL archiving requires an independently budgeted archive WAL policy")
    if await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_publication WHERE puballtables)"):
        raise ArchiveBlocked("Columnar history cannot participate in all-table logical publications")
    if await conn.fetchval("""SELECT relrowsecurity OR relforcerowsecurity OR relhasrules
            FROM pg_class WHERE oid='public.mqtt_messages'::regclass"""):
        raise ArchiveBlocked("Custom parent rules or row-security policies require an explicit archive policy")
    if await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
            JOIN pg_namespace n ON n.oid=p.pronamespace WHERE t.tgrelid='public.mqtt_messages'::regclass
            AND NOT t.tgisinternal AND (t.tgname<>'trg_mqtt_messages_extract_node_ids'
              OR p.proname<>'mqtt_messages_extract_node_ids' OR n.nspname<>'public' OR t.tgenabled<>'O'))"""):
        raise ArchiveBlocked("Unrecognized parent history triggers require an explicit archive policy")
    if await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM pg_index i
            JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam
            WHERE i.indrelid='public.mqtt_messages'::regclass AND a.amname NOT IN ('btree','hash'))"""):
        raise ArchiveBlocked("Prepare leaf-only topic indexes before attaching columnar history")


_LEAVES_SQL = """
SELECT c.oid,c.relname AS name,pg_get_expr(c.relpartbound,c.oid) AS bound,a.amname AS method,
       pg_table_size(c.oid)::bigint AS table_bytes,pg_total_relation_size(c.oid)::bigint AS total_bytes,
       coalesce((SELECT sum(pg_relation_size(i.indexrelid)) FROM pg_index i
         JOIN pg_class ic ON ic.oid=i.indexrelid JOIN pg_am ia ON ia.oid=ic.relam
         WHERE i.indrelid=c.oid AND ia.amname IN ('btree','hash')),0)::bigint AS supported_index_bytes
FROM pg_class c JOIN pg_inherits inh ON inh.inhrelid=c.oid
JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_am a ON a.oid=c.relam
WHERE inh.inhparent='public.mqtt_messages'::regclass AND n.nspname='public' AND c.relkind='r'
"""


async def plan_archive(conn, config):
    policy = archive_policy(config)
    async with conn.transaction(readonly=True):
        await _settings(conn, policy)
        now = await conn.fetchval("SELECT clock_timestamp()")
        cutoff = complete_month_cutoff(now, policy.after_months)
        retention = retention_policy(config)
        expires = complete_month_cutoff(now, retention.months_for("packets")) if retention.enabled else None
        leaves = await conn.fetch(_LEAVES_SQL)
        topic_bounds = set()
        if await conn.fetchval("SELECT to_regclass('public.mqtt_archive_topics') IS NOT NULL"):
            for topic in await conn.fetch("""SELECT pg_get_expr(c.relpartbound,c.oid) AS bound
                FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid
                WHERE i.inhparent='public.mqtt_archive_topics'::regclass"""):
                topic_bounds.add(monthly_partition_bounds(topic["bound"]))
        jobs = []
        failed = set()
        if await conn.fetchval("SELECT to_regclass('public.storage_archive_jobs') IS NOT NULL"):
            jobs = [dict(row) for row in await conn.fetch("""SELECT job_id::text,source_name,stage_name,source_method,target_method,phase,
                source_frozen,blocked_reason,rows_copied,rows_verified,source_bytes,estimated_stage_bytes,
                aborted_bytes,abort_count,created_at,updated_at,finished_at FROM public.storage_archive_jobs
                ORDER BY created_at DESC LIMIT 100""")]
            failed = {row["source_name"] for row in await conn.fetch("""SELECT source_name FROM public.storage_archive_jobs j
                WHERE phase='failed' AND NOT EXISTS(SELECT 1 FROM public.storage_archive_jobs newer
                  WHERE newer.source_name=j.source_name AND newer.created_at>j.created_at)""")}
    candidates = []
    for row in leaves:
        bounds = monthly_partition_bounds(row["bound"])
        reason = ("unsupported_access_method" if row["method"] not in ("heap","columnar") else
                  "not_monthly_partition" if bounds is None else
                  "awaiting_retention" if expires is not None and bounds[1] <= expires else
                  "previous_attempt_failed_cancel_job_to_retry" if row["name"] in failed else
                  "already_archived" if row["method"] == "columnar" and bounds in topic_bounds else
                  "too_recent" if row["method"] == "heap" and bounds[1] > cutoff else None)
        candidates.append({**dict(row), "eligible": reason is None, "reason": reason,
                           "target_method": "topics" if row["method"] == "columnar" else "columnar"})
    candidates.sort(key=lambda item: (not item["eligible"], item["target_method"] != "topics", item["total_bytes"], item["name"]))
    return {"enabled": policy.enabled, "after_months": policy.after_months, "jobs": jobs,
            "partitions": candidates, "note": "Catalog preview; copy requires a measured staging estimate and database-filesystem headroom."}


async def _fingerprint(conn, oid, source_method="heap"):
    relation = await conn.fetchrow("""SELECT n.nspname,c.relname,c.relkind::text,c.relpersistence::text,
        a.amname,pg_get_userbyid(c.relowner) AS owner,c.relrowsecurity,c.relforcerowsecurity,
        c.relhasrules,c.reltablespace,c.relacl::text,c.reloftype,
        pg_get_expr(c.relpartbound,c.oid) AS bound,
        (SELECT count(*) FROM pg_inherits WHERE inhrelid=c.oid) AS parents,
        (SELECT inhparent='public.mqtt_messages'::regclass FROM pg_inherits WHERE inhrelid=c.oid LIMIT 1) AS expected_parent,
        pg_get_partkeydef('public.mqtt_messages'::regclass) AS parent_key
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_am a ON a.oid=c.relam
        WHERE c.oid=$1""", oid)
    if (not relation or relation["nspname"] != "public" or relation["relkind"] != "r"
            or relation["relpersistence"] != "p" or relation["amname"] != source_method
            or relation["parents"] != 1 or not relation["expected_parent"]
            or relation["parent_key"] != "RANGE (created_at)"
            or relation["relrowsecurity"] or relation["relforcerowsecurity"] or relation["relhasrules"]
            or relation["reltablespace"] or relation["relacl"] or relation["reloftype"]):
        raise ArchiveBlocked("Source topology, tablespace, ownership permissions, rules or row security are unsupported")
    columns = [dict(row) for row in await conn.fetch("""SELECT a.attname,format_type(a.atttypid,a.atttypmod) AS type,
        a.attnotnull,a.attidentity::text,a.attgenerated::text,a.attstorage::text,a.attcompression::text,
        a.attcollation::regcollation::text AS collation,a.attacl::text,
        pg_get_expr(d.adbin,d.adrelid) AS default_expression
        FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
        WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum""", oid)]
    expected_types = ("bigint", "text", "text", "integer", "boolean", "bigint",
                      "timestamp with time zone", "character varying(8)", "character varying(8)", "bigint")
    if (tuple(row["attname"] for row in columns) != MQTT_COLUMNS
            or tuple(row["type"] for row in columns) != expected_types
            or any(row["attidentity"] or row["attgenerated"] or row["attacl"] for row in columns)
            or not columns[0]["attnotnull"] or not columns[6]["attnotnull"]):
        raise ArchiveBlocked("Archive requires the exact canonical MQTT column layout and nonnull primary identity")
    if await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM pg_constraint
            WHERE contype='f' AND (conrelid=ANY($1::oid[]) OR confrelid=ANY($1::oid[])))""",
            [oid, await conn.fetchval("SELECT 'public.mqtt_messages'::regclass::oid")]):
        raise ArchiveBlocked("Foreign-key dependencies require an explicit archive policy")
    if await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_publication_rel WHERE prrelid=$1 OR prrelid='public.mqtt_messages'::regclass)", oid):
        raise ArchiveBlocked("Published source partitions cannot be converted to columnar")
    if await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM pg_depend d
        WHERE d.refclassid='pg_class'::regclass AND d.refobjid=$1 AND d.deptype='n'
          AND NOT (d.classid='pg_class'::regclass AND d.objid IN
                   (SELECT indexrelid FROM pg_index WHERE indrelid=$1))
          AND NOT (d.classid='pg_constraint'::regclass AND d.objid IN
                   (SELECT oid FROM pg_constraint WHERE conrelid=$1)))""", oid):
        raise ArchiveBlocked("Source has external dependencies; archive never drops them with CASCADE")
    indexes = [dict(row) for row in await conn.fetch("""SELECT c.relname,a.amname,c.reltablespace,
        i.indisvalid,i.indisready,i.indisprimary,i.indisunique,pg_get_indexdef(c.oid) AS definition
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam
        WHERE i.indrelid=$1 ORDER BY c.relname""", oid)]
    if any(not row["indisvalid"] or not row["indisready"] or row["reltablespace"]
           or row["amname"] not in ("btree", "hash", "gin") for row in indexes):
        raise ArchiveBlocked("Source has unsupported, invalid or custom-tablespace indexes")
    if any(row["amname"] == "gin" for row in indexes):
        # Only canonical-topic trigram search is replaced by the verified heap
        # lookup. Silently dropping a user payload/expression/partial GIN would
        # discard an independent access path that this worker cannot preserve.
        from storage.db.archive_catalog import (
            _TOPIC_METADATA_SQL, _TOPIC_JOINS_SQL, supported_parent,
        )
        gin_indexes = await conn.fetch("SELECT " + _TOPIC_METADATA_SQL + """
            FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid
            JOIN pg_am am ON am.oid=idx.relam """ + _TOPIC_JOINS_SQL + """
            WHERE i.indrelid=$1 AND am.amname='gin'""", oid)
        if not all(supported_parent(index) for index in gin_indexes):
            raise ArchiveBlocked("Only standard canonical-topic pg_trgm GIN indexes can be replaced by the archive lookup")
    constraints = [dict(row) for row in await conn.fetch("""SELECT conname,contype::text,convalidated,condeferrable,condeferred,
        pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE conrelid=$1 ORDER BY conname""", oid)]
    if not any(row["contype"] == "p" and row["definition"] == "PRIMARY KEY (id, created_at)" for row in constraints):
        raise ArchiveBlocked("Source requires PRIMARY KEY (id, created_at)")
    if any(row["contype"] not in ("p", "u", "c", "n") for row in constraints):
        raise ArchiveBlocked("Source has unsupported constraints")
    if any(not row["convalidated"] or row["condeferrable"] or row["condeferred"] for row in constraints):
        raise ArchiveBlocked("Archive requires validated, nondeferrable source constraints")
    triggers = [dict(row) for row in await conn.fetch("""SELECT t.tgname,t.tgenabled::text,
        p.proname,n.nspname,pg_get_triggerdef(t.oid) AS definition,pg_get_functiondef(p.oid) AS function
        FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE t.tgrelid=$1 AND NOT t.tgisinternal
          AND t.tgname NOT IN ('meshinfo_archive_freeze','meshinfo_archive_freeze_truncate') ORDER BY t.tgname""", oid)]
    if any(row["tgname"] != "trg_mqtt_messages_extract_node_ids" or row["proname"] != "mqtt_messages_extract_node_ids"
           or row["nspname"] != "public" or row["tgenabled"] != "O" for row in triggers):
        raise ArchiveBlocked("Source has unrecognized triggers; their semantics cannot be discarded")
    return {"relation": dict(relation), "columns": columns, "indexes": indexes,
            "constraints": constraints, "triggers": triggers}


async def _freeze(conn, name, job_id):
    table = _q(name)
    for trigger, events, granularity in (("meshinfo_archive_freeze", "INSERT OR UPDATE OR DELETE", "ROW"),
                                          ("meshinfo_archive_freeze_truncate", "TRUNCATE", "STATEMENT")):
        await conn.execute(f"CREATE TRIGGER {trigger} BEFORE {events} ON {table} FOR EACH {granularity} "
                           f"EXECUTE FUNCTION public.meshinfo_archive_freeze('{job_id}')")
        await conn.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {trigger}")


async def _guarded(conn, oid, job_id):
    rows = await conn.fetch("""SELECT tgname,tgtype,tgenabled::text,tgargs
        FROM pg_trigger WHERE tgrelid=$1 AND tgfoid='public.meshinfo_archive_freeze()'::regprocedure
          AND tgname IN ('meshinfo_archive_freeze','meshinfo_archive_freeze_truncate')""", oid)
    expected = {"meshinfo_archive_freeze": 31, "meshinfo_archive_freeze_truncate": 34}
    return len(rows) == 2 and all(row["tgtype"] == expected[row["tgname"]] and row["tgenabled"] == "A"
        and bytes(row["tgargs"]) == str(job_id).encode()+b"\0" for row in rows)


def _free(free_bytes, policy, required=0):
    if free_bytes is None:
        raise ArchiveBlocked("A verified free-space probe for the database filesystem is required")
    available = free_bytes()
    if type(available) is not int or available < 0:
        raise ArchiveBlocked("Database filesystem probe returned an invalid byte count")
    if available < policy.reserve_bytes + required:
        raise ArchiveBlocked(f"Archive needs {policy.reserve_bytes+required} free bytes on its database filesystem; {available} available")
    return available


async def _size(conn, name):
    return await conn.fetchval("SELECT pg_total_relation_size($1::regclass)::bigint", _q(name))


async def _stage_size(conn,job):
    total = await _size(conn,job["stage_name"])
    if job.get("topic_stage_name") and job["topic_stage_name"] != job["stage_name"]:
        total += await _size(conn,job["topic_stage_name"])
    return total


async def _options(conn, name, options):
    await conn.execute(f"ALTER TABLE {_q(name)} SET (columnar.compression='zstd',"
        f"columnar.compression_level={int(options['compression_level'])},columnar.stripe_row_limit=150000,"
        f"columnar.chunk_group_row_limit={int(options['chunk_group_rows'])})")


def _copy_batch_policy(policy, index_count):
    """One row/WAL budget for the real writer and its geometry-aware pilot."""
    if type(index_count) is not int or index_count < 1:
        raise ArchiveBlocked("Archive copy requires a positive index-count budget")
    gin_budget = policy.max_wal_bytes//4
    rows = (policy.max_wal_bytes-2*policy.batch_bytes-gin_budget)//(32768*index_count)
    if rows < 1:
        raise ArchiveBlocked("One row's index-write estimate exceeds max_wal_mib; verify headroom before increasing it")
    return replace(policy,batch_rows=min(policy.batch_rows,rows)),gin_budget


async def _fill_columnar_pilot(conn,heap,columnar,policy):
    """Seal the same bounded write stripes while retaining one outer rollback."""
    if not conn.is_in_transaction():
        raise ValueError("Archive pilots require an enclosing transaction")
    last_id = last_time = None
    rows = 0
    while True:
        selected,_bytes = await _batch(conn,heap,last_id,last_time,policy,build_topic_index=True)
        if not selected:
            return rows
        last = selected[-1]
        # Citus seals subtransaction write state on RELEASE SAVEPOINT. The
        # complete pilot still rolls back with its enclosing preflight.
        async with conn.transaction():
            command = await conn.execute(f"""INSERT INTO {_q(columnar)} ({_columns()})
                SELECT {_columns()} FROM {_q(heap)}
                WHERE ($1::bigint IS NULL OR (id,created_at)>($1,$2::timestamptz))
                  AND (id,created_at)<=($3,$4) ORDER BY id,created_at""",
                last_id,last_time,last["id"],last["created_at"])
            if command != f"INSERT 0 {len(selected)}":
                raise ArchiveBlocked("Compression pilot changed its bounded row cohort")
        rows += len(selected)
        last_id,last_time = last["id"],last["created_at"]


async def _pilot_metadata(conn,name):
    """Upper-biased logical metadata cost, independent of reusable catalog pages.

Pinned Citus14.1 catalogs have two stripe indexes and one index each for chunk
groups/chunks. Allow96B per index entry plus padded fixed heap tuple costs.
Raw min/max byte lengths cover even externally TOASTed metadata. Sampling and
the policy multiplier remain estimates; real chunks retain filesystem guards.
"""
    return dict(await conn.fetchrow("""SELECT
        (SELECT coalesce(sum(row_count),0)::bigint FROM columnar.stripe WHERE relation=$1::regclass) AS rows,
        (SELECT count(*)::bigint FROM columnar.stripe WHERE relation=$1::regclass) AS stripes,
        (SELECT coalesce(max(row_count),0)::bigint FROM columnar.stripe WHERE relation=$1::regclass) AS max_stripe_rows,
        (SELECT coalesce(max(row_count),0)::bigint FROM columnar.chunk_group WHERE relation=$1::regclass) AS max_group_rows,
        ((SELECT count(*)*320 FROM columnar.stripe WHERE relation=$1::regclass)
         +(SELECT count(*)*192 FROM columnar.chunk_group WHERE relation=$1::regclass)
         +(SELECT coalesce(sum(256::bigint+coalesce(octet_length(minimum_value),0)
             +coalesce(octet_length(maximum_value),0)),0) FROM columnar.chunk WHERE relation=$1::regclass))::bigint AS bytes""",_q(name)))


async def _estimate(conn, source, policy, *, target_method="columnar", extra_indexes=()):
    """Bounded real pilot; its heap/columnar ratio is conservatively inflated.

    The pilot is transactional and is dropped before commit. It is an estimate,
    never a reservation guarantee; every real chunk rechecks actual disk use.
    """
    if not conn.is_in_transaction():
        raise ValueError("Archive staging estimates require an enclosing transaction")
    index_count = sum(row["amname"] != "gin" for row in await _indexes(conn,source["oid"]))
    index_count += 3 if target_method == "columnar" else len(extra_indexes)
    batch_policy,_gin_budget = _copy_batch_policy(policy,index_count)
    sample_limit = min(1000,32*batch_policy.batch_rows)
    token = uuid.uuid4().hex[:16]
    heap, columnar = "meshinfo_archive_pilot_h_"+token, "meshinfo_archive_pilot_c_"+token
    await conn.execute(f"CREATE TABLE {_q(heap)} (LIKE {_q(source['name'])} INCLUDING DEFAULTS INCLUDING STORAGE INCLUDING COMPRESSION) USING heap")
    # Length metadata bounds transfer/memory without materializing enormous text.
    sample_method = "TABLESAMPLE SYSTEM (1) REPEATABLE (731)" if source["method"] == "heap" else ""
    sample_order = "" if source["method"] == "heap" else "ORDER BY id,created_at"
    index_sample_limit = policy.max_wal_bytes//(4*128)-1 if extra_indexes else policy.batch_bytes
    await conn.execute(f"""WITH sample AS MATERIALIZED (
        SELECT id,created_at,128+coalesce(octet_length(payload),0)+coalesce(octet_length(topic),0) AS n,
          1+coalesce(octet_length(topic),0) AS topic_bytes
        FROM {_q(source['name'])} {sample_method}
        WHERE coalesce(octet_length(payload),0)+coalesce(octet_length(topic),0)<=$1 {sample_order} LIMIT $4),
        bounded AS (SELECT id,created_at,sum(n) OVER (ORDER BY id,created_at) AS bytes,
          sum(topic_bytes) OVER(ORDER BY id,created_at) AS topic_bytes FROM sample)
        INSERT INTO {_q(heap)} ({_columns()}) SELECT {_columns('s')}
        FROM {_q(source['name'])} s JOIN bounded b USING(id,created_at)
        WHERE b.bytes<=$2 AND b.topic_bytes<=$3 ORDER BY s.id,s.created_at""",
        min(MIB, policy.batch_bytes), policy.batch_bytes,index_sample_limit,sample_limit)
    extra_bytes_per_row = metadata_estimate = 0
    if not await conn.fetchval(f"SELECT EXISTS(SELECT 1 FROM {_q(heap)})"):
        await conn.execute(f"INSERT INTO {_q(heap)} ({_columns()}) SELECT {_columns()} FROM {_q(source['name'])} "
            "WHERE coalesce(octet_length(payload),0)+coalesce(octet_length(topic),0)<=$1 "
            "AND coalesce(octet_length(topic),0)<=$2 ORDER BY id,created_at LIMIT 1", policy.batch_bytes,index_sample_limit)
    if not await conn.fetchval(f"SELECT EXISTS(SELECT 1 FROM {_q(heap)})"):
        if await conn.fetchval(f"SELECT EXISTS(SELECT 1 FROM {_q(source['name'])})"):
            raise ArchiveBlocked("No bounded compression sample is available; increase the batch byte budget")
        table_estimate = 0
    else:
        heap_bytes = await _size(conn, heap)
        count = await conn.fetchval(f"SELECT count(*) FROM {_q(heap)}")
        if target_method == "columnar":
            await conn.execute(f"CREATE TABLE {_q(columnar)} (LIKE {_q(heap)} INCLUDING ALL) USING columnar")
            await _options(conn, columnar, {"compression_level": policy.compression_level, "chunk_group_rows": policy.chunk_group_rows})
            written = await _fill_columnar_pilot(conn,heap,columnar,batch_policy)
            metadata = await _pilot_metadata(conn,columnar)
            if (written != count or metadata["rows"] != count or
                    metadata["max_stripe_rows"] > batch_policy.batch_rows or
                    metadata["max_group_rows"] > min(batch_policy.batch_rows,policy.chunk_group_rows)):
                raise ArchiveBlocked("Compression pilot did not retain the bounded worker geometry")
            ratio = max(.05,(await _size(conn,columnar))/max(1,heap_bytes))
            row_estimate = await conn.fetchval("SELECT reltuples::float8 FROM pg_class WHERE oid=$1",source["oid"])
            row_estimate = max(count,row_estimate,source["table_bytes"]*count/max(1,heap_bytes))
            metadata_estimate = metadata["bytes"]*row_estimate/count
            table_estimate = source["table_bytes"]*ratio
            await conn.execute(f"DROP TABLE {_q(columnar)}")
        else:
            # The source may have different settings/stripe geometry. Its
            # catalog provides exact row counts, so estimate the expanded heap
            # directly instead of inverting a mismatched compression ratio.
            row_estimate = await conn.fetchval("""SELECT coalesce(sum(row_count),0)::bigint
                FROM columnar.stripe WHERE relation=$1::oid::regclass""",source["oid"])
            if row_estimate < count:
                raise ArchiveBlocked("Source columnar row-count metadata is incomplete")
            table_estimate = heap_bytes*row_estimate/count
        for number, tail in enumerate(extra_indexes):
            await conn.execute(f"CREATE INDEX {quote_identifier(heap+'_g'+str(number))} ON {_q(heap)} USING {tail}")
        if extra_indexes:
            extra_bytes_per_row = await conn.fetchval("SELECT pg_indexes_size($1::regclass)::float8", _q(heap))/count
            table_estimate += extra_bytes_per_row*row_estimate
    await conn.execute(f"DROP TABLE {_q(heap)}")
    return math.ceil((table_estimate+metadata_estimate)*policy.estimate_multiplier
                     +source["supported_index_bytes"]+16*MIB)


def _index_signature(row):
    return (row["amname"], row["indisprimary"], row["indisunique"], row["definition"].split(" USING ", 1)[1])


async def _indexes(conn, oid):
    return await conn.fetch("""SELECT a.amname,i.indisprimary,i.indisunique,i.indisvalid,i.indisready,
        c.reltablespace,pg_get_indexdef(c.oid) AS definition FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
        JOIN pg_am a ON a.oid=c.relam WHERE i.indrelid=$1""", oid)


async def _constraints(conn, oid):
    rows = await conn.fetch("""SELECT contype::text,pg_get_constraintdef(oid) AS definition,
        convalidated,condeferrable,condeferred FROM pg_constraint WHERE conrelid=$1""", oid)
    return sorted(tuple(row) for row in rows)


async def _start(conn, source, policy, free_bytes, *, target_method="columnar"):
    job_id = uuid.uuid4()
    stage = "meshinfo_archive_stage_" + job_id.hex[:24]
    bounds = monthly_partition_bounds(source["bound"])
    async with conn.transaction():
        await _settings(conn, policy)
        await _environment(conn)
        source_method = source["method"]
        if (source_method, target_method) not in (("heap", "columnar"), ("columnar", "heap"), ("columnar","topics")):
            raise ArchiveBlocked("Archive conversion requires opposite supported source/target access methods")
        fingerprint = await _fingerprint(conn, source["oid"], source_method)
        previous_guard_job = None
        existing_guards = await conn.fetch("SELECT tgargs FROM pg_trigger WHERE tgrelid=$1 AND tgname='meshinfo_archive_freeze'", source["oid"])
        if existing_guards:
            if source_method != "columnar":
                raise ArchiveBlocked("Heap source is already frozen by another archive job")
            previous_guard_job = uuid.UUID(bytes(existing_guards[0]["tgargs"]).rstrip(b"\0").decode())
            if not await _guarded(conn, source["oid"], previous_guard_job):
                raise ArchiveBlocked("Existing columnar immutability guards are inconsistent")
        original_comment = await conn.fetchval("SELECT obj_description($1,'pg_class')", source["oid"])
        extra_indexes = []
        if target_method == "heap":
            previous = await conn.fetchrow("""SELECT schema_fingerprint,original_comment FROM public.storage_archive_jobs
                WHERE source_name=$1 AND target_method='columnar' AND phase='complete' ORDER BY finished_at DESC LIMIT 1""", source["name"])
            if previous:
                extra_indexes = [row["definition"].split(" USING ", 1)[1]
                                 for row in _json(previous["schema_fingerprint"])["indexes"] if row["amname"] == "gin"]
                original_comment = previous["original_comment"]
            if not extra_indexes:
                extra_indexes = ["gin (topic gin_trgm_ops)"]
        _free(free_bytes, policy, 4*policy.batch_bytes + policy.max_wal_bytes)
        await archive_topics.validate_parent(conn)
        if target_method in ("columnar","topics") and await archive_topics.attached_partition(conn,*bounds) is not None:
            raise ArchiveBlocked("Canonical topic lookup already covers this source month")
        estimated = 0 if target_method == "topics" else await _estimate(
            conn, source, policy, target_method=target_method, extra_indexes=extra_indexes)
        if target_method in ("columnar","topics"):
            estimated += await archive_topics.estimate(conn,source,policy)
        _free(free_bytes, policy, estimated + policy.max_wal_bytes)
        # This brief lock drains earlier writers before installing ALWAYS guards.
        await conn.execute(f"LOCK TABLE ONLY {_q(source['name'])} IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        if await _fingerprint(conn, source["oid"], source_method) != fingerprint:
            raise ArchiveBlocked("Source schema changed during archive preflight")
        options = {"compression_level": policy.compression_level, "chunk_group_rows": policy.chunk_group_rows}
        await conn.execute("""INSERT INTO public.storage_archive_jobs
            (job_id,source_oid,source_name,stage_name,bound_start,bound_end,phase,source_frozen,
             schema_fingerprint,compression_options,source_bytes,estimated_stage_bytes,
             source_method,target_method,previous_guard_job,original_comment)
            VALUES($1,$2,$3,$4,$5,$6,'copying',true,$7::jsonb,$8::jsonb,$9,$10,$11,$12,$13,$14)""",
            job_id, source["oid"], source["name"], stage, *bounds, json.dumps(fingerprint),
            json.dumps(options), source["total_bytes"], estimated, source_method,target_method,previous_guard_job,original_comment)
        if previous_guard_job is not None:
            await conn.execute(f"DROP TRIGGER meshinfo_archive_freeze ON {_q(source['name'])}")
            await conn.execute(f"DROP TRIGGER meshinfo_archive_freeze_truncate ON {_q(source['name'])}")
        await _freeze(conn, source["name"], job_id)
        if target_method in ("columnar","topics"):
            topic_name = stage if target_method == "topics" else "meshinfo_topics_stage_"+job_id.hex[:24]
            topic_oid, topic_fingerprint = await archive_topics.create_stage(
                conn,topic_name,job_id,bounds,fingerprint["relation"]["owner"])
            await conn.execute("""UPDATE public.storage_archive_jobs SET topic_stage_name=$2,topic_stage_oid=$3,
                topic_stage_fingerprint=$4::jsonb WHERE job_id=$1""",job_id,topic_name,topic_oid,json.dumps(topic_fingerprint))
            if target_method == "topics":
                await conn.execute("""UPDATE public.storage_archive_jobs SET stage_oid=$2,target_index_signatures=$3::jsonb,
                    target_constraint_signatures=$4::jsonb WHERE job_id=$1""",job_id,topic_oid,
                    json.dumps(topic_fingerprint["indexes"]),json.dumps(topic_fingerprint["constraints"]))
                return job_id
        # Copy exact constraints/index definitions into an empty heap first;
        # remove only unsupported GIN copies before the cheap empty conversion.
        await conn.execute(f"CREATE TABLE {_q(stage)} (LIKE {_q(source['name'])} INCLUDING ALL) USING heap")
        stage_oid = await conn.fetchval("SELECT $1::regclass::oid", _q(stage))
        for row in await conn.fetch("""SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
            JOIN pg_am a ON a.oid=c.relam WHERE i.indrelid=$1 AND a.amname='gin'""", stage_oid):
            await conn.execute(f"DROP INDEX {_q(row['relname'])}")
        if target_method == "columnar":
            await conn.execute(f"ALTER TABLE {_q(stage)} SET ACCESS METHOD columnar")
            await _options(conn, stage, options)
        else:
            for number, tail in enumerate(extra_indexes):
                await conn.execute(f"CREATE INDEX {quote_identifier(stage+'_g'+str(number))} ON {_q(stage)} USING {tail}")
        await conn.execute(f"ALTER TABLE {_q(stage)} OWNER TO {quote_identifier(fingerprint['relation']['owner'])}")
        # A job-specific validated bound avoids trusting a preexisting constraint
        # merely because it happens to use an archive-looking name.
        bound_name = "meshinfo_archive_bound_"+job_id.hex[:16]
        await conn.execute(f"ALTER TABLE {_q(stage)} ADD CONSTRAINT {quote_identifier(bound_name)} "
            f"CHECK (created_at >= '{bounds[0].isoformat()}'::timestamptz AND created_at < '{bounds[1].isoformat()}')")
        await conn.execute(f"COMMENT ON TABLE {_q(stage)} IS 'meshinfo archive stage {job_id}'")
        await conn.execute(f"CREATE TRIGGER meshinfo_archive_stage_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE "
            f"ON {_q(stage)} FOR EACH STATEMENT EXECUTE FUNCTION public.meshinfo_archive_stage_guard('{job_id}')")
        await conn.execute(f"ALTER TABLE {_q(stage)} ENABLE ALWAYS TRIGGER meshinfo_archive_stage_guard")
        signatures = sorted(_index_signature(row) for row in await _indexes(conn, stage_oid))
        constraints = await _constraints(conn, stage_oid)
        await conn.execute("""UPDATE public.storage_archive_jobs SET stage_oid=$2,target_index_signatures=$3::jsonb,
            target_constraint_signatures=$4::jsonb WHERE job_id=$1""",
            job_id, stage_oid, json.dumps(signatures), json.dumps(constraints))
    return job_id


async def _job(conn, job_id):
    row = await conn.fetchrow("SELECT * FROM public.storage_archive_jobs WHERE job_id=$1", job_id)
    if not row:
        raise ArchiveBlocked("Archive job does not exist")
    return dict(row)


async def _validate_job(conn, job):
    previous_topic_oid = job.get("topic_stage_oid")
    source_oid = await conn.fetchval("SELECT to_regclass($1)::oid", _q(job["source_name"]))
    stage_oid = await conn.fetchval("SELECT to_regclass($1)::oid", _q(job["stage_name"]))
    if source_oid is None or stage_oid is None:
        raise ArchiveBlocked("Archive source or owned staging relation is missing")
    if not await _guarded(conn, source_oid, job["job_id"]):
        raise ArchiveBlocked("Archive source freeze guards changed or were disabled")
    if await _fingerprint(conn, source_oid, job.get("source_method", "heap")) != _json(job["schema_fingerprint"]):
        raise ArchiveBlocked("Archive source identity or schema changed")
    if job.get("target_method", "columnar") in ("columnar","topics"):
        await archive_topics.validate_parent(conn)
        await archive_topics.validate_stage(conn,job)
    if job.get("target_method") == "topics":
        await _rebind_job_identity(conn,job,source_oid,stage_oid,previous_topic_oid)
        return
    stage = await conn.fetchrow("""SELECT a.amname,c.reltablespace,c.relrowsecurity,c.relforcerowsecurity,
        c.relhasrules,c.relacl::text,pg_get_userbyid(c.relowner) AS owner,
        obj_description(c.oid,'pg_class') AS comment,
        EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid) AS attached
        FROM pg_class c JOIN pg_am a ON a.oid=c.relam WHERE c.oid=$1""", stage_oid)
    if (not stage or stage["amname"] != job.get("target_method", "columnar") or stage["reltablespace"] or stage["attached"]
            or stage["relrowsecurity"] or stage["relforcerowsecurity"] or stage["relhasrules"] or stage["relacl"]
            or stage["owner"] != _json(job["schema_fingerprint"])["relation"]["owner"]
            or stage["comment"] != f"meshinfo archive stage {job['job_id']}"):
        raise ArchiveBlocked("Staging identity, ownership marker or detached access method changed")
    guard = await conn.fetchrow("""SELECT tgenabled::text,tgargs,tgtype FROM pg_trigger
        WHERE tgrelid=$1 AND tgname='meshinfo_archive_stage_guard'
          AND tgfoid='public.meshinfo_archive_stage_guard()'::regprocedure""", stage_oid)
    if not guard or guard["tgenabled"] != "A" or guard["tgtype"] != 62 or bytes(guard["tgargs"]) != str(job["job_id"]).encode()+b"\0":
        raise ArchiveBlocked("Staging write guard changed or was disabled")
    if await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=$1 AND NOT tgisinternal
            AND tgname<>'meshinfo_archive_stage_guard')""", stage_oid):
        raise ArchiveBlocked("Unrecognized staging triggers could change copied history")
    different = await conn.fetchval("""WITH s AS (SELECT * FROM pg_attribute WHERE attrelid=$1 AND attnum>0),
        t AS (SELECT * FROM pg_attribute WHERE attrelid=$2 AND attnum>0)
        SELECT EXISTS(SELECT 1 FROM s FULL JOIN t USING(attnum)
        WHERE ROW(s.attname,s.atttypid,s.atttypmod,s.attnotnull,s.attidentity,s.attgenerated,
                  s.attcollation,s.attstorage,s.attcompression,s.attisdropped,s.attacl::text)
          IS DISTINCT FROM ROW(t.attname,t.atttypid,t.atttypmod,t.attnotnull,t.attidentity,t.attgenerated,
                  t.attcollation,t.attstorage,t.attcompression,t.attisdropped,t.attacl::text))""", source_oid, stage_oid)
    if different:
        raise ArchiveBlocked("Staging column definitions differ from the immutable source")
    default_difference = await conn.fetchval("""WITH s AS (SELECT a.attname,pg_get_expr(d.adbin,d.adrelid) AS expr
        FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
        WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped),
        t AS (SELECT a.attname,pg_get_expr(d.adbin,d.adrelid) AS expr
        FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
        WHERE a.attrelid=$2 AND a.attnum>0 AND NOT a.attisdropped)
        SELECT EXISTS(SELECT 1 FROM s FULL JOIN t USING(attname) WHERE s.expr IS DISTINCT FROM t.expr)""", source_oid, stage_oid)
    if default_difference:
        raise ArchiveBlocked("Staging column defaults differ from the immutable source")
    expected_constraints = _json(job.get("target_constraint_signatures", "[]"))
    if not expected_constraints or await _constraints(conn, stage_oid) != sorted(tuple(row) for row in expected_constraints):
        raise ArchiveBlocked("Staging constraints or their validity differ from the verified job definition")
    stored_signatures = _json(job.get("target_index_signatures", "[]"))
    expected_indexes = sorted(tuple(row) for row in stored_signatures) if stored_signatures else sorted(
        _index_signature(row) for row in _json(job["schema_fingerprint"])["indexes"] if row["amname"] != "gin")
    actual_indexes = await _indexes(conn, stage_oid)
    if (any(not row["indisvalid"] or not row["indisready"] or row["reltablespace"] for row in actual_indexes)
            or sorted(_index_signature(row) for row in actual_indexes) != expected_indexes):
        raise ArchiveBlocked("Staging indexes differ from the supported source indexes")
    options = _json(job["compression_options"])
    if job.get("target_method", "columnar") == "columnar":
        actual_options = await conn.fetchrow("""SELECT compression,compression_level,chunk_group_row_limit,stripe_row_limit
            FROM columnar.options WHERE relation=$1::oid::regclass""", stage_oid)
        if actual_options is None:
            raise ArchiveBlocked("Columnar staging options are missing")
        if (actual_options["compression"] != "zstd" or actual_options["compression_level"] != options["compression_level"]
                or actual_options["chunk_group_row_limit"] != options["chunk_group_rows"]
                or actual_options["stripe_row_limit"] != 150000):
            # Ordinary logical restores can reset AM options. Restore tools
            # should apply them before COPY; protect future resumed chunks too.
            await _options(conn, job["stage_name"], options)
    # Names, semantic schema, UUID guard arguments and ownership markers survive
    # pg_dump/restore; OIDs do not. Rebind only after validating those identities.
    await _rebind_job_identity(conn,job,source_oid,stage_oid,previous_topic_oid)


async def _rebind_job_identity(conn,job,source_oid,stage_oid,previous_topic_oid):
    if ((source_oid,stage_oid) == (job["source_oid"],job["stage_oid"])
            and previous_topic_oid == job.get("topic_stage_oid")):
        return
    # Logical restore preserves the journal but recreates physical relations.
    # Their copied progress flags do not establish new planner statistics.
    await conn.execute("""UPDATE public.storage_archive_jobs SET source_oid=$2,stage_oid=$3,
        analyzed=false,stage_analyzed=false,topic_analyzed=false WHERE job_id=$1""",
        job["job_id"],source_oid,stage_oid)
    job.update(source_oid=source_oid,stage_oid=stage_oid,
               analyzed=False,stage_analyzed=False,topic_analyzed=False)


async def _batch(conn, name, last_id, last_time, policy, *, topics_only=False, build_topic_index=False):
    payload_size = "0" if topics_only else "coalesce(octet_length(payload),0)"
    rows = await conn.fetch(f"""SELECT id,created_at,
        128+{payload_size}+coalesce(octet_length(topic),0) AS logical_bytes,
        128*(1+coalesce(octet_length(topic),0)::bigint) AS topic_index_budget
        FROM {_q(name)} WHERE ($1::bigint IS NULL OR (id,created_at)>($1,$2::timestamptz))
        ORDER BY id,created_at LIMIT $3""", last_id, last_time, policy.batch_rows)
    selected, size, topic_budget = [], 0, 0
    for row in rows:
        if (size + row["logical_bytes"] > policy.batch_bytes or
                build_topic_index and topic_budget+row["topic_index_budget"] > policy.max_wal_bytes//4):
            break
        selected.append(row)
        size += row["logical_bytes"]
        if build_topic_index:
            topic_budget += row["topic_index_budget"]
    if rows and not selected:
        if build_topic_index and rows[0]["topic_index_budget"] > policy.max_wal_bytes//4:
            raise ArchiveBlocked("One canonical topic exceeds the conservative archive GIN/WAL budget; increase max_wal_mib with verified headroom")
        raise ArchiveBlocked("One history row exceeds the configured archive batch byte limit")
    return selected, size


async def _recover_inflight(conn, job, policy):
    if job["inflight_bytes"] is None:
        return
    actual = await _stage_size(conn,job)
    leaked = max(0, actual - job["inflight_bytes"])
    await conn.execute("""UPDATE public.storage_archive_jobs SET inflight_bytes=NULL,
        aborted_bytes=aborted_bytes+$2,abort_count=abort_count+1,updated_at=clock_timestamp()
        WHERE job_id=$1""", job["job_id"], leaked)
    job["inflight_bytes"] = None
    job["aborted_bytes"] += leaked
    job["abort_count"] += 1


async def _copy_chunk(conn, job, policy, free_bytes):
    # Persist this marker before the copying transaction. An uncertain commit
    # resolves atomically: either both rows and checkpoint exist, or neither.
    indexes = len(_json(job.get("target_index_signatures", "[]"))) or sum(
        row["amname"] != "gin" for row in _json(job["schema_fingerprint"])["indexes"])
    if job.get("topic_stage_name") and job["topic_stage_name"] != job["stage_name"]:
        indexes += 3
    # Budget multiple index-page images per row as well as raw column buffers.
    # The observed WAL counter is global and may also include live ingestion.
    # Trigram entries add work proportional to topic bytes, independently of
    # the number of row/index-page images. Reserve a quarter of the WAL budget
    # and bound that fanout before reading/copying text or modifying either stage.
    batch_policy,gin_budget = _copy_batch_policy(policy,indexes)
    before = await _stage_size(conn,job)
    _free(free_bytes, policy, max(4*policy.batch_bytes+32768*indexes*batch_policy.batch_rows+gin_budget, min(policy.max_wal_bytes,
          max(0, job["estimated_stage_bytes"]-before))))
    await conn.execute("UPDATE public.storage_archive_jobs SET inflight_bytes=$2,blocked_reason=NULL WHERE job_id=$1",
                       job["job_id"], before)
    async with conn.transaction():
        await _settings(conn, policy)
        selected, logical = await _batch(conn, job["source_name"], job["last_id"], job["last_created_at"], batch_policy,
                                         topics_only=job.get("target_method") == "topics",build_topic_index=True)
        if not selected:
            await conn.execute("""UPDATE public.storage_archive_jobs SET phase='verifying',inflight_bytes=NULL,
                updated_at=clock_timestamp() WHERE job_id=$1""", job["job_id"])
            return
        last = selected[-1]
        await conn.execute("SELECT set_config('meshinfo.archive_job',$1,true)", str(job["job_id"]))
        if job.get("target_method") != "topics":
            command = await conn.execute(f"""INSERT INTO {_q(job['stage_name'])} ({_columns()})
                SELECT {_columns()} FROM {_q(job['source_name'])}
                WHERE ($1::bigint IS NULL OR (id,created_at)>($1,$2::timestamptz))
                  AND (id,created_at)<=($3,$4) ORDER BY id,created_at""",
                job["last_id"], job["last_created_at"], last["id"], last["created_at"])
            if command != f"INSERT 0 {len(selected)}":
                raise ArchiveBlocked("Frozen source changed while copying an archive chunk")
        if job.get("topic_stage_name"):
            await archive_topics.copy_chunk(conn,job,last,len(selected))
        _free(free_bytes, policy, 2*policy.batch_bytes)
        await conn.execute("""UPDATE public.storage_archive_jobs SET last_id=$2,last_created_at=$3,
            rows_copied=rows_copied+$4,copied_logical_bytes=copied_logical_bytes+$5,inflight_bytes=NULL,
            blocked_reason=NULL,updated_at=clock_timestamp() WHERE job_id=$1""",
            job["job_id"], last["id"], last["created_at"], len(selected), logical)


def _exact_equal():
    text = {"topic", "payload", "from_node_id", "to_node_id"}
    return " AND ".join(
        f"convert_to(s.{quote_identifier(name)}::text,'UTF8') IS NOT DISTINCT FROM convert_to(t.{quote_identifier(name)}::text,'UTF8')"
        if name in text else f"s.{quote_identifier(name)} IS NOT DISTINCT FROM t.{quote_identifier(name)}"
        for name in MQTT_COLUMNS)


async def _verify_chunk(conn, job, policy):
    async with conn.transaction():
        await _settings(conn, policy)
        selected, _size_bytes = await _batch(conn, job["source_name"], job["verify_id"], job["verify_created_at"], policy,
                                             topics_only=job.get("target_method") == "topics")
        if job.get("topic_stage_name"):
            await archive_topics.verify_chunk(conn,job,selected)
        if not selected:
            extra = await conn.fetchval(f"SELECT EXISTS(SELECT 1 FROM {_q(job['stage_name'])} "
                "WHERE ($1::bigint IS NULL OR (id,created_at)>($1,$2::timestamptz)) LIMIT 1)",
                job["verify_id"], job["verify_created_at"])
            if extra or job["rows_verified"] != job["rows_copied"]:
                raise ArchiveBlocked("Archive cardinality verification failed")
            await conn.execute("UPDATE public.storage_archive_jobs SET phase='ready',updated_at=clock_timestamp() WHERE job_id=$1", job["job_id"])
            return
        last = selected[-1]
        if job.get("target_method") == "topics":
            await conn.execute("""UPDATE public.storage_archive_jobs SET verify_id=$2,verify_created_at=$3,
                rows_verified=rows_verified+$4,blocked_reason=NULL,updated_at=clock_timestamp() WHERE job_id=$1""",
                job["job_id"],last["id"],last["created_at"],len(selected))
            return
        # FULL JOIN detects extra and missing keys; bytea equality makes text
        # checks exact even under a case-insensitive database collation.
        good = await conn.fetchval(f"""WITH s AS MATERIALIZED (
            SELECT {_columns()} FROM {_q(job['source_name'])}
            WHERE ($1::bigint IS NULL OR (id,created_at)>($1,$2::timestamptz)) AND (id,created_at)<=($3,$4)),
            t AS MATERIALIZED (SELECT {_columns()} FROM {_q(job['stage_name'])}
            WHERE ($1::bigint IS NULL OR (id,created_at)>($1,$2::timestamptz)) AND (id,created_at)<=($3,$4)
            LIMIT $5)
            SELECT count(*)=$6 AND coalesce(bool_and(s.id IS NOT NULL AND t.id IS NOT NULL AND ({_exact_equal()})),false)
            FROM s FULL JOIN t USING(id,created_at)""", job["verify_id"], job["verify_created_at"],
            last["id"], last["created_at"], len(selected)+1, len(selected))
        if not good:
            raise ArchiveBlocked("Archive exact row-by-row verification failed; source remains intact")
        await conn.execute("""UPDATE public.storage_archive_jobs SET verify_id=$2,verify_created_at=$3,
            rows_verified=rows_verified+$4,blocked_reason=NULL,updated_at=clock_timestamp() WHERE job_id=$1""",
            job["job_id"], last["id"], last["created_at"], len(selected))


async def _cutover(conn, job, policy, free_bytes):
    async with conn.transaction():
        await _settings(conn, policy)
        await conn.execute("LOCK TABLE ONLY public.mqtt_messages IN ACCESS EXCLUSIVE MODE NOWAIT")
        await conn.execute("LOCK TABLE ONLY public.mqtt_archive_topics IN ACCESS EXCLUSIVE MODE NOWAIT")
        await conn.execute(f"LOCK TABLE ONLY {_q(job['source_name'])}, {_q(job['stage_name'])} IN ACCESS EXCLUSIVE MODE NOWAIT")
        if job.get("topic_stage_name") and job["topic_stage_name"] != job["stage_name"]:
            await conn.execute(f"LOCK TABLE ONLY {_q(job['topic_stage_name'])} IN ACCESS EXCLUSIVE MODE NOWAIT")
        defaults = await conn.fetch("""SELECT c.relname FROM pg_class c JOIN pg_inherits i ON i.inhrelid=c.oid
            WHERE i.inhparent='public.mqtt_messages'::regclass AND pg_get_expr(c.relpartbound,c.oid)='DEFAULT'""")
        for row in defaults:
            await conn.execute(f"LOCK TABLE ONLY {_q(row['relname'])} IN ACCESS EXCLUSIVE MODE NOWAIT")
            if await conn.fetchval(f"SELECT EXISTS(SELECT 1 FROM {_q(row['relname'])})"):
                raise ArchiveBlocked("Nonempty default history partition prevents a bounded archive cutover")
        await _environment(conn)
        await _validate_job(conn, job)
        _free(free_bytes, policy, 2*policy.batch_bytes)
        if job["rows_verified"] != job["rows_copied"] or job["inflight_bytes"] is not None or not job["analyzed"]:
            raise ArchiveBlocked("Archive is not fully verified")
        if job.get("target_method") == "topics":
            await archive_topics.attach(conn,job)
            await conn.execute("""UPDATE public.storage_archive_jobs SET phase='complete',source_frozen=false,
                blocked_reason=NULL,updated_at=clock_timestamp(),finished_at=clock_timestamp() WHERE job_id=$1""",job["job_id"])
            return
        if job.get("target_method") == "heap":
            helper = await archive_topics.attached_partition(conn,job["bound_start"],job["bound_end"])
            if helper is not None:
                await conn.execute(f"LOCK TABLE ONLY {_q(helper['name'])} IN ACCESS EXCLUSIVE MODE NOWAIT")
                await conn.execute(f"DROP TABLE {_q(helper['name'])}")
        retired = "meshinfo_archive_retired_"+job["job_id"].hex[:24]
        await conn.execute(f"ALTER TABLE public.mqtt_messages DETACH PARTITION {_q(job['source_name'])}")
        await conn.execute(f"ALTER TABLE {_q(job['source_name'])} RENAME TO {quote_identifier(retired)}")
        await conn.execute(f"DROP TRIGGER meshinfo_archive_stage_guard ON {_q(job['stage_name'])}")
        if job.get("target_method", "columnar") == "columnar":
            await _freeze(conn, job["stage_name"], job["job_id"])
        await conn.execute(f"ALTER TABLE public.mqtt_messages ATTACH PARTITION {_q(job['stage_name'])} "
            f"FOR VALUES FROM ('{job['bound_start'].isoformat()}') TO ('{job['bound_end'].isoformat()}')")
        await conn.execute(f"ALTER TABLE {_q(job['stage_name'])} RENAME TO {quote_identifier(job['source_name'])}")
        # No CASCADE: a dependency discovered at the last instant aborts this
        # entire transaction, including detach/attach/name changes.
        await conn.execute(f"DROP TABLE {_q(retired)}")
        comment = (f"meshinfo archived {job['job_id']}" if job.get("target_method", "columnar") == "columnar"
                   else job.get("original_comment"))
        comment_sql = await conn.fetchval("SELECT quote_nullable($1::text)", comment)
        await conn.execute(f"COMMENT ON TABLE {_q(job['source_name'])} IS {comment_sql}")
        if job.get("target_method", "columnar") == "columnar":
            await archive_topics.attach(conn,job)
        await conn.execute("""UPDATE public.storage_archive_jobs SET phase='complete',source_frozen=false,
            blocked_reason=NULL,updated_at=clock_timestamp(),finished_at=clock_timestamp() WHERE job_id=$1""", job["job_id"])


async def _cancel_locked(conn, job, policy):
    if job["phase"] == "failed" and not job["source_frozen"] and job["stage_oid"] is None:
        await conn.execute("UPDATE public.storage_archive_jobs SET phase='cancelled',updated_at=clock_timestamp() WHERE job_id=$1", job["job_id"])
        return {"status": "cancelled", "job_id": str(job["job_id"]), "source_preserved": True}
    if job["phase"] in ("complete", "cancelled", "failed"):
        return {"status": job["phase"], "job_id": str(job["job_id"])}
    async with conn.transaction():
        await _settings(conn, policy)
        await conn.execute(f"LOCK TABLE ONLY {_q(job['source_name'])}, {_q(job['stage_name'])} IN ACCESS EXCLUSIVE MODE NOWAIT")
        source = await conn.fetchrow("""SELECT c.oid,pg_get_expr(c.relpartbound,c.oid) AS bound,a.amname
            FROM pg_class c JOIN pg_am a ON a.oid=c.relam JOIN pg_inherits i ON i.inhrelid=c.oid
            WHERE c.oid=to_regclass($1) AND i.inhparent='public.mqtt_messages'::regclass""", _q(job["source_name"]))
        stage = await conn.fetchrow("""SELECT c.oid,obj_description(c.oid,'pg_class') AS comment,
            EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid) AS attached
            FROM pg_class c WHERE c.oid=to_regclass($1)""", _q(job["stage_name"]))
        if (not source or source["amname"] != job.get("source_method", "heap")
                or monthly_partition_bounds(source["bound"]) != (job["bound_start"],job["bound_end"])
                or not await _guarded(conn, source["oid"], job["job_id"])
                or not stage or stage["attached"] or stage["comment"] != (
                    f"meshinfo archive topics stage {job['job_id']}" if job.get("target_method") == "topics"
                    else f"meshinfo archive stage {job['job_id']}")):
            raise ArchiveBlocked("Cannot establish source/staging ownership for safe cancellation")
        # Cancellation retains the source and its external dependencies, so
        # source schema changes or a damaged staging index need not trap a job.
        # DROP still deliberately omits CASCADE for dependencies on staging.
        if job.get("topic_stage_name"):
            await archive_topics.drop_owned_stage(conn,job)
        if job.get("target_method") != "topics":
            await conn.execute(f"DROP TABLE {_q(job['stage_name'])}")
        await conn.execute(f"DROP TRIGGER meshinfo_archive_freeze ON {_q(job['source_name'])}")
        await conn.execute(f"DROP TRIGGER meshinfo_archive_freeze_truncate ON {_q(job['source_name'])}")
        if job.get("previous_guard_job") is not None:
            await _freeze(conn, job["source_name"], job["previous_guard_job"])
        await conn.execute("""UPDATE public.storage_archive_jobs SET phase='cancelled',source_frozen=false,
            stage_oid=NULL,topic_stage_oid=NULL,topic_stage_name=NULL,inflight_bytes=NULL,blocked_reason=NULL,updated_at=clock_timestamp(),
            finished_at=clock_timestamp() WHERE job_id=$1""", job["job_id"])
    return {"status": "cancelled", "job_id": str(job["job_id"]), "source_preserved": True}


async def _analyze_stage(conn, job, policy):
    """Commit statistics for at most one physical relation per bounded step."""
    separate_topic = bool(job.get("topic_stage_name") and job["topic_stage_name"] != job["stage_name"])
    stage_done = bool(job.get("stage_analyzed"))
    topic_done = bool(job.get("topic_analyzed")) or not separate_topic
    relation = job["stage_name"] if not stage_done else job.get("topic_stage_name") if not topic_done else None
    async with conn.transaction():
        await _settings(conn, policy)
        seconds = min(policy.analyze_seconds, policy.cycle_seconds-.5)
        await conn.execute(f"SET LOCAL statement_timeout='{math.ceil(seconds*1000)}ms'")
        if not stage_done and job.get("target_method", "columnar") == "heap":
            for index in await conn.fetch("""SELECT c.oid FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
                    JOIN pg_am a ON a.oid=c.relam WHERE i.indrelid=$1 AND a.amname='gin'""", job["stage_oid"]):
                await conn.fetchval("SELECT gin_clean_pending_list($1::oid::regclass)", index["oid"])
        if relation is not None:
            await conn.execute(f"ANALYZE {_q(relation)}",timeout=seconds+.25)
            if not stage_done:
                stage_done = True
            else:
                topic_done = True
        await conn.execute("""UPDATE public.storage_archive_jobs SET stage_analyzed=$2,topic_analyzed=$3,
            analyzed=($2 AND $3),blocked_reason=NULL,updated_at=clock_timestamp() WHERE job_id=$1""",
            job["job_id"],stage_done,topic_done)
    job.update(stage_analyzed=stage_done,topic_analyzed=topic_done,analyzed=stage_done and topic_done)


async def cancel_archive(conn, job, *, config=None):
    policy = archive_policy(config or {})
    job_id = uuid.UUID(str(job))
    async with maintenance_lock(conn) as acquired:
        if not acquired:
            return {"status": "busy"}
        return await _cancel_locked(conn, await _job(conn, job_id), policy)


async def start_restore_archive(conn, partition: str, config, *, free_bytes: Callable[[], int] | None = None):
    """Explicitly request reversible columnar-to-heap conversion of one leaf.

    This uses the same staging, full verification, source guards and atomic
    cutover as archiving. It requires space for the expanded heap and indexes
    while the original compressed source remains intact. No deletion or in-place
    rewrite is used to manufacture missing headroom. Pending restores continue
    through run_archive_cycle even when automatic archiving is disabled.
    """
    policy = archive_policy(config)
    if policy.enabled:
        return {"status": "blocked", "reason": "Disable automatic archiving before requesting heap restoration", "source_preserved": True}
    if conn.is_in_transaction():
        raise ValueError("Heap restoration requires a dedicated connection outside a transaction")
    async with maintenance_lock(conn) as acquired:
        if not acquired:
            return {"status": "busy"}
        try:
            async with asyncio.timeout(policy.cycle_seconds):
                if await conn.fetchval("SELECT EXISTS(SELECT 1 FROM public.storage_archive_jobs WHERE phase IN ('copying','verifying','ready'))"):
                    return {"status": "busy", "reason": "Another conversion job is active"}
                source = await conn.fetchrow(_LEAVES_SQL + " AND c.relname=$1", partition)
                if source is None or source["method"] != "columnar" or monthly_partition_bounds(source["bound"]) is None:
                    raise ArchiveBlocked("Heap restoration requires a monthly columnar MQTT partition")
                job_id = await _start(conn, source, policy, free_bytes, target_method="heap")
                return {"status": "copying", "job_id": str(job_id), "partition": partition, "target_method": "heap"}
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return {"status": "blocked", "reason": _reason(exc), "source_preserved": True}


async def run_archive_cycle(conn, config, *, free_bytes: Callable[[], int] | None = None):
    policy = archive_policy(config)
    if not policy.enabled:
        exists = await conn.fetchval("SELECT to_regclass('public.storage_archive_jobs') IS NOT NULL")
        if not exists or not await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM public.storage_archive_jobs
                WHERE phase IN ('copying','verifying','ready') AND target_method='heap')"""):
            return {"status": "disabled"}
    if conn.is_in_transaction():
        raise ValueError("Archiving requires a dedicated connection outside a transaction")
    async with maintenance_lock(conn) as acquired:
        if not acquired:
            return {"status": "busy"}
        job_id = None
        started = time.monotonic()
        try:
            async with asyncio.timeout(policy.cycle_seconds):
                await _environment(conn)
                _free(free_bytes, policy, 4*policy.batch_bytes)
                job_id = await conn.fetchval("SELECT job_id FROM public.storage_archive_jobs WHERE phase IN ('copying','verifying','ready') LIMIT 1")
                if job_id is None:
                    if not policy.enabled:
                        return {"status": "disabled"}
                    preview = await plan_archive(conn, config)
                    candidates = [row for row in preview["partitions"] if row["eligible"]]
                    if not candidates:
                        return {"status": "idle"}
                    job_id = await _start(conn, candidates[0], policy, free_bytes,
                                          target_method=candidates[0].get("target_method","columnar"))
                job = await _job(conn, job_id)
                async with conn.transaction():
                    await _settings(conn, policy)
                    await _validate_job(conn, job)
                    await _recover_inflight(conn, job, policy)
                if job["abort_count"] >= 3:
                    result = await _cancel_locked(conn, job, policy)
                    result["reason"] = "Three interrupted copy batches; staging reclaimed and original source preserved"
                    if job.get("blocked_reason"):
                        result["reason"] += ". Last copy refusal: "+job["blocked_reason"]
                    await conn.execute("UPDATE public.storage_archive_jobs SET phase='failed',blocked_reason=$2 WHERE job_id=$1",
                                       job["job_id"], result["reason"])
                    result["status"] = "failed"
                    return result
                wal_start = await conn.fetchval("SELECT pg_current_wal_insert_lsn()::text")
                while time.monotonic()-started < policy.cycle_seconds-policy.statement_seconds:
                    job = await _job(conn, job_id)
                    wal = int(await conn.fetchval("SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::text::pg_lsn)", wal_start))
                    if wal >= policy.max_wal_bytes:
                        break
                    if job["phase"] == "copying":
                        await _copy_chunk(conn, job, policy, free_bytes)
                    elif job["phase"] == "verifying":
                        await _verify_chunk(conn, job, policy)
                    elif job["phase"] == "ready":
                        if not job["analyzed"]:
                            # Start this independent read-only statistics step
                            # in a fresh cycle when little wall time remains.
                            if time.monotonic()-started > max(.5, policy.cycle_seconds-policy.analyze_seconds):
                                break
                            await _analyze_stage(conn, job, policy)
                        else:
                            await _cutover(conn, job, policy, free_bytes)
                    else:
                        break
                final = await _job(conn, job_id)
                return {"status": final["phase"], "job_id": str(job_id), "partition": final["source_name"],
                        "target_method": final["target_method"],
                        "rows_copied": final["rows_copied"], "rows_verified": final["rows_verified"],
                        "elapsed_seconds": round(time.monotonic()-started, 3)}
        except asyncio.CancelledError:
            # The committed in-flight marker distinguishes a cancelled chunk
            # from a committed checkpoint after reconnect. Never drop source.
            raise
        except Exception as exc:
            reason = _reason(exc)
            if job_id is not None and not conn.is_closed():
                await conn.execute("""UPDATE public.storage_archive_jobs SET blocked_reason=$2,
                    updated_at=clock_timestamp() WHERE job_id=$1 AND phase IN ('copying','verifying','ready')""",
                    job_id, reason, timeout=2)
            return {"status": "blocked", "job_id": str(job_id) if job_id else None, "reason": reason,
                    "source_preserved": True}
