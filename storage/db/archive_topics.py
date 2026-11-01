"""Verified heap topic lookup for immutable Columnar packet partitions.

An attached leaf is a completeness promise: its exact canonical topic/key
triples were independently verified before attachment in the same transaction
as the canonical archive cutover. Reception topics are deliberately unrelated.
"""
from __future__ import annotations

import json
import math
import re
import uuid

from storage.db.compression import MIB, quote_identifier
from storage.db.storage_report import monthly_partition_bounds

PARENT = "mqtt_archive_topics"
COLUMNS = ("id", "created_at", "topic")
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS public.mqtt_archive_topics (
    id BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL, topic TEXT,
    PRIMARY KEY(id,created_at)
) PARTITION BY RANGE(created_at);
CREATE INDEX IF NOT EXISTS mqtt_archive_topics_topic_idx
    ON public.mqtt_archive_topics USING gin(topic gin_trgm_ops) WITH(fastupdate=off);
CREATE INDEX IF NOT EXISTS mqtt_archive_topics_created_idx
    ON public.mqtt_archive_topics(created_at DESC,id DESC);
"""


def engine():
    from storage.db import archive
    return archive


def q(name):
    return '"public".' + quote_identifier(name)


def leaf_name(start):
    return f"mqtt_archive_topics_{start:%Y_%m}"


async def signature(conn, oid):
    columns = [dict(row) for row in await conn.fetch("""SELECT attname,format_type(atttypid,atttypmod) AS type,
        attnotnull,attidentity::text,attgenerated::text,attcollation::regcollation::text AS collation,
        attacl::text,pg_get_expr(d.adbin,d.adrelid) AS default_expression
        FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
        WHERE attrelid=$1 AND attnum>0 AND NOT attisdropped ORDER BY attnum""", oid)]
    indexes = await engine()._indexes(conn, oid)
    if any(not row["indisvalid"] or not row["indisready"] or row["reltablespace"] for row in indexes):
        raise engine().ArchiveBlocked("Topic lookup has invalid or custom-tablespace indexes")
    return {"columns": columns, "indexes": sorted(engine()._index_signature(row) for row in indexes),
            "constraints": await engine()._constraints(conn, oid)}


async def validate_parent(conn):
    row = await conn.fetchrow("""SELECT c.oid,c.relkind::text,c.relrowsecurity,c.relforcerowsecurity,c.relhasrules,
        c.reltablespace,c.relacl::text,pg_get_partkeydef(c.oid) AS key FROM pg_class c
        WHERE c.oid=to_regclass('public.mqtt_archive_topics')""")
    if (not row or row["relkind"] != "p" or row["key"] != "RANGE (created_at)" or
            any(row[key] for key in ("relrowsecurity","relforcerowsecurity","relhasrules","reltablespace","relacl"))):
        raise engine().ArchiveBlocked("Canonical topic lookup parent has unsupported topology or policies")
    await validate_columns(conn, row["oid"])
    if await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM pg_constraint
        WHERE (contype='f' AND (conrelid=$1 OR confrelid=$1))
           OR (conrelid=$1 AND (contype NOT IN ('p','n') OR NOT convalidated OR condeferrable OR condeferred)))""",row["oid"]):
        raise engine().ArchiveBlocked("Canonical topic lookup parent has unsupported constraints or foreign keys")
    if await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=$1 AND NOT tgisinternal)", row["oid"]):
        raise engine().ArchiveBlocked("Canonical topic lookup parent has custom triggers")
    await validate_indexes(conn,row["oid"])


async def validate_indexes(conn,oid):
    expected = sorted([("btree", True, True, "btree (id, created_at)"),
                       ("btree", False, False, "btree (created_at DESC, id DESC)"),
                       ("gin", False, False, "gin (topic gin_trgm_ops)")])
    indexes = await engine()._indexes(conn,oid)
    actual = []
    for index in indexes:
        method,primary,unique,tail = engine()._index_signature(index)
        actual.append((method,primary,unique,tail.split(" WITH ",1)[0]))
    gin_options_ok = await conn.fetchval("""SELECT coalesce(bool_and('fastupdate=off'=ANY(c.reloptions)),false)
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam
        WHERE i.indrelid=$1 AND a.amname='gin'""",oid)
    if sorted(actual) != expected or not gin_options_ok or any(
            not i["indisvalid"] or not i["indisready"] or i["reltablespace"] for i in indexes):
        raise engine().ArchiveBlocked("Canonical topic lookup requires valid primary, ordered and bounded-GIN indexes")


async def validate_columns(conn, oid):
    rows = await conn.fetch("""SELECT attname,format_type(atttypid,atttypmod) AS type,attnotnull,
        attidentity::text,attgenerated::text,attacl::text,
        EXISTS(SELECT 1 FROM pg_attrdef d WHERE d.adrelid=a.attrelid AND d.adnum=a.attnum) AS has_default,
        attcollation FROM pg_attribute a WHERE attrelid=$1 AND attnum>0 AND NOT attisdropped ORDER BY attnum""", oid)
    if ([(r["attname"],r["type"],r["attnotnull"]) for r in rows] !=
            [("id","bigint",True),("created_at","timestamp with time zone",True),("topic","text",False)] or
            any(r["attidentity"] or r["attgenerated"] or r["attacl"] or r["has_default"] for r in rows)):
        raise engine().ArchiveBlocked("Canonical topic lookup requires exact nullable-topic/key columns")
    canonical_collation = await conn.fetchval("""SELECT attcollation FROM pg_attribute
        WHERE attrelid=to_regclass('public.mqtt_messages') AND attname='topic' AND NOT attisdropped""")
    if canonical_collation is not None and rows[2]["attcollation"] != canonical_collation:
        raise engine().ArchiveBlocked("Canonical topic lookup collation differs from packet topic semantics")


async def attached_partition(conn, bound_start, bound_end, *, validate=True):
    """Return the owned attached lookup for a canonical month, or None.

Callers hold the appropriate parent locks across this lookup and any detach or
drop. Neither timestamps nor stale journal OIDs alone establish ownership.
"""
    rows = await conn.fetch("""SELECT c.oid,c.relname AS name,pg_get_expr(c.relpartbound,c.oid) AS bound,
        a.amname,c.relrowsecurity,c.relforcerowsecurity,c.relhasrules,c.reltablespace,c.relacl::text,
        obj_description(c.oid,'pg_class') AS comment,pg_get_userbyid(c.relowner) AS owner,
        pg_total_relation_size(c.oid)::bigint AS total_bytes
        FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid JOIN pg_am a ON a.oid=c.relam
        WHERE i.inhparent=to_regclass('public.mqtt_archive_topics')""")
    matches = [dict(row) for row in rows if monthly_partition_bounds(row["bound"]) == (bound_start,bound_end)]
    if len(matches) > 1:
        raise engine().ArchiveBlocked("Ambiguous topic lookup partition coverage")
    if not matches:
        return None
    row = matches[0]
    if not validate:
        return row
    await validate_parent(conn)
    marker = re.fullmatch(r"meshinfo archive topics ([a-f0-9-]{36})", row["comment"] or "")
    if (row["name"] != leaf_name(bound_start) or row["amname"] != "heap" or not marker or
            any(row[key] for key in ("relrowsecurity","relforcerowsecurity","relhasrules","reltablespace","relacl"))):
        raise engine().ArchiveBlocked("Attached topic lookup identity or ownership marker is invalid")
    guard_job = uuid.UUID(marker.group(1))
    await validate_columns(conn, row["oid"])
    if not await engine()._guarded(conn, row["oid"], guard_job):
        raise engine().ArchiveBlocked("Attached topic lookup immutability guards changed")
    source_owner = await conn.fetchval("""SELECT pg_get_userbyid(c.relowner) FROM pg_class c
        JOIN pg_inherits i ON i.inhrelid=c.oid WHERE i.inhparent='public.mqtt_messages'::regclass
        AND pg_get_expr(c.relpartbound,c.oid)=$1""",row["bound"])
    if source_owner is None or source_owner != row["owner"]:
        raise engine().ArchiveBlocked("Topic lookup owner differs from its canonical partition")
    if await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=$1 AND NOT tgisinternal
        AND tgname NOT IN ('meshinfo_archive_freeze','meshinfo_archive_freeze_truncate'))""", row["oid"]):
        raise engine().ArchiveBlocked("Attached topic lookup has unexpected triggers")
    await validate_indexes(conn,row["oid"])
    row["guard_job"] = guard_job
    return row


async def create_stage(conn, name, job_id, bounds, owner):
    await validate_parent(conn)
    await conn.execute(f"CREATE TABLE {q(name)} (LIKE public.mqtt_archive_topics INCLUDING ALL) USING heap")
    await conn.execute(f"ALTER TABLE {q(name)} OWNER TO {quote_identifier(owner)}")
    await conn.execute(f"ALTER TABLE {q(name)} ADD CONSTRAINT {quote_identifier('meshinfo_topics_bound_'+job_id.hex[:16])} "
                      f"CHECK(created_at >= '{bounds[0].isoformat()}'::timestamptz AND created_at < '{bounds[1].isoformat()}')")
    await conn.execute(f"COMMENT ON TABLE {q(name)} IS 'meshinfo archive topics stage {job_id}'")
    await conn.execute(f"CREATE TRIGGER meshinfo_archive_stage_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE "
                      f"ON {q(name)} FOR EACH STATEMENT EXECUTE FUNCTION public.meshinfo_archive_stage_guard('{job_id}')")
    await conn.execute(f"ALTER TABLE {q(name)} ENABLE ALWAYS TRIGGER meshinfo_archive_stage_guard")
    oid = await conn.fetchval("SELECT $1::regclass::oid", q(name))
    return oid, await signature(conn, oid)


async def validate_stage(conn, job):
    name = job.get("topic_stage_name")
    if not name:
        raise engine().ArchiveBlocked("Archive job lacks its canonical topic lookup staging checkpoint")
    row = await conn.fetchrow("""SELECT c.oid,a.amname,c.reltablespace,c.relrowsecurity,c.relforcerowsecurity,
        c.relhasrules,c.relacl::text,pg_get_userbyid(c.relowner) AS owner,
        obj_description(c.oid,'pg_class') AS comment,
        EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid) AS attached
        FROM pg_class c JOIN pg_am a ON a.oid=c.relam WHERE c.oid=to_regclass($1)""", q(name))
    if (not row or row["amname"] != "heap" or row["attached"] or
            row["owner"] != engine()._json(job["schema_fingerprint"])["relation"]["owner"] or
            row["comment"] != f"meshinfo archive topics stage {job['job_id']}" or
            any(row[key] for key in ("reltablespace","relrowsecurity","relforcerowsecurity","relhasrules","relacl"))):
        raise engine().ArchiveBlocked("Topic staging identity, ownership or detached heap method changed")
    guards = await conn.fetch("""SELECT tgname,tgenabled::text,tgargs,tgtype,tgfoid
        FROM pg_trigger WHERE tgrelid=$1 AND NOT tgisinternal""", row["oid"])
    expected_function = await conn.fetchval("SELECT 'public.meshinfo_archive_stage_guard()'::regprocedure::oid")
    if len(guards) != 1 or any(g["tgname"] != "meshinfo_archive_stage_guard" or g["tgenabled"] != "A" or
            g["tgtype"] != 62 or g["tgfoid"] != expected_function or
            bytes(g["tgargs"]) != str(job["job_id"]).encode()+b"\0" for g in guards):
        raise engine().ArchiveBlocked("Topic staging write guard changed")
    actual = json.loads(json.dumps(await signature(conn, row["oid"])))
    if actual != engine()._json(job.get("topic_stage_fingerprint", "{}")):
        raise engine().ArchiveBlocked("Topic staging columns, defaults, constraints or indexes changed")
    if row["oid"] != job.get("topic_stage_oid"):
        await conn.execute("UPDATE public.storage_archive_jobs SET topic_stage_oid=$2 WHERE job_id=$1", job["job_id"], row["oid"])
        job["topic_stage_oid"] = row["oid"]
    return row["oid"]


async def estimate(conn, source, policy):
    """Measure actual heap/index pilot bytes; live per-chunk guards remain final."""
    name = "meshinfo_topics_pilot_"+uuid.uuid4().hex[:20]
    await validate_parent(conn)
    await conn.execute(f"CREATE TABLE {q(name)} (LIKE public.mqtt_archive_topics INCLUDING ALL) USING heap")
    sampling = "TABLESAMPLE SYSTEM (1) REPEATABLE (731)" if source["method"] == "heap" else ""
    await conn.execute(f"""WITH sample AS MATERIALIZED (
        SELECT id,created_at,128+coalesce(octet_length(topic),0) AS n,
          128*(1+coalesce(octet_length(topic),0)::bigint) AS index_work FROM {q(source['name'])} {sampling}
        WHERE coalesce(octet_length(topic),0)<=$1 ORDER BY id,created_at LIMIT 1000),
        bounded AS (SELECT id,created_at,sum(n) OVER(ORDER BY id,created_at) AS bytes,
          sum(index_work) OVER(ORDER BY id,created_at) AS index_work FROM sample)
        INSERT INTO {q(name)} SELECT s.id,s.created_at,s.topic FROM {q(source['name'])} s
        JOIN bounded b USING(id,created_at) WHERE b.bytes<=$1 AND b.index_work<=$2""",
        policy.batch_bytes,policy.max_wal_bytes//4)
    count = await conn.fetchval(f"SELECT count(*) FROM {q(name)}")
    if not count:
        await conn.execute(f"INSERT INTO {q(name)} SELECT id,created_at,topic FROM {q(source['name'])} "
                           "WHERE coalesce(octet_length(topic),0)<=$1 ORDER BY id,created_at LIMIT 1",
                           min(policy.batch_bytes,policy.max_wal_bytes//(4*128)-1))
        count = await conn.fetchval(f"SELECT count(*) FROM {q(name)}")
        if not count and await conn.fetchval(f"SELECT EXISTS(SELECT 1 FROM {q(source['name'])})"):
            raise engine().ArchiveBlocked("No canonical topic fits the bounded archive GIN/WAL pilot budget")
    measured = await engine()._size(conn, name)
    estimated_rows = await conn.fetchval("SELECT reltuples::float8 FROM pg_class WHERE oid=$1", source["oid"])
    estimated_rows = max(count, estimated_rows if estimated_rows > 0 else source["table_bytes"]//64)
    await conn.execute(f"DROP TABLE {q(name)}")
    return math.ceil(estimated_rows*max(128, measured/max(1,count))*policy.estimate_multiplier+16*MIB)


async def copy_chunk(conn, job, last, count):
    command = await conn.execute(f"""INSERT INTO {q(job['topic_stage_name'])}(id,created_at,topic)
        SELECT id,created_at,topic FROM {q(job['source_name'])}
        WHERE ($1::bigint IS NULL OR (id,created_at)>($1,$2::timestamptz))
          AND (id,created_at)<=($3,$4) ORDER BY id,created_at""",
        job["last_id"],job["last_created_at"],last["id"],last["created_at"])
    if command != f"INSERT 0 {count}":
        raise engine().ArchiveBlocked("Frozen canonical topics changed during a copy chunk")


async def verify_chunk(conn, job, selected):
    if not selected:
        extra = await conn.fetchval(f"SELECT EXISTS(SELECT 1 FROM {q(job['topic_stage_name'])} "
            "WHERE ($1::bigint IS NULL OR (id,created_at)>($1,$2::timestamptz)))",
            job["verify_id"],job["verify_created_at"])
        if extra:
            raise engine().ArchiveBlocked("Topic lookup cardinality verification failed")
        return
    last = selected[-1]
    good = await conn.fetchval(f"""WITH s AS MATERIALIZED (
        SELECT id,created_at,topic FROM {q(job['source_name'])}
        WHERE ($1::bigint IS NULL OR (id,created_at)>($1,$2::timestamptz)) AND (id,created_at)<=($3,$4)),
        t AS MATERIALIZED (SELECT id,created_at,topic FROM {q(job['topic_stage_name'])}
        WHERE ($1::bigint IS NULL OR (id,created_at)>($1,$2::timestamptz)) AND (id,created_at)<=($3,$4) LIMIT $5)
        SELECT count(*)=$6 AND coalesce(bool_and(s.id IS NOT NULL AND t.id IS NOT NULL
          AND convert_to(s.topic,'UTF8') IS NOT DISTINCT FROM convert_to(t.topic,'UTF8')),false)
        FROM s FULL JOIN t USING(id,created_at)""", job["verify_id"],job["verify_created_at"],
        last["id"],last["created_at"],len(selected)+1,len(selected))
    if not good:
        raise engine().ArchiveBlocked("Topic lookup exact row-by-row verification failed; source remains intact")


async def attach(conn, job):
    await validate_parent(conn)
    await validate_stage(conn, job)
    if await attached_partition(conn,job["bound_start"],job["bound_end"]) is not None:
        raise engine().ArchiveBlocked("Canonical topic lookup coverage already exists")
    name = job["topic_stage_name"]
    final = leaf_name(job["bound_start"])
    await conn.execute(f"DROP TRIGGER meshinfo_archive_stage_guard ON {q(name)}")
    await engine()._freeze(conn,name,job["job_id"])
    await conn.execute(f"ALTER TABLE public.mqtt_archive_topics ATTACH PARTITION {q(name)} "
        f"FOR VALUES FROM ('{job['bound_start'].isoformat()}') TO ('{job['bound_end'].isoformat()}')")
    await conn.execute(f"ALTER TABLE {q(name)} RENAME TO {quote_identifier(final)}")
    await conn.execute(f"COMMENT ON TABLE {q(final)} IS 'meshinfo archive topics {job['job_id']}'")
    await conn.execute("UPDATE public.storage_archive_jobs SET topic_stage_name=$2 WHERE job_id=$1",job["job_id"],final)


async def drop_owned_stage(conn, job):
    name = job.get("topic_stage_name")
    if not name:
        return
    row = await conn.fetchrow("""SELECT c.oid,obj_description(c.oid,'pg_class') AS comment,
        EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid) AS attached
        FROM pg_class c WHERE c.oid=to_regclass($1)""", q(name))
    if not row or row["attached"] or row["comment"] != f"meshinfo archive topics stage {job['job_id']}":
        raise engine().ArchiveBlocked("Cannot establish topic staging ownership for cancellation")
    await conn.execute(f"DROP TABLE {q(name)}")
