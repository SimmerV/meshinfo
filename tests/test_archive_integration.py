"""Actual columnar archive transactions; only a dedicated *_archive_test DB.

Set MESHINFO_ARCHIVE_TEST_DSN to an otherwise empty isolated database with the
standalone citus_columnar extension available. No production DSN is accepted.
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import os
import re
import hashlib
import json
from pathlib import Path
import shutil
import uuid
from urllib.parse import unquote, urlsplit, urlunsplit

import asyncpg
import pytest

from storage.db import archive


DSN = os.environ.get("MESHINFO_ARCHIVE_TEST_DSN", "")
pytestmark = pytest.mark.skipif(not DSN, reason="MESHINFO_ARCHIVE_TEST_DSN not set")
STAMP = datetime(1901, 1, 2, tzinfo=timezone.utc)
POLICY = {"storage": {"archive": {"enabled": True, "after_months": 1, "batch_rows": 3,
          "batch_mib": 1, "cycle_seconds": 4, "statement_seconds": 1}}}
RESTORE_POLICY = {"storage": {"archive": {**POLICY["storage"]["archive"], "enabled": False}}}
FREE = lambda: 20*1024**3


@asynccontextmanager
async def fixture():
    database = unquote(urlsplit(DSN).path.lstrip("/"))
    assert re.fullmatch(r"[A-Za-z0-9_]+_archive_test", database), "Dedicated archive test database required"
    conn = await asyncpg.connect(DSN, command_timeout=10)
    assert await conn.fetchval("SELECT current_database()") == database
    assert await conn.fetchval("SELECT to_regclass('public.mqtt_messages')") is None, "Archive fixture requires its own empty database"
    try:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS citus_columnar; CREATE EXTENSION IF NOT EXISTS pg_trgm")
        await archive.ensure_archive_schema(conn)
        await conn.execute("""CREATE TABLE public.mqtt_messages (
            id BIGSERIAL,topic TEXT,payload TEXT COMPRESSION lz4,qos INTEGER,retain BOOLEAN,
            timestamp BIGINT,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            from_node_id VARCHAR(8),to_node_id VARCHAR(8),packet_id BIGINT,
            PRIMARY KEY(id,created_at)) PARTITION BY RANGE(created_at);
            CREATE TABLE public.mqtt_messages_1901_01 PARTITION OF public.mqtt_messages
              FOR VALUES FROM ('1901-01-01') TO ('1901-02-01');
            CREATE TABLE public.mqtt_messages_default PARTITION OF public.mqtt_messages DEFAULT;
            CREATE INDEX mqtt_from ON public.mqtt_messages(from_node_id,created_at DESC);
            CREATE INDEX mqtt_packet ON public.mqtt_messages(from_node_id,packet_id,created_at DESC) WHERE packet_id IS NOT NULL;
            CREATE INDEX mqtt_backfill ON public.mqtt_messages(id) WHERE from_node_id IS NULL;
            CREATE INDEX mqtt_topic_leaf ON public.mqtt_messages_1901_01 USING gin(topic gin_trgm_ops)""")
        hot_start = datetime.now(timezone.utc).replace(day=1,hour=0,minute=0,second=0,microsecond=0)
        hot_end = hot_start.replace(year=hot_start.year+1,month=1) if hot_start.month == 12 else hot_start.replace(month=hot_start.month+1)
        await conn.execute(f"CREATE TABLE public.mqtt_messages_hot PARTITION OF public.mqtt_messages "
                           f"FOR VALUES FROM ('{hot_start.isoformat()}') TO ('{hot_end.isoformat()}')")
        rows = [(index+1, "msh/test/%_\\literal", (' {"text":"雪 🛰️", "n":null} \n' if index % 4 else None),
                 index % 3, bool(index % 2), index, STAMP, "00000001" if index % 3 else None,
                 "ffffffff", index if index % 2 else None) for index in range(33)]
        await conn.copy_records_to_table("mqtt_messages", schema_name="public", columns=archive.MQTT_COLUMNS, records=rows)
        # Use the real extraction trigger, after loading source-exact NULLs.
        from scripts.sample_columnar_history import extract_trigger
        await conn.execute(extract_trigger().decode())
        expected = [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")]
        yield conn, expected
    finally:
        # This database is dedicated and all named objects were created above.
        for job in await conn.fetch("SELECT stage_name,topic_stage_name FROM public.storage_archive_jobs"):
            for name in {job["stage_name"],job["topic_stage_name"]}-{None}:
                await conn.execute(f'DROP TABLE IF EXISTS public."{name}"')
        await conn.execute("DROP TABLE IF EXISTS public.mqtt_messages; DROP TABLE IF EXISTS public.mqtt_archive_topics; DROP TABLE IF EXISTS public.storage_archive_jobs")
        await conn.close()


async def until_done(conn, config=POLICY):
    result = None
    for _ in range(30):
        result = await archive.run_archive_cycle(conn, config, free_bytes=FREE)
        if result["status"] == "complete":
            return result
        assert result["status"] in ("copying", "verifying", "ready"), result
    raise AssertionError(f"Archive did not finish: {result}")


async def pause_after_first_checkpoint(conn, monkeypatch):
    original = archive._copy_chunk

    async def stop(*arguments):
        await original(*arguments)
        raise asyncio.CancelledError

    monkeypatch.setattr(archive, "_copy_chunk", stop)
    try:
        with pytest.raises(asyncio.CancelledError):
            await archive.run_archive_cycle(conn, POLICY, free_bytes=FREE)
    finally:
        monkeypatch.setattr(archive, "_copy_chunk", original)
    return dict(await conn.fetchrow("SELECT * FROM public.storage_archive_jobs WHERE phase='copying'"))


async def create_paused_job(conn):
    """Real committed checkpoint for backup/restore tests, while fixture lives."""
    assert re.fullmatch(r"[A-Za-z0-9_]+_archive_test", await conn.fetchval("SELECT current_database()"))
    policy = archive.archive_policy(POLICY)
    plan = await archive.plan_archive(conn, POLICY)
    source = next(row for row in plan["partitions"] if row["eligible"])
    async with archive.maintenance_lock(conn) as acquired:
        assert acquired
        job_id = await archive._start(conn, source, policy, FREE)
        job = await archive._job(conn, job_id)
        await archive._copy_chunk(conn, job, policy, FREE)
    return await archive._job(conn, job_id)


def test_cancel_resume_guard_and_exact_atomic_cutover(monkeypatch):
    async def scenario():
        async with fixture() as (conn, expected):
            original = archive._copy_chunk

            async def stop_after_checkpoint(*arguments):
                await original(*arguments)
                raise asyncio.CancelledError

            monkeypatch.setattr(archive, "_copy_chunk", stop_after_checkpoint)
            with pytest.raises(asyncio.CancelledError):
                await archive.run_archive_cycle(conn, POLICY, free_bytes=FREE)
            job = await conn.fetchrow("SELECT * FROM public.storage_archive_jobs WHERE phase='copying'")
            assert job and job["rows_copied"] == 3 and job["inflight_bytes"] is None
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
            for query in ("INSERT INTO public.mqtt_messages(id,created_at) VALUES(999,'1901-01-03')",
                          "UPDATE public.mqtt_messages SET topic='changed' WHERE id=1",
                          "DELETE FROM public.mqtt_messages_1901_01 WHERE id=1",
                          "TRUNCATE public.mqtt_messages_1901_01"):
                with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
                    await conn.execute(query)
            monkeypatch.setattr(archive, "_copy_chunk", original)
            result = await until_done(conn)
            assert result["rows_copied"] == result["rows_verified"] == len(expected)
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
            assert await conn.fetchval("SELECT a.amname FROM pg_class c JOIN pg_am a ON a.oid=c.relam WHERE c.oid='public.mqtt_messages_1901_01'::regclass") == "columnar"
            assert not await conn.fetchval("SELECT source_frozen FROM public.storage_archive_jobs WHERE job_id=$1", job["job_id"])
    asyncio.run(scenario())


def test_explicit_cancellation_reclaims_only_staging_and_unfreezes_source(monkeypatch):
    async def scenario():
        async with fixture() as (conn, expected):
            original = archive._copy_chunk

            async def stop_after_checkpoint(*arguments):
                await original(*arguments)
                raise asyncio.CancelledError

            monkeypatch.setattr(archive, "_copy_chunk", stop_after_checkpoint)
            with pytest.raises(asyncio.CancelledError):
                await archive.run_archive_cycle(conn, POLICY, free_bytes=FREE)
            job = await conn.fetchrow("SELECT * FROM public.storage_archive_jobs WHERE phase='copying'")
            result = await archive.cancel_archive(conn, job["job_id"], config=POLICY)
            assert result["status"] == "cancelled" and result["source_preserved"]
            assert await conn.fetchval("SELECT to_regclass($1)", "public."+job["stage_name"]) is None
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
            assert await conn.execute("UPDATE public.mqtt_messages_1901_01 SET qos=qos WHERE id=1") == "UPDATE 1"
    asyncio.run(scenario())


def test_source_dependencies_and_insufficient_space_are_safe_refusals():
    async def scenario():
        async with fixture() as (conn, expected):
            result = await archive.run_archive_cycle(conn, POLICY, free_bytes=lambda: 1024)
            assert result["status"] == "blocked" and result["source_preserved"]
            assert await conn.fetchval("SELECT count(*) FROM public.storage_archive_jobs") == 0
            await conn.execute("CREATE VIEW public.archive_dependency AS SELECT id FROM public.mqtt_messages_1901_01")
            try:
                result = await archive.run_archive_cycle(conn, POLICY, free_bytes=FREE)
                assert result["status"] == "blocked" and "dependencies" in result["reason"]
                assert await conn.fetchval("SELECT count(*) FROM public.storage_archive_jobs") == 0
                assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
            finally:
                await conn.execute("DROP VIEW public.archive_dependency")
    asyncio.run(scenario())


def test_aborted_columnar_chunk_recovers_durable_marker_without_extra_rows(monkeypatch):
    async def scenario():
        async with fixture() as (conn, expected):
            job = await pause_after_first_checkpoint(conn, monkeypatch)
            before = await archive._size(conn, job["stage_name"])
            await conn.execute("UPDATE public.storage_archive_jobs SET inflight_bytes=$2 WHERE job_id=$1", job["job_id"], before)
            transaction = conn.transaction()
            await transaction.start()
            await conn.execute("SELECT set_config('meshinfo.archive_job',$1,true)", str(job["job_id"]))
            await conn.execute(f"INSERT INTO {archive._q(job['stage_name'])}(id,created_at,payload) VALUES(999,$1,'aborted')", STAMP)
            assert await conn.fetchval(f"SELECT count(*) FROM {archive._q(job['stage_name'])}") == 4
            await transaction.rollback()
            await until_done(conn)
            recovered = await conn.fetchrow("SELECT abort_count,aborted_bytes,inflight_bytes FROM public.storage_archive_jobs WHERE job_id=$1", job["job_id"])
            assert recovered["abort_count"] == 1 and recovered["aborted_bytes"] >= 0 and recovered["inflight_bytes"] is None
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
    asyncio.run(scenario())


def test_repeated_interruptions_reclaim_stage_and_require_explicit_retry(monkeypatch):
    async def scenario():
        async with fixture() as (conn, expected):
            job = await pause_after_first_checkpoint(conn, monkeypatch)
            await conn.execute("UPDATE public.storage_archive_jobs SET abort_count=2,inflight_bytes=0 WHERE job_id=$1", job["job_id"])
            result = await archive.run_archive_cycle(conn, POLICY, free_bytes=FREE)
            assert result["status"] == "failed"
            assert await conn.fetchval("SELECT to_regclass($1)", "public."+job["stage_name"]) is None
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
            assert await conn.execute("UPDATE public.mqtt_messages_1901_01 SET qos=qos WHERE id=1") == "UPDATE 1"
            planned = await archive.plan_archive(conn, POLICY)
            assert next(row for row in planned["partitions"] if row["name"] == job["source_name"])["reason"] == "previous_attempt_failed_cancel_job_to_retry"
            assert (await archive.cancel_archive(conn, job["job_id"], config=POLICY))["status"] == "cancelled"
            planned = await archive.plan_archive(conn, POLICY)
            assert next(row for row in planned["partitions"] if row["name"] == job["source_name"])["eligible"]
    asyncio.run(scenario())


def test_extra_staging_row_and_disabled_source_guard_prevent_cutover(monkeypatch):
    async def scenario():
        async with fixture() as (conn, expected):
            job = await pause_after_first_checkpoint(conn, monkeypatch)
            await conn.execute("ALTER TABLE public.mqtt_messages_1901_01 DISABLE TRIGGER meshinfo_archive_freeze")
            result = await archive.run_archive_cycle(conn, POLICY, free_bytes=FREE)
            assert result["status"] == "blocked" and "freeze guards" in result["reason"]
            await conn.execute("ALTER TABLE public.mqtt_messages_1901_01 ENABLE ALWAYS TRIGGER meshinfo_archive_freeze")
            async with conn.transaction():
                await conn.execute("SELECT set_config('meshinfo.archive_job',$1,true)", str(job["job_id"]))
                await conn.execute(f"INSERT INTO {archive._q(job['stage_name'])}(id,created_at,payload) VALUES(999,$1,'extra')", STAMP)
            for _ in range(30):
                result = await archive.run_archive_cycle(conn, POLICY, free_bytes=FREE)
                if result["status"] == "blocked":
                    break
            assert result["status"] == "blocked" and "cardinality" in result["reason"]
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
            assert (await archive.cancel_archive(conn, job["job_id"], config=POLICY))["status"] == "cancelled"
    asyncio.run(scenario())


def test_cutover_lock_refusal_preserves_parent_and_hot_writes(monkeypatch):
    async def scenario():
        async with fixture() as (conn, expected):
            job = await pause_after_first_checkpoint(conn, monkeypatch)
            reader = await asyncpg.connect(DSN)
            try:
                await reader.execute("INSERT INTO public.mqtt_messages(id,created_at,payload) VALUES(888,now(),'hot writer')")
                tx = reader.transaction()
                await tx.start()
                assert await reader.fetchval("SELECT count(*) FROM public.mqtt_messages") == len(expected)+1
                for _ in range(30):
                    result = await archive.run_archive_cycle(conn, POLICY, free_bytes=FREE)
                    if result["status"] == "blocked":
                        break
                assert result["status"] == "blocked" and "55P03" in result["reason"]
                state = await conn.fetchrow("SELECT phase,analyzed FROM public.storage_archive_jobs WHERE job_id=$1", job["job_id"])
                assert state["phase"] == "ready" and state["analyzed"]
                assert await reader.fetchval("SELECT count(*) FROM public.mqtt_messages") == len(expected)+1
                await tx.rollback()
                await until_done(conn)
                assert await conn.fetchval("SELECT payload FROM public.mqtt_messages WHERE id=888") == "hot writer"
                assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages_1901_01 ORDER BY id,created_at")] == expected
            finally:
                await reader.close()
    asyncio.run(scenario())


def test_restore_oid_rebinding_and_low_space_or_wal_pause_do_not_lose_checkpoint(monkeypatch):
    async def scenario():
        async with fixture() as (conn, expected):
            job = await pause_after_first_checkpoint(conn, monkeypatch)
            await conn.execute("UPDATE public.storage_archive_jobs SET source_oid=1,stage_oid=2 WHERE job_id=$1", job["job_id"])

            class BusyWAL:
                def __getattr__(self, name):
                    return getattr(conn, name)

                async def fetchval(self, query, *args, **kwargs):
                    if "pg_wal_lsn_diff" in query:
                        return 1024**3
                    return await conn.fetchval(query, *args, **kwargs)

            result = await archive.run_archive_cycle(BusyWAL(), POLICY, free_bytes=FREE)
            assert result["status"] == "copying" and result["rows_copied"] == 3
            state = await conn.fetchrow("SELECT source_oid,stage_oid,rows_copied FROM public.storage_archive_jobs WHERE job_id=$1", job["job_id"])
            assert state["source_oid"] == job["source_oid"] and state["stage_oid"] == job["stage_oid"]
            result = await archive.run_archive_cycle(conn, POLICY, free_bytes=lambda: 1024)
            assert result["status"] == "blocked"
            assert await conn.fetchval("SELECT rows_copied FROM public.storage_archive_jobs WHERE job_id=$1", job["job_id"]) == 3
            await until_done(conn)
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
    asyncio.run(scenario())


def test_finite_packet_retention_takes_precedence_over_archiving():
    async def scenario():
        async with fixture() as (conn, _expected):
            config = {"storage": {"archive": POLICY["storage"]["archive"], "retention": {"enabled": True, "months": 1}}}
            plan = await archive.plan_archive(conn, config)
            source = next(row for row in plan["partitions"] if row["name"] == "mqtt_messages_1901_01")
            assert not source["eligible"] and source["reason"] == "awaiting_retention"
            assert (await archive.run_archive_cycle(conn, config, free_bytes=FREE))["status"] == "idle"
    asyncio.run(scenario())


def test_exact_verifier_detects_changed_payload_even_with_identical_keys_and_counts(monkeypatch):
    async def scenario():
        async with fixture() as (conn, expected):
            job = await pause_after_first_checkpoint(conn, monkeypatch)
            async with conn.transaction():
                await conn.execute("SELECT set_config('meshinfo.archive_job',$1,true)", str(job["job_id"]))
                await conn.execute(f"TRUNCATE {archive._q(job['stage_name'])}")
                projection = ",".join("'changed-null-payload'::text" if name == "payload" else archive.quote_identifier(name)
                                      for name in archive.MQTT_COLUMNS)
                await conn.execute(f"INSERT INTO {archive._q(job['stage_name'])} ({archive._columns()}) "
                                   f"SELECT {projection} FROM public.mqtt_messages_1901_01 ORDER BY id,created_at LIMIT 3")
            for _ in range(30):
                result = await archive.run_archive_cycle(conn, POLICY, free_bytes=FREE)
                if result["status"] == "blocked":
                    break
            assert result["status"] == "blocked" and "row-by-row" in result["reason"]
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
            await archive.cancel_archive(conn, job["job_id"], config=POLICY)
    asyncio.run(scenario())


def test_cancel_after_unrelated_source_schema_change_preserves_that_change(monkeypatch):
    async def scenario():
        async with fixture() as (conn, expected):
            job = await pause_after_first_checkpoint(conn, monkeypatch)
            await conn.execute("CREATE INDEX archive_added_during_pause ON public.mqtt_messages_1901_01(qos)")
            result = await archive.run_archive_cycle(conn, POLICY, free_bytes=FREE)
            assert result["status"] == "blocked" and "schema changed" in result["reason"]
            assert (await archive.cancel_archive(conn, job["job_id"], config=POLICY))["status"] == "cancelled"
            assert await conn.fetchval("SELECT to_regclass('public.archive_added_during_pause') IS NOT NULL")
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
    asyncio.run(scenario())


def test_real_active_checkpoint_snapshot_restore_and_resume(tmp_path):
    if any(shutil.which(tool) is None for tool in ("psql", "pg_dump", "pg_restore")):
        pytest.skip("PostgreSQL client tools are required for the actual checkpoint restore gate")

    async def scenario():
        from storage.backup import create_snapshot
        from scripts import restore_database

        async with fixture() as (conn, expected):
            job = await create_paused_job(conn)
            assert job["rows_copied"] == 3 and job["source_frozen"]
            parsed = urlsplit(DSN)
            pg = {"host": parsed.hostname, "port": parsed.port or 5432,
                  "database": unquote(parsed.path.lstrip("/")), "username": unquote(parsed.username or "postgres"),
                  "password": unquote(parsed.password or "")}
            config = {"storage": {"postgres": pg}, "backups": {"mode": "local", "dir": str(tmp_path/"snapshots"),
                      "keep_days": 1, "min_free_mb": 1, "timeout_seconds": 60}}
            private_config = tmp_path/"restore.toml"
            private_config.write_text("[storage.postgres]\n"+"".join(f"{key} = {json.dumps(value)}\n" for key,value in pg.items()))
            private_config.chmod(0o600)
            async with archive.maintenance_lock(conn) as acquired:
                assert acquired
                snapshot = await create_snapshot(config, Path(__file__).resolve().parents[1],
                                                  owner_id=hashlib.sha256(str(job["job_id"]).encode()).hexdigest())
            assert snapshot.successful
            target = "meshinfo_resume_"+uuid.uuid4().hex[:10]+"_archive_test"
            assert re.fullmatch(r"meshinfo_resume_[a-f0-9]{10}_archive_test", target)
            await conn.execute(f'CREATE DATABASE "{target}" TEMPLATE template0')
            restored = None
            try:
                args = restore_database.parser().parse_args([snapshot.destination, "--config", str(private_config),
                                                            "--database", target, "--timeout-seconds", "60", "--apply"])
                report = await restore_database.run(args)
                assert report["status"] == "restored" and report["columnar_relations"] == 1
                restored_dsn = urlunsplit((parsed.scheme, parsed.netloc, "/"+target, parsed.query, parsed.fragment))
                restored = await asyncpg.connect(restored_dsn, command_timeout=10)
                checkpoint = await restored.fetchrow("SELECT * FROM public.storage_archive_jobs WHERE job_id=$1", job["job_id"])
                assert checkpoint["rows_copied"] == 3 and checkpoint["source_frozen"]
                source_oid = await restored.fetchval("SELECT 'public.mqtt_messages_1901_01'::regclass::oid")
                assert await archive._guarded(restored, source_oid, job["job_id"])
                assert await restored.fetchval(f"SELECT count(*) FROM {archive._q(job['stage_name'])}") == 3
                assert await restored.fetchval(f"SELECT count(*) FROM {archive._q(job['topic_stage_name'])}") == 3
                await until_done(restored)
                final = await restored.fetchrow("SELECT * FROM public.storage_archive_jobs WHERE job_id=$1", job["job_id"])
                assert final["phase"] == "complete" and final["rows_verified"] == len(expected)
                assert final["source_oid"] == source_oid
                assert [tuple(row) for row in await restored.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
                options = await restored.fetchrow("SELECT compression,compression_level,chunk_group_row_limit FROM columnar.options WHERE relation='public.mqtt_messages_1901_01'::regclass")
                assert tuple(options) == ("zstd", 3, 1000)
                assert await restored.fetchval("SELECT count(*) FROM public.mqtt_archive_topics") == len(expected)
            finally:
                if restored is not None:
                    await restored.close()
                await conn.execute(f'DROP DATABASE "{target}" WITH (FORCE)')
    asyncio.run(scenario())


def test_topic_lookup_exact_nullable_literal_data_and_atomic_visibility(monkeypatch):
    async def scenario():
        async with fixture() as (conn, _expected):
            await conn.execute("UPDATE public.mqtt_messages SET topic=NULL WHERE id=1")
            await conn.execute("UPDATE public.mqtt_messages SET topic=$1 WHERE id=2", "雪_%\\literal ")
            expected = [tuple(row) for row in await conn.fetch("SELECT id,created_at,topic FROM public.mqtt_messages ORDER BY id,created_at")]
            job = await pause_after_first_checkpoint(conn,monkeypatch)
            assert await conn.fetchval("SELECT count(*) FROM public.mqtt_archive_topics") == 0
            assert await conn.fetchval(f"SELECT count(*) FROM {archive._q(job['topic_stage_name'])}") == 3
            await until_done(conn)
            actual = [tuple(row) for row in await conn.fetch("SELECT id,created_at,topic FROM public.mqtt_archive_topics ORDER BY id,created_at")]
            assert actual == expected
            helper = await archive.archive_topics.attached_partition(conn,job["bound_start"],job["bound_end"])
            assert helper["name"] == "mqtt_archive_topics_1901_01" and helper["total_bytes"] > 0
            for sql in ("UPDATE public.mqtt_archive_topics SET topic='changed' WHERE id=1",
                        "DELETE FROM public.mqtt_archive_topics WHERE id=1",
                        "TRUNCATE public.mqtt_archive_topics_1901_01",
                        "INSERT INTO public.mqtt_archive_topics VALUES(999,'1901-01-02',NULL)"):
                with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
                    await conn.execute(sql)
    asyncio.run(scenario())


@pytest.mark.parametrize("mutation", ["change", "delete", "extra"])
def test_topic_lookup_corruption_blocks_cutover_and_cancellation_preserves_source(mutation):
    async def scenario():
        async with fixture() as (conn,expected):
            job = await create_paused_job(conn)
            stage = archive._q(job["topic_stage_name"])
            async with conn.transaction():
                await conn.execute("SELECT set_config('meshinfo.archive_job',$1,true)",str(job["job_id"]))
                if mutation == "change":
                    await conn.execute(f"UPDATE {stage} SET topic=NULL WHERE id=1")
                elif mutation == "delete":
                    await conn.execute(f"DELETE FROM {stage} WHERE id=1")
                else:
                    await conn.execute(f"INSERT INTO {stage} VALUES(0,'1901-01-02','extra')")
            for _ in range(30):
                result = await archive.run_archive_cycle(conn,POLICY,free_bytes=FREE)
                if result["status"] == "blocked":
                    break
            assert result["status"] == "blocked" and "Topic lookup exact" in result["reason"]
            assert await conn.fetchval("SELECT count(*) FROM public.mqtt_archive_topics") == 0
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
            assert (await archive.cancel_archive(conn,job["job_id"],config=POLICY))["status"] == "cancelled"
            assert await conn.fetchval("SELECT to_regclass($1) IS NULL",stage)
    asyncio.run(scenario())


def test_existing_columnar_topic_backfill_is_resumable_without_payload_rewrite(monkeypatch):
    async def scenario():
        async with fixture() as (conn,expected):
            await until_done(conn)
            source_before = await conn.fetchrow("SELECT oid,relfilenode FROM pg_class WHERE oid='public.mqtt_messages_1901_01'::regclass")
            await conn.execute("DROP TABLE public.mqtt_archive_topics_1901_01")
            plan = await archive.plan_archive(conn,POLICY)
            assert next(row for row in plan["partitions"] if row["eligible"])["target_method"] == "topics"
            job = await pause_after_first_checkpoint(conn,monkeypatch)
            assert job["target_method"] == "topics" and job["stage_name"] == job["topic_stage_name"]
            assert await conn.fetchval("SELECT count(*) FROM public.mqtt_archive_topics") == 0
            await until_done(conn)
            source_after = await conn.fetchrow("SELECT oid,relfilenode FROM pg_class WHERE oid='public.mqtt_messages_1901_01'::regclass")
            assert source_after == source_before
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
            assert await conn.fetchval("SELECT count(*) FROM public.mqtt_archive_topics") == len(expected)
            assert (await archive.plan_archive(conn,POLICY))["partitions"][0]["reason"] == "already_archived"
    asyncio.run(scenario())


def test_cancel_topic_backfill_restores_original_columnar_guard(monkeypatch):
    async def scenario():
        async with fixture() as (conn,expected):
            completed = await until_done(conn)
            await conn.execute("DROP TABLE public.mqtt_archive_topics_1901_01")
            job = await pause_after_first_checkpoint(conn,monkeypatch)
            assert job["previous_guard_job"] == uuid.UUID(completed["job_id"])
            await archive.cancel_archive(conn,job["job_id"],config=POLICY)
            assert await archive._guarded(conn,job["source_oid"],job["previous_guard_job"])
            assert await conn.fetchval("SELECT to_regclass($1) IS NULL",archive._q(job["stage_name"]))
            assert await conn.fetchval("SELECT count(*) FROM public.mqtt_archive_topics") == 0
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
    asyncio.run(scenario())


def test_heap_reversal_atomically_removes_owned_topic_lookup():
    async def scenario():
        async with fixture() as (conn,expected):
            await until_done(conn)
            assert await conn.fetchval("SELECT count(*) FROM public.mqtt_archive_topics") == len(expected)
            requested = await archive.start_restore_archive(conn,"mqtt_messages_1901_01",RESTORE_POLICY,free_bytes=FREE)
            assert requested["status"] == "copying"
            assert await conn.fetchval("SELECT count(*) FROM public.mqtt_archive_topics") == len(expected)
            await until_done(conn,RESTORE_POLICY)
            assert await conn.fetchval("SELECT to_regclass('public.mqtt_archive_topics_1901_01') IS NULL")
            assert await conn.fetchval("SELECT count(*) FROM public.mqtt_archive_topics") == 0
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
    asyncio.run(scenario())


def test_oversized_imported_topic_pauses_before_gin_write_then_resumes_with_budget():
    async def scenario():
        async with fixture() as (conn,_expected):
            await conn.execute("UPDATE public.mqtt_messages SET topic=$1 WHERE id=1","oversized/import/"+"x"*150000)
            expected = [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")]
            result = await archive.run_archive_cycle(conn,POLICY,free_bytes=FREE)
            assert result["status"] == "blocked" and "GIN/WAL budget" in result["reason"]
            if result["job_id"] is not None:
                job = await archive._job(conn,uuid.UUID(result["job_id"]))
                assert job["rows_copied"] == 0
                assert await conn.fetchval(f"SELECT count(*) FROM {archive._q(job['topic_stage_name'])}") == 0
            else:
                # A geometry-aware pilot may catch this before freezing source.
                assert not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM public.storage_archive_jobs WHERE phase IN ('copying','verifying','ready'))")
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
            larger = {"storage":{"archive":{**POLICY["storage"]["archive"],"max_wal_mib":128}}}
            await until_done(conn,larger)
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
            assert await conn.fetchval("SELECT octet_length(topic) FROM public.mqtt_archive_topics WHERE id=1") == 150017
    asyncio.run(scenario())


def test_compression_pilot_seals_worker_sized_stripes_and_counts_wide_metadata():
    async def scenario():
        async with fixture() as (conn,_expected):
            transaction = conn.transaction()
            await transaction.start()
            try:
                policy = archive.ArchivePolicy(batch_rows=7,batch_mib=1)
                await archive._settings(conn,policy)
                await conn.execute("""CREATE TABLE public.archive_pilot_input
                    (LIKE public.mqtt_messages INCLUDING DEFAULTS INCLUDING STORAGE INCLUDING COMPRESSION) USING heap;
                    CREATE TABLE public.archive_pilot_small(LIKE public.archive_pilot_input INCLUDING ALL) USING columnar;
                    CREATE TABLE public.archive_pilot_wide(LIKE public.archive_pilot_input INCLUDING ALL) USING columnar;
                    INSERT INTO public.archive_pilot_input(id,created_at,topic,payload)
                    SELECT g,'1901-01-02'::timestamptz,'msh/pilot/'||(g%31),'payload-'||g FROM generate_series(1,1000)g""")
                for name in ("archive_pilot_small","archive_pilot_wide"):
                    await archive._options(conn,name,{"compression_level":3,"chunk_group_rows":1000})
                bounded,_ = archive._copy_batch_policy(policy,11)
                assert await archive._fill_columnar_pilot(conn,"archive_pilot_input","archive_pilot_small",bounded) == 1000
                small = await archive._pilot_metadata(conn,"archive_pilot_small")
                assert small["rows"] == 1000 and small["stripes"] == 143
                assert small["max_stripe_rows"] == small["max_group_rows"] == 7
                assert await conn.fetchval("SELECT count(*) FROM public.archive_pilot_small") == 1000
                await conn.execute("UPDATE public.archive_pilot_input SET payload=repeat('x',8192)||id")
                assert await archive._fill_columnar_pilot(conn,"archive_pilot_input","archive_pilot_wide",bounded) == 1000
                wide = await archive._pilot_metadata(conn,"archive_pilot_wide")
                assert wide["rows"] == 1000 and wide["stripes"] == 143
                assert wide["bytes"] > small["bytes"]+100000
            finally:
                await transaction.rollback()
            assert await conn.fetchval("SELECT to_regclass('public.archive_pilot_small') IS NULL")
    asyncio.run(scenario())


@pytest.mark.parametrize("definition", [
    "gin(payload gin_trgm_ops)",
    "gin(lower(topic) gin_trgm_ops)",
    "gin(topic gin_trgm_ops) WHERE qos=1",
])
def test_custom_leaf_gin_refuses_conversion_without_discarding_access_path(definition):
    async def scenario():
        async with fixture() as (conn,expected):
            await conn.execute("CREATE INDEX custom_history_gin ON public.mqtt_messages_1901_01 USING " + definition)
            result = await archive.run_archive_cycle(conn,POLICY,free_bytes=FREE)
            assert result["status"] == "blocked" and "canonical-topic pg_trgm GIN" in result["reason"]
            assert not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM public.storage_archive_jobs)")
            assert await conn.fetchval("SELECT to_regclass('public.custom_history_gin') IS NOT NULL")
            assert await conn.fetchval("SELECT a.amname FROM pg_class c JOIN pg_am a ON a.oid=c.relam "
                                       "WHERE c.oid='public.mqtt_messages_1901_01'::regclass") == "heap"
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
    asyncio.run(scenario())


def test_reverse_estimate_does_not_invert_a_different_compression_geometry(monkeypatch):
    async def scenario():
        async with fixture() as (conn,_expected):
            await until_done(conn)
            source = await conn.fetchrow(archive._LEAVES_SQL+" AND c.relname='mqtt_messages_1901_01'")
            async def forbidden(*args,**kwargs):
                raise AssertionError("Reverse estimate must derive heap size directly, not create a compression pilot")
            monkeypatch.setattr(archive,"_fill_columnar_pilot",forbidden)
            async with conn.transaction():
                await archive._settings(conn,archive.ArchivePolicy())
                estimated = await archive._estimate(conn,source,archive.ArchivePolicy(),target_method="heap",
                                                     extra_indexes=["gin (topic gin_trgm_ops)"])
            assert estimated > source["supported_index_bytes"]+16*1024**2
    asyncio.run(scenario())


def test_verified_heap_restoration_restores_indexes_and_works_with_archive_disabled():
    async def scenario():
        async with fixture() as (conn, expected):
            await until_done(conn)
            disabled = {"storage": {"archive": {**POLICY["storage"]["archive"], "enabled": False}}}
            requested = await archive.start_restore_archive(conn, "mqtt_messages_1901_01", disabled, free_bytes=FREE)
            assert requested["status"] == "copying" and requested["target_method"] == "heap"
            completed = await until_done(conn, disabled)
            assert completed["target_method"] == "heap"
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
            assert await conn.fetchval("SELECT a.amname FROM pg_class c JOIN pg_am a ON a.oid=c.relam WHERE c.oid='public.mqtt_messages_1901_01'::regclass") == "heap"
            assert await conn.fetchval("""SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
                JOIN pg_am a ON a.oid=c.relam WHERE i.indrelid='public.mqtt_messages_1901_01'::regclass
                  AND a.amname='gin' AND i.indisvalid""") == 1
            assert await conn.execute("UPDATE public.mqtt_messages_1901_01 SET qos=qos WHERE id=1") == "UPDATE 1"
            assert (await archive.run_archive_cycle(conn, disabled, free_bytes=FREE))["status"] == "disabled"
    asyncio.run(scenario())


def test_cancelling_heap_restore_preserves_original_columnar_guards(monkeypatch):
    async def scenario():
        async with fixture() as (conn, expected):
            original = await until_done(conn)
            requested = await archive.start_restore_archive(conn, "mqtt_messages_1901_01", RESTORE_POLICY, free_bytes=FREE)
            assert requested["status"] == "copying"
            job = await pause_after_first_checkpoint(conn, monkeypatch)
            assert job["target_method"] == "heap" and job["source_method"] == "columnar"
            await archive.cancel_archive(conn, job["job_id"], config=POLICY)
            oid = await conn.fetchval("SELECT 'public.mqtt_messages_1901_01'::regclass::oid")
            assert await archive._guarded(conn, oid, uuid.UUID(original["job_id"]))
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
            with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
                await conn.execute("INSERT INTO public.mqtt_messages(id,created_at) VALUES(999,'1901-01-03')")
    asyncio.run(scenario())


def test_heap_restore_refuses_missing_expansion_space_without_touching_source():
    async def scenario():
        async with fixture() as (conn, expected):
            await until_done(conn)
            result = await archive.start_restore_archive(conn, "mqtt_messages_1901_01", RESTORE_POLICY, free_bytes=lambda: 1024)
            assert result["status"] == "blocked" and result["source_preserved"]
            assert not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM public.storage_archive_jobs WHERE phase IN ('copying','verifying','ready'))")
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
    asyncio.run(scenario())


def test_large_payloads_and_exact_batch_boundary_fail_closed_then_resume_with_larger_budget():
    async def scenario():
        async with fixture() as (conn, _expected):
            topic = "large-payload-witness"
            boundary_bytes = 2*1024**2-128-len(topic.encode())
            await conn.execute("INSERT INTO public.mqtt_messages(id,created_at,topic,payload) VALUES(40,$1,$2,$3),(41,$1,$2,$4)",
                               STAMP, topic, "x"*boundary_bytes, "y"*(boundary_bytes+1))
            expected = [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")]
            small = {"storage": {"archive": {**POLICY["storage"]["archive"], "batch_mib": 2}}}
            for _ in range(30):
                result = await archive.run_archive_cycle(conn, small, free_bytes=FREE)
                if result["status"] == "blocked":
                    break
            assert result["status"] == "blocked" and "One history row" in result["reason"]
            assert await conn.fetchval("SELECT rows_copied FROM public.storage_archive_jobs WHERE phase='copying'") == 34
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
            larger = {"storage": {"archive": {**POLICY["storage"]["archive"], "batch_mib": 4}}}
            await until_done(conn, larger)
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
    asyncio.run(scenario())


def test_restore_requires_disabling_automatic_recompression():
    async def scenario():
        async with fixture() as (conn, expected):
            await until_done(conn)
            result = await archive.start_restore_archive(conn, "mqtt_messages_1901_01", POLICY, free_bytes=FREE)
            assert result["status"] == "blocked" and "Disable automatic" in result["reason"]
            assert not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM public.storage_archive_jobs WHERE phase IN ('copying','verifying','ready'))")
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
    asyncio.run(scenario())


@pytest.mark.parametrize("tamper", ["default", "bound", "trigger"])
def test_staging_schema_tampering_refuses_cutover_and_remains_cancellable(monkeypatch, tamper):
    async def scenario():
        async with fixture() as (conn, expected):
            job = await pause_after_first_checkpoint(conn, monkeypatch)
            table = archive._q(job["stage_name"])
            if tamper == "default":
                await conn.execute(f"ALTER TABLE {table} ALTER COLUMN qos SET DEFAULT 99")
            elif tamper == "bound":
                constraint = await conn.fetchval("SELECT conname FROM pg_constraint WHERE conrelid=$1 AND conname LIKE 'meshinfo_archive_bound_%'", job["stage_oid"])
                await conn.execute(f"ALTER TABLE {table} DROP CONSTRAINT {archive.quote_identifier(constraint)}")
            else:
                await conn.execute(f"CREATE TRIGGER unexpected_archive_trigger BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION public.mqtt_messages_extract_node_ids()")
            result = await archive.run_archive_cycle(conn, POLICY, free_bytes=FREE)
            assert result["status"] == "blocked" and "Staging" in result["reason"] or (
                result["status"] == "blocked" and "staging" in result["reason"])
            assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected
            assert (await archive.cancel_archive(conn, job["job_id"], config=POLICY))["status"] == "cancelled"
    asyncio.run(scenario())


def test_late_keyset_chunks_keep_index_range_plans_after_prepare_threshold():
    async def scenario():
        async with fixture() as (conn, _expected):
            await conn.execute("""INSERT INTO public.mqtt_messages(id,created_at,topic,payload)
                SELECT i,'1901-01-02'::timestamptz,'keyset-plan','{}' FROM generate_series(1000,6999) i""")
            await conn.execute("ANALYZE public.mqtt_messages_1901_01")
            policy = archive.archive_policy(POLICY)
            async with conn.transaction():
                await archive._settings(conn, policy)
                assert await conn.fetchval("SHOW plan_cache_mode") == "force_custom_plan"
                for _ in range(7):
                    rows, _size = await archive._batch(conn, "mqtt_messages_1901_01", 6990, STAMP, policy)
                    assert [row["id"] for row in rows] == [6991,6992,6993]
                plan = await conn.fetchval("""EXPLAIN (FORMAT JSON)
                    SELECT id,created_at,128+coalesce(octet_length(payload),0)+coalesce(octet_length(topic),0)
                    FROM public.mqtt_messages_1901_01
                    WHERE ($1::bigint IS NULL OR (id,created_at)>($1,$2::timestamptz))
                    ORDER BY id,created_at LIMIT $3""", 6990, STAMP, 3)
                assert "Index Cond" in plan and "6990" in plan
    asyncio.run(scenario())
