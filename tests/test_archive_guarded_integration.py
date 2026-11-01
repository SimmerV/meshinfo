"""Explicit estimate override and durable source-preserving capacity cleanup."""
import asyncio
import hashlib
import json
from pathlib import Path
import shutil
import time
import uuid
from urllib.parse import unquote,urlsplit,urlunsplit

import asyncpg
import pytest

from storage.db import archive
from test_archive_integration import DSN, POLICY, fixture, create_paused_job

pytestmark = pytest.mark.skipif(not DSN,reason="MESHINFO_ARCHIVE_TEST_DSN not set")
FREE_BYTES = 2*1024**3


def expensive_pilot(monkeypatch):
    original = archive._estimate
    calls = []

    async def measured(*args,**kwargs):
        result = await original(*args,**kwargs)
        calls.append(result)
        return max(result,5*1024**3)

    monkeypatch.setattr(archive,"_estimate",measured)
    return calls


async def guarded_job(conn):
    result = await archive.start_guarded_archive(conn,"mqtt_messages_1901_01",POLICY,free_bytes=lambda:FREE_BYTES)
    assert result["status"] == "copying",result
    job = await archive._job(conn,uuid.UUID(result["job_id"]))
    async with archive.maintenance_lock(conn) as acquired:
        assert acquired
        await archive._copy_chunk(conn,job,archive.archive_policy(POLICY),lambda:FREE_BYTES)
    return await archive._job(conn,job["job_id"])


async def exact_source(conn,expected):
    assert [tuple(row) for row in await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")] == expected


def test_strict_refuses_but_named_guarded_job_retains_full_pilot_and_completes(monkeypatch):
    async def scenario():
        calls = expensive_pilot(monkeypatch)
        async with fixture() as (conn,expected):
            refused = await archive.run_archive_cycle(conn,POLICY,free_bytes=lambda:FREE_BYTES)
            assert refused["status"] == "blocked" and "free bytes" in refused["reason"]
            assert len(calls) == 1 and not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM storage_archive_jobs)")
            job = await guarded_job(conn)
            assert len(calls) == 2 and job["admission_mode"] == "guarded"
            assert job["admission_free_bytes"] == FREE_BYTES and job["estimated_stage_bytes"] > FREE_BYTES
            for cycle in range(30):
                result = await archive.run_archive_cycle(conn,POLICY,free_bytes=lambda:FREE_BYTES,
                                                        target_job_id=job["job_id"] if cycle else None)
                if result["status"] == "complete":
                    break
                assert result["status"] in ("copying","verifying","ready"),result
            assert result["status"] == "complete"
            assert len(calls) == 2
            await exact_source(conn,expected)
            assert await conn.fetchval("SELECT a.amname FROM pg_class c JOIN pg_am a ON a.oid=c.relam WHERE c.oid='mqtt_messages_1901_01'::regclass") == "columnar"
            # A terminal requested job bypasses admission and never chooses a
            # different newly eligible source, even with an unusable probe.
            await conn.execute("CREATE TABLE mqtt_messages_1901_02 PARTITION OF mqtt_messages FOR VALUES FROM ('1901-02-01') TO ('1901-03-01')")
            terminal = await archive.run_archive_cycle(conn,POLICY,free_bytes=None,target_job_id=job["job_id"])
            assert terminal["status"] == "complete"
            assert await conn.fetchval("SELECT count(*) FROM storage_archive_jobs") == 1
    asyncio.run(scenario())


@pytest.mark.parametrize("partition",["mqtt_messages_hot","mqtt_messages_default","missing_partition"])
def test_guarded_admission_requires_a_named_eligible_heap_month(monkeypatch,partition):
    async def scenario():
        calls = expensive_pilot(monkeypatch)
        async with fixture() as (conn,expected):
            result = await archive.start_guarded_archive(conn,partition,POLICY,free_bytes=lambda:FREE_BYTES)
            assert result["status"] == "blocked" and "named eligible monthly heap" in result["reason"]
            assert not calls and not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM storage_archive_jobs)")
            await exact_source(conn,expected)
    asyncio.run(scenario())


def test_capacity_at_first_cycle_gate_reclaims_owned_stages_and_sticks_failed(monkeypatch):
    async def scenario():
        expensive_pilot(monkeypatch)
        async with fixture() as (conn,expected):
            job = await guarded_job(conn)
            first = await archive.run_archive_cycle(conn,POLICY,free_bytes=lambda:0)
            assert first["status"] == "cleanup_pending" and first["cleanup_topics_dropped"]
            assert not first["cleanup_stage_dropped"]
            state = await archive._job(conn,job["job_id"])
            assert state["cleanup_requested"] and state["source_frozen"] and state["topic_stage_oid"] is None
            assert await conn.fetchval("SELECT to_regclass($1) IS NULL",archive._q(job["topic_stage_name"]))
            # Cleanup must continue without a free probe and with policy off.
            disabled = {"storage":{"archive":{**POLICY["storage"]["archive"],"enabled":False}}}
            final = await archive.run_archive_cycle(conn,disabled,free_bytes=None)
            assert final["status"] == "failed" and not final["cleanup_requested"]
            assert final["cleanup_topics_dropped"]
            await exact_source(conn,expected)
            assert await conn.execute("UPDATE mqtt_messages_1901_01 SET qos=qos WHERE id=1") == "UPDATE 1"
            preview = await archive.plan_archive(conn,POLICY)
            assert next(row for row in preview["partitions"] if row["name"] == job["source_name"])["reason"] == "previous_attempt_failed_cancel_job_to_retry"
            assert (await archive.run_archive_cycle(conn,POLICY,free_bytes=lambda:FREE_BYTES))["status"] == "idle"
            assert await conn.fetchval("SELECT count(*) FROM storage_archive_jobs") == 1
    asyncio.run(scenario())


def test_cleanup_intent_survives_interruption_before_any_drop(monkeypatch):
    async def scenario():
        expensive_pilot(monkeypatch)
        async with fixture() as (conn,expected):
            job = await guarded_job(conn)
            original = archive._capacity_cleanup

            async def interrupted(*args,**kwargs):
                raise asyncio.CancelledError

            monkeypatch.setattr(archive,"_capacity_cleanup",interrupted)
            with pytest.raises(asyncio.CancelledError):
                await archive.run_archive_cycle(conn,POLICY,free_bytes=lambda:0)
            state = await archive._job(conn,job["job_id"])
            assert state["cleanup_requested"] and not state["cleanup_stage_dropped"]
            monkeypatch.setattr(archive,"_capacity_cleanup",original)
            assert (await archive.run_archive_cycle(conn,POLICY,free_bytes=None))["status"] == "cleanup_pending"
            assert (await archive.run_archive_cycle(conn,POLICY,free_bytes=None))["status"] == "failed"
            await exact_source(conn,expected)
    asyncio.run(scenario())


def test_capacity_failure_after_insert_rolls_back_checkpoint_before_reclaim(monkeypatch):
    async def scenario():
        expensive_pilot(monkeypatch)
        async with fixture() as (conn,expected):
            job = await guarded_job(conn)
            checks = []

            def falling_space():
                checks.append(True)
                return FREE_BYTES if len(checks) < 3 else 0

            result = await archive.run_archive_cycle(conn,POLICY,free_bytes=falling_space)
            assert len(checks) == 3 and result["status"] == "cleanup_pending"
            state = await archive._job(conn,job["job_id"])
            assert state["rows_copied"] == job["rows_copied"] == 3
            assert state["last_id"] == job["last_id"] and state["cleanup_requested"]
            assert (await archive.run_archive_cycle(conn,POLICY,free_bytes=None))["status"] == "failed"
            await exact_source(conn,expected)
    asyncio.run(scenario())


def test_interrupted_drop_checkpoint_rolls_back_drop_and_then_retries(monkeypatch):
    async def scenario():
        expensive_pilot(monkeypatch)
        async with fixture() as (conn,expected):
            job = await guarded_job(conn)

            class InterruptDropCheckpoint:
                def __getattr__(self,name):
                    return getattr(conn,name)

                async def execute(self,sql,*args,**kwargs):
                    if "SET cleanup_topics_dropped=true" in sql:
                        raise asyncio.CancelledError
                    return await conn.execute(sql,*args,**kwargs)

            with pytest.raises(asyncio.CancelledError):
                await archive.run_archive_cycle(InterruptDropCheckpoint(),POLICY,free_bytes=lambda:0)
            state = await archive._job(conn,job["job_id"])
            assert state["cleanup_requested"] and not state["cleanup_topics_dropped"]
            assert await conn.fetchval("SELECT to_regclass($1) IS NOT NULL",archive._q(job["topic_stage_name"]))
            assert (await archive.run_archive_cycle(conn,POLICY,free_bytes=None))["status"] == "cleanup_pending"
            assert (await archive.run_archive_cycle(conn,POLICY,free_bytes=None))["status"] == "failed"
            await exact_source(conn,expected)
    asyncio.run(scenario())


def test_capacity_drop_keeps_source_readable_and_final_lock_refusal_is_resumable(monkeypatch):
    async def scenario():
        expensive_pilot(monkeypatch)
        async with fixture() as (conn,expected):
            job = await guarded_job(conn)
            config = {"storage":{"archive":{**POLICY["storage"]["archive"],
                       "cycle_seconds":1,"statement_seconds":.5,"cleanup_seconds":3}}}
            reader = await asyncpg.connect(DSN,command_timeout=2)
            transaction = reader.transaction()
            await transaction.start()
            try:
                await reader.fetchval("SELECT count(*) FROM mqtt_messages_1901_01")
                reads = []

                class ReadDuringDrop:
                    def __getattr__(self,name):
                        return getattr(conn,name)

                    async def execute(self,sql,*args,**kwargs):
                        if sql == "DROP TABLE "+archive._q(job["stage_name"]):
                            # A new query can still read the original while the
                            # drop transaction holds its source lock.
                            reads.append(await reader.fetchval("SELECT count(*) FROM mqtt_messages_1901_01"))
                            await conn.execute("SELECT pg_sleep(1.1)",timeout=kwargs["timeout"])
                        return await conn.execute(sql,*args,**kwargs)

                first = await archive.run_archive_cycle(conn,config,free_bytes=lambda:0)
                assert first["status"] == "cleanup_pending" and first["cleanup_topics_dropped"]
                assert not first["cleanup_stage_dropped"] and not reads
                started = time.monotonic()
                second = await archive.run_archive_cycle(ReadDuringDrop(),config,free_bytes=None)
                assert reads == [len(expected)] and time.monotonic()-started > 1
                assert second["status"] == "cleanup_pending" and "55P03" in second["cleanup_error"]
                state = await archive._job(conn,job["job_id"])
                assert state["cleanup_stage_dropped"] and state["cleanup_topics_dropped"] and state["cleanup_requested"]
                assert state["source_frozen"] and state["phase"] == "copying"
                await transaction.rollback()
                final = await archive.run_archive_cycle(conn,POLICY,free_bytes=None)
                assert final["status"] == "failed"
                await exact_source(conn,expected)
            finally:
                await reader.close()
    asyncio.run(scenario())


def test_interrupted_final_failure_commit_preserves_guard_and_cleanup_intent(monkeypatch):
    async def scenario():
        expensive_pilot(monkeypatch)
        async with fixture() as (conn,expected):
            job = await guarded_job(conn)
            assert (await archive.run_archive_cycle(conn,POLICY,free_bytes=lambda:0))["status"] == "cleanup_pending"

            class InterruptFailureCommit:
                def __getattr__(self,name):
                    return getattr(conn,name)

                async def execute(self,sql,*args,**kwargs):
                    if "SET phase=$3,source_frozen=false" in sql:
                        raise asyncio.CancelledError
                    return await conn.execute(sql,*args,**kwargs)

            with pytest.raises(asyncio.CancelledError):
                await archive.run_archive_cycle(InterruptFailureCommit(),POLICY,free_bytes=None)
            state = await archive._job(conn,job["job_id"])
            assert state["phase"] == "copying" and state["cleanup_requested"] and state["source_frozen"]
            assert state["cleanup_stage_dropped"] and state["cleanup_topics_dropped"]
            assert await archive._guarded(conn,state["source_oid"],job["job_id"])
            assert (await archive.run_archive_cycle(conn,POLICY,free_bytes=None))["status"] == "failed"
            await exact_source(conn,expected)
    asyncio.run(scenario())


@pytest.mark.parametrize("tamper",["source_guard","stage_comment","stage_disabled","stage_guard_missing","stage_argument","stage_absent"])
def test_cleanup_refuses_unknown_source_or_stage_ownership(monkeypatch,tamper):
    async def scenario():
        expensive_pilot(monkeypatch)
        async with fixture() as (conn,expected):
            job = await guarded_job(conn)
            if tamper == "source_guard":
                await conn.execute("ALTER TABLE mqtt_messages_1901_01 DISABLE TRIGGER meshinfo_archive_freeze")
            elif tamper == "stage_comment":
                await conn.execute(f"COMMENT ON TABLE {archive._q(job['stage_name'])} IS 'unrecognized object'")
            elif tamper == "stage_disabled":
                await conn.execute(f"ALTER TABLE {archive._q(job['stage_name'])} DISABLE TRIGGER meshinfo_archive_stage_guard")
            elif tamper == "stage_absent":
                await conn.execute(f"DROP TABLE {archive._q(job['stage_name'])}")
            else:
                await conn.execute(f"DROP TRIGGER meshinfo_archive_stage_guard ON {archive._q(job['stage_name'])}")
                if tamper == "stage_argument":
                    await conn.execute(f"CREATE TRIGGER meshinfo_archive_stage_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE "
                        f"ON {archive._q(job['stage_name'])} FOR EACH STATEMENT EXECUTE FUNCTION meshinfo_archive_stage_guard('{uuid.uuid4()}')")
                    await conn.execute(f"ALTER TABLE {archive._q(job['stage_name'])} ENABLE ALWAYS TRIGGER meshinfo_archive_stage_guard")
            result = await archive.run_archive_cycle(conn,POLICY,free_bytes=lambda:0)
            if not result.get("cleanup_error"):
                result = await archive.run_archive_cycle(conn,POLICY,free_bytes=None)
            assert result["status"] == "cleanup_pending"
            assert ("42P01" if tamper == "stage_absent" else "ownership") in result["cleanup_error"]
            assert not result["cleanup_stage_dropped"]
            assert bool(await conn.fetchval("SELECT to_regclass($1) IS NOT NULL",archive._q(job["stage_name"]))) == (tamper != "stage_absent")
            await exact_source(conn,expected)
    asyncio.run(scenario())


def test_guarded_interruption_limit_uses_durable_readable_cleanup(monkeypatch):
    async def scenario():
        expensive_pilot(monkeypatch)
        async with fixture() as (conn,expected):
            job = await guarded_job(conn)
            await conn.execute("UPDATE storage_archive_jobs SET abort_count=3 WHERE job_id=$1",job["job_id"])
            config = {"storage":{"archive":{**POLICY["storage"]["archive"],
                       "cycle_seconds":1,"statement_seconds":.5,"cleanup_seconds":3}}}

            class SlowDrop:
                def __getattr__(self,name):
                    return getattr(conn,name)

                async def execute(self,sql,*args,**kwargs):
                    if sql == "DROP TABLE "+archive._q(job["stage_name"]):
                        await conn.execute("SELECT pg_sleep(1.1)",timeout=kwargs["timeout"])
                    return await conn.execute(sql,*args,**kwargs)

            first = await archive.run_archive_cycle(SlowDrop(),config,free_bytes=lambda:FREE_BYTES)
            assert first["status"] == "failed" and "Three interrupted" in first["cleanup_reason"]
            assert first["cleanup_stage_dropped"] and first["cleanup_topics_dropped"]
            await exact_source(conn,expected)
    asyncio.run(scenario())


@pytest.mark.parametrize("admission",["strict","guarded"])
def test_explicit_forward_cancel_preserves_recorded_outcome_across_final_commit_interruption(monkeypatch,admission):
    async def scenario():
        expensive_pilot(monkeypatch)
        async with fixture() as (conn,expected):
            job = await guarded_job(conn) if admission == "guarded" else await create_paused_job(conn)

            class InterruptedCancel:
                def __getattr__(self,name):
                    return getattr(conn,name)

                async def execute(self,sql,*args,**kwargs):
                    if "SET phase=$3,source_frozen=false" in sql:
                        raise asyncio.CancelledError
                    return await conn.execute(sql,*args,**kwargs)

            with pytest.raises(asyncio.CancelledError):
                await archive.cancel_archive(InterruptedCancel(),job["job_id"],config=POLICY)
            state = await archive._job(conn,job["job_id"])
            assert state["cleanup_requested"] and state["cleanup_terminal_phase"] == "cancelled"
            assert state["cleanup_stage_dropped"] and state["cleanup_topics_dropped"]
            assert state["phase"] == "copying" and await archive._guarded(conn,state["source_oid"],job["job_id"])
            assert (await archive.run_archive_cycle(conn,POLICY,free_bytes=None))["status"] == "cancelled"
            await exact_source(conn,expected)
    asyncio.run(scenario())


@pytest.mark.parametrize("admission",["strict","guarded"])
def test_explicit_forward_cancel_allows_readers_during_long_drop(monkeypatch,admission):
    async def scenario():
        expensive_pilot(monkeypatch)
        async with fixture() as (conn,expected):
            job = await guarded_job(conn) if admission == "guarded" else await create_paused_job(conn)
            config = {"storage":{"archive":{**POLICY["storage"]["archive"],
                       "cycle_seconds":1,"statement_seconds":.5,"cleanup_seconds":3}}}
            reader = await asyncpg.connect(DSN,command_timeout=2)
            transaction = reader.transaction()
            await transaction.start()
            try:
                await reader.fetchval("SELECT count(*) FROM mqtt_messages_1901_01")

                class SlowReadableDrop:
                    def __getattr__(self,name):
                        return getattr(conn,name)

                    async def execute(self,sql,*args,**kwargs):
                        if sql == "DROP TABLE "+archive._q(job["stage_name"]):
                            await conn.execute("SELECT pg_sleep(1.1)",timeout=kwargs["timeout"])
                            assert await reader.fetchval("SELECT count(*) FROM mqtt_messages_1901_01") == len(expected)
                        return await conn.execute(sql,*args,**kwargs)

                started = time.monotonic()
                pending = await archive.cancel_archive(SlowReadableDrop(),job["job_id"],config=config)
                assert time.monotonic()-started > 1
                assert pending["status"] == "cleanup_pending" and pending["cleanup_terminal_phase"] == "cancelled"
                assert pending["cleanup_stage_dropped"] and pending["cleanup_topics_dropped"]
                assert "55P03" in pending["cleanup_error"]
                await transaction.rollback()
                assert (await archive.run_archive_cycle(conn,POLICY,free_bytes=None))["status"] == "cancelled"
                await exact_source(conn,expected)
            finally:
                await reader.close()
    asyncio.run(scenario())


def test_cancel_during_capacity_cleanup_preserves_failed_outcome_until_acknowledged(monkeypatch):
    async def scenario():
        expensive_pilot(monkeypatch)
        async with fixture() as (conn,expected):
            job = await guarded_job(conn)
            assert (await archive.run_archive_cycle(conn,POLICY,free_bytes=lambda:0))["status"] == "cleanup_pending"
            result = await archive.cancel_archive(conn,job["job_id"],config=POLICY)
            assert result["status"] == "failed" and result["cleanup_terminal_phase"] == "failed"
            assert (await archive.cancel_archive(conn,job["job_id"],config=POLICY))["status"] == "cancelled"
            await exact_source(conn,expected)
    asyncio.run(scenario())


def test_readonly_plan_supports_journal_before_guarded_schema_upgrade():
    async def scenario():
        async with fixture() as (conn,_expected):
            await create_paused_job(conn)
            await conn.execute("ALTER TABLE storage_archive_jobs DROP COLUMN admission_mode,DROP COLUMN admission_free_bytes,"
                               "DROP COLUMN cleanup_requested,DROP COLUMN cleanup_reason,DROP COLUMN cleanup_terminal_phase,"
                               "DROP COLUMN cleanup_stage_dropped,DROP COLUMN cleanup_topics_dropped")
            async with conn.transaction(readonly=True):
                plan = await archive.plan_archive(conn,POLICY)
            assert plan["jobs"][0]["admission_mode"] == "strict" and not plan["jobs"][0]["cleanup_requested"]
    asyncio.run(scenario())


@pytest.mark.parametrize("layout,outcome",[("helper","failed"),("helper","cancelled"),
    ("both","failed"),("both","cancelled"),("payload","failed")])
def test_real_snapshot_restores_partial_cleanup_and_its_terminal_outcome(tmp_path,monkeypatch,outcome,layout):
    if any(shutil.which(tool) is None for tool in ("psql","pg_dump","pg_restore")):
        pytest.skip("PostgreSQL client tools are required for the actual cleanup restore gate")

    async def scenario():
        from storage.backup import create_snapshot
        from scripts import restore_database

        expensive_pilot(monkeypatch)
        async with fixture() as (conn,expected):
            job = await guarded_job(conn)
            async with archive.maintenance_lock(conn) as acquired:
                assert acquired
                pending = await archive._request_capacity_cleanup(conn,job,archive.archive_policy(POLICY),
                    "Cleanup restore witness; original source preserved",terminal_phase=outcome,
                    max_steps=0 if layout == "payload" else 1)
                if layout == "payload":
                    # Legacy checkpoint order: record real intent, then perform
                    # the previously used owned payload DROP and its checkpoint.
                    await archive._drop_capacity_stage(conn,await archive._job(conn,job["job_id"]),archive.archive_policy(POLICY))
            assert pending["status"] == "cleanup_pending"
            if layout == "both":
                class PauseBeforeOutcome:
                    def __getattr__(self,name):
                        return getattr(conn,name)

                    async def execute(self,sql,*args,**kwargs):
                        if "SET phase=$3,source_frozen=false" in sql:
                            raise asyncio.CancelledError
                        return await conn.execute(sql,*args,**kwargs)

                with pytest.raises(asyncio.CancelledError):
                    await archive.run_archive_cycle(PauseBeforeOutcome(),POLICY,free_bytes=None)
            original = await archive._job(conn,job["job_id"])
            assert original["cleanup_requested"] and original["cleanup_terminal_phase"] == outcome
            stage_gone,topics_gone = layout in ("payload","both"),layout in ("helper","both")
            assert original["cleanup_stage_dropped"] == stage_gone and original["cleanup_topics_dropped"] == topics_gone
            parsed = urlsplit(DSN)
            pg = {"host":parsed.hostname,"port":parsed.port or 5432,
                  "database":unquote(parsed.path.lstrip("/")),"username":unquote(parsed.username or "postgres"),
                  "password":unquote(parsed.password or "")}
            config = {"storage":{"postgres":pg},"backups":{"mode":"local","dir":str(tmp_path/"snapshots"),
                      "keep_days":1,"min_free_mb":1,"timeout_seconds":60}}
            private_config = tmp_path/"restore.toml"
            private_config.write_text("[storage.postgres]\n"+"".join(f"{key} = {json.dumps(value)}\n" for key,value in pg.items()))
            private_config.chmod(0o600)
            async with archive.maintenance_lock(conn) as acquired:
                assert acquired
                snapshot = await create_snapshot(config,Path(__file__).resolve().parents[1],
                    owner_id=hashlib.sha256(str(job["job_id"]).encode()).hexdigest())
            assert snapshot.successful
            target = "meshinfo_cleanup_"+uuid.uuid4().hex[:10]+"_archive_test"
            await conn.execute(f'CREATE DATABASE "{target}" TEMPLATE template0')
            restored = reader = None
            try:
                args = restore_database.parser().parse_args([snapshot.destination,"--config",str(private_config),
                    "--database",target,"--timeout-seconds","60","--apply"])
                report = await restore_database.run(args)
                assert report["status"] == "restored" and report["columnar_relations"] == int(not stage_gone)
                restored_dsn = urlunsplit((parsed.scheme,parsed.netloc,"/"+target,parsed.query,parsed.fragment))
                restored = await asyncpg.connect(restored_dsn,command_timeout=10)
                checkpoint = await archive._job(restored,job["job_id"])
                for field in ("cleanup_requested","cleanup_stage_dropped","cleanup_topics_dropped","cleanup_terminal_phase",
                              "rows_copied","rows_verified","admission_mode","admission_free_bytes","estimated_stage_bytes"):
                    assert checkpoint[field] == original[field]
                await exact_source(restored,expected)
                source_oid = await restored.fetchval("SELECT 'public.mqtt_messages_1901_01'::regclass::oid")
                assert await archive._guarded(restored,source_oid,job["job_id"])
                assert bool(await restored.fetchval("SELECT to_regclass($1) IS NULL",archive._q(job["stage_name"]))) == stage_gone
                assert bool(await restored.fetchval("SELECT to_regclass($1) IS NULL",archive._q(job["topic_stage_name"]))) == topics_gone
                reader = await asyncpg.connect(restored_dsn,command_timeout=2)
                transaction = reader.transaction()
                await transaction.start()
                assert await reader.fetchval("SELECT count(*) FROM public.mqtt_messages_1901_01") == len(expected)
                disabled = {"storage":{"archive":{"enabled":False}}}
                resumed = await archive.run_archive_cycle(restored,disabled,free_bytes=None)
                assert resumed["status"] == "cleanup_pending" and "55P03" in resumed["cleanup_error"]
                assert await reader.fetchval("SELECT count(*) FROM public.mqtt_messages_1901_01") == len(expected)
                await transaction.rollback()
                finished = await archive.run_archive_cycle(restored,disabled,free_bytes=None)
                assert finished["status"] == outcome and not finished["cleanup_requested"]
                final = await archive._job(restored,job["job_id"])
                assert final["source_oid"] == source_oid and not final["source_frozen"]
                assert final["cleanup_stage_dropped"] and final["cleanup_topics_dropped"]
                assert final["cleanup_terminal_phase"] == outcome
                await exact_source(restored,expected)
                plan = await archive.plan_archive(restored,POLICY)
                source = next(row for row in plan["partitions"] if row["name"] == job["source_name"])
                assert source["eligible"] == (outcome == "cancelled")
                terminal = await archive.run_archive_cycle(restored,disabled,free_bytes=None,target_job_id=job["job_id"])
                assert terminal["status"] == outcome
            finally:
                if reader is not None:
                    await reader.close()
                if restored is not None:
                    await restored.close()
                await conn.execute(f'DROP DATABASE "{target}" WITH (FORCE)')
    asyncio.run(scenario())
