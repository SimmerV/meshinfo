#!/usr/bin/env python3
"""SIGKILL/restart recovery proof for the explicitly isolated archive-test PG.

Requires a container named exactly meshinfo-archive-test with the label
io.meshinfo.archive-test=true, no other client sessions, only disposable *_test
databases, and a dedicated empty *_archive_test database. Docker and asyncpg
must be available to this runner. It never accepts a production container name.

The test commits a source/partial-copy checkpoint and hot write, flushes an
uncommitted columnar insert, kills only the verified container ID, restarts it,
and resumes the exact job. This simulates a PostgreSQL/container crash, not
hardware power loss. Fixture cleanup happens only after reconnecting to the
same cluster and checking the fixture ownership marker.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from urllib.parse import unquote, urlsplit
import uuid

import asyncpg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from storage.db import archive
from scripts.sample_columnar_history import extract_trigger

CONTAINER = "meshinfo-archive-test"
LABEL = "io.meshinfo.archive-test"
POLICY = {"storage": {"archive": {"enabled": True, "after_months": 1, "batch_rows": 64,
    "batch_mib": 1, "cycle_seconds": 8, "statement_seconds": 2, "analyze_seconds": 6}}}


class CrashProofRefused(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise CrashProofRefused(message)


async def command(*arguments, environment=None, timeout=30):
    proc = await asyncio.create_subprocess_exec(*arguments, env=environment,
        stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        stdout, _errors = await asyncio.wait_for(proc.communicate(), timeout)
        require(proc.returncode == 0, "An isolated test control command failed")
        require(len(stdout) <= 65536, "Unexpectedly large test control response")
        return stdout.decode().strip()
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()


def digest(rows):
    text = json.dumps([tuple(row) for row in rows], ensure_ascii=False, separators=(",", ":"),
                      default=lambda value: value.astimezone(timezone.utc).isoformat())
    return hashlib.sha256(text.encode()).hexdigest()


async def seed(conn, marker):
    require(await conn.fetchval("SELECT to_regclass('public.mqtt_messages') IS NULL"), "Crash fixture database already has history")
    require(await conn.fetchval("SELECT to_regclass('public.storage_archive_jobs') IS NULL"), "Crash fixture database already has an archive journal")
    require(await conn.fetchval("SELECT to_regclass('public.mqtt_archive_topics') IS NULL"), "Crash fixture database already has topic lookup coverage")
    await archive.ensure_archive_schema(conn)
    async with conn.transaction():
        await conn.execute("""CREATE TABLE public.mqtt_messages (
            id BIGSERIAL,topic TEXT,payload TEXT COMPRESSION lz4,qos INTEGER,retain BOOLEAN,
            timestamp BIGINT,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            from_node_id VARCHAR(8),to_node_id VARCHAR(8),packet_id BIGINT,
            PRIMARY KEY(id,created_at)) PARTITION BY RANGE(created_at);
            CREATE TABLE public.mqtt_messages_1901_01 PARTITION OF public.mqtt_messages
              FOR VALUES FROM ('1901-01-01') TO ('1901-02-01');
            CREATE TABLE public.mqtt_messages_default PARTITION OF public.mqtt_messages DEFAULT;
            CREATE INDEX mqtt_crash_from ON public.mqtt_messages(from_node_id,created_at DESC);
            CREATE INDEX mqtt_crash_packet ON public.mqtt_messages(from_node_id,packet_id,created_at DESC) WHERE packet_id IS NOT NULL;
            CREATE INDEX mqtt_crash_topic ON public.mqtt_messages_1901_01 USING gin(topic gin_trgm_ops)""")
        await conn.execute(f"COMMENT ON TABLE public.mqtt_messages IS '{marker}'")
        start = await conn.fetchval("SELECT date_trunc('month',now())")
        end = start.replace(year=start.year+1,month=1) if start.month == 12 else start.replace(month=start.month+1)
        await conn.execute(f"CREATE TABLE public.mqtt_messages_hot PARTITION OF public.mqtt_messages "
                           f"FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}')")
        stamp = datetime(1901,1,2,tzinfo=timezone.utc)
        rows = [(number+1,"msh/crash-test/%_literal", json.dumps({"text":"雪 "*40,"n":number,"nullable":None}),
                 number%3,bool(number%2),number,stamp,"00000001",None,number)
                for number in range(512)]
        await conn.copy_records_to_table("mqtt_messages",schema_name="public",columns=archive.MQTT_COLUMNS,records=rows)
        await conn.execute(extract_trigger().decode())
        await conn.execute("INSERT INTO public.mqtt_messages(id,topic,payload,created_at) VALUES(900000,'msh/hot','committed hot writer',now())")


async def reconnect(dsn):
    deadline = time.monotonic()+45
    while time.monotonic()<deadline:
        try:
            return await asyncpg.connect(dsn,timeout=2,command_timeout=10,
                server_settings={"application_name":"meshinfo-archive-crash-proof","timezone":"UTC"})
        except (OSError,asyncpg.PostgresError,TimeoutError):
            await asyncio.sleep(.2)
    raise CrashProofRefused("Isolated PostgreSQL did not become ready after restart")


async def run(args):
    require(args.container == CONTAINER, "Only the explicitly named archive-test container is accepted")
    require(re.fullmatch(r"[A-Z][A-Z0-9_]*",args.dsn_env), "Invalid DSN environment variable")
    dsn = os.environ.get(args.dsn_env, "")
    parsed = urlsplit(dsn)
    database = unquote(parsed.path.lstrip("/"))
    require(re.fullmatch(r"[A-Za-z0-9_]+_archive_test",database), "A dedicated *_archive_test database is required")
    metadata = json.loads(await command("docker","inspect",args.container))[0]
    require(metadata["Name"] == "/"+CONTAINER and (metadata["Config"].get("Labels") or {}).get(LABEL) == "true",
            "Isolated container ownership label is missing")
    require(metadata["State"]["Running"], "The isolated PostgreSQL container must initially be running")
    container_id = metadata["Id"]
    require(re.fullmatch(r"[a-f0-9]{64}",container_id), "Invalid immutable container ID")
    environment = {**os.environ,"PGPASSWORD":unquote(parsed.password or "")}
    clients = ("docker","exec","-e","PGPASSWORD",container_id,"psql","-X","-qAt",
               "--set=ON_ERROR_STOP=1","--username",unquote(parsed.username or "postgres"),"--dbname",database)
    conn = await reconnect(dsn)
    marker = "meshinfo archive crash proof "+uuid.uuid4().hex
    owned, killed, restarted = False, False, False
    report = {"container_crash_only":True,"all_checks_passed":False}
    try:
        require(await conn.fetchval("SELECT current_database()") == database, "Connected database differs from guarded target")
        cluster = str(await conn.fetchval("SELECT system_identifier FROM pg_control_system()"))
        local_cluster = await command(*clients,"-c","SELECT system_identifier FROM pg_control_system()",environment=environment)
        require(cluster == local_cluster, "DSN and Docker container do not identify the same PostgreSQL cluster")
        databases = await conn.fetch("SELECT datname FROM pg_database WHERE NOT datistemplate AND datname<>'postgres'")
        require(all(re.fullmatch(r"[A-Za-z0-9_]+_(archive|columnar|storage|restore)_test",row["datname"]) for row in databases),
                "Container contains a database outside the disposable test allowlist")
        require(not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid())"),
                "Other database client sessions are active; run the crash proof exclusively")
        for setting in ("fsync","full_page_writes","synchronous_commit"):
            require(await conn.fetchval("SHOW "+setting) == "on", "Crash proof requires normal WAL/fsync durability")
        await conn.execute("CREATE EXTENSION IF NOT EXISTS citus_columnar; CREATE EXTENSION IF NOT EXISTS pg_trgm")
        data_directory = await conn.fetchval("SHOW data_directory")
        require(Path(data_directory).is_absolute(), "Invalid isolated PostgreSQL data directory")

        def free_bytes():
            result = subprocess.run(["docker","exec",container_id,"stat","-f","-c","%a:%S",data_directory],
                                    check=True,capture_output=True,text=True,timeout=5)
            require(re.fullmatch(r"\d+:\d+\s*",result.stdout), "Invalid isolated filesystem-space probe")
            blocks,size = (int(part) for part in result.stdout.strip().split(":"))
            return blocks*size

        require(free_bytes()>2*1024**3, "Crash proof requires two GiB of free scratch space")
        await seed(conn,marker)
        owned = True
        expected = digest(await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at"))
        policy = archive.archive_policy(POLICY)
        async with archive.maintenance_lock(conn) as acquired:
            require(acquired, "Another storage operation is active")
            plan = await archive.plan_archive(conn,POLICY)
            source = next(row for row in plan["partitions"] if row["eligible"])
            job_id = await archive._start(conn,source,policy,free_bytes)
            job = await archive._job(conn,job_id)
            await archive._copy_chunk(conn,job,policy,free_bytes)
        job = await archive._job(conn,job_id)
        require(0<job["rows_copied"]<512, "Expected a partial committed copy checkpoint")
        before = await archive._stage_size(conn,job)
        await conn.execute("UPDATE public.storage_archive_jobs SET inflight_bytes=$2 WHERE job_id=$1",job_id,before)
        transaction = conn.transaction()
        await transaction.start()
        await conn.execute("SELECT set_config('meshinfo.archive_job',$1,true)",str(job_id))
        await conn.execute(f"INSERT INTO {archive._q(job['stage_name'])}(id,created_at,payload) VALUES(7777777,'1901-01-02','uncommitted crash witness')")
        require(await conn.fetchval(f"SELECT count(*) FROM {archive._q(job['stage_name'])}") == job["rows_copied"]+1,
                "Uncommitted crash witness was not visible to its transaction")
        # Flush WAL and dirty pages while the inserting transaction remains
        # uncommitted, then close this separate short-lived checkpoint session.
        await command(*clients,"-c","CHECKPOINT",environment=environment)
        require(not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid())"),
                "Another database client connected before the crash; refusing to kill it")
        print("Isolated checkpoint and uncommitted columnar write prepared; killing verified test container.",flush=True)
        await command("docker","kill","--signal=KILL",container_id)
        killed = True
        await conn.close()
        conn = None
        await command("docker","start",container_id)
        restarted = True
        conn = await reconnect(dsn)
        require(str(await conn.fetchval("SELECT system_identifier FROM pg_control_system()")) == cluster,
                "Restarted PostgreSQL cluster identity changed")
        require(await conn.fetchval("SELECT obj_description('public.mqtt_messages'::regclass,'pg_class')") == marker,
                "Crash fixture ownership marker changed")
        recovered = await archive._job(conn,job_id)
        require(recovered["rows_copied"] == job["rows_copied"] and recovered["inflight_bytes"] == before,
                "Committed checkpoint/in-flight marker did not survive crash recovery")
        require(await conn.fetchval(f"SELECT count(*) FROM {archive._q(job['stage_name'])}") == job["rows_copied"],
                "Uncommitted columnar row survived crash recovery")
        require(digest(await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")) == expected,
                "Original history or committed hot write changed during crash recovery")
        for _ in range(100):
            result = await archive.run_archive_cycle(conn,POLICY,free_bytes=free_bytes)
            require(result["status"] in ("copying","verifying","ready","complete"), "Recovered archive job refused to progress")
            if result["status"] == "complete":
                break
        require(result["status"] == "complete", "Recovered archive did not finish within bounded cycles")
        final = await archive._job(conn,job_id)
        require(final["rows_copied"] == final["rows_verified"] == 512 and final["abort_count"] == 1,
                "Recovered archive checkpoint accounting differs")
        require(digest(await conn.fetch("SELECT * FROM public.mqtt_messages ORDER BY id,created_at")) == expected,
                "Completed archive differs after crash recovery")
        report.update(all_checks_passed=True,canonical_rows=513,archived_rows=512,
                      committed_chunk_rows=job["rows_copied"],aborted_bytes=final["aborted_bytes"],
                      source_digest=expected,uncommitted_row_rejected=True,hot_write_preserved=True,
                      checkpoint_resumed=True,wal_recovery=True)
        return report
    finally:
        if killed and not restarted:
            await command("docker","start",container_id)
        if conn is None or conn.is_closed():
            conn = await reconnect(dsn)
        if owned:
            owner = await conn.fetchval("SELECT obj_description(to_regclass('public.mqtt_messages'),'pg_class')")
            require(owner == marker, "Cleanup refused: fixture ownership marker changed")
            for row in await conn.fetch("SELECT stage_name,topic_stage_name FROM public.storage_archive_jobs"):
                for name in {row["stage_name"],row["topic_stage_name"]}-{None}:
                    await conn.execute(f"DROP TABLE IF EXISTS {archive._q(name)}")
            await conn.execute("DROP TABLE public.mqtt_messages; DROP TABLE public.mqtt_archive_topics; DROP TABLE public.storage_archive_jobs")
        await conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--container",required=True)
    parser.add_argument("--dsn-env",required=True)
    parser.add_argument("--output",type=Path,required=True)
    args = parser.parse_args()
    try:
        report = asyncio.run(run(args))
    except Exception as exc:
        print("Archive crash verification failed: "+(str(exc) if isinstance(exc,CrashProofRefused) else type(exc).__name__),file=sys.stderr)
        return 1
    args.output.write_text(json.dumps(report,indent=2)+"\n")
    print(json.dumps({"all_checks_passed":True,"output":str(args.output)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
