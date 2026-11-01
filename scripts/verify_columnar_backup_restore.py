#!/usr/bin/env python3
"""Real isolated Columnar backup/restore integration gate (native PG18 clients).

Uses only newly created *_columnar_test databases and tiny synthetic rows.
Requires a disposable Columnar-enabled PostgreSQL server and PostgreSQL clients
on PATH. Credentials come from standard PG* environment variables. It never
opens the application's config.toml or existing MeshInfo databases. Every
created database and temporary backup is removed in finally blocks.
"""
import asyncio
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import restore_database as restore
from storage import backup, backup_remote
from storage.columnar_backup import OPTIONS_SQL


def sql(database, text, *, expect_failure=False):
    import subprocess
    result = subprocess.run(["psql", "-X", "-qAt", "--set=ON_ERROR_STOP=1", "--set=VERBOSITY=sqlstate", "--dbname", database],
                            input=text.encode(), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            timeout=30, check=False)
    if expect_failure:
        if result.returncode == 0:
            raise AssertionError("expected isolated SQL failure")
        return ""
    if result.returncode:
        state = re.search(r"ERROR:\s*([A-Z0-9]{5})", result.stderr.decode(errors="replace"))
        raise AssertionError("isolated PostgreSQL command failed" + (" (SQLSTATE " + state[1] + ")" if state else ""))
    return result.stdout.decode().strip()


def require(value, label):
    if not value:
        raise AssertionError(label)


def verify_analysis_checkpoints(database):
    """Cover active/done jobs, coincident OIDs and older/no-journal schemas."""
    expected = [[phase, phase not in ('copying','verifying','ready')]
                for phase in sorted(('copying','verifying','ready','complete','cancelled','failed'))]
    flags_sql = """SELECT json_agg(row_to_json(s) ORDER BY phase) FROM (
        SELECT phase,analyzed,stage_analyzed,topic_analyzed FROM public.storage_archive_jobs) s"""
    for row, (phase, finished) in zip(json.loads(sql(database, flags_sql)), expected, strict=True):
        require(row == {'phase': phase, 'analyzed': finished,
                        'stage_analyzed': finished, 'topic_analyzed': finished},
                'logical restore trusted copied statistics checkpoints')
    # Simulate the important edge directly: every recorded OID already equals
    # its destination relation OID. Invalidation must not depend on a mismatch.
    sql(database, """UPDATE public.storage_archive_jobs SET
        source_oid='public.cold'::regclass,stage_oid='archive_stage.copy'::regclass,
        topic_stage_oid='public.hot'::regclass,
        analyzed=true,stage_analyzed=true,topic_analyzed=true
        WHERE phase IN ('copying','verifying','ready');""")
    require(sql(database, """SELECT bool_and(source_oid='public.cold'::regclass
        AND stage_oid='archive_stage.copy'::regclass AND topic_stage_oid='public.hot'::regclass)
        FROM public.storage_archive_jobs WHERE phase IN ('copying','verifying','ready')""") == 't',
        'coincident-OID witness is incomplete')
    sql(database, restore.reset_archive_analysis_sql())
    require(sql(database, """SELECT bool_and(NOT analyzed AND NOT stage_analyzed AND NOT topic_analyzed)
        FROM public.storage_archive_jobs WHERE phase IN ('copying','verifying','ready')""") == 't',
        'coincident OIDs retained stale statistics checkpoints')
    # The original journal had only analyzed. A heap-only legacy dump can lack
    # all checkpoint columns or the journal itself. None requires new metadata.
    for alteration, verification in (
        ("ALTER TABLE public.storage_archive_jobs DROP COLUMN stage_analyzed,DROP COLUMN topic_analyzed; "
         "UPDATE public.storage_archive_jobs SET analyzed=true;",
         "SELECT bool_and(analyzed=(phase NOT IN ('copying','verifying','ready'))) FROM public.storage_archive_jobs;"),
        ("ALTER TABLE public.storage_archive_jobs DROP COLUMN analyzed,DROP COLUMN stage_analyzed,DROP COLUMN topic_analyzed;",
         "SELECT count(*)=6 FROM public.storage_archive_jobs;"),
        ("ALTER TABLE public.storage_archive_jobs RENAME TO hidden_archive_jobs;",
         "SELECT count(*)=6 FROM public.hidden_archive_jobs;"),
    ):
        require(sql(database, 'BEGIN; '+alteration+restore.reset_archive_analysis_sql()+verification+' ROLLBACK;') == 't',
                'legacy journal compatibility failed')


async def run():
    suffix = uuid.uuid4().hex[:10]
    source = "backup_" + suffix + "_columnar_test"
    target = "restore_" + suffix + "_columnar_test"
    rejected = "reject_" + suffix + "_columnar_test"
    databases = (source, target, rejected)
    created = []
    lock = None
    original_ssh = backup.ssh_command
    original_stream = restore.stream_section
    original_reset = restore.reset_archive_analysis_sql
    pg = {"host": os.environ.get("PGHOST", "/var/run/postgresql"),
          "port": int(os.environ.get("PGPORT", "5432")),
          "username": os.environ.get("PGUSER", "postgres"),
          "password": os.environ.get("PGPASSWORD", ""), "database": source}
    try:
        for name in databases:
            require(re.fullmatch(r"[a-z0-9_]+_columnar_test", name), "unsafe isolated name")
            sql("postgres", 'CREATE DATABASE "' + name + '" TEMPLATE template0;')
            created.append(name)
        sql(source, """CREATE EXTENSION citus_columnar;
            CREATE TABLE history(id bigint, day date, payload text, PRIMARY KEY(id,day)) PARTITION BY RANGE(day);
            CREATE TABLE cold PARTITION OF history FOR VALUES FROM ('2025-01-01') TO ('2025-02-01') USING columnar;
            ALTER TABLE cold SET(columnar.compression='zstd',columnar.compression_level=7,
                columnar.chunk_group_row_limit=1000,columnar.stripe_row_limit=150000);
            CREATE TABLE hot PARTITION OF history FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
            INSERT INTO history SELECT i,'2025-01-15',repeat('lossless 雪 ',100)||i FROM generate_series(1,4096)i;
            INSERT INTO history SELECT i,'2025-02-15','hot '||i FROM generate_series(4097,4146)i;
            CREATE SCHEMA archive_stage;
            CREATE TABLE archive_stage.copy(id bigint,payload text,"雪
""column" text) USING columnar;
            ALTER TABLE archive_stage.copy SET(columnar.compression='lz4',columnar.compression_level=1,
                columnar.chunk_group_row_limit=2000,columnar.stripe_row_limit=200000);
            INSERT INTO archive_stage.copy SELECT i,'staging '||i,'quoted UTF8 雪 '||i FROM generate_series(1,128)i;
            INSERT INTO archive_stage.copy SELECT i,'staging '||i,'quoted UTF8 雪 '||i FROM generate_series(129,256)i;
            INSERT INTO archive_stage.copy SELECT i,CASE WHEN i=384 THEN repeat('large COPY row 雪 ',131072)
                ELSE 'staging '||i END,'quoted UTF8 雪 '||i FROM generate_series(257,384)i;
            CREATE FUNCTION reject_frozen() RETURNS trigger LANGUAGE plpgsql AS
                $$BEGIN RAISE EXCEPTION 'historical source is frozen'; END$$;
            CREATE TRIGGER frozen_source BEFORE INSERT ON cold FOR EACH ROW EXECUTE FUNCTION reject_frozen();
            CREATE TABLE archive_job_state(schema_name text,table_name text,marker uuid,state text);
            INSERT INTO archive_job_state VALUES('archive_stage','copy','12345678-1234-1234-1234-123456789abc','copying');
            CREATE TABLE storage_archive_jobs(job_id uuid PRIMARY KEY,phase text,source_oid oid,
                stage_oid oid,topic_stage_oid oid,analyzed boolean,stage_analyzed boolean,topic_analyzed boolean,
                rows_copied bigint,guard_token uuid);
            INSERT INTO storage_archive_jobs SELECT md5(phase)::uuid,phase,'public.cold'::regclass,
                'archive_stage.copy'::regclass,'public.hot'::regclass,true,true,true,17,
                '12345678-1234-1234-1234-123456789abc'::uuid
                FROM unnest(ARRAY['copying','verifying','ready','complete','cancelled','failed']) AS phase;""")
        journal_sql = """SELECT json_agg(row_to_json(j) ORDER BY phase) FROM (
            SELECT job_id,phase,source_oid,stage_oid,topic_stage_oid,rows_copied,guard_token FROM storage_archive_jobs) j"""
        expected_journal = sql(source, journal_sql)
        expected_rows = sql(source, "SELECT count(*),md5(string_agg(id::text||':'||day::text||':'||payload,'|' ORDER BY day,id)) FROM history")
        expected_stage = sql(source, "SELECT count(*),md5(string_agg(row_to_json(copy)::text,'|' ORDER BY id)) FROM archive_stage.copy")
        require(int(sql(source, "SELECT max(octet_length(payload)) FROM archive_stage.copy")) > 2*1024*1024,
                "fixture must stream a COPY row larger than two MiB")
        expected_options = json.loads(sql(source, OPTIONS_SQL))
        expected_groups = sql(source, "SELECT sum(chunk_group_count),sum(row_count) FROM columnar.stripe")
        require(expected_groups.startswith("8|"), "fixture must distinguish 1000-row chunks from defaults")
        # The normal low-level snapshot contract requires the caller to hold
        # the shared storage-maintenance lock for its entire operation.
        lock = await asyncio.create_subprocess_exec("psql", "-X", "-qAt", "--set=ON_ERROR_STOP=1", "--dbname", source,
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        lock.stdin.write(f"SELECT pg_advisory_lock({0x4D455348494E464F}); SELECT 'locked';\n".encode())
        await lock.stdin.drain()
        while (await lock.stdout.readline()).strip() != b"locked":
            if lock.returncode is not None:
                raise AssertionError("isolated maintenance lock failed")
        with tempfile.TemporaryDirectory(prefix="meshinfo-columnar-restore-") as directory:
            root = Path(directory)
            config_path = root / "config.toml"
            fd = os.open(config_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as config:
                config.write('[storage.postgres]\n' + ''.join(
                    key + '=' + json.dumps(value) + '\n' for key, value in pg.items()))
            settings = {"backups": {"dir": str(root / "local"), "min_free_mb": 0, "timeout_seconds": 60},
                        "storage": {"postgres": pg}}
            owner = hashlib.sha256(suffix.encode()).hexdigest()
            local = await backup.create_snapshot(settings, ROOT, owner_id=owner)
            archive = Path(local.destination)
            marker = archive.with_name(archive.name + backup_remote.MANIFEST_SUFFIX)
            manifest = json.loads(marker.read_text())
            require(manifest["restore_metadata"]["relations"] == expected_options, "source option metadata changed")
            args = restore.parser().parse_args([str(archive), "--config", str(config_path), "--database", target, "--apply"])
            restored = await restore.run(args)
            require(restored["status"] == "restored", "restore did not commit")
            require(sql(target, "SELECT count(*),md5(string_agg(id::text||':'||day::text||':'||payload,'|' ORDER BY day,id)) FROM history") == expected_rows, "packet rows changed")
            require(sql(target, "SELECT count(*),md5(string_agg(row_to_json(copy)::text,'|' ORDER BY id)) FROM archive_stage.copy") == expected_stage, "staged rows changed")
            require(json.loads(sql(target, OPTIONS_SQL)) == expected_options, "restored options changed")
            require(sql(target, "SELECT sum(chunk_group_count),sum(row_count) FROM columnar.stripe") == expected_groups, "physical chunk geometry proves options were not applied before COPY")
            require(sql(target, "SELECT state FROM archive_job_state") == "copying", "resume state was lost")
            require(sql(target, journal_sql) == expected_journal, "statistics reset changed job identities or copy checkpoints")
            verify_analysis_checkpoints(target)
            sql(target, "INSERT INTO history VALUES(9000,'2025-01-20','must reject')", expect_failure=True)
            args.database = rejected
            # A missing metadata manifest must fail before data loading.
            saved = marker.read_bytes()
            marker.unlink()
            try:
                await restore.run(args)
            except restore.RestoreRefused:
                pass
            else:
                raise AssertionError("Columnar restore accepted missing metadata")
            marker.write_bytes(saved)
            # A valid checksum over an incomplete inventory is still unsafe;
            # its SQL assertion must abort the whole transaction before COPY.
            incomplete = deepcopy(manifest)
            incomplete["restore_metadata"]["relations"] = incomplete["restore_metadata"]["relations"][:1]
            incomplete["restore_metadata_sha256"] = backup_remote.restore_metadata_checksum(incomplete["restore_metadata"])
            marker.write_text(json.dumps(incomplete))
            try:
                await restore.run(args)
            except restore.RestoreRefused:
                pass
            else:
                raise AssertionError("restore accepted an incomplete Columnar inventory")
            require(sql(rejected, restore.EMPTY_QUERY) == "0", "failed restore left schema or data behind")
            marker.write_bytes(saved)
            # Producer failure after rows reached psql must not turn EOF into a
            # commit. The real PostgreSQL transaction must leave the DB empty.
            async def interrupted(fd, section, process, **options):
                await original_stream(fd, section, process, **options)
                if section == "data":
                    raise restore.RestoreRefused("injected producer failure")
            restore.stream_section = interrupted
            try:
                await restore.run(args)
            except restore.RestoreRefused:
                pass
            else:
                raise AssertionError("restore committed after producer failure")
            finally:
                restore.stream_section = original_stream
            require(sql(rejected, restore.EMPTY_QUERY) == "0", "producer failure left restored objects")
            # Reset is inside the same transaction as COPY and post-data. A
            # failure here must still remove all restored schema/data.
            restore.reset_archive_analysis_sql = lambda: original_reset() + (
                "DO $$ BEGIN RAISE EXCEPTION 'injected statistics reset failure'; END $$;\n")
            try:
                await restore.run(args)
            except restore.RestoreRefused:
                pass
            else:
                raise AssertionError("restore committed after statistics reset failure")
            finally:
                restore.reset_archive_analysis_sql = original_reset
            require(sql(rejected, restore.EMPTY_QUERY) == "0", "statistics reset failure left restored objects")
            # Exercise actual receiver framing/decoding/publication locally;
            # this validates transport payload, not an SSH server/network.
            backup.ssh_command = lambda policy, filename, owner_id: (
                sys.executable, str(Path(backup_remote.__file__)), policy.remote_directory, filename,
                str(policy.min_free_bytes), str(policy.remote_keep_days), str(policy.timeout_seconds), owner_id)
            settings["backups"].update(mode="ssh", remote_target="backup@isolated:" + str(root / "remote"))
            remote = await backup.create_snapshot(settings, ROOT, owner_id=owner)
            remote_marker = root / "remote" / (remote.filename + backup_remote.MANIFEST_SUFFIX)
            require(json.loads(remote_marker.read_text())["restore_metadata"] == manifest["restore_metadata"], "remote metadata differs")
            return {"status": "passed", "postgres_major": manifest["restore_metadata"]["postgres_major"],
                    "columnar_version": manifest["restore_metadata"]["extension"],
                    "rows": 4530, "columnar_relations": 2, "chunk_groups": 8,
                    "physical_geometry_preserved": restored["physical_geometry_preserved"],
                    "multi_megabyte_copy_row_verified": True,
                    "local_and_remote_receiver": True, "single_transaction_rollback": True,
                    "active_archive_statistics_invalidated": True, "coincident_oids_handled": True,
                    "legacy_analysis_checkpoints_supported": True, "statistics_reset_rollback": True,
                    "stage_state_and_guards_preserved": True}
    finally:
        backup.ssh_command = original_ssh
        restore.stream_section = original_stream
        restore.reset_archive_analysis_sql = original_reset
        if lock is not None:
            lock.stdin.close()
            await backup._stop(lock, drain_stdout=True)
        for name in reversed(created):
            sql("postgres", 'DROP DATABASE "' + name + '" WITH(FORCE);')


def main():
    try:
        result = asyncio.run(run())
    except Exception as exc:
        print("Columnar backup/restore integration failed: " + type(exc).__name__, file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
