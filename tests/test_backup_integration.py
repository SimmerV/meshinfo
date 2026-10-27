"""Real custom-format backup, verification and complete database restore checks.

Opt in with MESHINFO_STORAGE_TEST_DSN naming a dedicated *_storage_test database.
Docker PostgreSQL clients are used through the actual host backup controller.
The second test executes the actual remote receiver over a local Docker pipe;
it validates the framed transport/receiver, not an SSH server or SSH network.
Only tiny synthetic fixtures are seeded. Generated *_restore_test databases and
all dump directories are removed in finally blocks. Never use a live mesh DSN.
"""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import time
from urllib.parse import unquote, urlsplit, urlunsplit
import uuid

import asyncpg
import pytest

from scripts import backup_database
from storage import backup, backup_remote
from storage.db.postgres import PostgresStorage
from storage.db.uplink_dedup import gateway_from_msg, reception_fields, reception_patch, reception_template

DSN = os.environ.get("MESHINFO_STORAGE_TEST_DSN", "")
PG_CONTAINER = os.environ.get("MESHINFO_TEST_POSTGRES_CONTAINER", "meshinfo-postgres-1")
RECEIVER_CONTAINER = os.environ.get("MESHINFO_TEST_BACKEND_CONTAINER", "meshinfo-meshinfo-1")
ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(not DSN, reason="MESHINFO_STORAGE_TEST_DSN not set")
BASE = 8_700_000_000_000_000
NODE = "bacc1901"
PACKETS = 64
TABLES = (
    "nodes", "node_positions", "node_neighborinfo", "node_telemetry_current", "telemetry",
    "chat_channels", "chat_messages", "traceroutes", "node_neighborinfo_history",
    "mqtt_messages", "packet_receptions", "mqtt_topics", "storage_identity",
)


def _connection_config():
    url = urlsplit(DSN)
    database = unquote(url.path.lstrip("/"))
    if not re.fullmatch(r"[A-Za-z0-9_]+_storage_test", database):
        pytest.fail("Backup integration requires a dedicated *_storage_test database")
    for container in (PG_CONTAINER, RECEIVER_CONTAINER):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", container):
            pytest.fail("Invalid integration container name")
    # Docker clients execute inside PostgreSQL's container. No production
    # configuration or database name is copied into this test configuration.
    return {"enabled": True, "host": "127.0.0.1", "port": 5432,
            "username": unquote(url.username or "postgres"),
            "password": unquote(url.password or ""), "database": database}


def _restore_dsn(database):
    url = urlsplit(DSN)
    return urlunsplit((url.scheme, url.netloc, "/" + database, url.query, url.fragment))


def _part(table, month):
    return f"{table}_backup_test_1901_{month:02d}"


async def _seed(conn):
    # These are the only newer schema additions needed by the backup controller;
    # the base MeshInfo schema must already be installed in this isolated DB.
    assert await conn.fetchval("SELECT to_regclass('public.mqtt_messages') IS NOT NULL")
    prefix = (ROOT / "postgres/sql/schema.sql").read_text().split("-- Nodes table", 1)[0]
    await conn.execute(prefix)
    await conn.execute("INSERT INTO nodes(id,longname,shortname,active,created_at) VALUES($1,'中央谷 Café 🌲','BKP',true,'1901-01-01')", NODE)
    await conn.execute("INSERT INTO node_positions(node_id,latitude_i,longitude_i,altitude,created_at) VALUES($1,370123456,-1207654321,123,'1901-01-01')", NODE)
    await conn.execute("INSERT INTO node_neighborinfo(node_id,neighbors,created_at) VALUES($1,'[{\"node_id\":\"abcd1234\",\"snr\":7.25}]','1901-01-01')", NODE)
    await conn.execute("INSERT INTO node_telemetry_current(node_id,battery_level,voltage,temperature,created_at) VALUES($1,87,4.125,22.75,'1901-01-01')", NODE)
    for month in (1, 2):
        for table in ("mqtt_messages", "packet_receptions"):
            await conn.execute(f"CREATE TABLE {_part(table, month)} PARTITION OF {table} FOR VALUES FROM ('1901-{month:02d}-01 00:00:00+00') TO ('1901-{month+1:02d}-01 00:00:00+00')")
    storage = PostgresStorage({"server": {"timezone": "UTC"}, "storage": {"postgres": {"enabled": True}}})
    originals = {}
    for index in range(PACKETS):
        created = datetime(1901, 1 if index < 32 else 2, 15, 23, 59, 59, 123456, tzinfo=timezone.utc) + timedelta(seconds=index)
        canonical = {
            "from": "!" + NODE, "to": "!ffffffff", "id": 424200 + index,
            "topic": "msh/backup-integration/!bacc1001", "sender": "!bacc1001",
            "rx_rssi": -80, "rx_snr": 4.125, "timestamp": 1700000000 + index,
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "text": f"中央谷 Café 🌲 — packet {index}"},
            "extension": {"empty": "", "none": None, "boolean": True, "float": 3.141592653589793,
                          "list": [1, "two", None], "escapes": "line\nquote\"backslash\\", "padding": "lossless-" * 128},
        }
        raw = json.dumps(canonical, ensure_ascii=False, separators=(", ", ": "))
        await conn.execute("""INSERT INTO mqtt_messages(id,topic,payload,qos,retain,timestamp,created_at,from_node_id,to_node_id,packet_id)
                              VALUES($1,$2,$3,1,false,$4,$5,$6,'ffffffff',$7)""",
                           BASE + index, canonical["topic"], raw, canonical["timestamp"], created, NODE, canonical["id"])
        originals[index] = []
        for gateway_number in (1, 2, 3):
            copy = deepcopy(canonical)
            copy.update(sender=f"!bacc100{gateway_number}", topic=f"msh/backup-integration/!bacc100{gateway_number}",
                        rx_rssi=-80 - gateway_number, rx_snr=4.125 / gateway_number,
                        receiver_note=f"récepteur {gateway_number} 🌲")
            fields = reception_fields(copy)
            gateway = gateway_from_msg(copy)
            patch = reception_patch(reception_template(canonical, gateway, copy["topic"], fields), copy)
            topic_id = await storage._intern_topic(conn, copy["topic"])
            await conn.execute("""INSERT INTO packet_receptions(mqtt_row_id,created_at,gateway,topic_id,rx_rssi,rx_snr,rx_time,
                                   hop_limit,hops_away,relay_node,transport,extras)
                                  VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)""",
                               BASE + index, created + timedelta(microseconds=gateway_number), gateway, topic_id,
                               fields["rx_rssi"], fields["rx_snr"], fields["rx_time"], fields["hop_limit"],
                               fields["hops_away"], fields["relay_node"], fields["transport"], json.dumps(patch, ensure_ascii=False))
            originals[index].append(copy)
    for index in range(4):
        created = datetime(1901, 1, 15, microsecond=index, tzinfo=timezone.utc)
        await conn.execute("INSERT INTO telemetry(id,from_node_id,message_id,payload,created_at) VALUES($1,$2,$1,$3::jsonb,$4)",
                           BASE + index, NODE, json.dumps({"temperature": 22.75 + index, "label": "Café 🌲"}, ensure_ascii=False), created)
        await conn.execute("INSERT INTO traceroutes(id,from_node_id,message_id,route,route_back_ids,created_at) VALUES($1,$2,$1,'[1,2,3]','[\"aabbccdd\"]',$3)", BASE + index, NODE, created)
        await conn.execute("INSERT INTO chat_messages(id,from_node_id,channel_id,text,created_at) VALUES($1,$2,'0',$3,$4)", BASE + index, NODE, f"中央谷 message {index} 🌲", created)
        await conn.execute("INSERT INTO node_neighborinfo_history(id,node_id,neighbors,created_at) VALUES($1,$2,'[{\"node_id\":\"abcd1234\",\"snr\":3.25}]',$3)", BASE + index, NODE, created)
    return originals


async def _snapshot(conn):
    await conn.execute("SET TIME ZONE 'UTC'")
    rows = {}
    for table in TABLES:
        rows[table] = [dict(row) for row in await conn.fetch(f"SELECT * FROM public.{table} AS t ORDER BY to_jsonb(t)::text")]
    indexes = [dict(row) for row in await conn.fetch("SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='public' ORDER BY tablename,indexname")]
    constraints = [dict(row) for row in await conn.fetch("""
        SELECT t.relname,c.conname,c.contype,pg_get_constraintdef(c.oid) AS definition
        FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid JOIN pg_namespace n ON n.oid=t.relnamespace
        WHERE n.nspname='public' ORDER BY t.relname,c.conname,c.contype
    """)]
    sequences = [dict(row) for row in await conn.fetch("""
        SELECT sequencename,start_value,min_value,max_value,increment_by,cycle,cache_size,last_value
        FROM pg_sequences WHERE schemaname='public' ORDER BY sequencename
    """)]
    return {"rows": rows, "indexes": indexes, "constraints": constraints, "sequences": sequences}


async def _restore_archive(pg, database, *, archive=None, remote_archive=None):
    environment = {**os.environ, "PGPASSWORD": pg["password"]}
    command = ("docker", "exec", "-i", "-e", "PGPASSWORD", PG_CONTAINER, "pg_restore", "--exit-on-error",
               "--no-owner", "--no-privileges", "--host", "127.0.0.1", "--username", pg["username"], "--dbname", database)
    proc = await asyncio.create_subprocess_exec(*command, env=environment, stdin=asyncio.subprocess.PIPE,
                                               stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
    errors = asyncio.create_task(backup._tail(proc.stderr))
    reader = None
    reader_errors = None
    try:
        async with asyncio.timeout(60):
            if remote_archive:
                reader = await asyncio.create_subprocess_exec("docker", "exec", RECEIVER_CONTAINER, "cat", remote_archive,
                                                              stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                reader_errors = asyncio.create_task(backup._tail(reader.stderr))
                while chunk := await reader.stdout.read(backup_remote.CHUNK_BYTES):
                    proc.stdin.write(chunk)
                    await proc.stdin.drain()
                assert await reader.wait() == 0, (await reader_errors).decode(errors="replace")
            else:
                with Path(archive).open("rb") as source:
                    while chunk := source.read(backup_remote.CHUNK_BYTES):
                        proc.stdin.write(chunk)
                        await proc.stdin.drain()
            await backup._close_stdin(proc)
            code = await proc.wait()
            assert code == 0, (await errors).decode(errors="replace")
    finally:
        await backup._stop(reader, drain_stdout=True)
        await backup._stop(proc)
        await asyncio.gather(errors, *([reader_errors] if reader_errors else []), return_exceptions=True)


async def _reconstructed_samples(database, originals):
    storage = PostgresStorage({"server": {"timezone": "UTC"}, "storage": {"postgres": {"enabled": True}}})
    storage.pool = await asyncpg.create_pool(_restore_dsn(database), min_size=1, max_size=1)
    try:
        for index in (0, 31, 32, 63):
            packet = await storage.query_mqtt_message_by_id(BASE + index, include_copies=True)
            assert packet and packet["reception_count"] == 3
            assert [reception["reconstructed"] for reception in packet["receptions"]] == originals[index]
        first = await storage.query_mqtt_messages(limit=7, search="backup-integration")
        second = await storage.query_mqtt_messages(limit=7, search="backup-integration", before=first["next_cursor"])
        assert len(first["messages"]) == len(second["messages"]) == 7
        assert not ({row["mqtt_row_id"] for row in first["messages"]} & {row["mqtt_row_id"] for row in second["messages"]})
    finally:
        await storage.pool.close()


async def _cleanup_receiver(directory):
    assert re.fullmatch(r"/tmp/meshinfo-backup-integration-[a-f0-9]{32}", directory)
    proc = await asyncio.create_subprocess_exec("docker", "exec", RECEIVER_CONTAINER, "python3", "-c",
                                               "import shutil,sys; shutil.rmtree(sys.argv[1], ignore_errors=True)", directory,
                                               stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
    _output, errors = await proc.communicate()
    assert proc.returncode == 0, errors.decode(errors="replace")


@asynccontextmanager
async def _fixture():
    pg = _connection_config()
    source = await asyncpg.connect(DSN, command_timeout=10)
    assert await source.fetchval("SELECT current_database()") == pg["database"]
    restore_name = "meshinfo_backup_" + uuid.uuid4().hex[:12] + "_restore_test"
    assert re.fullmatch(r"meshinfo_backup_[a-f0-9]{12}_restore_test", restore_name)
    created_restore = False
    try:
        size = await source.fetchval("SELECT pg_database_size(current_database())")
        assert size < 64 * 1024 * 1024, "Integration source must remain a tiny isolated database"
        originals = await _seed(source)
        await source.execute(f'CREATE DATABASE "{restore_name}" TEMPLATE template0')
        created_restore = True
        yield source, pg, restore_name, originals
    finally:
        if created_restore:
            await source.execute(f'DROP DATABASE "{restore_name}" WITH (FORCE)')
        await source.execute("DELETE FROM packet_receptions WHERE mqtt_row_id BETWEEN $1 AND $2", BASE, BASE + 9999)
        await source.execute("DELETE FROM mqtt_messages WHERE id BETWEEN $1 AND $2", BASE, BASE + 9999)
        await source.execute("DELETE FROM chat_messages WHERE id BETWEEN $1 AND $2", BASE, BASE + 9999)
        await source.execute("DELETE FROM nodes WHERE id=$1", NODE)
        await source.execute("DELETE FROM mqtt_topics WHERE topic LIKE 'msh/backup-integration/%'")
        for month in (1, 2):
            for table in ("packet_receptions", "mqtt_messages"):
                await source.execute(f"DROP TABLE IF EXISTS {_part(table, month)}")
        await source.close()


async def _round_trip(directory, *, through_receiver, monkeypatch):
    receiver_directory = "/tmp/meshinfo-backup-integration-" + uuid.uuid4().hex
    started = time.monotonic()
    async with _fixture() as (source, pg, restore_name, originals):
        config = {"storage": {"postgres": pg}, "backups": {"mode": "local", "schedule": "off",
                  "dir": str(directory / "local"), "keep_days": 4, "min_free_mb": 1, "timeout_seconds": 120}}
        if through_receiver:
            config["backups"].update(mode="ssh", remote_target="receiver-test:" + receiver_directory)
            receiver_source = Path(backup_remote.__file__).read_text()

            def receiver_command(policy, filename, owner_id):
                return ("docker", "exec", "-i", RECEIVER_CONTAINER, "python3", "-c", receiver_source,
                        policy.remote_directory, filename, str(policy.min_free_bytes),
                        str(policy.remote_keep_days), str(policy.timeout_seconds), owner_id)

            monkeypatch.setattr(backup, "ssh_command", receiver_command)
        expected = await _snapshot(source)
        try:
            result = await backup_database.run_backup(config, ROOT, PG_CONTAINER)
            assert result.successful and result.verification == "archive_decode"
            assert result.bytes > 0
            status = json.loads(await source.fetchval("SELECT result FROM storage_maintenance_status WHERE operation='backup'"))
            assert status["status"] == "complete" and status["last_success_at"]
            identity = await source.fetchval("SELECT instance_id::text FROM storage_identity WHERE singleton")
            expected_owner = hashlib.sha256(identity.encode()).hexdigest()
            if through_receiver:
                assert not (directory / "local").exists(), "Remote stream must not stage a local dump"
                await _restore_archive(pg, restore_name, remote_archive=receiver_directory + "/" + result.filename)
            else:
                archive = Path(result.destination)
                manifest = json.loads(archive.with_name(archive.name + backup_remote.MANIFEST_SUFFIX).read_text())
                assert manifest["owner"] == expected_owner
                assert hashlib.sha256(archive.read_bytes()).hexdigest() == result.sha256
                await _restore_archive(pg, restore_name, archive=archive)
            restored = await asyncpg.connect(_restore_dsn(restore_name), command_timeout=10)
            try:
                actual = await _snapshot(restored)
                assert actual == expected
            finally:
                await restored.close()
            await _reconstructed_samples(restore_name, originals)
            assert await source.fetchval("SELECT count(*) FROM packet_receptions WHERE mqtt_row_id BETWEEN $1 AND $2", BASE, BASE + 9999) == PACKETS * 3
            print(json.dumps({"backup_integration": "docker_receiver_pipe" if through_receiver else "local_host_wrapper",
                              "archive_bytes": result.bytes, "elapsed_seconds": round(time.monotonic() - started, 3),
                              "canonical_packets": PACKETS, "receptions": PACKETS * 3,
                              "compared_indexes": len(expected["indexes"]), "compared_constraints": len(expected["constraints"]),
                              "compared_sequences": len(expected["sequences"]), "restore": "passed"}, sort_keys=True))
        finally:
            if through_receiver:
                await _cleanup_receiver(receiver_directory)


def test_real_host_backup_restores_all_data_schema_and_reconstructed_copies(tmp_path, monkeypatch):
    with tempfile.TemporaryDirectory(prefix="backup-roundtrip-", dir=tmp_path) as directory:
        asyncio.run(_round_trip(Path(directory), through_receiver=False, monkeypatch=monkeypatch))


def test_real_remote_receiver_over_docker_pipe_restores_without_local_staging(tmp_path, monkeypatch):
    with tempfile.TemporaryDirectory(prefix="backup-roundtrip-", dir=tmp_path) as directory:
        asyncio.run(_round_trip(Path(directory), through_receiver=True, monkeypatch=monkeypatch))
