"""Columnar backup/restore integrity, ordering and process-failure contracts."""
import asyncio
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import signal
import sys

import pytest

from scripts import restore_database as restore
from storage.backup_remote import (
    BackupError, MANIFEST_SUFFIX, restore_metadata_checksum,
    validate_manifest_restore_metadata, validate_restore_metadata,
)
from storage.columnar_backup import inventory_assertion_sql, options_sql


def metadata():
    return {"schema": 1, "postgres_major": 18, "extension": "14.1-1", "relations": [
        {"schema": "archive_stage", "name": "history_2025_01", "chunk_group_row_limit": 1000,
         "stripe_row_limit": 150000, "compression_level": 7, "compression": "zstd"},
        {"schema": "public", "name": "mqtt_messages_2025_02", "chunk_group_row_limit": 5000,
         "stripe_row_limit": 200000, "compression_level": 3, "compression": "lz4"}]}


@pytest.mark.parametrize("change", [
    lambda m: m.update(schema=True),
    lambda m: m.update(extension=None),
    lambda m: m.update(relations=m["relations"] * 2),
    lambda m: m["relations"][0].update(compression="unknown"),
    lambda m: m["relations"][0].update(compression_level=True),
    lambda m: m["relations"][0].update(stripe_row_limit=0),
    lambda m: m["relations"][0].update(chunk_group_row_limit=None),
    lambda m: m["relations"][0].update(name="x\x00y"),
    lambda m: m["relations"][0].update(extra_sql="DROP TABLE precious"),
])
def test_metadata_rejects_missing_unsupported_or_ambiguous_values(change):
    value = metadata()
    change(value)
    with pytest.raises(BackupError):
        validate_restore_metadata(value)


def test_checksum_binds_all_relation_options_and_rejects_missing_metadata():
    value = metadata()
    manifest = {"restore_metadata": value, "restore_metadata_sha256": restore_metadata_checksum(value)}
    assert validate_manifest_restore_metadata(manifest) == value
    value["relations"][0]["compression_level"] += 1
    with pytest.raises(BackupError, match="checksum"):
        validate_manifest_restore_metadata(manifest)
    with pytest.raises(BackupError, match="missing"):
        validate_manifest_restore_metadata({"restore_metadata_sha256": "a" * 64})
    assert validate_manifest_restore_metadata({"schema": 1}) is None


def test_sql_quotes_identifiers_and_applies_exact_settings_for_stage_and_attached_tables():
    value = metadata()
    value["relations"][0]["name"] = 'table";SELECT dangerous;--'
    text = options_sql(value)
    assert '"archive_stage"."table"";SELECT dangerous;--"' in text
    assert "columnar.chunk_group_row_limit=1000" in text
    assert "columnar.compression_level=7" in text
    assert "columnar.compression='lz4'" in text
    assertion = inventory_assertion_sql(value)
    assert 'SELECT dangerous' not in assertion  # JSON travels as hex, never SQL.
    assert "EXCEPT SELECT * FROM required" in assertion
    assert "EXCEPT SELECT * FROM actual" in assertion


@pytest.fixture
def clients(tmp_path, monkeypatch):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    target = {"postgres_major": 18, "objects": 0, "preload": "citus_columnar",
              "installed": None, "available": ["14.1-1"]}
    monkeypatch.setenv("RESTORE_TEST_TARGET", json.dumps(target))
    monkeypatch.setenv("RESTORE_TEST_EVENTS", str(tmp_path / "events"))
    monkeypatch.setenv("RESTORE_TEST_SQL", str(tmp_path / "received.sql"))
    program = r'''import json,os,signal,sys,time
from pathlib import Path
name = Path(sys.argv[0]).name
if name == 'pg_restore':
    archive = Path(sys.argv[-1]).read_bytes()
    if not archive.startswith(b'PGDMP'):
        raise SystemExit(9)
    if '--file=/dev/null' in sys.argv:
        raise SystemExit(0)
    section = next(arg.split('=',1)[1] for arg in sys.argv if arg.startswith('--section='))
    if section == 'pre-data':
        if os.getenv('RESTORE_TEST_COLUMNAR', '1') == '1':
            print('SET default_table_access_method = columnar;')
        print('CREATE TABLE archive_stage.history_2025_01 (id bigint);')
    elif section == 'data':
        print('COPY archive_stage.history_2025_01 (id) FROM stdin;\n1\n\\.')
    else:
        print('CREATE INDEX sample_index ON archive_stage.history_2025_01(id);')
    sys.stdout.flush()
    if os.getenv('RESTORE_TEST_FAIL_SECTION') == section:
        raise SystemExit(12)
    raise SystemExit(0)
if '--single-transaction' not in sys.argv:
    sys.stdin.read()
    print(os.environ['RESTORE_TEST_TARGET'])
    raise SystemExit(0)
events=Path(os.environ['RESTORE_TEST_EVENTS'])
def stopped(*_args):
    events.write_text('rolled_back')
    raise SystemExit(17)
signal.signal(signal.SIGTERM, stopped)
with open(os.environ['RESTORE_TEST_SQL'], 'w') as output:
    for line in sys.stdin:
        output.write(line)
        output.flush()
        if os.getenv('RESTORE_TEST_FAIL_SQL') and os.environ['RESTORE_TEST_FAIL_SQL'] in line:
            stopped()
events.write_text('committed')
'''
    for name in ("psql", "pg_restore"):
        path = bindir / name
        path.write_text(f"#!{sys.executable}\n" + program)
        path.chmod(0o700)
    monkeypatch.setenv("PATH", str(bindir) + os.pathsep + os.environ["PATH"])
    config = tmp_path / "config.toml"
    config.write_text('[storage.postgres]\nhost="example.invalid"\nusername="postgres"\npassword="private-password"\ndatabase="source"\n')
    archive = tmp_path / "fixture.dump"
    archive.write_bytes(b"PGDMPsmall-test-archive")
    value = metadata()
    manifest = {"schema": 1, "filename": archive.name, "verification": "archive_decode",
                "bytes": archive.stat().st_size, "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                "restore_metadata": value, "restore_metadata_sha256": restore_metadata_checksum(value)}
    marker = archive.with_name(archive.name + MANIFEST_SUFFIX)
    marker.write_text(json.dumps(manifest))
    args = restore.parser().parse_args([str(archive), "--config", str(config), "--database", "isolated_restore_test"])
    return args, marker, target


def test_default_verifies_without_restoring_or_exposing_credentials(tmp_path, clients):
    args, _, _ = clients
    result = asyncio.run(restore.run(args))
    assert result["status"] == "verified" and not result["data_loaded"]
    assert result["columnar_relations"] == 2
    assert not (tmp_path / "events").exists()
    assert "private-password" not in json.dumps(result)


def test_options_are_applied_before_first_copy_in_single_restore_transaction(tmp_path, clients):
    args, _, _ = clients
    args.apply = True
    result = asyncio.run(restore.run(args))
    text = (tmp_path / "received.sql").read_text()
    assert text.index('CREATE TABLE') < text.index('ALTER TABLE "archive_stage"') < text.index('COPY ')
    assert text.index("Columnar options do not match") < text.index('COPY ')
    assert 'columnar.compression_level=7' in text
    assert (tmp_path / "events").read_text() == "committed"
    assert result["status"] == "restored"


def test_missing_columnar_manifest_fails_before_destination_mutation(tmp_path, clients):
    args, marker, _ = clients
    marker.unlink()
    args.apply = True
    with pytest.raises(restore.RestoreRefused, match="missing"):
        asyncio.run(restore.run(args))
    assert not (tmp_path / "events").exists()


def test_legacy_heap_dump_without_manifest_remains_restorable(tmp_path, clients, monkeypatch):
    args, marker, _ = clients
    marker.unlink()
    monkeypatch.setenv("RESTORE_TEST_COLUMNAR", "0")
    args.apply = True
    result = asyncio.run(restore.run(args))
    assert result["status"] == "restored" and not result["manifest_verified"]
    # Even legacy restores reject any actual Columnar table before COPY.
    assert "Columnar table inventory does not match" in (tmp_path / "received.sql").read_text()


@pytest.mark.parametrize("change", [
    lambda target: target.update(objects=1),
    lambda target: target.update(postgres_major=17),
    lambda target: target.update(available=[]),
    lambda target: target.update(installed="14.0-1"),
    lambda target: target.update(preload=""),
])
def test_destination_prerequisites_fail_before_restore(tmp_path, clients, monkeypatch, change):
    args, _, target = clients
    change(target)
    monkeypatch.setenv("RESTORE_TEST_TARGET", json.dumps(target))
    args.apply = True
    with pytest.raises(restore.RestoreRefused):
        asyncio.run(restore.run(args))
    assert not (tmp_path / "events").exists()


def test_archive_checksum_mismatch_fails_before_any_destination_process(tmp_path, clients):
    args, _, _ = clients
    args.archive.write_bytes(b"PGDMPmodified-test-archive")
    args.apply = True
    with pytest.raises(restore.RestoreRefused, match="checksum"):
        asyncio.run(restore.run(args))
    assert not (tmp_path / "events").exists()


def test_producer_failure_never_gives_psql_clean_eof_to_commit(tmp_path, clients, monkeypatch):
    args, _, _ = clients
    args.apply = True
    monkeypatch.setenv("RESTORE_TEST_FAIL_SECTION", "data")
    with pytest.raises(restore.RestoreRefused, match="rolled back"):
        asyncio.run(restore.run(args))
    assert (tmp_path / "events").read_text() == "rolled_back"


def test_sql_error_rolls_back_entire_restore(tmp_path, clients, monkeypatch):
    args, _, _ = clients
    args.apply = True
    monkeypatch.setenv("RESTORE_TEST_FAIL_SQL", "ALTER TABLE")
    with pytest.raises(restore.RestoreRefused, match="rolled back"):
        asyncio.run(restore.run(args))
    assert (tmp_path / "events").read_text() == "rolled_back"
