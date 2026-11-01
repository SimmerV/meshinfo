"""Large benchmark replication keeps canonical keys and every linked copy valid."""

import asyncio
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripts import benchmark_archive_workload as workload


HOT = datetime(2020, 2, 1, tzinfo=timezone.utc)


def packet(identity, stamp):
    return (identity, "literal_%", ' {"text":"雪"} ', 0, False, 123, stamp, None, None, None)


def test_replication_preserves_months_raw_payload_and_unique_composite_keys():
    source = [packet(1, HOT-timedelta(microseconds=1)), packet(2, HOT)]
    rows = workload.expanded_packets(source, 3, 2)
    assert len(rows) == len({(row[0], row[6]) for row in rows}) == 6
    assert {row[0] for row in rows} == set(range(1, 7))
    assert all(row[2] == source[0][2] for row in rows)
    assert all(row[6] < HOT for row in rows if row[0] % 2)
    assert all(row[6] >= HOT for row in rows if not row[0] % 2)


def test_receipt_shift_preserves_relative_age_and_cross_month_links(tmp_path):
    source_time = HOT-timedelta(microseconds=1)
    receipt_time = HOT+timedelta(seconds=1)
    receipt = [1, receipt_time.isoformat(), "00000002", 1, None, None, None,
               None, None, None, 0, '{"set":{"nullable":null}}']
    (tmp_path / "receptions.jsonl").write_text(json.dumps(receipt) + "\n")
    rows = list(workload.expanded_receptions(tmp_path, {1: source_time}, 3, 10))
    for replica, row in enumerate(rows):
        assert row[0] == 1+replica*10
        assert row[1]-workload.shifted_time(source_time, replica) == receipt_time-source_time
        assert row[1] >= HOT
        assert row[11] == receipt[11]


def test_orphan_reception_and_bigint_overflow_are_refused(tmp_path):
    receipt = [999, HOT.isoformat(), None, None, None, None, None, None, None, None, None, None]
    (tmp_path / "receptions.jsonl").write_text(json.dumps(receipt) + "\n")
    with pytest.raises(ValueError, match="unlinked"):
        list(workload.expanded_receptions(tmp_path, {}, 1, 10))
    with pytest.raises(ValueError, match="bigint"):
        workload.expanded_packets([packet((1 << 63)-1, HOT)], 2, 1)


def test_synthetic_alignment_moves_only_named_packets_and_keeps_receipt_age(tmp_path):
    source_time = HOT-timedelta(microseconds=1)
    source = [packet(1, source_time), packet(2, source_time)]
    offset = timedelta(days=-31)
    copies = workload.expanded_packets(source, 1, 2, {1: offset})
    by_id = {row[0]: row for row in copies}
    assert by_id[1][6] == source_time+offset
    assert by_id[2] == source[1]
    receipt = [1, (HOT+timedelta(seconds=1)).isoformat(), None, None,
               None, None, None, None, None, None, None, None]
    (tmp_path / "receptions.jsonl").write_text(json.dumps(receipt) + "\n")
    copied, = workload.expanded_receptions(tmp_path, {1: source_time}, 1, 2, {1: offset})
    assert copied[1]-by_id[1][6] == timedelta(seconds=1, microseconds=1)


def test_large_workload_rejects_live_dsn_before_any_database_connection(monkeypatch):
    monkeypatch.setenv("ARCHIVE_WORKLOAD_TEST_DSN", "postgresql://localhost/meshinfo")
    connector = AsyncMock()
    monkeypatch.setattr(workload.asyncpg, "connect", connector)
    with pytest.raises(ValueError, match="NEW dedicated"):
        asyncio.run(workload.run(SimpleNamespace(dsn_env="ARCHIVE_WORKLOAD_TEST_DSN")))
    connector.assert_not_awaited()


def test_metadata_backfill_samples_do_not_change_original_columnar_stripe_samples():
    original = {"rows":[192,192],"wal_bytes":[100,200],"milliseconds":[2,3]}
    report = {"copy_batch_samples":original}
    samples = workload.copy_samples(report)
    samples["topics"] = {"rows":[1000],"wal_bytes":[20],"milliseconds":[1]}
    assert samples["columnar"] is original
    assert report["copy_batch_samples"]["rows"] == [192,192]
    assert workload.distribution(samples["columnar"]["rows"])["median"] == 192


def test_topic_digest_covers_canonical_cold_topics_including_null_not_receptions():
    cold = list(packet(1,HOT-timedelta(microseconds=1)))
    cold[1] = None
    packets = [tuple(cold),packet(2,HOT)]
    expected = workload.digest_records([(1,cold[6],None)],("id","created_at","topic"))
    assert workload.topic_lookup_digest(packets,HOT) == expected


def test_resume_requires_matching_fixture_owned_schemas_and_completed_load(tmp_path):
    digests = {"packets": {"rows": 10, "sha256_sum": "fixed"}}
    report = {"dedicated_target": True, "fixture": {"original_digests": digests, "replicas": 8},
              "packet_schemas": {"heap": "benchmark_heap_full_012345abcd", "columnar": "public"},
              "shared_schema": "benchmark_shared_012345abcd", "source_load": {},
              "shared_storage": {}, "hot_month": "2026-10-01",
              "error": {"phase": "prepare_leaf_indexes"}}
    path = tmp_path / "resume.json"
    path.write_text(json.dumps(report))
    assert workload.resume_report(path, digests, 8, "work_columnar_test") == report
    with pytest.raises(ValueError, match="identity"):
        workload.resume_report(path, digests, 7, "work_columnar_test")
    report["packet_schemas"]["heap"] = "public"
    path.write_text(json.dumps(report))
    with pytest.raises(ValueError, match="owned"):
        workload.resume_report(path, digests, 8, "work_columnar_test")
    report["packet_schemas"]["heap"] = "benchmark_heap_full_012345abcd"
    report["error"]["phase"] = "move_generated_source_to_public"
    path.write_text(json.dumps(report))
    with pytest.raises(ValueError, match="failed phase"):
        workload.resume_report(path, digests, 8, "work_columnar_test")
