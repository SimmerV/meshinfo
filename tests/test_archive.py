"""Bounded archive policy, cursor selection, and failure-marker unit checks."""

import asyncio
from datetime import datetime, timezone
import json
import uuid

import pytest

from storage.db.archive import (
    ArchiveBlocked, ArchivePolicy, _batch, _exact_equal, _free, _guarded,
    _recover_inflight, archive_policy, _copy_batch_policy, _rebind_job_identity,
)


def test_archive_is_opt_in_and_rejects_unsafe_resource_policies():
    assert archive_policy({}).enabled is False
    assert archive_policy({}).after_months == 6
    for settings in ({"enabled": 1}, {"reserve_mib": 10}, {"batch_rows": 1000000},
                     {"cycle_seconds": 20}, {"estimate_multiplier": .5},
                     {"batch_mib": 64, "max_wal_mib": 16}, {"typo": 1}):
        with pytest.raises(ValueError):
            archive_policy({"storage": {"archive": settings}})


def test_database_filesystem_probe_is_required_and_floor_is_reserved():
    policy = ArchivePolicy()
    with pytest.raises(ArchiveBlocked, match="verified free-space"):
        _free(None, policy)
    with pytest.raises(ArchiveBlocked, match="needs"):
        _free(lambda: policy.reserve_bytes+999, policy, 1000)
    assert _free(lambda: policy.reserve_bytes+1000, policy, 1000) == policy.reserve_bytes+1000
    with pytest.raises(ArchiveBlocked, match="invalid"):
        _free(lambda: True, policy)


def test_chunk_boundary_never_skips_the_first_nonfitting_row():
    stamp = datetime(1901, 1, 1, tzinfo=timezone.utc)

    class Connection:
        async def fetch(self, query, *arguments):
            assert "ORDER BY id,created_at" in query
            assert arguments == (None, None, 1000)
            return [{"id": 1, "created_at": stamp, "logical_bytes": 700000},
                    {"id": 2, "created_at": stamp, "logical_bytes": 500000},
                    {"id": 3, "created_at": stamp, "logical_bytes": 1}]

    rows, size = asyncio.run(_batch(Connection(), "mqtt_messages_1901_01", None, None,
                                    ArchivePolicy(batch_mib=1)))
    assert [row["id"] for row in rows] == [1]
    assert size == 700000


def test_oversized_single_row_blocks_instead_of_silently_skipping_it():
    class Connection:
        async def fetch(self, *arguments):
            return [{"id": 1, "created_at": None, "logical_bytes": 2*1024*1024}]

    with pytest.raises(ArchiveBlocked, match="One history row"):
        asyncio.run(_batch(Connection(), "example", None, None, ArchivePolicy(batch_mib=1)))


def test_topic_index_budget_bounds_prefix_and_refuses_one_oversized_topic():
    policy = ArchivePolicy(batch_mib=1,max_wal_mib=16)

    class Connection:
        large = False

        async def fetch(self,*arguments):
            return [{"id":1,"created_at":None,"logical_bytes":100,
                     "topic_index_budget":policy.max_wal_bytes if self.large else 3*1024**2},
                    {"id":2,"created_at":None,"logical_bytes":100,"topic_index_budget":2*1024**2},
                    {"id":3,"created_at":None,"logical_bytes":100,"topic_index_budget":1}]

    conn = Connection()
    rows,size = asyncio.run(_batch(conn,"example",None,None,policy,build_topic_index=True))
    assert [row["id"] for row in rows] == [1] and size == 100
    conn.large = True
    with pytest.raises(ArchiveBlocked,match="GIN/WAL budget"):
        asyncio.run(_batch(conn,"example",None,None,policy,build_topic_index=True))


def test_pilot_and_worker_share_the_measured_row_budget():
    bounded,gin_budget = _copy_batch_policy(ArchivePolicy(),11)
    assert bounded.batch_rows == 93  # independently observed in the full workload
    assert gin_budget == 16*1024**2
    tiny,_ = _copy_batch_policy(ArchivePolicy(batch_rows=3),11)
    assert tiny.batch_rows == 3
    with pytest.raises(ArchiveBlocked,match="index-write estimate"):
        _copy_batch_policy(ArchivePolicy(),1000000)


def test_exact_comparison_includes_all_columns_and_uses_binary_text_equality():
    sql = _exact_equal()
    for name in ("payload", "topic", "from_node_id", "to_node_id"):
        assert f'convert_to(s."{name}"::text' in sql
    assert 's."retain" IS NOT DISTINCT FROM t."retain"' in sql
    assert 's."timestamp" IS NOT DISTINCT FROM t."timestamp"' in sql
    assert sql.count("IS NOT DISTINCT FROM") == 10


def test_freeze_guards_require_always_mode_all_events_and_exact_job_identity():
    job = uuid.uuid4()

    class Connection:
        enabled = "A"

        async def fetch(self, *arguments):
            return [{"tgname": name, "tgtype": kind, "tgenabled": self.enabled,
                     "tgargs": str(job).encode()+b"\0"}
                    for name, kind in (("meshinfo_archive_freeze", 31), ("meshinfo_archive_freeze_truncate", 34))]

    conn = Connection()
    assert asyncio.run(_guarded(conn, 123, job))
    assert not asyncio.run(_guarded(conn, 123, uuid.uuid4()))
    conn.enabled = "O"
    assert not asyncio.run(_guarded(conn, 123, job))


def test_interrupted_copy_accounts_retained_physical_space_once():
    job = {"job_id": uuid.uuid4(), "stage_name": "owned_stage", "inflight_bytes": 8192,
           "aborted_bytes": 0, "abort_count": 0}

    class Connection:
        calls = []

        async def fetchval(self, *arguments):
            return 32768

        async def execute(self, query, *arguments):
            self.calls.append(arguments)

    conn = Connection()
    asyncio.run(_recover_inflight(conn, job, ArchivePolicy()))
    asyncio.run(_recover_inflight(conn, job, ArchivePolicy()))
    assert len(conn.calls) == 1
    assert job["aborted_bytes"] == 24576 and job["abort_count"] == 1
    assert job["inflight_bytes"] is None


def test_identity_rebind_invalidates_statistics_even_when_only_topic_oid_changed():
    class Connection:
        calls = []

        async def execute(self, query, *arguments):
            self.calls.append((query,arguments))

    job = {"job_id":uuid.uuid4(),"source_oid":10,"stage_oid":20,"topic_stage_oid":30,
           "analyzed":True,"stage_analyzed":True,"topic_analyzed":True}
    conn = Connection()
    asyncio.run(_rebind_job_identity(conn,job,10,20,30))
    assert not conn.calls and job["analyzed"]
    asyncio.run(_rebind_job_identity(conn,job,10,20,29))
    assert len(conn.calls) == 1 and conn.calls[0][1] == (job["job_id"],10,20)
    assert not any(job[key] for key in ("analyzed","stage_analyzed","topic_analyzed"))
