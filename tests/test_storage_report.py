"""Catalog-only reporting must remain safe on partial and legacy schemas."""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json

import pytest

from storage.db.storage_report import build_storage_report, monthly_partition_bounds, public_history_report

NOW = datetime(2026, 10, 27, tzinfo=timezone.utc)
CONFIG = {"storage": {"retention": {"enabled": True, "months": 3}}}


def catalog_row(parent, *, month=None, kind=None, bound=None, heap=8192, rows=10, size=16384):
    is_parent = month is None and bound is None
    if month:
        next_month = datetime.fromisoformat(month).replace(day=28)
        from datetime import timedelta
        next_month = (next_month + timedelta(days=4)).replace(day=1).date().isoformat()
        bound = f"FOR VALUES FROM ('{month} 00:00:00+00') TO ('{next_month} 00:00:00+00')"
    return {
        "parent_name": parent, "partition_key": "RANGE (created_at)", "is_parent": is_parent,
        "relation_oid": 123, "schema_name": "public",
        "relation_name": parent if is_parent else f"{parent}_{month or 'default'}",
        "relation_kind": kind or ("p" if is_parent else "r"),
        "partition_bound": bound, "total_bytes": 0 if is_parent and kind != "r" else size,
        "table_bytes": 0 if is_parent and kind != "r" else size - 4096,
        "index_bytes": 0 if is_parent and kind != "r" else 4096,
        "heap_bytes": 0 if is_parent and kind != "r" else heap, "estimated_rows": rows,
    }


def paired_catalog():
    result = []
    for parent in ("mqtt_messages", "packet_receptions"):
        result.extend([
            catalog_row(parent),
            catalog_row(parent, month="2026-06-01"),
            catalog_row(parent, month="2026-07-01"),
            catalog_row(parent, month="2026-10-01"),
            catalog_row(parent, month="2026-11-01", heap=0, rows=-1),
            catalog_row(parent, bound="DEFAULT", heap=0, rows=-1),
        ])
    for parent in ("telemetry", "chat_messages", "traceroutes", "node_neighborinfo_history"):
        result.append(catalog_row(parent, kind="r"))
    return result


class FakeConnection:
    def __init__(self, catalog, *, error=None):
        self.catalog = catalog
        self.error = error
        self.commands = []
        self.transaction_options = None
        self.active = False
        self.closed = False

    @asynccontextmanager
    async def transaction(self, **kwargs):
        self.transaction_options = kwargs
        self.active = True
        try:
            yield
        finally:
            self.active = False
            self.closed = True

    async def execute(self, query):
        assert self.active
        self.commands.append(query)

    async def fetch(self, query, names):
        assert self.active
        self.commands.append(query)
        assert "COUNT(" not in query.upper()
        assert set(names) == {"mqtt_messages", "packet_receptions", "telemetry", "chat_messages", "traceroutes", "node_neighborinfo_history", "mqtt_archive_topics"}
        if self.error:
            raise self.error
        return self.catalog

    async def fetchval(self, query, *, timeout=None):
        assert self.active
        self.commands.append(query)
        if "to_regclass" in query:
            return None
        return 1_000_000


class FakePool:
    def __init__(self, connection):
        self.connection = connection
        self.timeout = None
        self.released = False

    @asynccontextmanager
    async def acquire(self, *, timeout):
        self.timeout = timeout
        try:
            yield self.connection
        finally:
            self.released = True


def report(catalog=None, config=CONFIG, **kwargs):
    conn = FakeConnection(catalog if catalog is not None else paired_catalog())
    return asyncio.run(build_storage_report(conn, config, now=NOW, **kwargs)), conn


def test_only_whole_expired_paired_partitions_are_candidates():
    output, conn = report()
    packets = output["details"]["scopes"]["packets"]
    candidates = [r for r in packets["relations"] if r["eligible"]]
    assert len(candidates) == 2
    assert all(r["start"] == "2026-06-01T00:00:00Z" for r in candidates)
    assert packets["candidate_partition_bytes"] == 32768
    assert packets["reclaimable_bytes"] is None
    assert packets["layout_issues"] == []
    assert output["details"]["database_bytes"] == 1_000_000
    assert output["public"]["enforcement_enabled"] is False
    assert output["public"]["scopes"]["packets"]["effective_cutoff"] == "2026-07-01T00:00:00Z"
    assert output["details"]["scopes"]["telemetry"]["candidate_partition_bytes"] == 0
    assert conn.transaction_options == {"isolation": "repeatable_read", "readonly": True}
    assert "SET LOCAL statement_timeout = '3s'" in conn.commands
    assert "SET LOCAL lock_timeout = '250ms'" in conn.commands
    assert conn.closed


def test_topic_lookup_storage_is_counted_once_without_becoming_a_retention_scope():
    before, _ = report()
    catalog = paired_catalog() + [catalog_row('mqtt_archive_topics'),
                                catalog_row('mqtt_archive_topics', month='2026-06-01')]
    after, _ = report(catalog)
    lookup = after['details']['archive']['topic_lookup']
    assert lookup['total_bytes'] == 16384
    assert lookup['index_bytes'] == 4096
    assert after['details']['history_bytes'] == before['details']['history_bytes'] + 16384
    assert after['details']['scopes'] == before['details']['scopes']
    assert 'topic_lookup' not in after['public']['archive']
    assert 'mqtt_archive_topics' not in json.dumps(after['public'])


def test_bounds_exclude_empty_future_partitions_and_identify_approximation():
    output, _ = report()
    packets = output["public"]["scopes"]["packets"]
    assert packets["oldest_available"] == "2026-06-01T00:00:00Z"
    assert packets["newest_available"] == "2026-11-01T00:00:00Z"
    assert packets["available_bounds_kind"] == "partition"
    assert output["public"]["scopes"]["telemetry"]["available_bounds_kind"] == "unknown"


def test_public_report_contains_no_operator_capacity_or_config_secrets():
    cfg = {"storage": {"retention": {"months": 3}, "postgres": {"password": "sekrit", "host": "private-host"}}, "backups": {"dir": "/private/backup/path"}}
    conn = FakeConnection(paired_catalog())
    public = asyncio.run(public_history_report(conn, cfg, now=NOW))
    encoded = json.dumps(public)
    for secret in ("sekrit", "private-host", "/private", "database_bytes", "relation_name", "oid"):
        assert secret not in encoded


def test_guarded_admission_audit_is_operator_only_and_legacy_columns_are_optional():
    audit = {'admission_mode': 'guarded', 'admission_free_bytes': 12_000_000_000,
             'estimated_stage_bytes': 17_000_000_000, 'cleanup_requested': True,
             'cleanup_stage_dropped': True, 'cleanup_topics_dropped': False,
             'cleanup_terminal_phase': 'cancelled',
             'cleanup_reason': 'Actual reserve refused further work', 'source_name': 'private_month'}

    class ArchiveConnection(FakeConnection):
        async def fetchval(self, query, *, timeout=None):
            assert self.active
            self.commands.append(query)
            if "to_regclass('public.storage_archive_jobs')" in query:
                return 123
            if "'journal_bytes'" in query:
                # Old journals lack admission/cleanup columns. The reader must
                # extract those optional fields from the row JSON, not name
                # missing SQL columns or mutate a read-only report session.
                for field in ('admission_mode', 'admission_free_bytes', 'cleanup_requested', 'cleanup_reason',
                              'cleanup_stage_dropped', 'cleanup_topics_dropped', 'cleanup_terminal_phase'):
                    assert "to_jsonb(storage_archive_jobs)->>'" + field + "'" in query
                return json.dumps({'jobs': [audit], 'staging_bytes': 1024,
                                   'metadata_bytes': 2048, 'journal_bytes': 4096})
            if 'to_regclass' in query:
                return None
            return 1_000_000

    conn = ArchiveConnection(paired_catalog())
    output = asyncio.run(build_storage_report(conn, CONFIG, now=NOW))
    assert output['details']['archive']['jobs'] == [audit]
    public = json.dumps(output['public'])
    for private in (*audit.keys(), 'private_month', '12000000000', '17000000000'):
        assert private not in public


def test_unknown_statistics_are_not_reported_as_zero():
    catalog = paired_catalog()
    next(r for r in catalog if r["parent_name"] == "telemetry")["estimated_rows"] = -1
    output, _ = report(catalog)
    assert output["details"]["scopes"]["telemetry"]["estimated_rows"] is None
    assert output["details"]["scopes"]["packets"]["estimated_rows"] == 60


def test_legacy_unpartitioned_packet_tables_never_become_drop_candidates():
    output, _ = report([catalog_row("mqtt_messages", kind="r"), catalog_row("packet_receptions", kind="r")])
    packets = output["details"]["scopes"]["packets"]
    assert packets["candidate_partition_bytes"] == 0
    assert packets["layout_issues"]
    assert output["public"]["scopes"]["packets"]["available_bounds_kind"] == "unknown"
    assert all(not r["eligible"] for r in packets["relations"])


def test_default_partition_with_allocated_pages_blocks_estimated_reclamation():
    catalog = paired_catalog()
    row = next(r for r in catalog if r["parent_name"] == "mqtt_messages" and r["partition_bound"] == "DEFAULT")
    row["heap_bytes"] = 8192
    output, _ = report(catalog)
    packets = output["details"]["scopes"]["packets"]
    assert packets["candidate_partition_bytes"] == 0
    assert any("default" in issue for issue in packets["layout_issues"])
    assert output["public"]["scopes"]["packets"]["available_bounds_kind"] == "unknown"


def test_unpaired_old_partition_is_not_a_candidate():
    catalog = [r for r in paired_catalog() if r["relation_name"] != "packet_receptions_2026-06-01"]
    output, _ = report(catalog)
    assert output["details"]["scopes"]["packets"]["candidate_partition_bytes"] == 0
    row = next(r for r in output["details"]["scopes"]["packets"]["relations"] if r["name"] == "mqtt_messages_2026-06-01")
    assert row["eligibility"] == "unpaired_partition"


@pytest.mark.parametrize("mutation", ["partition_key", "non_monthly", "foreign", "nested"])
def test_unsupported_layout_is_excluded(mutation):
    catalog = paired_catalog()
    if mutation == "partition_key":
        catalog[0]["partition_key"] = "RANGE (timestamp)"
    elif mutation == "non_monthly":
        catalog[1]["partition_bound"] = "FOR VALUES FROM ('2026-06-02 00:00:00+00') TO ('2026-07-01 00:00:00+00')"
    else:
        catalog[1]["relation_kind"] = "f" if mutation == "foreign" else "p"
    output, _ = report(catalog)
    assert output["details"]["scopes"]["packets"]["candidate_partition_bytes"] == 0
    assert output["details"]["scopes"]["packets"]["layout_issues"]


def test_forever_preview_proposes_no_expiration():
    output, _ = report(config={})
    assert output["details"]["candidate_partition_bytes"] == 0
    assert all(s["months"] == 0 and s["effective_cutoff"] is None for s in output["public"]["scopes"].values())


def test_missing_tables_are_explicit_and_do_not_break_reporting():
    output, _ = report([])
    assert output["details"]["history_bytes"] == 0
    assert all(s["layout_issues"] for s in output["details"]["scopes"].values())
    assert all(s["oldest_available"] is None for s in output["public"]["scopes"].values())


def test_pool_acquisition_is_bounded_and_released_on_success():
    pool = FakePool(FakeConnection(paired_catalog()))
    output = asyncio.run(build_storage_report(pool, CONFIG, now=NOW))
    assert output["details"]["history_bytes"] > 0
    assert pool.timeout == 3.0 and pool.released


@pytest.mark.parametrize("error", [TimeoutError("statement timeout"), asyncio.CancelledError()])
def test_errors_and_cancellation_propagate_and_release_resources(error):
    conn = FakeConnection([], error=error)
    pool = FakePool(conn)
    with pytest.raises(type(error)):
        asyncio.run(build_storage_report(pool, CONFIG, now=NOW))
    assert conn.closed and pool.released


@pytest.mark.parametrize("expression", [
    None, "DEFAULT", "FOR VALUES FROM (MINVALUE) TO (MAXVALUE)",
    "FOR VALUES FROM ('2026-06-01') TO ('2026-07-01')",  # timezone absent
    "FOR VALUES FROM ('2026-06-01 00:00:00+00') TO ('2026-08-01 00:00:00+00')",
    "FOR VALUES FROM ('2026-06-01 00:00:00+01') TO ('2026-07-01 00:00:00+01')",
    "FOR VALUES FROM ('2026-06-01 00:00:00+00', 1) TO ('2026-07-01 00:00:00+00', 2)",
    "FOR VALUES FROM ('oops') TO ('2026-07-01 00:00:00+00')",
])
def test_nonmonthly_or_unrecognized_bounds_are_rejected(expression):
    assert monthly_partition_bounds(expression) is None


def test_partition_bounds_are_independent_of_session_timezone_and_month_names():
    bounds = monthly_partition_bounds("FOR VALUES FROM ('2025-12-31 16:00:00-08') TO ('2026-01-31 16:00:00-08')")
    assert bounds == (datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 2, 1, tzinfo=timezone.utc))
