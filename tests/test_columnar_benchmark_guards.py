"""The isolated prototype refuses unsafe targets and misleading fixtures."""

import asyncio
from datetime import datetime, timezone
import json
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripts import benchmark_columnar_history as benchmark


@pytest.mark.parametrize("dsn", [
    "postgresql://localhost/meshinfo",
    "postgresql://localhost/meshinfo_columnar_test_extra",
    "postgresql://localhost/meshinfo%2F_columnar_test",
    "",
])
def test_nonisolated_target_rejected_before_fixture_or_connection(dsn, monkeypatch):
    monkeypatch.setenv("BENCHMARK_TEST_DSN", dsn)
    connector = AsyncMock()
    monkeypatch.setattr(benchmark.asyncpg, "connect", connector)
    args = SimpleNamespace(dsn_env="BENCHMARK_TEST_DSN")
    with pytest.raises(ValueError, match="dedicated"):
        asyncio.run(benchmark.run(args))
    connector.assert_not_awaited()


def fixture(tmp_path):
    manifest = {
        "version": 1,
        "packet_columns": benchmark.MQTT_COLUMNS,
        "reception_columns": benchmark.RECEPTION_COLUMNS,
        "topic_columns": benchmark.TOPIC_COLUMNS,
        "hot_month": "2026-10-01",
    }
    raw = '{ "value": "caf\u00e9", "from": 12 }\n'
    packets = [
        [1, "topic", raw, 0, False, 123, "2026-09-30T23:59:59.123456Z", None, None, None],
        [2, "topic", "[1,2]", 0, False, 456, "2026-10-01T00:00:00Z", None, None, None],
    ]
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    (tmp_path / "packets.jsonl").write_text("".join(json.dumps(row) + "\n" for row in packets))
    (tmp_path / "receptions.jsonl").write_text("")
    (tmp_path / "topics.jsonl").write_text("")
    (tmp_path / "trigger.sql").write_text("-- mqtt_messages_extract_node_ids test fixture")
    return packets


def test_fixture_preserves_raw_payload_historical_nulls_and_microseconds(tmp_path):
    original = fixture(tmp_path)
    _, packets, digests, _, _, _ = benchmark.load_fixture(tmp_path)
    assert packets[0][2] == original[0][2]
    assert packets[0][7:] == (None, None, None)
    assert packets[0][6].microsecond == 123456
    assert digests["packets"]["rows"] == 2


def test_oversized_fixture_rejected_before_parsing_records(tmp_path, monkeypatch):
    fixture(tmp_path)
    monkeypatch.setattr(benchmark, "MAX_FIXTURE_BYTES", 1)
    with pytest.raises(ValueError, match="input budget"):
        benchmark.load_fixture(tmp_path)


def test_naive_fixture_time_refused(tmp_path):
    packets = fixture(tmp_path)
    packets[0][6] = "2026-09-30T23:59:59"
    (tmp_path / "packets.jsonl").write_text("".join(json.dumps(row) + "\n" for row in packets))
    with pytest.raises(ValueError, match="timezone"):
        benchmark.load_fixture(tmp_path)


def test_duplicate_deep_link_ids_refused_even_across_partition_months(tmp_path):
    packets = fixture(tmp_path)
    packets[1][0] = packets[0][0]
    (tmp_path / "packets.jsonl").write_text("".join(json.dumps(row) + "\n" for row in packets))
    with pytest.raises(ValueError, match="distinct deep-link IDs"):
        benchmark.load_fixture(tmp_path)


def test_jsonb_value_comparison_preserves_duplicate_multiplicity_and_raw_text():
    first, second = benchmark.MultisetDigest(), benchmark.MultisetDigest()
    columns = ("payload", "extras")
    first.add(benchmark.logical_row((' {"a":1} ', '{"b": 2, "a": 1}'), columns))
    second.add(benchmark.logical_row((' {"a":1} ', '{"a":1,"b":2}'), columns))
    assert first.result() == second.result()
    second.add(benchmark.logical_row((' {"a":1} ', '{"a":1,"b":2}'), columns))
    assert first.result() != second.result()
    changed_text = benchmark.MultisetDigest()
    changed_text.add(benchmark.logical_row(('{"a":1}', '{"a":1,"b":2}'), columns))
    assert first.result() != changed_text.result()


def test_index_inventory_preserves_partial_expression_and_sort_order():
    indexes = [{"name": "source_index", "primary": False, "method": "btree",
                "definition": "CREATE INDEX source_index ON ONLY public.mqtt_messages USING btree (from_node_id, packet_id, created_at DESC) WHERE (packet_id IS NOT NULL)"}]
    spec, = benchmark.index_specifications({"source_packet_indexes": indexes})
    assert spec["tail"] == "(from_node_id, packet_id, created_at DESC) WHERE (packet_id IS NOT NULL)"
    indexes[0]["definition"] += "; DROP TABLE mqtt_messages"
    with pytest.raises(ValueError, match="Unrecognized"):
        benchmark.index_specifications({"source_packet_indexes": indexes})


def test_timeout_is_a_reported_failure_and_does_not_skip_other_queries():
    class Connection:
        def __init__(self):
            self.schema = None
            self.calls = []

        async def execute(self, query, path):
            self.schema = path.split(",")[0].strip('"')

        async def fetch(self, query, **kwargs):
            self.calls.append((self.schema, query))
            if self.schema == "columnar" and query == "slow":
                raise TimeoutError()
            return [(1, "exact payload")]

        async def fetchval(self, *args, **kwargs):
            return '[{"Plan":{"Node Type":"Seq Scan"}}]'

    conn = Connection()
    results = asyncio.run(benchmark.measure_queries(
        conn, {name: name for name in benchmark.VARIANTS}, "shared",
        [("slow_case", "slow", ()), ("other_case", "fast", ())],
        3, datetime(2026, 10, 1, tzinfo=timezone.utc)))
    failed = results["columnar"]["slow_case"]
    assert failed["status"] == "failed"
    assert failed["error"]["category"] == "TimeoutError"
    assert failed["result"] is None
    assert failed["warm_median_ms"] is None
    assert conn.calls.count(("columnar", "slow")) == 1
    assert results["columnar"]["other_case"]["status"] == "passed"
    assert results["heap_full"]["slow_case"]["successful_executions"] == 4


def test_failed_query_run_still_writes_report_and_exits_nonzero(tmp_path, monkeypatch):
    output = tmp_path / "result.json"
    report = {"all_checks_passed": False, "savings": {},
              "query_failures": [{"variant": "columnar", "case": "slow_case"}]}
    monkeypatch.setattr(benchmark, "run", AsyncMock(return_value=report))
    monkeypatch.setattr(sys, "argv", ["benchmark", "--dsn-env", "TARGET_DSN",
        "--fixture-dir", str(tmp_path), "--output", str(output)])
    assert benchmark.main() == 2
    assert json.loads(output.read_text()) == report


@pytest.mark.parametrize("original_succeeds", [False, True])
def test_candidate_cannot_claim_parity_without_matching_a_completed_original(original_succeeds):
    async def fetch(query, **kwargs):
        if query == "original":
            if not original_succeeds:
                raise TimeoutError()
            return [(1,)]
        return [(2,)]

    connection = SimpleNamespace(execute=AsyncMock(), fetch=fetch,
        fetchval=AsyncMock(return_value='[{"Plan":{"Node Type":"Seq Scan"}}]'))
    results = asyncio.run(benchmark.measure_queries(
        connection, {name: name for name in benchmark.VARIANTS}, "shared",
        [("reception_topic_search", "original", ()),
         ("reception_topic_search_topic_preselect", "candidate", ())],
        3, datetime(2026, 10, 1, tzinfo=timezone.utc), 2))
    for measured in results.values():
        candidate = measured["reception_topic_search_topic_preselect"]
        if original_succeeds:
            assert candidate["status"] == "failed"
            assert candidate["error"]["phase"] == "rewrite_parity"
        else:
            assert candidate["status"] == "passed"
            assert candidate["original_query_parity"] == "unavailable_original_failed"


def test_gin_normalization_is_schema_bounded_quotes_names_and_reports_compaction():
    schema = "benchmark_heap_full_012345abcd"
    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=[{"relname": 'leaf"gin', "bytes": 1048576}]),
        execute=AsyncMock(), fetchval=AsyncMock(return_value=32768))
    report = asyncio.run(benchmark.compact_fixture_gin_indexes(connection, schema))
    inventory = connection.fetch.await_args
    assert inventory.args[1] == schema
    assert "c.relkind='i'" in inventory.args[0]
    assert "am.amname='gin'" in inventory.args[0]
    connection.execute.assert_awaited_once_with(
        'REINDEX INDEX "benchmark_heap_full_012345abcd"."leaf""gin"', timeout=30)
    assert report["count"] == 1
    assert report["before_bytes"] == 1048576
    assert report["after_bytes"] == 32768
    assert report["names"] == ['leaf"gin']


def test_gin_normalization_refuses_nonbenchmark_schema_before_database_access():
    connection = SimpleNamespace(fetch=AsyncMock(), execute=AsyncMock())
    with pytest.raises(ValueError, match="isolated benchmark"):
        asyncio.run(benchmark.compact_fixture_gin_indexes(connection, "public"))
    connection.fetch.assert_not_awaited()
    connection.execute.assert_not_awaited()
