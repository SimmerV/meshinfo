"""Packet readers expose failures and preserve filtering/pagination contracts."""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest

from storage.db.history_queries import (
    HistoryReadError, HistoryUnavailableError, literal_topic_pattern,
    packet_page_sql, fetch_packet_page, topic_coverage_intervals, topic_candidate_sql,
    fetch_topic_packet_page, ordered_packet_rows, packet_payload_sql,
    topic_match_cte, TOPIC_MATCH_ARRAY_LIMIT,
    remap_conditions,
)
from storage.db.postgres import PostgresStorage, _decode_cursor


STAMP = datetime(2026, 1, 31, 23, 59, 59, 123456, tzinfo=timezone.utc)


def packet(identity, payload='{"from":1}', **updates):
    return {"id": identity, "topic": "msh/test", "payload": payload,
            "qos": 0, "retain": False, "timestamp": 123, "created_at": STAMP,
            "reception_count": 0, **updates}


def storage_with_connection(connection):
    if not hasattr(connection, "transaction"):
        transaction = MagicMock()
        transaction.__aenter__ = AsyncMock()
        transaction.__aexit__ = AsyncMock(return_value=False)
        connection.transaction = lambda **kwargs: transaction
    if not hasattr(connection, "execute"):
        connection.execute = AsyncMock()
    if not hasattr(connection, "fetchval"):
        connection.fetchval = AsyncMock(return_value=None)
    if not hasattr(connection, "fetchrow"):
        connection.fetchrow = AsyncMock(return_value={"plan":"auto","scan":None})
    class Acquisition:
        def __init__(self, **kwargs):
            assert kwargs.get("timeout") == 3

        async def __aenter__(self):
            return connection

        async def __aexit__(self, *exc):
            return False

    storage = PostgresStorage({"server": {"timezone": "UTC"},
                               "storage": {"postgres": {"enabled": True}}})
    storage.pool = SimpleNamespace(acquire=Acquisition)
    return storage


@pytest.mark.parametrize("method,args,driver_method", [
    ("query_mqtt_messages", {}, "fetch"),
    ("query_node_mqtt_messages", {"node_id": "00000001"}, "fetch"),
    ("query_mqtt_message_by_id", {"row_id": 1}, "fetch"),
    ("query_mqtt_message_by_packet", {"from_id": "00000001", "mesh_packet_id": 1}, "fetch"),
])
@pytest.mark.parametrize("failure", [TimeoutError(), asyncpg.QueryCanceledError(),
                                     asyncpg.UndefinedTableError("private SQL details")])
def test_read_failure_is_never_an_empty_page_or_missing_packet(method, args, driver_method, failure):
    connection = SimpleNamespace(**{driver_method: AsyncMock(side_effect=failure)})
    storage = storage_with_connection(connection)
    with pytest.raises(HistoryReadError) as caught:
        asyncio.run(getattr(storage, method)(**args))
    assert caught.value.operation == method
    assert caught.value.__cause__ is failure
    assert caught.value.timed_out == isinstance(failure, (TimeoutError, asyncpg.QueryCanceledError))
    assert "private SQL details" not in str(caught.value)


@pytest.mark.parametrize("enabled", [False, True])
def test_unavailable_history_is_explicit(enabled):
    storage = PostgresStorage({"server": {"timezone": "UTC"},
                               "storage": {"postgres": {"enabled": enabled}}})
    with pytest.raises(HistoryUnavailableError):
        asyncio.run(storage.query_mqtt_messages())


def test_request_cancellation_is_not_wrapped_or_swallowed():
    storage = storage_with_connection(SimpleNamespace(fetch=AsyncMock(side_effect=asyncio.CancelledError())))
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(storage.query_mqtt_messages())


def test_selective_topic_planner_setting_is_local_and_rolled_back_on_failure():
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock()
    transaction.__aexit__ = AsyncMock(return_value=False)
    failure = TimeoutError()
    connection = SimpleNamespace(transaction=lambda **kwargs: transaction,
                                 execute=AsyncMock(), fetch=AsyncMock(side_effect=failure))
    with pytest.raises(TimeoutError):
        asyncio.run(fetch_packet_page(connection, "SELECT $1", "private", custom_plan=True))
    connection.execute.assert_awaited_once_with("SET LOCAL plan_cache_mode = force_custom_plan")
    assert transaction.__aexit__.await_args.args[1] is failure


def test_topic_lookup_coverage_preserves_gaps_and_binds_half_open_months():
    intervals = topic_coverage_intervals([
        "FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-02-01 00:00:00+00')",
        "FOR VALUES FROM ('2020-02-01 00:00:00+00') TO ('2020-03-01 00:00:00+00')",
        "FOR VALUES FROM ('2020-04-01 00:00:00+00') TO ('2020-05-01 00:00:00+00')"])
    assert len(intervals) == 2
    query, parameters = topic_candidate_sql(["topic ILIKE $1", "created_at <= $2"], 3, intervals)
    assert "UNION ALL" in query and "AND NOT (" in query
    assert "FROM mqtt_archive_topics" in query and "FROM mqtt_messages" in query
    assert query.count("created_at <= $2") == 2
    assert query.count("ORDER BY created_at DESC,id DESC LIMIT $3::integer") == 3
    assert len(parameters) == 4 and "$4::timestamptz" in query and "$7::timestamptz" in query
    unbounded, same_parameters = topic_candidate_sql(
        ["topic ILIKE $1", "created_at <= $2"], 3, intervals, bounded=False)
    assert same_parameters == parameters
    assert "LIMIT" not in unbounded and "ORDER BY" not in unbounded


def test_topic_bounds_rendered_in_local_timezone_still_cover_exact_utc_month():
    intervals = topic_coverage_intervals([
        "FOR VALUES FROM ('2020-01-31 16:00:00-08') TO ('2020-02-29 16:00:00-08')"])
    assert intervals == [(datetime(2020,2,1,tzinfo=timezone.utc), datetime(2020,3,1,tzinfo=timezone.utc))]


@pytest.mark.parametrize("bound", ["DEFAULT", "FOR VALUES FROM (MINVALUE) TO (MAXVALUE)",
    "FOR VALUES FROM ('2020-01-15') TO ('2020-02-01')",
    "FOR VALUES FROM ('2020-01-01T00:00:00Z') TO ('2020-03-01T00:00:00Z')"])
def test_topic_lookup_refuses_unverified_coverage_shapes(bound):
    with pytest.raises(ValueError):
        topic_coverage_intervals([bound])


def test_topic_lookup_never_silently_omits_a_key_without_its_canonical_payload():
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock()
    transaction.__aexit__ = AsyncMock(return_value=False)
    connection = SimpleNamespace(transaction=lambda **kwargs: transaction, execute=AsyncMock(),
        fetchrow=AsyncMock(return_value={"plan": "force_custom_plan", "scan": "on"}),
        fetchval=AsyncMock(return_value=42), fetch=AsyncMock(side_effect=[
            [{"bounds": "FOR VALUES FROM ('2026-01-01T00:00:00Z') TO ('2026-02-01T00:00:00Z')"}],
            [{"id": 1, "created_at": STAMP}], []]))
    with pytest.raises(HistoryReadError, match="could not be read"):
        asyncio.run(fetch_topic_packet_page(connection, ["topic ILIKE $1"], ["literal", 2], 2, "0"))
    assert transaction.__aexit__.await_args.args[0] is HistoryReadError


def test_bound_payload_fetch_restores_cursor_order_and_checks_both_identity_columns():
    first, second = packet(2), packet(1)
    keys = [{"id": row["id"], "created_at": row["created_at"]} for row in (first, second)]
    assert ordered_packet_rows(keys, [second, first]) == [first, second]
    sql, parameters = packet_payload_sql(keys, "0")
    assert "ORDER BY id,created_at" in sql and "wanted.created_at=mqtt_messages.created_at" in sql
    assert parameters[0] == [2, 1] and parameters[1] == [STAMP, STAMP]
    with pytest.raises(HistoryReadError):
        ordered_packet_rows(keys, [first, first])
    with pytest.raises(HistoryReadError):
        ordered_packet_rows([keys[0], keys[0]], [first])


def test_optional_chunk_envelopes_cover_every_key_and_keep_exact_membership_filter():
    from scripts.benchmark_history_read_plans import envelope_payload_sql
    ids = [1, 2, 3, *range(100, 10_000, 97)]
    keys = [{"id": identity, "created_at": STAMP} for identity in reversed(ids)]
    sql, parameters = envelope_payload_sql(keys, "0")
    spans = list(zip(parameters[4::2], parameters[5::2]))
    assert 1 <= len(spans) <= 16
    assert all(any(low <= identity <= high for low, high in spans) for identity in ids)
    assert "id=ANY($1::bigint[])" in sql
    assert "wanted.created_at=mqtt_messages.created_at" in sql


def test_search_text_and_literal_topic_are_bound_with_distinct_wildcard_semantics():
    connection = SimpleNamespace(fetch=AsyncMock(return_value=[]))
    storage = storage_with_connection(connection)
    search = "gateway_%' OR true --"
    topic = "My_%\\Channel"
    assert asyncio.run(storage.query_mqtt_messages(search=search, topic=topic, limit=2)) == {
        "messages": [], "next_cursor": None}
    query, *parameters = connection.fetch.await_args.args
    assert search not in query and topic not in query
    assert parameters == [search, literal_topic_pattern(topic), 3]
    assert "packet_topic_matches AS MATERIALIZED" in query
    assert "SELECT cardinality(ids)>0 FROM packet_topic_matches" in query
    assert "pr.topic_id = ANY((SELECT ids FROM packet_topic_matches)::integer[])" in query
    assert "JOIN mqtt_topics" not in query
    assert "pr.created_at >= mqtt_messages.created_at" in query
    assert "pr.created_at < mqtt_messages.created_at" in query


def test_matching_topic_dictionary_array_has_a_hard_row_bound_and_exact_overflow_path():
    cte = topic_match_cte(2)
    assert f"LIMIT {TOPIC_MATCH_ARRAY_LIMIT+1}" in cte
    assert f"count(*)>{TOPIC_MATCH_ARRAY_LIMIT} AS overflow" in cte
    connection = SimpleNamespace(fetch=AsyncMock(return_value=[]))
    asyncio.run(storage_with_connection(connection).query_mqtt_messages(search="%"))
    sql = connection.fetch.await_args.args[0]
    assert "CASE WHEN (SELECT overflow FROM packet_topic_matches) THEN EXISTS" in sql
    assert "matched_topic.id=pr.topic_id" in sql
    assert "matched_topic.topic ILIKE '%' || $1 || '%'" in sql


def test_metadata_probe_remaps_parameters_omitted_from_payload_or_reported_time_filters():
    conditions, arguments = remap_conditions(["topic ILIKE $3", "created_at >= $4", "(created_at,id)<($5,$6)"],
        [123, "payload search", "literal", STAMP, STAMP, 99, 2])
    assert conditions == ["topic ILIKE $1", "created_at >= $2", "(created_at,id)<($3,$4)"]
    assert arguments == ["literal", STAMP, STAMP, 99]


def prefix_connection(payload_rows, fallback_rows=None):
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock()
    transaction.__aexit__ = AsyncMock(return_value=False)
    keys = [{"id": identity, "created_at": STAMP} for identity in range(129, 0, -1)]
    effects = [[{"bounds": "FOR VALUES FROM ('2026-01-01T00:00:00Z') TO ('2026-02-01T00:00:00Z')"}],
               keys, payload_rows]
    if fallback_rows is not None:
        effects.append(fallback_rows)
    return SimpleNamespace(transaction=lambda **kwargs: transaction, execute=AsyncMock(),
        fetchrow=AsyncMock(return_value={"plan": "force_custom_plan", "scan": "on"}),
        fetchval=AsyncMock(return_value=42), fetch=AsyncMock(side_effect=effects)), transaction


def test_ordered_prefix_returns_proven_full_page_even_with_older_candidates():
    expected = [packet(129), packet(128)]
    connection, transaction = prefix_connection(expected)
    actual = asyncio.run(fetch_topic_packet_page(connection,
        ["payload ILIKE $1", "created_at <= $2"], ["needle", STAMP, 2], 3, "0",
        candidate_conditions=["created_at <= $2"]))
    assert actual == expected
    assert connection.fetch.await_count == 3  # coverage, ordered prefix, exact filtered payloads
    assert transaction.__aexit__.await_args.args[0] is None


def test_incomplete_prefix_restores_scan_mode_and_checks_older_matches():
    connection, _ = prefix_connection([], [packet(0)])
    actual = asyncio.run(fetch_topic_packet_page(connection,
        ["payload ILIKE $1", "created_at <= $2"], ["needle", STAMP, 2], 3, "0",
        candidate_conditions=["created_at <= $2"]))
    assert [row["id"] for row in actual] == [0]
    assert connection.fetch.await_count == 4
    assert "id=ANY" not in connection.fetch.await_args.args[0]
    assert connection.execute.await_args.args == (
        "SET LOCAL plan_cache_mode = force_custom_plan; SET LOCAL columnar.enable_custom_scan = on",)


def test_multistatement_history_deadline_cancels_read_and_exits_transaction(monkeypatch):
    import storage.db.history_queries as queries
    monkeypatch.setattr(queries, "HISTORY_READ_TIMEOUT_SECONDS", .01)
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock()
    transaction.__aexit__ = AsyncMock(return_value=False)
    calls = 0
    async def fetch(*args):
        nonlocal calls
        calls += 1
        if calls == 1:
            return [{"bounds": "FOR VALUES FROM ('2026-01-01T00:00:00Z') TO ('2026-02-01T00:00:00Z')"}]
        if calls == 2:
            return [{"id": 1, "created_at": STAMP}]
        await asyncio.Event().wait()
    connection = SimpleNamespace(transaction=lambda **kwargs: transaction,
        fetchrow=AsyncMock(return_value={"plan": "force_custom_plan", "scan": "on"}),
        fetchval=AsyncMock(return_value=42), execute=AsyncMock(), fetch=fetch)
    with pytest.raises(TimeoutError):
        asyncio.run(fetch_topic_packet_page(connection,
            ["payload ILIKE $1", "created_at <= $2"], ["needle", STAMP, 2], 3, "0",
            candidate_conditions=["created_at <= $2"]))
    assert transaction.__aexit__.await_args.args[0] is asyncio.CancelledError


def test_page_preserves_tied_timestamp_cursor_and_malformed_scalar_null_payloads():
    connection = SimpleNamespace(fetch=AsyncMock(return_value=[
        packet(4, '42'), packet(3, '{broken'), packet(2, None), packet(1)]))
    storage = storage_with_connection(connection)
    result = asyncio.run(storage.query_mqtt_messages(limit=3))
    assert [row["mqtt_row_id"] for row in result["messages"]] == [4, 3, 2]
    assert result["messages"][0]["raw"] == 42
    assert result["messages"][1]["raw"] == "{broken"
    assert "raw" not in result["messages"][2]
    assert _decode_cursor(result["next_cursor"]) == (STAMP, 2)
    asyncio.run(storage.query_mqtt_messages(limit=3, before=result["next_cursor"]))
    query, *parameters = connection.fetch.await_args.args
    assert "(created_at, id) < ($1, $2)" in query
    assert parameters == [STAMP, 2, 4]


def test_reused_packet_lookup_has_a_deterministic_timestamp_tiebreaker():
    connection = SimpleNamespace(fetch=AsyncMock(return_value=[]))
    storage = storage_with_connection(connection)
    assert asyncio.run(storage.query_mqtt_message_by_packet("00000001", 99)) is None
    assert "ORDER BY created_at DESC,id DESC LIMIT 1" in connection.fetch.await_args.args[0]


def test_requested_copy_reconstruction_does_not_return_partial_success():
    reception = {"gateway": "00000002", "topic": "msh/test", "rx_rssi": None,
                 "rx_snr": None, "rx_time": None, "hop_limit": None, "hops_away": None,
                 "relay_node": None, "transport": None, "extras": '["invalid patch"]',
                 "created_at": STAMP}
    connection = SimpleNamespace(fetch=AsyncMock(side_effect=[[packet(1)], [reception]]))
    storage = storage_with_connection(connection)
    with pytest.raises(HistoryReadError) as caught:
        asyncio.run(storage.query_mqtt_message_by_id(1, include_copies=True))
    assert caught.value.operation == "reconstruct_packet_copy"


@pytest.mark.parametrize("limit", [0, -1, 50_001, True, "200"])
def test_invalid_or_unbounded_page_limit_never_reaches_database(limit):
    connection = SimpleNamespace(fetch=AsyncMock())
    storage = storage_with_connection(connection)
    with pytest.raises(ValueError, match="limit"):
        asyncio.run(storage.query_mqtt_messages(limit=limit))
    connection.fetch.assert_not_awaited()


def test_candidate_plan_limits_identities_then_fetches_payload_by_full_partition_key():
    query = packet_page_sql(["(from_node_id=$1 OR to_node_id=$1)"], 2,
                            "(SELECT 0)", strategy="candidates")
    selection, projection = query.split(") SELECT ", 1)
    assert "SELECT id,created_at FROM mqtt_messages" in selection
    assert "payload" not in selection
    assert "LIMIT $2::integer" in selection
    assert "mqtt_messages.payload" in projection
    assert "mqtt_messages.id=candidate.id AND mqtt_messages.created_at=candidate.created_at" in projection


def test_node_branch_plan_bounds_both_arms_and_excludes_only_self_address_duplicates():
    query = packet_page_sql(["(from_node_id = $1 OR to_node_id = $1)", "created_at <= $2"],
                            3, "(SELECT 0)", strategy="node_branches")
    assert query.count("LIMIT $3::integer") == 3
    assert "UNION ALL" in query
    assert "to_node_id=$1 AND from_node_id IS DISTINCT FROM $1" in query
    assert query.count("created_at <= $2") == 2
    with pytest.raises(ValueError, match="node reader"):
        packet_page_sql(["topic=$1"], 2, "(SELECT 0)", strategy="node_branches")


def test_full_row_node_union_avoids_second_payload_lookup_and_bounds_each_arm():
    query = packet_page_sql(["(from_node_id = $1 OR to_node_id = $1)", "created_at <= $2"],
                            3, "(SELECT 0)", strategy="node_rows")
    assert query.count("LIMIT $3::integer") == 3
    assert "UNION ALL" in query and " JOIN " not in query
    assert "to_node_id=$1 AND from_node_id IS DISTINCT FROM $1" in query
    assert query.count("SELECT id,topic,payload,qos,retain,timestamp,created_at") == 3


def test_read_plan_runner_rejects_live_target_before_opening_any_pool(monkeypatch):
    from scripts import benchmark_history_read_plans as benchmark
    monkeypatch.setenv("HISTORY_PLAN_TEST_DSN", "postgresql://localhost/meshinfo")
    opener = AsyncMock()
    monkeypatch.setattr(benchmark, "open_storage", opener)
    with pytest.raises(ValueError, match="dedicated"):
        asyncio.run(benchmark.run(SimpleNamespace(dsn_env="HISTORY_PLAN_TEST_DSN")))
    opener.assert_not_awaited()


@pytest.mark.parametrize("schema", ["public", "pg_catalog", 'benchmark_x";DROP TABLE x', "other"])
def test_read_plan_runner_requires_isolated_schema_names(schema):
    from scripts.benchmark_history_read_plans import schema_name
    with pytest.raises(ValueError, match="isolated"):
        schema_name(schema)
