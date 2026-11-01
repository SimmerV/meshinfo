"""Packet readers expose failures and preserve filtering/pagination contracts."""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest

from storage.db.history_queries import (
    HistoryReadError, HistoryUnavailableError, literal_topic_pattern,
    packet_page_sql,
)
from storage.db.postgres import PostgresStorage, _decode_cursor


STAMP = datetime(2026, 1, 31, 23, 59, 59, 123456, tzinfo=timezone.utc)


def packet(identity, payload='{"from":1}', **updates):
    return {"id": identity, "topic": "msh/test", "payload": payload,
            "qos": 0, "retain": False, "timestamp": 123, "created_at": STAMP,
            "reception_count": 0, **updates}


def storage_with_connection(connection):
    class Acquisition:
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
    ("query_mqtt_message_by_id", {"row_id": 1}, "fetchrow"),
    ("query_mqtt_message_by_packet", {"from_id": "00000001", "mesh_packet_id": 1}, "fetchval"),
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
    assert "pr.topic_id = ANY(ARRAY(SELECT id FROM mqtt_topics" in query
    assert "JOIN mqtt_topics" not in query
    assert "pr.created_at >= mqtt_messages.created_at" in query
    assert "pr.created_at < mqtt_messages.created_at" in query


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
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=None))
    storage = storage_with_connection(connection)
    assert asyncio.run(storage.query_mqtt_message_by_packet("00000001", 99)) is None
    assert "ORDER BY created_at DESC, id DESC" in connection.fetchval.await_args.args[0]


def test_requested_copy_reconstruction_does_not_return_partial_success():
    reception = {"gateway": "00000002", "topic": "msh/test", "rx_rssi": None,
                 "rx_snr": None, "rx_time": None, "hop_limit": None, "hops_away": None,
                 "relay_node": None, "transport": None, "extras": '["invalid patch"]',
                 "created_at": STAMP}
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=packet(1)),
                                 fetch=AsyncMock(return_value=[reception]))
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
