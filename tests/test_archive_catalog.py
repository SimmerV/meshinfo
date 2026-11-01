"""Topic-index startup compatibility and mixed-history safety checks."""
import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from storage.db import archive_catalog as catalog


def leaf(name="mqtt_messages_2020_01", *, populated=True, covered=False):
    return {"oid": 123, "schema": "public", "name": name, "kind": "r", "reltablespace": 0,
            "bytes": 8192 if populated else 0, "heap_bytes": 8192 if populated else 0,
            "covered": covered}


def parent(**changes):
    return {"oid": 44, "name": "idx_mqtt_messages_topic_trgm", "indisvalid": True,
            "indisready": True, "indisunique": False, "indnkeyatts": 1, "indnatts": 1,
            "column_name": "topic", "text_column": True, "matching_collation": True, "opclass": "gin_trgm_ops", "extension_opclass": True, "predicate": None, "expressions": None, **changes}


class Connection:
    def __init__(self, leaves=(), parents=(), *, entirely_heap=True):
        self.leaves = list(leaves)
        self.parents = list(parents)
        self.entirely_heap = entirely_heap
        self.events = []
        self.active = False
        self.occupied = False
        self.changed_leaf = None
        self.locked = False
        self.in_transaction = False

    @asynccontextmanager
    async def transaction(self):
        assert not self.in_transaction
        self.in_transaction = True
        try:
            yield
        finally:
            self.in_transaction = False

    async def execute(self, query, *arguments, **kwargs):
        self.events.append(query)
        if query.startswith("LOCK TABLE public.mqtt_messages"):
            assert self.locked and self.in_transaction
        return "OK"

    async def fetchval(self, query, *arguments, **kwargs):
        if "pg_try_advisory_lock" in query:
            self.locked = True
            return True
        if "pg_advisory_unlock" in query:
            self.locked = False
            return True
        if "WITH RECURSIVE family" in query:
            assert self.locked and self.in_transaction
            assert any(query.startswith("LOCK TABLE public.mqtt_messages") for query in self.events)
            return self.entirely_heap
        if "to_regclass('public.storage_archive_jobs')" in query:
            return True
        if "FROM public.storage_archive_jobs" in query:
            return self.active
        if "to_regclass" in query:
            return self.occupied
        raise AssertionError(query)

    async def fetchrow(self, query, *arguments):
        if "FROM pg_opclass opc" in query:
            return {"schema": "public", "name": "gin_trgm_ops"}
        schema, name = arguments
        source = next(item for item in self.leaves if item["schema"] == schema and item["name"] == name)
        return self.changed_leaf or {"oid": source["oid"], "amname": "heap", "bytes": source["heap_bytes"]}


@pytest.fixture
def inventory(monkeypatch):
    monkeypatch.setattr(catalog, "parent_gin_indexes", AsyncMock(side_effect=lambda conn: conn.parents))
    monkeypatch.setattr(catalog, "heap_leaves", AsyncMock(side_effect=lambda conn: conn.leaves))
    monkeypatch.setattr(catalog, "has_topic_index", AsyncMock(side_effect=lambda conn, row: row["covered"]))


def creates(conn):
    return [query for query in conn.events if query.startswith("CREATE INDEX")]


def test_archive_disabled_legacy_heap_gets_its_missing_parent_topic_index(inventory):
    conn = Connection([leaf()])
    asyncio.run(catalog.ensure_heap_topic_indexes(conn, allow_legacy_parent=True))
    assert creates(conn) == ['CREATE INDEX idx_mqtt_messages_topic_trgm ON public.mqtt_messages USING gin (topic "public"."gin_trgm_ops")']
    assert not conn.locked and not conn.in_transaction


def test_mixed_history_never_recreates_parent_gin_even_if_archiving_was_disabled(inventory):
    conn = Connection([leaf()], entirely_heap=False)
    with pytest.raises(RuntimeError, match="space-checked"):
        asyncio.run(catalog.ensure_heap_topic_indexes(conn, allow_legacy_parent=True))
    assert creates(conn) == []
    assert not conn.locked


def test_archive_enabled_populated_heap_requires_real_space_checked_maintenance(inventory):
    conn = Connection([leaf()])
    with pytest.raises(RuntimeError, match="space-checked"):
        asyncio.run(catalog.ensure_heap_topic_indexes(conn))
    assert creates(conn) == []


def test_active_archive_and_occupied_legacy_name_refuse_without_replacing_objects(inventory):
    for attribute in ("active", "occupied"):
        conn = Connection([leaf()])
        setattr(conn, attribute, True)
        with pytest.raises(RuntimeError):
            asyncio.run(catalog.ensure_heap_topic_indexes(conn, allow_legacy_parent=True))
        assert creates(conn) == [] and not conn.locked


def test_valid_parent_or_standalone_topic_coverage_is_not_duplicated(inventory):
    for conn in (Connection([leaf()], [parent()]), Connection([leaf(covered=True)])):
        asyncio.run(catalog.ensure_heap_topic_indexes(conn, allow_legacy_parent=True))
        assert creates(conn) == []


def test_unrelated_parent_gin_is_not_mistaken_for_topic_coverage(inventory):
    conn = Connection([leaf()], [parent(name="other_gin", column_name="payload")])
    asyncio.run(catalog.ensure_heap_topic_indexes(conn, allow_legacy_parent=True))
    assert len(creates(conn)) == 1


def test_new_empty_leaf_gets_a_short_locked_index_without_fake_capacity(inventory):
    conn = Connection([leaf(populated=False)])
    asyncio.run(catalog.ensure_heap_topic_indexes(conn))
    assert len(creates(conn)) == 1
    assert "CONCURRENTLY" not in creates(conn)[0]
    assert any(query.startswith('LOCK TABLE ONLY "public".') for query in conn.events)
    assert any("statement_timeout='2000ms'" in query for query in conn.events)


@pytest.mark.parametrize("changed", [
    {"oid": 124, "amname": "heap", "bytes": 0},
    {"oid": 123, "amname": "columnar", "bytes": 0},
    {"oid": 123, "amname": "heap", "bytes": 8192},
])
def test_raced_identity_access_method_or_new_rows_refuse_empty_leaf_shortcut(inventory, changed):
    conn = Connection([leaf(populated=False)])
    conn.changed_leaf = changed
    with pytest.raises(RuntimeError, match="changed"):
        asyncio.run(catalog.ensure_heap_topic_indexes(conn))
    assert creates(conn) == []


def test_old_missing_index_does_not_starve_new_empty_rollover_index(inventory):
    old = leaf()
    new = leaf("mqtt_messages_2030_01", populated=False)
    new["oid"] = 456
    conn = Connection([old, new])
    with pytest.raises(RuntimeError, match="space-checked"):
        asyncio.run(catalog.ensure_heap_topic_indexes(conn))
    assert len(creates(conn)) == 1 and '"mqtt_messages_2030_01"' in creates(conn)[0]


@pytest.mark.parametrize("archive_enabled", [False, True])
def test_actual_startup_passes_the_archive_policy_to_index_repair(monkeypatch, archive_enabled):
    from storage.db.postgres import PostgresStorage
    from storage.db import archive

    async def scenario():
        connection = type("StartupConnection", (), {"execute": AsyncMock(return_value="OK")})()
        class Pool:
            @asynccontextmanager
            async def acquire(self):
                yield connection
        index_repair = AsyncMock()
        monkeypatch.setattr(catalog, "ensure_heap_topic_indexes", index_repair)
        monkeypatch.setattr(archive, "ensure_archive_schema", AsyncMock())
        storage = PostgresStorage({"server": {"timezone": "UTC"}, "storage": {
            "postgres": {"enabled": True}, "archive": {"enabled": archive_enabled}}})
        storage.pool = Pool()
        storage.ensure_mqtt_partitions = AsyncMock()
        storage._backfill_mqtt_node_ids = AsyncMock()
        await storage.ensure_schema()
        await storage._backfill_task
        index_repair.assert_awaited_once_with(connection, allow_legacy_parent=not archive_enabled)
    asyncio.run(scenario())


@pytest.mark.parametrize("changes", [
    {"extension_opclass": False}, {"matching_collation": False}, {"column_name": "payload"}, {"text_column": False},
    {"indnkeyatts": 2}, {"indnatts": 2}, {"expressions": "lower(topic)"},
    {"predicate": "topic IS NOT NULL"}, {"opclass": "custom_trgm_ops"},
])
def test_topic_index_validation_checks_actual_catalog_semantics_not_name(changes):
    assert not catalog.supported_parent(parent(**changes))
    assert not catalog.valid_topic_index(parent(**changes))
    assert catalog.supported_parent(parent())
