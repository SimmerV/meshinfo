"""Public history reporting must not amplify queries or conceal DB failures."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api.api import API


class Acquisition:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *args):
        return False


class Pool:
    def acquire(self):
        return Acquisition()


def test_concurrent_requests_share_one_catalog_report(monkeypatch):
    from storage.db import storage_report

    expected = {"generated_at": "2026-10-01T00:00:00Z", "scopes": {}}
    reader = AsyncMock(return_value=expected)
    monkeypatch.setattr(storage_report, "public_history_report", reader)

    async def scenario():
        api = API({}, SimpleNamespace(pg_storage=SimpleNamespace(pool=Pool())))
        results = await asyncio.gather(*(api.history_status() for _ in range(12)))
        assert all(item == expected for item in results)
        assert reader.await_count == 1
        await api.history_status()
        assert reader.await_count == 1
        api._history_cache = (0, expected)
        await api.history_status()
        assert reader.await_count == 2

    asyncio.run(scenario())


def test_database_failure_is_not_cached_as_empty_history(monkeypatch):
    from storage.db import storage_report

    reader = AsyncMock(side_effect=[RuntimeError("offline"), {"scopes": {"packets": {"months": 0}}}])
    monkeypatch.setattr(storage_report, "public_history_report", reader)

    async def scenario():
        api = API({}, SimpleNamespace(pg_storage=SimpleNamespace(pool=Pool())))
        with pytest.raises(RuntimeError, match="offline"):
            await api.history_status()
        assert api._history_cache is None
        result = await api.history_status()
        assert result["scopes"]["packets"]["months"] == 0

    asyncio.run(scenario())


def test_missing_pool_is_unavailable():
    api = API({}, SimpleNamespace(pg_storage=SimpleNamespace(pool=None)))
    with pytest.raises(RuntimeError, match="unavailable"):
        asyncio.run(api.history_status())
