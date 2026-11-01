"""A destructive proof runner must refuse unapproved or non-isolated targets."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripts import recycle_archive_workload as recycle
from scripts.verify_archive_workload_restore import WorkloadRestoreRefused


def test_reversal_requires_explicit_apply_before_any_connection(monkeypatch):
    connector = AsyncMock()
    monkeypatch.setattr(recycle.asyncpg,"connect",connector)
    with pytest.raises(ValueError,match="--apply"):
        asyncio.run(recycle.run(SimpleNamespace(apply=False)))
    connector.assert_not_awaited()


@pytest.mark.parametrize("database",["meshinfo","production","unrelated_columnar_test"])
def test_reversal_cannot_open_a_live_or_unrelated_database(tmp_path,monkeypatch,database):
    monkeypatch.setenv("RECYCLE_TEST_DSN","postgresql://user:secret@localhost/"+database)
    monkeypatch.setattr(recycle,"load_report",lambda _path:{"target_database":"meshinfo_workload_columnar_test"})
    connector = AsyncMock()
    monkeypatch.setattr(recycle.asyncpg,"connect",connector)
    args = SimpleNamespace(apply=True,dsn_env="RECYCLE_TEST_DSN",database_filesystem=tmp_path,
                           reserve_mib=1024,max_seconds=600,workload_report=tmp_path/"identity.json")
    with pytest.raises(WorkloadRestoreRefused,match="isolated source"):
        asyncio.run(recycle.run(args))
    connector.assert_not_awaited()
