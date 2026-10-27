"""Native index packing must be opt-in, bounded and restricted to closed leaves."""

import asyncio
from datetime import datetime, timezone

import pytest

from storage.db.index_maintenance import IndexPackingLimits, _reason, packing_policy, run_index_packing_cycle


NOW = datetime(2026, 10, 27, tzinfo=timezone.utc)


def index(**overrides):
    return {"index_kind": "i", "table_kind": "r", "persistence": "p", "partition_key": "RANGE (created_at)",
            "method": "btree", "indisvalid": True, "indisready": True, "index_bytes": 1048576,
            "bound": "FOR VALUES FROM ('2026-03-01 00:00:00+00') TO ('2026-04-01 00:00:00+00')",
            "reloptions": None, **overrides}


def test_existing_config_disables_packing():
    enabled, limits, path = packing_policy({})
    assert not enabled and not path
    assert limits.after_months == 6 and limits.max_seconds == 5
    assert asyncio.run(run_index_packing_cycle(None, {})) == {"status": "disabled"}


def test_enabling_without_actual_database_filesystem_does_not_touch_database():
    cfg = {"storage": {"compression": {"enabled": True}}}
    assert asyncio.run(run_index_packing_cycle(None, cfg))["status"] == "blocked"


@pytest.mark.parametrize("policy", [
    {"enabled": 1}, {"after_months": True}, {"after_months": -1}, {"index_fillfactor": 50},
    {"max_index_mib": False}, {"reserve_mib": 100}, {"max_seconds": float('nan')},
    {"max_seconds": 31}, {"database_filesystem": None}, {"typo": True},
])
def test_invalid_packing_configuration_is_rejected(policy):
    with pytest.raises(ValueError):
        packing_policy({"storage": {"compression": policy}})


def test_only_aged_closed_months_are_candidates():
    limits = IndexPackingLimits()
    assert _reason(index(), NOW, limits) is None
    recent = index(bound="FOR VALUES FROM ('2026-04-01 00:00:00+00') TO ('2026-05-01 00:00:00+00')")
    assert _reason(recent, NOW, limits) == 'too_recent'
    current = index(bound="FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00')")
    assert _reason(current, NOW, IndexPackingLimits(after_months=0)) == 'too_recent'


@pytest.mark.parametrize('mutation', [
    {'method':'gin'}, {'indisvalid':False}, {'indisready':False}, {'bound':'DEFAULT'},
    {'index_kind':'I'}, {'table_kind':'f'}, {'persistence':'u'}, {'partition_key':'RANGE (timestamp)'},
])
def test_unsupported_or_invalid_relations_are_excluded(mutation):
    assert _reason(index(**mutation), NOW, IndexPackingLimits()) == 'unsupported_index_or_partition'


def test_already_packed_and_oversize_indexes_are_skipped():
    assert _reason(index(reloptions=['fillfactor=100']), NOW, IndexPackingLimits()) == 'already_configured'
    assert _reason(index(index_bytes=1024**3), NOW, IndexPackingLimits()) == 'above_size_limit'
    assert _reason(index(index_bytes=8192), NOW, IndexPackingLimits()) == 'too_small'
