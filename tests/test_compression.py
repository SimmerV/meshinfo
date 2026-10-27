"""Compression safety: bounded resources, exact preservation and explicit execution."""

import asyncio

import pytest

from storage.db.compression import (
    GIB, MIB, benchmark_compression, quote_identifier, validate_target,
)
from storage.db.index_maintenance import IndexPackingLimits, required_headroom


@pytest.mark.parametrize('target', [128, 512, 1024, 2040, 8160])
def test_supported_targets(target):
    assert validate_target(target) == target


@pytest.mark.parametrize('target', [True, False, 127, 8161, 1024.0, '1024', None])
def test_invalid_targets_never_reach_sql(target):
    with pytest.raises(ValueError):
        validate_target(target)


def test_headroom_includes_replacement_work_wal_and_reserved_floor():
    assert required_headroom(512 * MIB) == 3 * GIB
    with pytest.raises(ValueError):
        required_headroom(-1)
    with pytest.raises(ValueError):
        required_headroom(0, 1)
    with pytest.raises(ValueError):
        IndexPackingLimits(max_seconds=31)


def test_catalog_identifiers_are_quoted():
    assert quote_identifier('a"; DROP TABLE nodes; --') == '"a""; DROP TABLE nodes; --"'


def test_benchmark_requires_nonempty_bounded_sample():
    with pytest.raises(ValueError):
        asyncio.run(benchmark_compression(None, []))
    with pytest.raises(ValueError):
        asyncio.run(benchmark_compression(None, [(1,)] * 20001))
