"""Pure guard checks; these tests never open database connections."""
import copy
import json

import pytest

from scripts.verify_archive_workload_restore import (
    WorkloadRestoreRefused, load_report, option_settings, source_configuration, verify_geometry,
)


def workload():
    digest = {"rows": 104480, "sha256_sum": "a"*64, "sha256_xor": "b"*64}
    return {"all_checks_passed": True, "dedicated_target": True,
            "target_database": "meshinfo_workload_columnar_test",
            "packet_schemas": {"columnar": "public", "heap": "benchmark_heap_full_0123456789"},
            "shared_schema": "benchmark_shared_0123456789",
            "fixture": {"canonical_rows": 104480, "reception_rows": 904472,
                        "expanded_packet_digest": digest,
                        "expanded_reception_digest": {**digest, "rows": 904472},
                        "original_digests": {"topics": {**digest, "rows": 610}}}}


@pytest.mark.parametrize("change", [
    lambda value: value.update(all_checks_passed=False),
    lambda value: value.update(dedicated_target=False),
    lambda value: value["packet_schemas"].update(heap="public"),
    lambda value: value.update(shared_schema="production_shared"),
    lambda value: value["fixture"].update(canonical_rows=100),
    lambda value: value["fixture"]["expanded_packet_digest"].update(rows=104481),
    lambda value: value["fixture"]["expanded_reception_digest"].update(sha256_sum="invalid"),
])
def test_workload_identity_or_fidelity_failure_refused(tmp_path, change):
    report = copy.deepcopy(workload())
    change(report)
    path = tmp_path/"workload.json"
    path.write_text(json.dumps(report))
    with pytest.raises(WorkloadRestoreRefused):
        load_report(path)


def test_valid_bounded_workload_report(tmp_path):
    path = tmp_path/"workload.json"
    path.write_text(json.dumps(workload()))
    assert load_report(path) == workload()


@pytest.mark.parametrize("dsn", [
    "postgresql://user:secret@db/production",
    "postgresql://user:secret@db/unrelated_columnar_test",
    "postgresql://user:secret@db/meshinfo_workload_columnar_test?dbname=production",
    "postgresql://user:secret@db/meshinfo_workload_columnar_test#fragment",
    "https://user:secret@db/meshinfo_workload_columnar_test",
])
def test_source_dsn_cannot_escape_report_identity(dsn):
    with pytest.raises(WorkloadRestoreRefused):
        source_configuration(dsn, workload())


def test_source_config_decodes_credentials_without_logging_them():
    parsed, pg = source_configuration(
        "postgresql://test%20user:p%40ss@127.0.0.1:55432/meshinfo_workload_columnar_test", workload())
    assert parsed.port == pg["port"] == 55432
    assert pg["username"] == "test user" and pg["password"] == "p@ss"
    assert pg["database"] == workload()["target_database"]


def test_geometry_upper_bounds_are_distinct_from_exact_effective_options():
    source = [{"schema":"public","name":"history","chunk_group_row_limit":1000,
               "geometry":{"row_count":10000,"max_chunk_group_rows":5000}}]
    restored = [{**source[0],"geometry":{"row_count":10000,"max_chunk_group_rows":1000}}]
    assert option_settings(source) == option_settings(restored)
    assert source != restored


def test_restore_geometry_accepts_smaller_groups_but_rejects_lost_rows_or_growth():
    source = [{"schema":"public","name":"history","geometry":{
        "row_count":10000,"max_stripe_rows":5000,"max_chunk_group_rows":1000}}]
    restored = copy.deepcopy(source)
    restored[0]["geometry"]["max_stripe_rows"] = 1000
    verify_geometry(source,restored)
    for field,value in (("row_count",9999),("max_stripe_rows",5001),("max_chunk_group_rows",1001)):
        invalid = copy.deepcopy(restored)
        invalid[0]["geometry"][field] = value
        with pytest.raises(WorkloadRestoreRefused):
            verify_geometry(source,invalid)
