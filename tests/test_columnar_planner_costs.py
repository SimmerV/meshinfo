"""Guard isolated native comparisons and do not discard planner regressions."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripts import verify_columnar_planner_costs as verifier


@pytest.mark.parametrize("dsn", ["", "postgresql://localhost/meshinfo", "postgresql://localhost/meshinfo_planner_test_extra",
    "postgresql://localhost/a%2F_columnar_test", "file:///meshinfo_planner_test"])
def test_live_or_ambiguous_target_refused_before_connection(dsn, monkeypatch):
    monkeypatch.setenv("PLANNER_BASELINE", dsn)
    monkeypatch.setenv("PLANNER_PATCHED", "postgresql://second/meshinfo_planner_test")
    connector = AsyncMock()
    monkeypatch.setattr(verifier.asyncpg, "connect", connector)
    with pytest.raises(ValueError, match="dedicated"):
        asyncio.run(verifier.run(SimpleNamespace(baseline_dsn_env="PLANNER_BASELINE", patched_dsn_env="PLANNER_PATCHED")))
    connector.assert_not_awaited()


def test_same_target_refused_before_connection(monkeypatch):
    for name in ("PLANNER_BASELINE", "PLANNER_PATCHED"):
        monkeypatch.setenv(name, "postgresql://localhost/meshinfo_planner_test")
    connector = AsyncMock()
    monkeypatch.setattr(verifier.asyncpg, "connect", connector)
    with pytest.raises(ValueError, match="distinct"):
        asyncio.run(verifier.run(SimpleNamespace(baseline_dsn_env="PLANNER_BASELINE", patched_dsn_env="PLANNER_PATCHED")))
    connector.assert_not_awaited()


def test_estimate_normalization_retains_structure_cost_conditions_and_ignores_timing():
    left = {"Node Type":"Index Scan", "Index Cond":"(id = $1)", "Total Cost":12.5,
        "Plan Rows":1, "Actual Total Time":4.4, "Shared Hit Blocks":8}
    right = {**left, "Actual Total Time":0.2, "Shared Hit Blocks":0}
    assert verifier.plan_signature(left) == verifier.plan_signature(right)
    for key, value in (("Total Cost", 12.51), ("Plan Rows", 2), ("Index Cond", "(id > $1)"), ("Node Type", "Seq Scan"), ("Disabled", True)):
        changed = verifier.plan_signature({**right, key:value})
        assert verifier.plan_differences(verifier.plan_signature(left), changed)


def test_missing_plan_child_is_not_silently_zipped_away():
    assert verifier.plan_differences({"Plans":[{"Node Type":"Index Scan"}]}, {"Plans":[]}) == ["Plan.Plans: length differs"]


def test_second_outer_transaction_start_failure_rolls_back_the_first():
    first = SimpleNamespace(start=AsyncMock(), rollback=AsyncMock())
    second = SimpleNamespace(start=AsyncMock(side_effect=ConnectionError()), rollback=AsyncMock())
    connections = [SimpleNamespace(transaction=lambda:first), SimpleNamespace(transaction=lambda:second)]
    with pytest.raises(ConnectionError):
        asyncio.run(verifier.subtransactions(connections, "native_cost_test_owned", {"cases":[]}))
    first.rollback.assert_awaited_once()
    second.rollback.assert_not_awaited()


def measurement(cost=3.0, response="same"):
    plan = {"Node Type":"Index Scan", "Total Cost":cost, "Index Cond":"(id = $1)"}
    return {"plan":plan, "warm_plan":plan, "rows":1, "response_sha256":response,
        "first_planning_ms":1.0, "warm_planning_ms":0.1}


@pytest.mark.parametrize("kind", ["cost", "response", "timeout"])
def test_failed_pair_is_preserved_and_does_not_become_an_empty_success(kind, monkeypatch):
    other = TimeoutError() if kind == "timeout" else measurement(cost=4.0 if kind == "cost" else 3.0, response="different" if kind == "response" else "same")
    monkeypatch.setattr(verifier, "measured", AsyncMock(side_effect=[measurement(), other]))
    report = {"cases":[]}
    asyncio.run(verifier.compare_case([object(), object()], report, "witness", "SELECT", (), ("on","on","on"), "force_custom_plan"))
    case, = report["cases"]
    assert case["passed"] is False
    if kind == "cost":
        assert case["plan_differences"] == ["Plan.Total Cost: value differs"]
    elif kind == "response":
        assert case["response_equal"] is False
    else:
        assert case["patched_error"] == {"type":"TimeoutError", "sqlstate":None}


def test_exact_pair_is_required_even_if_timings_differ(monkeypatch):
    baseline, patched = measurement(), measurement()
    patched["first_planning_ms"] = 1000
    monkeypatch.setattr(verifier, "measured", AsyncMock(side_effect=[baseline, patched]))
    report = {"cases":[]}
    asyncio.run(verifier.compare_case([object(), object()], report, "witness", "SELECT", (), ("on","on","on"), "force_custom_plan"))
    assert report["cases"][0]["passed"] is True


def test_existing_output_refused_before_any_database_work(tmp_path, monkeypatch):
    target = tmp_path / "evidence.json"
    target.write_text("preserved")
    operation = AsyncMock()
    monkeypatch.setattr(verifier, "run", operation)
    monkeypatch.setattr(verifier.sys, "argv", ["verify", "--baseline-dsn-env", "A", "--patched-dsn-env", "B", "--output", str(target)])
    with pytest.raises(FileExistsError):
        verifier.main()
    assert target.read_text() == "preserved"
    operation.assert_not_awaited()


def test_failed_main_writes_report_and_nonzero_exit(tmp_path, monkeypatch):
    target = tmp_path / "evidence.json"
    monkeypatch.setattr(verifier, "run", AsyncMock(return_value={"all_checks_passed":False,"cases":[{"passed":False}]}))
    monkeypatch.setattr(verifier.sys, "argv", ["verify", "--baseline-dsn-env", "A", "--patched-dsn-env", "B", "--output", str(target)])
    assert verifier.main() == 2
    assert json.loads(target.read_text())["all_checks_passed"] is False
    assert target.stat().st_mode & 0o777 == 0o600


def test_unexpected_result_codec_produces_a_complete_failed_artifact(tmp_path, monkeypatch):
    target = tmp_path / "evidence.json"
    monkeypatch.setattr(verifier, "run", AsyncMock(return_value={"all_checks_passed":True,
        "cases":[{"passed":True}],"unexpected":b"x","owned_schemas_removed":[True,True]}))
    monkeypatch.setattr(verifier.sys, "argv", ["verify", "--baseline-dsn-env", "A", "--patched-dsn-env", "B", "--output", str(target)])
    assert verifier.main() == 2
    report = json.loads(target.read_text())
    assert report["error"] == {"type":"ReportSerializationError"}
    assert report["completed_cases"] == 1
    assert report["owned_schemas_removed"] == [True,True]
