"""Optional paired native-server gate; ordinary tests never connect implicitly."""
import asyncio
import os
from types import SimpleNamespace

import pytest

from scripts.verify_columnar_planner_costs import run


pytestmark = pytest.mark.skipif(
    not os.environ.get("MESHINFO_PLANNER_BASELINE_DSN") or not os.environ.get("MESHINFO_PLANNER_PATCHED_DSN"),
    reason="Two isolated native planner DSNs are required")


def test_native_costs_paths_and_snapshot_visibility_are_identical():
    report = asyncio.run(run(SimpleNamespace(baseline_dsn_env="MESHINFO_PLANNER_BASELINE_DSN",
        patched_dsn_env="MESHINFO_PLANNER_PATCHED_DSN")))
    assert report["all_checks_passed"], {
        "error": report.get("error"),
        "failures": [{"name":case["name"],"state":case["state"],"plan_differences":case.get("plan_differences"),
            "baseline_error":case.get("baseline_error"),"patched_error":case.get("patched_error")}
            for case in report["cases"] if not case["passed"]][:8]}
    assert report["owned_schemas_removed"] == [True, True]
    assert len(report["cases"]) == 292
