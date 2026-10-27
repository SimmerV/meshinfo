"""Safety properties of configured history lifetime and calendar boundaries."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from config import ConfigValidationError, DEFAULT_CONFIG, _deep_merge, validate
from storage.retention import (
    MAX_RETENTION_MONTHS, RETENTION_SCOPES, calendar_cutoff,
    complete_month_cutoff, retention_policy, retention_summary,
)

UTC = timezone.utc


def config_with_policy(**policy):
    return {"storage": {"retention": policy}}


@pytest.mark.parametrize(("now", "months", "expected"), [
    ("2024-03-31T18:30:10+00:00", 1, "2024-02-29T18:30:10+00:00"),
    ("2025-03-31T18:30:10+00:00", 1, "2025-02-28T18:30:10+00:00"),
    ("2024-02-29T00:00:00+00:00", 12, "2023-02-28T00:00:00+00:00"),
    ("2026-01-01T00:00:00+00:00", 3, "2025-10-01T00:00:00+00:00"),
    ("2026-03-01T01:00:00+02:00", 1, "2026-01-28T23:00:00+00:00"),
    ("2026-01-31T23:00:00-08:00", 1, "2026-01-01T07:00:00+00:00"),
])
def test_calendar_month_subtraction_clamps_days_and_normalizes_before_arithmetic(now, months, expected):
    assert calendar_cutoff(datetime.fromisoformat(now), months) == datetime.fromisoformat(expected)


def test_partial_boundary_month_is_preserved():
    now = datetime(2026, 10, 27, 15, 16, tzinfo=UTC)
    cutoff = calendar_cutoff(now, 3)
    effective = complete_month_cutoff(now, 3)
    assert effective == datetime(2026, 7, 1, tzinfo=UTC)
    assert effective < cutoff
    assert not datetime(2026, 7, 5, tzinfo=UTC) < effective
    assert datetime(2026, 6, 30, 23, 59, 59, tzinfo=UTC) < effective


def test_exact_boundary_is_exclusive_and_no_month_is_skipped():
    now = datetime(2026, 10, 1, tzinfo=UTC)
    assert calendar_cutoff(now, 3) == complete_month_cutoff(now, 3) == datetime(2026, 7, 1, tzinfo=UTC)


def test_all_supported_months_keep_at_least_requested_window():
    # Exercise every supported lifetime over leap-day and UTC midnight inputs.
    for now in (datetime(2024, 2, 29, 23, 59, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)):
        for months in range(1, MAX_RETENTION_MONTHS + 1):
            cutoff = calendar_cutoff(now, months)
            effective = complete_month_cutoff(now, months)
            assert effective <= cutoff < now
            assert effective.day == 1 and effective.hour == effective.minute == effective.second == 0


def test_zero_is_forever_for_both_cutoffs():
    now = datetime.now(UTC)
    assert calendar_cutoff(now, 0) is None
    assert complete_month_cutoff(now, 0) is None


def test_naive_timestamp_is_rejected_instead_of_using_machine_timezone():
    with pytest.raises(ValueError, match="timezone"):
        calendar_cutoff(datetime(2026, 10, 1), 3)


def test_old_config_missing_policy_keeps_every_scope_forever():
    merged = _deep_merge(DEFAULT_CONFIG, {"storage": {"postgres": {"enabled": True}}})
    policy = retention_policy(merged)
    assert policy.enabled is False
    assert all(policy.months_for(scope) == 0 for scope in RETENTION_SCOPES)
    assert retention_policy({}).months == 0
    assert not any("retention" in warning for warning in validate(merged))


def test_overrides_can_keep_specific_scope_forever_and_do_not_mutate_input():
    cfg = config_with_policy(months=6, enabled=True, overrides={"packets": 3, "chat": 0})
    before = deepcopy(cfg)
    policy = retention_policy(cfg)
    assert policy.months_for("packets") == 3
    assert policy.months_for("chat") == 0
    assert policy.months_for("telemetry") == 6
    assert cfg == before
    cfg["storage"]["retention"]["overrides"]["chat"] = 12
    assert policy.months_for("chat") == 0
    with pytest.raises(TypeError):
        policy.overrides["chat"] = 1
    with pytest.raises(ValueError, match="Unknown"):
        policy.months_for("nodes")


@pytest.mark.parametrize("policy", [
    {"months": True}, {"months": False}, {"months": -1}, {"months": 1.5},
    {"months": "3"}, {"months": None}, {"months": MAX_RETENTION_MONTHS + 1},
    {"enabled": 1}, {"enabled": "true"}, {"overrides": []},
    {"overrides": {"packets": True}}, {"overrides": {"telemetry": -1}},
    {"overrides": {"nodes": 3}}, {"month": 3},
])
def test_invalid_policy_is_fatal_even_while_disabled(policy):
    cfg = _deep_merge(DEFAULT_CONFIG, config_with_policy(**policy))
    cfg["storage"]["postgres"]["enabled"] = True
    with pytest.raises(ConfigValidationError, match="retention"):
        validate(cfg)


@pytest.mark.parametrize("bad", [None, [], "forever", 6])
def test_wrong_shaped_policy_is_fatal(bad):
    cfg = deepcopy(DEFAULT_CONFIG)
    cfg["storage"]["postgres"]["enabled"] = True
    cfg["storage"]["retention"] = bad
    with pytest.raises(ConfigValidationError, match="retention"):
        validate(cfg)


def test_preview_separates_configured_enablement_from_executor_availability():
    cfg = config_with_policy(enabled=True, months=3, overrides={"chat": 0})
    now = datetime(2026, 10, 27, tzinfo=UTC)
    preview = retention_summary(cfg, now=now)
    assert preview["configured_enabled"] is True
    assert preview["enforcement_enabled"] is False
    assert preview["generated_at"] == "2026-10-27T00:00:00Z"
    assert preview["scopes"]["packets"]["cutoff"] == "2026-07-27T00:00:00Z"
    assert preview["scopes"]["packets"]["effective_cutoff"] == "2026-07-01T00:00:00Z"
    assert preview["scopes"]["chat"]["effective_cutoff"] is None
    assert retention_summary(cfg, now=now, enforcement_available=True)["enforcement_enabled"] is True
    cfg["storage"]["retention"]["enabled"] = False
    assert retention_summary(cfg, now=now, enforcement_available=True)["enforcement_enabled"] is False
