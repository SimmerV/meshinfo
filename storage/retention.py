"""Validated history policy and UTC calendar arithmetic, without database I/O.

A finite policy keeps *at least* the requested number of calendar months. The
executor may expire only complete UTC months before ``effective_cutoff``. Zero
means forever; it never means delete everything. Current node state, channel
configuration, and backups are outside these history scopes.
"""

from calendar import monthrange
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType

UTC = timezone.utc
MAX_RETENTION_MONTHS = 1200
RETENTION_SCOPES = ("packets", "telemetry", "chat", "traceroutes", "neighbors")
SCOPE_TABLES = {
    "packets": ("mqtt_messages", "packet_receptions"),
    "telemetry": ("telemetry",),
    "chat": ("chat_messages",),
    "traceroutes": ("traceroutes",),
    "neighbors": ("node_neighborinfo_history",),
}


def _validate_months(value: object, path: str) -> int:
    if type(value) is not int or not 0 <= value <= MAX_RETENTION_MONTHS:
        raise ValueError(f"{path} must be an integer from 0 to {MAX_RETENTION_MONTHS} (0 means forever)")
    return value


def utc_datetime(value: datetime) -> datetime:
    """Normalize an aware timestamp; reject ambiguous local/naive timestamps."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Retention timestamps must include a timezone")
    return value.astimezone(UTC)


def calendar_cutoff(now: datetime, months: int) -> datetime | None:
    """Subtract calendar months in UTC, clamping the day at the month end."""
    months = _validate_months(months, "months")
    now = utc_datetime(now)
    if months == 0:
        return None
    ordinal = now.year * 12 + now.month - 1 - months
    year, month_zero = divmod(ordinal, 12)
    if year < 1:
        raise ValueError("Retention cutoff is outside the supported datetime range")
    month = month_zero + 1
    return now.replace(year=year, month=month, day=min(now.day, monthrange(year, month)[1]))


def complete_month_cutoff(now: datetime, months: int) -> datetime | None:
    """Exclusive expiration boundary, rounded down to the start of a UTC month."""
    cutoff = calendar_cutoff(now, months)
    return None if cutoff is None else cutoff.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def timestamp_json(value: datetime | None) -> str | None:
    return None if value is None else utc_datetime(value).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class RetentionPolicy:
    enabled: bool
    months: int
    overrides: Mapping[str, int]

    def months_for(self, scope: str) -> int:
        if scope not in RETENTION_SCOPES:
            raise ValueError(f"Unknown retention scope: {scope}")
        return self.overrides.get(scope, self.months)


def retention_policy(config: Mapping) -> RetentionPolicy:
    """Read the full application config, rejecting unsafe policy mistakes.

    Missing configuration preserves all history. Invalid values raise even if
    enforcement is disabled, so enabling a previously mistyped policy cannot
    silently select an unexpected lifetime.
    """
    storage = config.get("storage", {})
    if not isinstance(storage, Mapping):
        raise ValueError("storage must be a table")
    raw = storage.get("retention", {})
    if not isinstance(raw, Mapping):
        raise ValueError("storage.retention must be a table")
    unknown = set(raw) - {"enabled", "months", "overrides"}
    if unknown:
        raise ValueError(f"Unknown storage.retention setting(s): {', '.join(sorted(unknown))}")
    enabled = raw.get("enabled", False)
    if type(enabled) is not bool:
        raise ValueError("storage.retention.enabled must be a boolean")
    months = _validate_months(raw.get("months", 0), "storage.retention.months")
    overrides = raw.get("overrides", {})
    if not isinstance(overrides, Mapping):
        raise ValueError("storage.retention.overrides must be a table")
    validated = {}
    for scope, value in overrides.items():
        if scope not in RETENTION_SCOPES:
            raise ValueError(f"Unknown storage.retention.overrides scope: {scope}")
        validated[scope] = _validate_months(value, f"storage.retention.overrides.{scope}")
    return RetentionPolicy(enabled, months, MappingProxyType(validated))


def retention_summary(config: Mapping, *, now: datetime | None = None,
                      enforcement_available: bool = False) -> dict:
    """Public policy envelope. Availability is added by the catalog reporter."""
    now = utc_datetime(now or datetime.now(UTC))
    policy = retention_policy(config)
    scopes = {}
    for scope in RETENTION_SCOPES:
        months = policy.months_for(scope)
        scopes[scope] = {
            "months": months,
            "cutoff": timestamp_json(calendar_cutoff(now, months)),
            "effective_cutoff": timestamp_json(complete_month_cutoff(now, months)),
            "oldest_available": None,
            "newest_available": None,
            "available_bounds_kind": "unknown",
        }
    return {
        "generated_at": timestamp_json(now),
        "enforcement_enabled": bool(enforcement_available and policy.enabled),
        "configured_enabled": policy.enabled,
        "granularity": "calendar_month",
        "scopes": scopes,
    }
