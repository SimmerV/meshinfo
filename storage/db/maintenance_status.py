"""Small durable maintenance results; public summaries exclude operator details."""

import json

from storage.retention import RETENTION_SCOPES, retention_policy

OPERATIONS = frozenset({"retention", "compression", "backup"})
STATUSES = frozenset({"disabled", "forever", "busy", "complete", "partial", "blocked", "error", "success", "unknown"})


async def prepare_retention_index(connection, config: dict) -> None:
    """Build the one missing age index only when that policy is enabled.

    CONCURRENTLY avoids blocking ingestion. An interrupted build can leave an
    invalid index; surface it for explicit repair instead of trusting IF EXISTS.
    """
    policy = retention_policy(config)
    if not policy.enabled or not policy.months_for("neighbors"):
        return
    from storage.db.retention_executor import MAINTENANCE_LOCK_KEY

    if not await connection.fetchval("SELECT pg_try_advisory_lock($1)", MAINTENANCE_LOCK_KEY):
        return
    try:
        state = await connection.fetchval(
            """SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
               JOIN pg_namespace n ON n.oid=c.relnamespace
               WHERE n.nspname='public' AND c.relname='idx_neighbor_history_created_at'""",
            timeout=3,
        )
        if state is False:
            raise RuntimeError("idx_neighbor_history_created_at is invalid; drop it CONCURRENTLY and retry maintenance")
        if state is None:
            await connection.execute("SET lock_timeout='250ms'")
            try:
                await connection.execute(
                    "CREATE INDEX CONCURRENTLY idx_neighbor_history_created_at "
                    "ON public.node_neighborinfo_history(created_at)", timeout=30,
                )
            finally:
                await connection.execute("RESET lock_timeout")
    finally:
        await connection.execute("SELECT pg_advisory_unlock($1)", MAINTENANCE_LOCK_KEY)


async def save_status(connection, operation: str, result: dict) -> None:
    if operation not in OPERATIONS:
        raise ValueError("Unknown maintenance operation")
    # Results contain counts and diagnostic codes, never credentials or dump data.
    await connection.execute(
        """INSERT INTO storage_maintenance_status (operation, updated_at, result)
           VALUES ($1, clock_timestamp(), $2::jsonb)
           ON CONFLICT (operation) DO UPDATE
           SET updated_at = EXCLUDED.updated_at, result = EXCLUDED.result""",
        operation, json.dumps(result, default=str), timeout=3,
    )


async def read_status(connection) -> dict:
    if not await connection.fetchval("SELECT to_regclass('public.storage_maintenance_status')", timeout=3):
        return {}
    rows = await connection.fetch(
        "SELECT operation, updated_at, result FROM storage_maintenance_status", timeout=3,
    )
    return {
        row["operation"]: {
            "updated_at": row["updated_at"].isoformat(),
            "result": json.loads(row["result"]) if isinstance(row["result"], str) else row["result"],
        }
        for row in rows
    }


def public_status(statuses: dict) -> dict:
    """Explicit allowlist: SQL diagnostics and remote destinations stay private."""
    result = {}
    for operation, entry in statuses.items():
        if operation not in OPERATIONS:
            continue
        detail = entry["result"]
        result[operation] = {
            "updated_at": entry["updated_at"],
            "status": detail.get("status") if detail.get("status") in STATUSES else "unknown",
            "scopes": {
                name: {"status": value.get("status") if value.get("status") in STATUSES else "unknown"}
                for name, value in detail.get("scopes", {}).items()
                if name in RETENTION_SCOPES and isinstance(value, dict)
            },
        }
    return result
