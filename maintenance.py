#!/usr/bin/env python3
"""
In-app scheduled maintenance (#526): pg_dump snapshot backups driven by
config.toml's [backups] section.

main.maintenance_loop enforces the schedule, so a config change takes effect
on the next restart — no host cron required. scripts/backup_db.sh remains the
host-side alternative using host SSH keys. Both paths share verification,
streaming, ownership and independent backup-retention rules (see STORAGE.md).
"""

import asyncio
import logging
import time
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent
BACKUP_GLOB = "meshinfo-pg-*.dump"
BACKUP_INTERVAL_DAYS = {"daily": 1, "weekly": 7, "monthly": 30}

# Failure backoff so a broken pg_dump doesn't retry every maintenance tick.
_next_backup_attempt_monotonic = 0.0


async def run_retention(config, storage) -> dict:
    """Run one bounded cycle and retain an operator-visible durable result."""
    from storage.db.maintenance_status import prepare_retention_index, save_status
    from storage.db.retention_executor import enforce_retention

    try:
        async with storage.pool.acquire(timeout=3) as conn:
            try:
                await prepare_retention_index(conn, config)
            except Exception:
                # Executor independently verifies usable indexes and reports a
                # blocked neighbor scope; other history families can progress.
                logger.exception("Retention age-index preparation failed")
            try:
                result = await enforce_retention(conn, config)
            except Exception as exc:
                logger.exception("Retention execution failed")
                result = {"status": "error", "reason": type(exc).__name__}
            try:
                await save_status(conn, "retention", result)
            except Exception:
                logger.exception("Retention status could not be persisted")
                result = {**result, "status": "error", "status_persisted": False}
        if result["status"] in {"error", "blocked", "partial"}:
            logger.warning("Retention cycle: %s", result)
        return result
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Retention maintenance failed")
        return {"status": "error", "status_persisted": False}


def backup_dir(config: Dict[str, Any]) -> Path:
    d = Path(config.get("backups", {}).get("dir") or "backups")
    return d if d.is_absolute() else REPO_ROOT / d


async def run_compression(config, storage, *, after=None) -> dict:
    """One opt-in, space-checked closed-index packing attempt per hour."""
    from storage.db.index_maintenance import run_index_packing_cycle
    from storage.db.compression import CompressionRefused
    from storage.db.maintenance_status import save_status

    result = {"status": "error"}
    try:
        if after is not None:
            await asyncio.shield(after)
        async with storage.pool.acquire(timeout=3) as conn:
            try:
                result = await run_index_packing_cycle(conn, config)
            except CompressionRefused as exc:
                result = {"status": "blocked", "reason": str(exc)}
            except Exception as exc:
                logger.exception("Index packing could not finish")
                result = {"status": "error", "reason": type(exc).__name__}
            await save_status(conn, "compression", result)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Index-packing status could not be persisted")
        result = {**result, "status": "error", "status_persisted": False}
    return result


def latest_backup_age_days(dir_path: Path, now: Optional[float] = None) -> Optional[float]:
    """Age in days of the newest finished dump, or None if there is none."""
    now = time.time() if now is None else now
    mtimes = [p.stat().st_mtime for p in dir_path.glob(BACKUP_GLOB) if p.is_file()]
    if not mtimes:
        return None
    return (now - max(mtimes)) / 86400.0


def backup_due(config: Dict[str, Any], now: Optional[float] = None) -> bool:
    interval = BACKUP_INTERVAL_DAYS.get(config.get("backups", {}).get("schedule", "off"))
    if not interval:
        return False
    if time.monotonic() < _next_backup_attempt_monotonic:
        return False
    age = latest_backup_age_days(backup_dir(config), now)
    return age is None or age >= interval


async def scheduled_backup_due(config, storage) -> bool:
    """Use durable status for both local and streamed backup schedules."""
    from storage.backup_controller import scheduled_backup_due as due
    return await due(config, storage)


async def run_backup(config, storage, *, after=None) -> dict:
    """Coordinated snapshot; keeps the maintenance lock through child cleanup."""
    from storage.backup_controller import run_backup as snapshot
    return await snapshot(config, storage, REPO_ROOT, after=after)
