"""Dedicated, bounded PostgreSQL sessions for long-running maintenance.

Maintenance never borrows a connection from the ingestion/API pool. One extra
session is used for the duration of an operation; backup pg_dump uses its own
additional connection. Connection acquisition and closure finish safely even
when shutdown cancellation repeats.
"""

import asyncio
from collections.abc import Mapping
from contextlib import asynccontextmanager
import logging

import asyncpg

logger = logging.getLogger(__name__)
CONNECT_TIMEOUT_SECONDS = 5
COMMAND_TIMEOUT_SECONDS = 10
CLOSE_TIMEOUT_SECONDS = 3
_DEFAULTS = {"host": "postgres", "port": 5432, "database": "meshinfo", "username": "postgres", "password": "password"}


def database_settings(storage, config) -> dict:
    """Copy only endpoint/credential settings, preferring the active storage configuration."""
    configured = config.get("storage", {}).get("postgres", {})
    runtime = getattr(storage, "pg_config", {}) if storage is not None else {}
    if not isinstance(configured, Mapping) or not isinstance(runtime, Mapping):
        raise ValueError("PostgreSQL maintenance settings must be a mapping")
    return {name: runtime.get(name, configured.get(name, default)) for name, default in _DEFAULTS.items()}


async def _drain(task):
    cancelled = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            cancelled = exc
        except Exception:
            break
    if cancelled is not None:
        raise cancelled


async def _close(conn):
    task = asyncio.create_task(conn.close(timeout=CLOSE_TIMEOUT_SECONDS))
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        try:
            await _drain(task)
        finally:
            if task.cancelled() or task.exception() is not None:
                conn.terminate()
        raise
    except Exception as exc:
        conn.terminate()
        logger.warning("Maintenance control session required termination after %s", type(exc).__name__)


@asynccontextmanager
async def maintenance_connection(storage, config, *, application_name="meshinfo-maintenance-control"):
    """Yield an owned session, never an application-pool slot.

    The five-second connect limit and ten-second command timeout mirror the
    application connection settings while bounding connection establishment.
    Callers may impose shorter SQL/operation limits. Explicit close is bounded
    to three seconds and falls back to immediate termination on failure.
    """
    pg = database_settings(storage, config)
    pending = asyncio.create_task(asyncpg.connect(
        host=pg["host"], port=pg["port"], database=pg["database"],
        user=pg["username"], password=pg["password"], timeout=CONNECT_TIMEOUT_SECONDS,
        command_timeout=COMMAND_TIMEOUT_SECONDS,
        server_settings={"application_name": application_name, "timezone": "UTC"},
    ))
    try:
        conn = await asyncio.shield(pending)
    except asyncio.CancelledError:
        # Resolve a connection that may have succeeded at the cancellation
        # boundary; it must not become an unobserved, leaked backend session.
        try:
            await _drain(pending)
        finally:
            if not pending.cancelled() and pending.exception() is None:
                await _close(pending.result())
        raise
    try:
        yield conn
    finally:
        await _close(conn)
