"""Shared scheduler/CLI entry point for archive preparation and bounded work."""
from pathlib import Path
import shutil

from storage.db.archive import archive_policy, cancel_archive, ensure_archive_schema, run_archive_cycle, start_restore_archive
from storage.db.archive_catalog import prepare_archive_indexes


async def run_archive_work(conn, config, *, free_bytes=None):
    policy = archive_policy(config)
    if not await conn.fetchval("SELECT to_regclass('public.storage_archive_jobs')"):
        if not policy.enabled:
            return {'status': 'disabled'}
        await ensure_archive_schema(conn)
    restoring = False
    if not policy.enabled:
        job = await conn.fetchrow("SELECT job_id,target_method FROM public.storage_archive_jobs "
                                  "WHERE phase IN ('copying','verifying','ready') LIMIT 1")
        restoring = job is not None and job['target_method'] == 'heap'
        if job is not None and not restoring:
            result = await cancel_archive(conn, job['job_id'], config=config)
            result['reason'] = 'Archiving disabled; unfinished replacement discarded, original history preserved'
            return result
        if not restoring:
            return {'status': 'disabled'}
    if not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='citus_columnar')"):
        return {'status': 'blocked', 'reason': 'Install and preload citus_columnar before enabling archiving'}
    if free_bytes is None:
        filesystem = policy.database_filesystem
        if not filesystem or not Path(filesystem).is_dir():
            return {'status': 'blocked', 'reason': 'Configure the actual PostgreSQL data filesystem for the free-space probe'}
        free_bytes = lambda: shutil.disk_usage(filesystem).free
    if not restoring:
        preparation = await prepare_archive_indexes(conn, config, free_bytes=free_bytes)
        if preparation['status'] != 'complete':
            return preparation
    return await run_archive_cycle(conn, config, free_bytes=free_bytes)


async def restore_archive_work(conn, partition, config, *, free_bytes=None):
    """Explicit decompression shares the same filesystem reserve as archiving."""
    policy = archive_policy(config)
    if policy.enabled:
        return {'status': 'blocked', 'reason': 'Disable automatic archiving before restoring heap history'}
    if free_bytes is None:
        if not policy.database_filesystem or not Path(policy.database_filesystem).is_dir():
            return {'status': 'blocked', 'reason': 'Configure the actual PostgreSQL data filesystem for the free-space probe'}
        free_bytes = lambda: shutil.disk_usage(policy.database_filesystem).free
    await ensure_archive_schema(conn)
    return await start_restore_archive(conn, partition, config, free_bytes=free_bytes)
