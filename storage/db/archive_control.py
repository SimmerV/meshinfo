"""Shared scheduler/CLI entry point for archive preparation and bounded work."""
from pathlib import Path
import shutil
import uuid

from storage.db.archive import (
    archive_policy, cancel_archive, ensure_archive_schema, plan_archive,
    run_archive_cycle, start_guarded_archive, start_restore_archive,
)
from storage.db.archive_catalog import prepare_archive_indexes


async def run_archive_work(conn, config, *, free_bytes=None, target_job_id=None):
    policy = archive_policy(config)
    if target_job_id is not None:
        target_job_id = uuid.UUID(str(target_job_id))
    if not await conn.fetchval("SELECT to_regclass('public.storage_archive_jobs')"):
        if target_job_id is not None:
            return {'status': 'blocked', 'reason': 'Requested archive job does not exist'}
        if not policy.enabled:
            return {'status': 'disabled'}
        await ensure_archive_schema(conn)
    restoring = False
    if target_job_id is not None:
        job = await conn.fetchrow("SELECT job_id,target_method,phase,"
                                  "coalesce((to_jsonb(storage_archive_jobs)->>'cleanup_requested')::boolean,false) AS cleanup_requested "
                                  "FROM public.storage_archive_jobs WHERE job_id=$1",
                                  target_job_id)
        if job is None:
            return {'status': 'blocked', 'job_id': str(target_job_id), 'reason': 'Requested archive job does not exist'}
        if job['phase'] in ('complete', 'failed', 'cancelled'):
            # Terminal target status must not depend on current filesystem
            # capacity, nor trigger preparation or another month's admission.
            return await run_archive_cycle(conn, config, free_bytes=free_bytes, target_job_id=target_job_id)
    else:
        job = await conn.fetchrow("SELECT job_id,target_method,phase,"
                                  "coalesce((to_jsonb(storage_archive_jobs)->>'cleanup_requested')::boolean,false) AS cleanup_requested "
                                  "FROM public.storage_archive_jobs "
                                  "WHERE phase IN ('copying','verifying','ready') LIMIT 1")
    if job is not None and job.get('cleanup_requested'):
        # Cleanup reclaims capacity and must survive a missing free-space
        # probe, disabled policy, or an interrupted index preparation. The
        # engine verifies ownership and performs only this exact job's cleanup.
        return await run_archive_cycle(conn, config, free_bytes=free_bytes,
                                       target_job_id=uuid.UUID(str(job['job_id'])))
    if not policy.enabled:
        restoring = job is not None and job['target_method'] == 'heap'
        if job is not None and not restoring:
            result = await cancel_archive(conn, job['job_id'], config=config)
            # Cancellation can be busy, refused, or only partly reclaimed.
            # Preserve its diagnostic and claim removal only after completion.
            result.setdefault('reason',
                'Archiving disabled; unfinished replacement discarded, original history preserved'
                if result['status'] == 'cancelled' else
                'Archiving disabled; cancellation requested but not yet complete')
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
    if not restoring and target_job_id is None:
        preparation = await prepare_archive_indexes(conn, config, free_bytes=free_bytes)
        if preparation['status'] != 'complete':
            return preparation
    if target_job_id is not None:
        return await run_archive_cycle(conn, config, free_bytes=free_bytes, target_job_id=target_job_id)
    return await run_archive_cycle(conn, config, free_bytes=free_bytes)


async def start_guarded_archive_work(conn, partition, config, *, free_bytes=None):
    """Explicitly admit one named month without relaxing actual capacity checks."""
    if not isinstance(partition, str) or not partition.strip():
        raise ValueError('Guarded admission requires a named partition')
    policy = archive_policy(config)
    if not policy.enabled:
        return {'status': 'blocked', 'reason': 'Enable archiving before requesting guarded admission'}
    if not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='citus_columnar')"):
        return {'status': 'blocked', 'reason': 'Install and preload citus_columnar before enabling archiving'}
    if free_bytes is None:
        filesystem = policy.database_filesystem
        if not filesystem or not Path(filesystem).is_dir():
            return {'status': 'blocked', 'reason': 'Configure the actual PostgreSQL data filesystem for the free-space probe'}
        free_bytes = lambda: shutil.disk_usage(filesystem).free
    await ensure_archive_schema(conn)
    preview = await plan_archive(conn, config)
    if not any(row['name'] == partition and row['eligible'] and row['method'] == 'heap'
               for row in preview['partitions']):
        return {'status': 'blocked', 'partition': partition,
                'reason': 'Guarded admission requires the named eligible heap month'}
    preparation = await prepare_archive_indexes(conn, config, free_bytes=free_bytes)
    if preparation['status'] != 'complete':
        return preparation
    return await start_guarded_archive(conn, partition, config, free_bytes=free_bytes)


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
