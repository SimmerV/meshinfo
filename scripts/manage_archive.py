#!/usr/bin/env python3
"""Preview or explicitly advance lossless historical packet compression."""
import argparse
import asyncio
import json
from pathlib import Path
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.manage_storage import load_config
from storage.db.archive import cancel_archive, plan_archive
from storage.db.archive_control import run_archive_work, restore_archive_work, start_guarded_archive_work
from storage.db.maintenance_connection import maintenance_connection
from storage.db.maintenance_status import save_status


class ArchiveArgumentsError(ValueError):
    """Operator argument errors with safe, actionable messages."""


async def run(args):
    guarded = args.allow_estimate_shortfall
    if guarded and (args.command != 'run' or not args.apply or not args.partition or args.job):
        raise ArchiveArgumentsError('--allow-estimate-shortfall requires run, --partition NAME and --apply, without --job')
    if args.partition and args.command != 'restore' and not guarded:
        raise ArchiveArgumentsError('--partition requires restore or run --allow-estimate-shortfall')
    if args.job and args.command not in ('run', 'cancel'):
        raise ArchiveArgumentsError('--job requires run or cancel')
    try:
        target_job_id = str(uuid.UUID(args.job)) if args.job and args.command == 'run' else None
    except ValueError as exc:
        raise ArchiveArgumentsError('run --job requires a valid UUID') from exc
    config = load_config(args.config)
    if args.host:
        config['storage']['postgres']['host'] = args.host
    if args.database:
        config['storage']['postgres']['database'] = args.database
    if args.command != 'plan' and not args.apply:
        raise ArchiveArgumentsError('run, cancel and restore require --apply; plan is read-only')
    if args.command == 'cancel' and not args.job:
        raise ArchiveArgumentsError('cancel requires --job UUID')
    if args.command == 'restore' and not args.partition:
        raise ArchiveArgumentsError('restore requires --partition NAME')
    async with maintenance_connection(None, config, application_name='meshinfo-archive-cli') as conn:
        if args.command == 'plan':
            await conn.execute('SET default_transaction_read_only=on')
            print(json.dumps(await plan_archive(conn, config), default=str, sort_keys=True))
            return 0
        if args.command == 'cancel':
            result = await cancel_archive(conn, args.job, config=config)
            await save_status(conn, 'archive', result)
            print(json.dumps(result, default=str, sort_keys=True))
            return 0 if result['status'] in ('cancelled', 'complete') else 2
        if args.command == 'restore':
            result = await restore_archive_work(conn, args.partition, config)
            await save_status(conn, 'archive', result)
            print(json.dumps(result, default=str, sort_keys=True), flush=True)
            if result['status'] != 'copying' or not args.until_idle:
                return 0 if result['status'] == 'copying' else 2
            target_job_id = str(uuid.UUID(str(result['job_id'])))
        for _ in range(args.max_cycles if args.until_idle else 1):
            if guarded and target_job_id is None:
                result = await start_guarded_archive_work(conn, args.partition, config)
                if result['status'] == 'copying':
                    if result.get('partition') != args.partition or not result.get('job_id'):
                        raise ValueError('Guarded admission returned an unexpected target')
                    target_job_id = str(uuid.UUID(str(result['job_id'])))
            elif target_job_id is not None:
                result = await run_archive_work(conn, config, target_job_id=target_job_id)
                if (result.get('job_id') is not None
                        and str(result['job_id']) != target_job_id):
                    raise ValueError('Archive progress returned an unexpected target job')
                if result['status'] in ('copying', 'verifying', 'ready', 'cleanup_pending', 'complete', 'failed', 'cancelled') and not result.get('job_id'):
                    raise ValueError('Archive progress did not identify the requested target job')
            else:
                result = await run_archive_work(conn, config)
            await save_status(conn, 'archive', result)
            print(json.dumps(result, default=str, sort_keys=True), flush=True)
            status = result['status']
            if (args.command == 'restore' or target_job_id is not None) and status == 'complete':
                return 0
            if status in ('idle', 'no_candidates', 'disabled'):
                return 2 if guarded or target_job_id is not None else 0
            if status in ('blocked', 'error', 'failed', 'cancelled'):
                return 2
            if not args.until_idle:
                return 0 if status == 'complete' else 2
            await asyncio.sleep(0.25 if status != 'busy' else 2)
        return 2


def bounded_cycles(value):
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError('cycles must be an integer from 1 to 10000') from exc
    if not 1 <= result <= 10000:
        raise argparse.ArgumentTypeError('cycles must be an integer from 1 to 10000')
    return result


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument('command', choices=('plan', 'run', 'cancel', 'restore'), nargs='?', default='plan')
    result.add_argument('--config', type=Path, default=Path('config.toml'))
    result.add_argument('--host', help='Explicit database hostname override')
    result.add_argument('--database', help='Explicit database name override')
    result.add_argument('--apply', action='store_true')
    result.add_argument('--job', help='Resume only this job with run, or cancel it while preserving its original')
    result.add_argument('--partition', help='Named monthly partition for restore or guarded admission')
    result.add_argument('--allow-estimate-shortfall', action='store_true',
                        help='Explicit named heap-month trial; retain actual reserve/WAL limits (requires run, --partition and --apply)')
    result.add_argument('--until-idle', action='store_true', help='Keep advancing checkpointed cycles until no work remains')
    result.add_argument('--max-cycles', type=bounded_cycles, default=10000, metavar='1..10000')
    return result


def main():
    try:
        return asyncio.run(run(parser().parse_args()))
    except KeyboardInterrupt:
        print('Interrupted; committed archive checkpoints remain resumable.', file=sys.stderr)
        return 130
    except ArchiveArgumentsError as exc:
        print('Archive arguments: ' + str(exc), file=sys.stderr)
        return 2
    except Exception as exc:
        print('Archive operation failed: ' + type(exc).__name__, file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
