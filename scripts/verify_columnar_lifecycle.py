#!/usr/bin/env python3
"""Exercise native conversion locks and rollback in an isolated prototype DB.

Uses synthetic data only. This is not an online migration implementation.
"""
import argparse
import asyncio
import json
import os
from pathlib import Path
import re
import time
from urllib.parse import unquote, urlsplit
import uuid

import asyncpg


def require(condition, message):
    if not condition:
        raise AssertionError(message)


async def run(args):
    dsn = os.environ[args.dsn_env]
    database = unquote(urlsplit(dsn).path.lstrip('/'))
    if not re.fullmatch(r'[A-Za-z0-9_]+_columnar_test', database):
        raise ValueError('A dedicated *_columnar_test database is required')
    settings = {'application_name': 'columnar-lifecycle-proof',
                'statement_timeout': '15000', 'lock_timeout': '250ms',
                'work_mem': '4MB', 'maintenance_work_mem': '32MB'}
    a = await asyncpg.connect(dsn, server_settings=settings, timeout=5)
    b = None
    schema = 'benchmark_lifecycle_' + uuid.uuid4().hex[:10]
    table = schema + '.history'
    report = {'synthetic_rows': 10000, 'prototype_only': True}

    async def fingerprint():
        return dict(await a.fetchrow(f'''SELECT count(*) AS rows,
            md5(string_agg(id::text || ':' || payload, '|' ORDER BY id)) AS digest
            FROM {table}'''))

    async def method():
        return await a.fetchval('''SELECT am.amname FROM pg_class c
            JOIN pg_am am ON am.oid=c.relam WHERE c.oid=$1::regclass''', table)

    try:
        if await a.fetchval('SELECT current_database()') != database:
            raise ValueError('Connected database differs from guarded target')
        b = await asyncpg.connect(dsn, server_settings=settings, timeout=5)
        await a.execute(f'CREATE SCHEMA {schema}')
        await a.execute(f'''CREATE TABLE {table}(id bigint PRIMARY KEY,payload text);
            INSERT INTO {table} SELECT i, repeat('lossless-row-',40)||i
            FROM generate_series(1,10000) i''')
        expected = await fingerprint()
        held = a.transaction()
        await held.start()
        try:
            await a.execute(f'LOCK TABLE {table} IN ACCESS SHARE MODE')
            started = time.perf_counter()
            try:
                await b.execute(f'ALTER TABLE {table} SET ACCESS METHOD columnar')
            except asyncpg.LockNotAvailableError as exc:
                report['busy_reader_conversion'] = {
                    'sqlstate': exc.sqlstate,
                    'elapsed_ms': round((time.perf_counter()-started)*1000, 3)}
            else:
                raise AssertionError('Conversion unexpectedly bypassed a reader lock')
        finally:
            await held.rollback()
        require(await method() == 'heap' and await fingerprint() == expected,
                'Timed-out conversion changed the original table')
        report['failed_conversion_preserved_heap_and_rows'] = True

        # DDL rollback must restore both access method and the original rows.
        tx = a.transaction()
        await tx.start()
        try:
            await a.execute(f'ALTER TABLE {table} SET ACCESS METHOD columnar')
            require(await method() == 'columnar' and await fingerprint() == expected,
                    'Uncommitted conversion did not preserve rows')
        finally:
            await tx.rollback()
        require(await method() == 'heap' and await fingerprint() == expected,
                'Conversion rollback did not restore the original table')
        report['conversion_rollback_preserved_heap_and_rows'] = True

        before_bytes = await a.fetchval('SELECT pg_total_relation_size($1::regclass)', table)
        lsn = await a.fetchval('SELECT pg_current_wal_insert_lsn()::text')
        tx = a.transaction()
        await tx.start()
        try:
            started = time.perf_counter()
            await a.execute(f'ALTER TABLE {table} SET ACCESS METHOD columnar')
            report['conversion_ms'] = round((time.perf_counter()-started)*1000, 3)
            # Keep the conversion transaction open to demonstrate that readers
            # cannot pass its ACCESS EXCLUSIVE lock until commit.
            try:
                await b.fetchval(f'SELECT count(*) FROM {table}')
            except asyncpg.LockNotAvailableError as exc:
                report['reader_during_conversion_transaction_sqlstate'] = exc.sqlstate
            else:
                raise AssertionError('Expected reader to wait for conversion commit')
            await tx.commit()
        except BaseException:
            await tx.rollback()
            raise
        require(await method() == 'columnar' and await fingerprint() == expected,
                'Committed conversion did not preserve rows')
        report['committed_conversion_preserved_rows'] = True
        report['relation_bytes'] = {
            'heap': before_bytes,
            'columnar': await a.fetchval('SELECT pg_total_relation_size($1::regclass)', table)}
        report['conversion_wal_bytes'] = int(await a.fetchval(
            'SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::text::pg_lsn)', lsn))
        await a.execute(f'REINDEX TABLE {table}')
        require(await fingerprint() == expected, 'REINDEX changed logical rows')
        report['plain_btree_reindex_preserved_rows'] = True

        # Confirm the startup backfill hazard, including the heap-only match:
        # without a date predicate UPDATE cannot prune the columnar child.
        await a.execute(f'''CREATE TABLE {schema}.mixed(id bigint,day date,payload text)
            PARTITION BY RANGE(day);
            CREATE TABLE {schema}.cold PARTITION OF {schema}.mixed
            FOR VALUES FROM ('2020-01-01') TO ('2020-02-01') USING columnar;
            CREATE TABLE {schema}.hot PARTITION OF {schema}.mixed
            FOR VALUES FROM ('2020-02-01') TO ('2020-03-01');
            INSERT INTO {schema}.mixed VALUES(1,'2020-01-01','cold'),(2,'2020-02-01','hot')''')
        try:
            await a.execute(f"UPDATE {schema}.mixed SET payload='changed' WHERE id=2")
        except asyncpg.FeatureNotSupportedError as exc:
            report['unpruned_parent_update_sqlstate'] = exc.sqlstate
        else:
            raise AssertionError('Expected columnar UPDATE restriction was not reproduced')
        require(await a.fetchval(f'SELECT payload FROM {schema}.mixed WHERE id=2') == 'hot',
                'Rejected UPDATE changed a hot row')
        report['pruned_hot_update'] = await a.execute(
            f"UPDATE {schema}.mixed SET payload='changed' WHERE id=2 AND day='2020-02-01'")
        report['all_checks_passed'] = True
        report['limits'] = [
            'Synthetic bounded table, not a large partition cutover benchmark.',
            'Native conversion blocks readers and writers until transaction commit.',
            'WAL bytes exclude checkpoint/FPI variation and are not a capacity forecast.',
            'DDL rollback verified; physical crash recovery and crash-safe migration not tested.']
        return report
    finally:
        if b is not None:
            await b.close()
        try:
            await a.execute(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
        finally:
            await a.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dsn-env', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        result = asyncio.run(run(args))
    except Exception as exc:
        print(f'Lifecycle verification failed: {type(exc).__name__}')
        return 1
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    print('Isolated conversion lifecycle checks passed.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
