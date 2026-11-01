"""Legacy tools refuse archive storage before mutations or service shutdown."""
import asyncio
from contextlib import asynccontextmanager
from datetime import date
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest

from scripts import compact_mqtt_partitions as compact

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def shell_tools(tmp_path):
    scripts = tmp_path / 'scripts'
    scripts.mkdir()
    for name in ('migrate-postgres.sh', 'migrate-mqtt-partitioning.sh'):
        shutil.copyfile(ROOT / 'scripts' / name, scripts / name)
    bindir = tmp_path / 'bin'
    bindir.mkdir()
    docker = bindir / 'docker'
    docker.write_text('''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
a = sys.argv[1:]
with open(os.environ['GUARD_EVENTS'], 'a') as f: f.write(json.dumps(a)+'\\n')
if a[:2] == ['compose', 'version'] or a[:2] == ['volume', 'inspect']: pass
elif a[:2] == ['compose', 'config']: print('services:\\n  postgres:\\n    image: postgres:18')
elif a[:2] == ['run', '--rm']: print('17')
elif 'psql' in a:
 q = a[a.index('-c')+1]
 if 'heap_only' in q:
  mode = os.environ['GUARD_MODE'] if a[0] == 'compose' else os.getenv('GUARD_SECOND', 'blocked')
  if mode == 'error': raise SystemExit(1)
  print(mode)
 elif 'SELECT relkind' in q: print('p')
 else: raise SystemExit(92)
elif a[:3] == ['compose', 'rm', '-sf']: pass
elif a[:2] == ['run', '-d']: print('owned-test-container')
elif 'pg_isready' in a: pass
elif a[:2] == ['rm', '-f']: pass
else: raise SystemExit(93)
''')
    docker.chmod(0o700)
    events = tmp_path / 'events'
    env = {**os.environ, 'PATH': str(bindir)+os.pathsep+os.environ['PATH'],
           'PGDATA_VOLUME': 'isolated_pgdata', 'GUARD_EVENTS': str(events),
           'GUARD_MODE': 'blocked', 'POSTGRES_PASSWORD': 'test-private-password'}
    def run(script, mode='blocked', second='blocked'):
        env.update(GUARD_MODE=mode, GUARD_SECOND=second)
        result = subprocess.run(['bash', str(scripts/script)], env=env, capture_output=True, text=True, timeout=10)
        calls = [json.loads(line) for line in events.read_text().splitlines()]
        assert 'test-private-password' not in result.stdout+result.stderr+json.dumps(calls)
        return result, calls
    return run


@pytest.mark.parametrize('script', ['migrate-postgres.sh', 'migrate-mqtt-partitioning.sh'])
@pytest.mark.parametrize('state', ['blocked', 'error', '', 'unexpected'])
def test_refusal_precedes_service_stop_dump_staging_and_volume_removal(shell_tools, script, state):
    result, calls = shell_tools(script, state)
    assert result.returncode != 0
    assert not any(call[:2] in (['volume', 'rm'], ['rm', '-f'], ['run', '-d']) for call in calls)
    assert not any(call[:2] == ['compose', 'rm'] or call[:2] == ['compose', 'stop'] for call in calls)
    assert not any('pg_dump' in call for call in calls)
    queries = [call[call.index('-c')+1] for call in calls if 'psql' in call]
    assert len(queries) == 1 and 'BEGIN READ ONLY' in queries[0]


def test_major_migration_rechecks_after_old_server_restart_before_any_dump_or_volume_removal(shell_tools):
    result, calls = shell_tools('migrate-postgres.sh', 'heap_only', 'blocked')
    assert result.returncode != 0
    assert any(call[:2] == ['run', '-d'] for call in calls)
    assert not any(call[:2] == ['volume', 'rm'] or 'pg_dump' in call for call in calls)
    assert calls[-1] == ['rm', '-f', 'meshinfo-pg-migrate']  # only its successfully started temporary container
    assert sum('psql' in call for call in calls) == 2


def test_heap_partitioned_installation_keeps_its_noop_behavior(shell_tools):
    result, calls = shell_tools('migrate-mqtt-partitioning.sh', 'heap_only')
    assert result.returncode == 0
    assert 'already partitioned' in result.stdout
    assert not any(call[:2] == ['compose', 'stop'] for call in calls)


class Connection:
    def __init__(self, *, available=True, heap=True):
        self.available, self.heap = available, heap
        self.locked = False
        self.guards = 0

    def is_in_transaction(self):
        return False

    async def fetchval(self, sql, *args, **kwargs):
        if 'pg_try_advisory_lock' in sql:
            self.locked = self.available
            return self.available
        if 'pg_advisory_unlock' in sql:
            self.locked = False
            return True
        assert self.locked
        if sql == compact.LEGACY_HEAP_SQL:
            self.guards += 1
            return self.heap
        if "AT TIME ZONE 'UTC'" in sql:
            return date(1902, 1, 1)
        if 'FROM pg_inherits' in sql:
            return 1
        if 'to_regclass' in sql or 'packet_id IS NOT NULL' in sql:
            return None
        if 'count(*)' in sql or 'pg_total_relation_size' in sql:
            return 0
        raise AssertionError(sql)

    async def execute(self, sql, *args, **kwargs):
        raise AssertionError('Read-only dry run attempted a mutation: '+sql)

    @asynccontextmanager
    async def transaction(self):
        assert self.locked
        yield

    async def cursor(self, *args, **kwargs):
        if False:
            yield None


@pytest.mark.parametrize('state', [False, None])
def test_compactor_refuses_mixed_frozen_or_unknown_before_writes_and_releases_lock(monkeypatch, state):
    conn = Connection(heap=state)
    async def forbidden(*args):
        raise AssertionError('Compactor entered its mutation path')
    monkeypatch.setattr(compact, '_compact_month_locked', forbidden)
    assert not asyncio.run(compact.compact_month(conn, '1901_01', 900, False, True, True))
    assert conn.guards == 1 and not conn.locked


def test_busy_compaction_never_inspects_or_mutates_database(monkeypatch):
    conn = Connection(available=False)
    assert not asyncio.run(compact.compact_month(conn, '1901_01', 900, False, True))
    assert conn.guards == 0 and not conn.locked


def test_compaction_dry_run_performs_no_ddl_or_data_writes():
    conn = Connection()
    assert asyncio.run(compact.compact_month(conn, '1901_01', 900, True, False))
    assert conn.guards == 1 and not conn.locked
