"""Prepare heap topic indexes for mixed heap/columnar history, without a gap.

An attached partitioned GIN index cannot cover a columnar child. Build separate
GIN indexes concurrently on every heap leaf first, then remove the old parent
index in a short transaction. Existing readers keep a usable topic index during
preparation; cancellation never removes an existing user index.
"""
import asyncio
import hashlib

from storage.db.retention_executor import maintenance_lock

MIB = 1024 ** 2


def available_bytes(probe):
    value = probe()
    if type(value) is not int or value < 0:
        raise ValueError('Database filesystem probe must return nonnegative integer bytes')
    return value


def quoted(name):
    return '"' + name.replace('"', '""') + '"'


def relation(schema, name):
    return quoted(schema) + '.' + quoted(name)


def topic_index_name(schema, table):
    return 'meshinfo_topic_' + hashlib.sha256((schema + '.' + table).encode()).hexdigest()[:20]


async def heap_leaves(conn):
    return await conn.fetch("""SELECT c.oid::bigint AS oid,n.nspname AS schema,c.relname AS name,
        c.relkind::text AS kind,c.reltablespace,pg_total_relation_size(c.oid) AS bytes,
        pg_relation_size(c.oid) AS heap_bytes
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        JOIN pg_am am ON am.oid=c.relam
        WHERE am.amname='heap' AND c.relkind='r' AND
          (c.oid='public.mqtt_messages'::regclass OR c.oid IN
            (SELECT inhrelid FROM pg_inherits WHERE inhparent='public.mqtt_messages'::regclass))
        ORDER BY pg_total_relation_size(c.oid),c.oid""")


_OPCLASS_OWNED_SQL = """EXISTS(SELECT 1 FROM pg_depend dependency JOIN pg_extension extension
    ON extension.oid=dependency.refobjid WHERE dependency.classid='pg_opclass'::regclass
      AND dependency.objid=opc.oid AND dependency.refclassid='pg_extension'::regclass
      AND dependency.deptype='e' AND extension.extname='pg_trgm')"""
_TOPIC_METADATA_SQL = """i.indisvalid,i.indisready,i.indisunique,i.indnkeyatts,i.indnatts,
    att.attname AS column_name,(att.atttypid='text'::regtype) AS text_column,
    (i.indcollation[0]=att.attcollation) AS matching_collation,
    opc.opcname AS opclass,""" + _OPCLASS_OWNED_SQL + """ AS extension_opclass,
    pg_get_expr(i.indpred,i.indrelid) AS predicate,
    pg_get_expr(i.indexprs,i.indrelid) AS expressions"""
_TOPIC_JOINS_SQL = """LEFT JOIN pg_attribute att ON att.attrelid=i.indrelid AND att.attnum=i.indkey[0]
    LEFT JOIN pg_opclass opc ON opc.oid=i.indclass[0]"""


async def topic_opclass(conn):
    row = await conn.fetchrow("""SELECT n.nspname AS schema,opc.opcname AS name
        FROM pg_opclass opc JOIN pg_namespace n ON n.oid=opc.opcnamespace
        JOIN pg_am am ON am.oid=opc.opcmethod
        WHERE am.amname='gin' AND opc.opcname='gin_trgm_ops' AND opc.opcintype='text'::regtype
          AND """ + _OPCLASS_OWNED_SQL)
    if row is None:
        raise RuntimeError('The pg_trgm extension-owned text GIN operator class is unavailable')
    return relation(row['schema'], row['name'])


async def parent_gin_indexes(conn):
    return await conn.fetch("""SELECT i.indexrelid::bigint AS oid,idx.relname AS name,"""
        + _TOPIC_METADATA_SQL + """ FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid
        JOIN pg_am am ON am.oid=idx.relam """ + _TOPIC_JOINS_SQL + """
        WHERE i.indrelid='public.mqtt_messages'::regclass AND am.amname='gin'
          AND idx.relkind='I' ORDER BY i.indexrelid""")


def topic_definition(index):
    return bool(index and not index['indisunique'] and index['indnkeyatts'] == 1
                and index['indnatts'] == 1 and index['column_name'] == 'topic'
                and index['text_column'] and index['matching_collation']
                and index['opclass'] == 'gin_trgm_ops'
                and index['extension_opclass'] and index['predicate'] is None
                and index['expressions'] is None)


def supported_parent(index):
    return topic_definition(index) and index['indisvalid'] and index['indisready']


async def standalone_topic_index(conn, leaf):
    return await conn.fetchrow("""SELECT idx.oid::bigint AS oid,idx.relname AS name,
        pg_relation_size(idx.oid) AS bytes,""" + _TOPIC_METADATA_SQL + """
        FROM pg_class idx JOIN pg_namespace n ON n.oid=idx.relnamespace
        JOIN pg_index i ON i.indexrelid=idx.oid JOIN pg_am am ON am.oid=idx.relam
        """ + _TOPIC_JOINS_SQL + """
        WHERE n.nspname=$1 AND idx.relname=$2 AND i.indrelid=$3::oid AND am.amname='gin'
          AND NOT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=idx.oid)""",
        leaf['schema'], topic_index_name(leaf['schema'], leaf['name']), leaf['oid'])


def valid_topic_index(index):
    return supported_parent(index)


async def has_topic_index(conn, leaf):
    """Accept only a genuine pg_trgm index; names and deparsed text are insufficient."""
    return await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM pg_index i
        JOIN pg_class idx ON idx.oid=i.indexrelid JOIN pg_am am ON am.oid=idx.relam
        """ + _TOPIC_JOINS_SQL + """
        WHERE i.indrelid=$1::oid AND am.amname='gin' AND i.indnkeyatts=1 AND i.indnatts=1
          AND i.indisvalid AND i.indisready AND NOT i.indisunique
          AND i.indpred IS NULL AND i.indexprs IS NULL
          AND att.attname='topic' AND att.atttypid='text'::regtype
          AND i.indcollation[0]=att.attcollation
          AND opc.opcname='gin_trgm_ops' AND """ + _OPCLASS_OWNED_SQL + """
          AND NOT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=idx.oid))""", leaf['oid'])


async def _build_one(conn, leaf, *, free_bytes, reserve_bytes, seconds):
    name = topic_index_name(leaf['schema'], leaf['name'])
    target = relation(leaf['schema'], name)
    if await has_topic_index(conn, leaf):
        return False
    previous = await standalone_topic_index(conn, leaf)
    if valid_topic_index(previous):
        return False
    if previous:
        if not topic_definition(previous):
            raise ValueError('Managed topic index has an unexpected definition')
        await conn.execute(f'DROP INDEX CONCURRENTLY {target}', timeout=seconds)
    elif await conn.fetchval('SELECT to_regclass($1)', leaf['schema'] + '.' + name):
        raise ValueError('Managed topic index name is occupied by another object')
    existing = await conn.fetchval("""SELECT COALESCE(max(pg_relation_size(i.indexrelid)),0)
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam
        WHERE i.indrelid=$1::oid AND a.amname='gin'""", leaf['oid'])
    # Existing GIN size is useful for a clone; no-index installations use a
    # deliberately conservative whole-table bound. Actual free space is watched.
    allowance = max(64 * MIB, int(existing or leaf['bytes']) * 3)
    if available_bytes(free_bytes) < reserve_bytes + allowance:
        return {'status': 'blocked', 'reason': 'insufficient_index_staging_space',
                'required_free_bytes': reserve_bytes + allowance, 'partition': leaf['name']}
    operator_class = await topic_opclass(conn)
    await conn.execute("SET lock_timeout='250ms'")
    await conn.execute("SET maintenance_work_mem='32MB'")
    await conn.execute("SET max_parallel_maintenance_workers=0")
    await conn.execute("SELECT set_config('statement_timeout',$1,false)", str(int(seconds * 1000)))
    task = asyncio.create_task(conn.execute(
        f'CREATE INDEX CONCURRENTLY {quoted(name)} ON {relation(leaf["schema"],leaf["name"])} '
        f'USING gin (topic {operator_class})', timeout=seconds + 1))
    try:
        while not task.done():
            done, _ = await asyncio.wait({task}, timeout=0.25)
            if not done and available_bytes(free_bytes) < reserve_bytes + 32 * MIB:
                raise RuntimeError('database_free_space_reserve_reached')
        await task
        if not valid_topic_index(await standalone_topic_index(conn, leaf)):
            raise RuntimeError('Concurrent topic index did not become valid')
    except BaseException:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        # An interrupted CONCURRENTLY may leave an invalid index. This name was
        # absent/owned above; next cycle also repairs it if shutdown interrupts
        # cleanup. Never remove the still-serving parent or another index.
        try:
            candidate = await standalone_topic_index(conn, leaf)
            if candidate and not candidate['indisvalid']:
                await conn.execute(f'DROP INDEX CONCURRENTLY {target}', timeout=5)
        finally:
            raise
    finally:
        await conn.execute('RESET statement_timeout; RESET lock_timeout; '
                           'RESET maintenance_work_mem; RESET max_parallel_maintenance_workers')
    return {'status': 'partial', 'reason': 'topic_index_prepared', 'partition': leaf['name']}


async def prepare_archive_indexes(conn, config, *, free_bytes):
    """At most one concurrent build; subsequent cycles perform a brief switch."""
    from storage.db.archive import archive_policy
    policy = archive_policy(config)
    async with maintenance_lock(conn) as acquired:
        if not acquired:
            return {'status': 'busy'}
        available_bytes(free_bytes)
        default_space = await conn.fetchval("SELECT t.spcname FROM pg_database d "
                                            "JOIN pg_tablespace t ON t.oid=d.dattablespace "
                                            "WHERE d.datname=current_database()")
        if (default_space != 'pg_default' or await conn.fetchval('SHOW default_tablespace') not in ('', 'pg_default')
                or await conn.fetchval('SHOW temp_tablespaces') not in ('', 'pg_default')):
            return {'status': 'blocked', 'reason': 'custom_tablespaces_require_separate_space_probes'}
        parents = await parent_gin_indexes(conn)
        if not all(supported_parent(index) for index in parents):
            return {'status': 'blocked', 'reason': 'unsupported_parent_gin_index'}
        leaves = await heap_leaves(conn)
        if any(leaf['reltablespace'] != 0 for leaf in leaves):
            return {'status': 'blocked', 'reason': 'custom_tablespace_requires_separate_space_probe'}
        for leaf in leaves:
            outcome = await _build_one(conn, leaf, free_bytes=free_bytes,
                reserve_bytes=policy.reserve_bytes, seconds=policy.index_build_seconds)
            if outcome:
                return outcome
        if not parents:
            return {'status': 'complete'}
        async with conn.transaction():
            await conn.execute("SET LOCAL lock_timeout='100ms'; SET LOCAL statement_timeout='2000ms'")
            await conn.execute('LOCK TABLE ONLY public.mqtt_messages IN ACCESS EXCLUSIVE MODE NOWAIT')
            current = await heap_leaves(conn)
            if {row['oid'] for row in current} != {row['oid'] for row in leaves}:
                return {'status': 'busy', 'reason': 'partition_layout_changed'}
            for leaf in current:
                await conn.execute(f'LOCK TABLE ONLY {relation(leaf["schema"],leaf["name"])} '
                                   'IN SHARE UPDATE EXCLUSIVE MODE NOWAIT')
                if not await has_topic_index(conn, leaf):
                    raise RuntimeError('Topic index coverage changed before parent removal')
            indexes = await parent_gin_indexes(conn)
            if [dict(row) for row in indexes] != [dict(row) for row in parents]:
                raise RuntimeError('Parent topic index changed during preparation')
            for index in indexes:
                await conn.execute(f'DROP INDEX {relation("public", index["name"])}')
        return {'status': 'complete', 'reason': 'topic_indexes_are_leaf_local'}


async def _legacy_parent_topic_index(conn):
    """Retain the pre-archive startup repair for entirely heap installations.

    The maintenance lock excludes archive preparation/cutover. A recursive SHARE
    lock then excludes writers and table-access-method changes while the catalog
    is rechecked. This is the existing bounded startup index build, used only
    when archiving is disabled and a populated heap table lacks topic coverage.
    """
    async with maintenance_lock(conn) as acquired:
        if not acquired:
            raise RuntimeError('Legacy topic index repair deferred by storage maintenance')
        async with conn.transaction():
            await conn.execute("SET LOCAL lock_timeout='100ms'; SET LOCAL statement_timeout='900s'; "
                               "SET LOCAL maintenance_work_mem='32MB'; SET LOCAL max_parallel_maintenance_workers=0")
            await conn.execute('LOCK TABLE public.mqtt_messages IN SHARE MODE NOWAIT')
            entirely_heap = await conn.fetchval("""WITH RECURSIVE family AS (
                SELECT c.oid,c.relkind,c.relam FROM pg_class c WHERE c.oid='public.mqtt_messages'::regclass
                UNION ALL SELECT c.oid,c.relkind,c.relam FROM family f
                  JOIN pg_inherits i ON i.inhparent=f.oid JOIN pg_class c ON c.oid=i.inhrelid)
                SELECT coalesce(bool_and(f.relkind='p' OR (f.relkind='r' AND a.amname='heap')),false)
                FROM family f LEFT JOIN pg_am a ON a.oid=f.relam""")
            if not entirely_heap:
                return False
            if await conn.fetchval("SELECT to_regclass('public.storage_archive_jobs')"):
                active = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM public.storage_archive_jobs "
                                            "WHERE source_frozen AND phase IN ('copying','verifying','ready'))")
                if active:
                    raise RuntimeError('Cancel or resume the active archive before legacy index repair')
            if any(supported_parent(index) for index in await parent_gin_indexes(conn)):
                return True
            if await conn.fetchval("SELECT to_regclass('public.idx_mqtt_messages_topic_trgm')"):
                raise RuntimeError('Legacy topic index name is occupied by an unsupported or invalid index')
            operator_class = await topic_opclass(conn)
            await conn.execute('CREATE INDEX idx_mqtt_messages_topic_trgm ON public.mqtt_messages '
                               f'USING gin (topic {operator_class})', timeout=900)
            return True


async def _empty_topic_index(conn, leaf):
    """Index a still-empty heap leaf under a short lock, without a fake probe."""
    async with conn.transaction():
        await conn.execute("SET LOCAL lock_timeout='100ms'; SET LOCAL statement_timeout='2000ms'; "
                           "SET LOCAL maintenance_work_mem='4MB'; SET LOCAL max_parallel_maintenance_workers=0")
        target = relation(leaf['schema'], leaf['name'])
        await conn.execute(f'LOCK TABLE ONLY {target} IN SHARE MODE NOWAIT')
        current = await conn.fetchrow("""SELECT c.oid::bigint AS oid,a.amname,pg_relation_size(c.oid) AS bytes
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_am a ON a.oid=c.relam
            WHERE n.nspname=$1 AND c.relname=$2""", leaf['schema'], leaf['name'])
        if not current or current['oid'] != leaf['oid'] or current['amname'] != 'heap' or current['bytes'] != 0:
            raise RuntimeError('History leaf changed before empty-table topic index creation')
        if await has_topic_index(conn, leaf):
            return
        name = topic_index_name(leaf['schema'], leaf['name'])
        if await conn.fetchval('SELECT to_regclass($1)', relation(leaf['schema'], name)):
            raise RuntimeError('Managed topic index needs explicit maintenance repair')
        operator_class = await topic_opclass(conn)
        await conn.execute(f'CREATE INDEX {quoted(name)} ON {target} USING gin (topic {operator_class})', timeout=2)


async def ensure_heap_topic_indexes(conn, *, allow_legacy_parent=False):
    """Keep heap topic coverage without ever adding GIN above Columnar leaves.

    Archive-disabled legacy installations retain their previous automatic parent
    index repair. Mixed or opt-in archive installations only create indexes on
    still-empty heap leaves here; populated missing indexes require the separate
    space-checked maintenance path. Existing valid parent indexes cover leaves.
    """
    if any(supported_parent(index) for index in await parent_gin_indexes(conn)):
        return
    leaves = await heap_leaves(conn)
    missing = [leaf for leaf in leaves if not await has_topic_index(conn, leaf)]
    populated = [leaf for leaf in missing if leaf['heap_bytes'] > 0]
    if populated and allow_legacy_parent and await _legacy_parent_topic_index(conn):
        return
    # Repair new empty partitions even if an older populated partition needs
    # separate maintenance, so one missing legacy index cannot starve rollover.
    for leaf in missing:
        if leaf['heap_bytes'] == 0:
            await _empty_topic_index(conn, leaf)
    if populated:
        raise RuntimeError('Existing history leaf needs a space-checked topic index repair')
