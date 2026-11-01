"""Snapshot-consistent Columnar options and fail-closed restore SQL.

Columnar options live in an extension catalog and are not preserved by ordinary
logical dumps. Read them under the same exported snapshot as pg_dump; never
write to that catalog. The existing archive checksum binds this bounded
metadata to its backup manifest. All restore SQL uses quoted identifiers and
validated values, and executes before any archived rows are copied.
"""
from __future__ import annotations

import asyncio
import json
import re
import uuid

from storage.backup_remote import BackupError, RESTORE_METADATA_BYTES, validate_restore_metadata

BASE_SQL = """SELECT json_build_object(
    'snapshot',pg_export_snapshot(),
    'postgres_major',current_setting('server_version_num')::int/10000,
    'extension',(SELECT extversion FROM pg_extension WHERE extname='citus_columnar'),
    'relation_count',(SELECT count(*) FROM pg_class c JOIN pg_am a ON a.oid=c.relam
        WHERE a.amname='columnar' AND c.relkind='r' AND c.relpersistence<>'t'))"""
# The public view exposes `relation`, not the internal catalog's `regclass`.
# Its ownership filter must not silently turn invisible options into defaults:
# NULL values from the left join are rejected by the metadata validator.
OPTIONS_SQL = """SELECT coalesce(json_agg(row_to_json(settings) ORDER BY settings.schema,settings.name),'[]'::json)
FROM (SELECT n.nspname AS schema,c.relname AS name,
    o.chunk_group_row_limit AS chunk_group_row_limit,
    o.stripe_row_limit AS stripe_row_limit,
    o.compression_level AS compression_level,
    o.compression::text AS compression,
    json_build_object('row_count',coalesce(g.row_count,0),
      'max_stripe_rows',coalesce(g.max_stripe_rows,0),
      'max_chunk_group_rows',coalesce(cg.max_chunk_group_rows,0)) AS geometry
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    JOIN pg_am a ON a.oid=c.relam LEFT JOIN columnar.options o ON o.relation=c.oid
    LEFT JOIN (SELECT relation,sum(row_count)::bigint AS row_count,max(row_count) AS max_stripe_rows
      FROM columnar.stripe GROUP BY relation) g ON g.relation=c.oid
    LEFT JOIN (SELECT relation,max(row_count) AS max_chunk_group_rows
      FROM columnar.chunk_group GROUP BY relation) cg ON cg.relation=c.oid
    WHERE a.amname='columnar' AND c.relkind='r' AND c.relpersistence<>'t') settings"""


def connection_arguments(pg):
    return ("--host", str(pg.get("host", "postgres")), "--port", str(pg.get("port", 5432)),
            "--username", str(pg.get("username", "postgres")),
            "--dbname", str(pg.get("database", "meshinfo")))


class SnapshotMetadata:
    """Keep a read-only exported snapshot alive until the dump has finished."""

    def __init__(self, pg, prefix, environment):
        self.pg, self.prefix, self.environment = pg, tuple(prefix), environment
        self.proc = None
        self.errors = None
        self.snapshot = None
        self.metadata = None

    async def query(self, sql):
        marker = "meshinfo_metadata_" + uuid.uuid4().hex
        async with asyncio.timeout(10):
            self.proc.stdin.write((sql + ";\nSELECT '" + marker + "';\n").encode())
            await self.proc.stdin.drain()
            rows, size = [], 0
            while True:
                line = await self.proc.stdout.readline()
                if not line:
                    raise BackupError("backup metadata session ended before acknowledgment")
                size += len(line)
                if size > RESTORE_METADATA_BYTES:
                    raise BackupError("Columnar restore metadata exceeds its bounded budget")
                value = line.decode("utf-8").strip()
                if value == marker:
                    return rows
                if value:
                    rows.append(value)

    async def start(self):
        from storage.backup import _tail
        self.proc = await asyncio.create_subprocess_exec(
            *self.prefix, "psql", "-X", "-qAt", "--set=ON_ERROR_STOP=1",
            *connection_arguments(self.pg), env={**self.environment, "PGCLIENTENCODING": "UTF8"},
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, limit=RESTORE_METADATA_BYTES + 1024)
        self.errors = asyncio.create_task(_tail(self.proc.stderr))
        await self.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; "
                         "SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='1s'; "
                         "SET LOCAL idle_in_transaction_session_timeout=0")
        rows = await self.query(BASE_SQL)
        try:
            if len(rows) != 1:
                raise ValueError
            base = json.loads(rows[0])
            snapshot = base["snapshot"]
            if not isinstance(snapshot, str) or not re.fullmatch(r"[0-9A-Fa-f]+-[0-9A-Fa-f]+-[0-9]+", snapshot):
                raise ValueError
            count = base["relation_count"]
            if type(count) is not int or not 0 <= count <= 4096:
                raise ValueError
            relations = []
            if count:
                if not base["extension"]:
                    raise ValueError
                options = await self.query(OPTIONS_SQL)
                if len(options) != 1:
                    raise ValueError
                relations = json.loads(options[0])
                if len(relations) != count:
                    raise ValueError
            self.snapshot = snapshot
            self.metadata = validate_restore_metadata({"schema": 2,
                "postgres_major": base["postgres_major"], "extension": base["extension"],
                "relations": relations})
            return self
        except (ValueError, TypeError, KeyError) as exc:
            raise BackupError("invalid Columnar snapshot metadata; backup was not published") from exc

    async def close(self):
        from storage.backup import _close_stdin, _stop
        if self.proc is None:
            return
        try:
            # EOF causes psql to close its session and roll back the read-only
            # snapshot, including when pg_dump or its controller was cancelled.
            await _close_stdin(self.proc)
            try:
                await asyncio.wait_for(self.proc.wait(), 3)
            except asyncio.TimeoutError:
                pass
        finally:
            await _stop(self.proc, drain_stdout=True)
            if self.errors is not None:
                await asyncio.gather(self.errors, return_exceptions=True)
            self.proc = None


def quote_identifier(value):
    return '"' + value.replace('"', '""') + '"'


def quote_literal(value):
    return "'" + value.replace("'", "''") + "'"


def options_sql(metadata):
    """Apply a manifest's exact settings before COPY, using public ALTER APIs."""
    metadata = validate_restore_metadata(metadata)
    commands = []
    for relation in metadata["relations"]:
        name = quote_identifier(relation["schema"]) + "." + quote_identifier(relation["name"])
        commands.append(f"ALTER TABLE {name} SET ("
                        f"columnar.chunk_group_row_limit={relation['chunk_group_row_limit']},"
                        f"columnar.stripe_row_limit={relation['stripe_row_limit']},"
                        f"columnar.compression_level={relation['compression_level']},"
                        f"columnar.compression={quote_literal(relation['compression'])});")
    return "\n".join(commands) + "\n"


def inventory_assertion_sql(metadata):
    """Fail before COPY if any Columnar relation is missing or undocumented."""
    metadata = validate_restore_metadata(metadata)
    expected = [{"schema": r["schema"], "name": r["name"]} for r in metadata["relations"]]
    encoded = json.dumps(expected, ensure_ascii=True, separators=(",", ":")).encode().hex()
    # Hex is generated internally, never an arbitrary SQL fragment. This also
    # safely handles quotes, backslashes and dollar signs in SQL identifiers.
    return f"""DO $meshinfo_columnar_inventory$
    DECLARE expected jsonb := convert_from(decode('{encoded}','hex'),'UTF8')::jsonb;
    BEGIN
      IF EXISTS (
        WITH actual AS (SELECT n.nspname::text AS schema,c.relname::text AS name
          FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_am a ON a.oid=c.relam
          WHERE a.amname='columnar' AND c.relkind='r' AND c.relpersistence<>'t'),
        required AS (SELECT x.schema,x.name FROM jsonb_to_recordset(expected) AS x(schema text,name text))
        SELECT * FROM ((SELECT * FROM actual EXCEPT SELECT * FROM required)
          UNION ALL (SELECT * FROM required EXCEPT SELECT * FROM actual)) differences
      ) THEN RAISE EXCEPTION 'Columnar table inventory does not match verified backup metadata'; END IF;
    END $meshinfo_columnar_inventory$;\n"""


def options_assertion_sql(metadata):
    """Verify effective options after applying them, without touching data."""
    metadata = validate_restore_metadata(metadata)
    if not metadata["relations"]:
        return ""
    encoded = json.dumps(metadata["relations"], ensure_ascii=True, separators=(",", ":")).encode().hex()
    return f"""DO $meshinfo_columnar_options$
    DECLARE expected jsonb := convert_from(decode('{encoded}','hex'),'UTF8')::jsonb;
    BEGIN
      IF EXISTS (SELECT 1 FROM jsonb_to_recordset(expected) AS x(
          schema text,name text,chunk_group_row_limit int,stripe_row_limit int,compression_level int,compression text)
        LEFT JOIN pg_namespace n ON n.nspname=x.schema
        LEFT JOIN pg_class c ON c.relnamespace=n.oid AND c.relname=x.name
        LEFT JOIN columnar.options o ON o.relation=c.oid
        WHERE o.chunk_group_row_limit IS DISTINCT FROM x.chunk_group_row_limit
          OR o.stripe_row_limit IS DISTINCT FROM x.stripe_row_limit
          OR o.compression_level IS DISTINCT FROM x.compression_level
          OR o.compression::text IS DISTINCT FROM x.compression)
      THEN RAISE EXCEPTION 'Columnar options do not match verified backup metadata'; END IF;
    END $meshinfo_columnar_options$;\n"""


def geometry_assertion_sql(metadata):
    """Verify exact row totals and physical upper bounds before outer commit."""
    metadata = validate_restore_metadata(metadata)
    if metadata["schema"] != 2 or not metadata["relations"]:
        return ""
    encoded = json.dumps(metadata["relations"], ensure_ascii=True, separators=(",", ":")).encode().hex()
    return f"""DO $meshinfo_columnar_geometry$
    DECLARE expected jsonb := convert_from(decode('{encoded}','hex'),'UTF8')::jsonb;
    BEGIN
      IF EXISTS (SELECT 1 FROM jsonb_to_recordset(expected) AS x(schema text,name text,geometry jsonb)
        LEFT JOIN pg_namespace n ON n.nspname=x.schema
        LEFT JOIN pg_class c ON c.relnamespace=n.oid AND c.relname=x.name
        LEFT JOIN (SELECT relation,sum(row_count)::bigint AS rows,max(row_count) AS stripe
          FROM columnar.stripe GROUP BY relation) s ON s.relation=c.oid
        LEFT JOIN (SELECT relation,max(row_count) AS chunk
          FROM columnar.chunk_group GROUP BY relation) g ON g.relation=c.oid
        WHERE coalesce(s.rows,0) <> (x.geometry->>'row_count')::bigint
           OR coalesce(s.stripe,0) > (x.geometry->>'max_stripe_rows')::bigint
           OR coalesce(g.chunk,0) > (x.geometry->>'max_chunk_group_rows')::bigint)
      THEN RAISE EXCEPTION 'Columnar restored physical geometry exceeds snapshot bounds'; END IF;
    END $meshinfo_columnar_geometry$;\n"""
