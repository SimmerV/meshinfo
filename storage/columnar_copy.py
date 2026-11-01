"""Bounded pg_restore text-COPY splitting inside one outer transaction.

Citus 14.1 keeps COPY write state until transaction end. Releasing a savepoint
flushes its subtransaction's write state without committing the outer restore.
Only validated, manifest-listed Columnar tables receive these boundaries. COPY
values are opaque bytes, including escaped newlines, backslashes and NULLs.
"""
from __future__ import annotations

import re

from storage.backup_remote import BackupError, CHUNK_BYTES, validate_restore_metadata

SQL_LIMIT = 1024 * 1024
SAVEPOINT = b'meshinfo_columnar_restore_batch'
IDENTIFIER = re.compile(rb'[A-Za-z_\x80-\xff][A-Za-z_0-9$\x80-\xff]*')
DOLLAR = re.compile(rb'\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$')
SPACE = b' \t\r\n\f'


class CopyStreamError(BackupError):
    pass


def _trivia(data, start=0):
    """Skip whitespace/comments without interpreting their contents as SQL."""
    p = start
    while p < len(data):
        if data[p] in SPACE:
            p += 1
        elif data[p:p+2] == b'--':
            end = data.find(b'\n', p+2)
            p = len(data) if end < 0 else end+1
        elif data[p:p+2] == b'/*':
            depth, p = 1, p+2
            while p < len(data) and depth:
                if data[p:p+2] == b'/*':
                    depth, p = depth+1, p+2
                elif data[p:p+2] == b'*/':
                    depth, p = depth-1, p+2
                else:
                    p += 1
            if depth:
                raise CopyStreamError('Incomplete SQL comment in archive data section')
        else:
            break
    return p


def statement_end(data):
    """Locate one SQL statement or standalone psql command, respecting quoting."""
    p, active = 0, False
    while p < len(data):
        c = data[p:p+1]
        if c[0] in SPACE:
            p += 1
            continue
        if data[p:p+2] == b'--':
            end = data.find(b'\n', p+2)
            if end < 0:
                return None
            p = end+1
            continue
        if data[p:p+2] == b'/*':
            depth, p = 1, p+2
            while p < len(data) and depth:
                if data[p:p+2] == b'/*':
                    depth, p = depth+1, p+2
                elif data[p:p+2] == b'*/':
                    depth, p = depth-1, p+2
                else:
                    p += 1
            if depth:
                return None
            continue
        if c == b'\\' and not active:
            end = data.find(b'\n', p)
            return (end+1, 'meta') if end >= 0 else None
        active = True
        if c in (b"'", b'"'):
            quote = c
            escaped = quote == b"'" and p > 0 and data[p-1:p] in (b'E', b'e') and (
                p == 1 or not re.match(rb'[A-Za-z_0-9$]', data[p-2:p-1]))
            p += 1
            while p < len(data):
                if escaped and data[p:p+1] == b'\\':
                    p += 2
                elif data[p:p+1] == quote:
                    if data[p+1:p+2] == quote:
                        p += 2
                    else:
                        p += 1
                        break
                else:
                    p += 1
            else:
                return None
            continue
        if c == b'$':
            match = DOLLAR.match(data, p)
            if match:
                end = data.find(match[0], match.end())
                if end < 0:
                    return None
                p = end+len(match[0])
                continue
        if c == b';':
            return p+1, 'sql'
        p += 1
    return None


def _identifier(data, p):
    p = _trivia(data, p)
    if data[p:p+1] == b'"':
        p += 1
        value = bytearray()
        while p < len(data):
            if data[p:p+1] == b'"':
                if data[p+1:p+2] == b'"':
                    value.append(34)
                    p += 2
                else:
                    return bytes(value).decode('utf-8'), p+1
            else:
                value.append(data[p])
                p += 1
        raise CopyStreamError('Incomplete quoted COPY identifier')
    match = IDENTIFIER.match(data, p)
    if not match:
        raise CopyStreamError('Invalid COPY identifier')
    raw = match[0].translate(bytes.maketrans(b'ABCDEFGHIJKLMNOPQRSTUVWXYZ', b'abcdefghijklmnopqrstuvwxyz'))
    return raw.decode('utf-8'), match.end()


def copy_relation(statement):
    """Accept exactly pg_dump's qualified text COPY FROM stdin command."""
    p = _trivia(statement)
    word = IDENTIFIER.match(statement, p)
    if not word or word[0].upper() != b'COPY':
        return None
    try:
        schema, p = _identifier(statement, word.end())
        p = _trivia(statement, p)
        if statement[p:p+1] != b'.':
            raise CopyStreamError('COPY relation must be schema-qualified')
        name, p = _identifier(statement, p+1)
        p = _trivia(statement, p)
        if statement[p:p+1] == b'(':
            p += 1
            while True:
                _, p = _identifier(statement, p)
                p = _trivia(statement, p)
                if statement[p:p+1] == b')':
                    p += 1
                    break
                if statement[p:p+1] != b',':
                    raise CopyStreamError('Invalid COPY column list')
                p += 1
        if not re.fullmatch(rb'\s*FROM\s+stdin\s*;\s*', statement[p:], re.I):
            raise CopyStreamError('Only text COPY FROM stdin is supported for geometry restoration')
        return schema, name
    except UnicodeError as exc:
        raise CopyStreamError('Geometry-aware COPY identifiers require UTF-8 dump encoding') from exc


def batch_rows(relation):
    geometry = relation['geometry']
    stripe = geometry['max_stripe_rows']
    group = geometry['max_chunk_group_rows']
    if not geometry['row_count']:
        return relation['chunk_group_row_limit']
    # Preserve both observed upper bounds even if effective options were changed
    # after older stripes were written. Smaller batches never enlarge a group.
    return stripe if relation['chunk_group_row_limit'] <= group else min(stripe, group)


class CopyBatchStream:
    def __init__(self, metadata, write):
        validate_restore_metadata(metadata)
        if metadata['schema'] != 2:
            raise CopyStreamError('Physical COPY boundaries require geometry metadata')
        self.relations = {(r['schema'], r['name']): r for r in metadata['relations']}
        self.write = write
        self.pending = b''
        self.copy = False
        self.relation = None
        self.header = b''
        self.open = False
        self.fragment = False
        self.batch = self.rows = 0
        self.seen = set()

    async def _begin(self):
        await self.write(b'SAVEPOINT '+SAVEPOINT+b';\n'+self.header)
        self.open, self.batch = True, 0

    async def _end(self):
        await self.write(b'\\.\nRELEASE SAVEPOINT '+SAVEPOINT+b';\n')
        self.open = False

    async def feed(self, chunk):
        if len(chunk) > CHUNK_BYTES:
            raise CopyStreamError('Restore input exceeded its streaming chunk budget')
        self.pending += chunk
        while self.pending:
            if not self.copy:
                if len(self.pending) > SQL_LIMIT:
                    raise CopyStreamError('Archive data SQL statement exceeds its bounded budget')
                boundary = statement_end(self.pending)
                if boundary is None:
                    return
                end, kind = boundary
                statement = self.pending[:end]
                relation = copy_relation(statement) if kind == 'sql' else None
                if relation is None:
                    await self.write(statement)
                    self.pending = self.pending[end:]
                    continue
                newline = self.pending.find(b'\n', end)
                if newline < 0:
                    return
                if self.pending[end:newline].strip():
                    raise CopyStreamError('Unexpected SQL after COPY header')
                self.header = self.pending[:newline+1]
                self.pending = self.pending[newline+1:]
                self.copy, self.fragment, self.rows, self.batch = True, False, 0, 0
                self.relation = self.relations.get(relation)
                if self.relation is not None:
                    if relation in self.seen:
                        raise CopyStreamError('Duplicate COPY for a Columnar relation')
                    self.seen.add(relation)
                    await self._begin()
                else:
                    await self.write(self.header)
                continue
            newline = self.pending.find(b'\n')
            if newline < 0:
                if len(self.pending) < 3 or self.pending == b'\\.\r':
                    return  # a possible COPY terminator needs its newline
                if self.relation is not None and not self.open:
                    await self._begin()
                await self.write(self.pending)
                self.pending, self.fragment = b'', True
                return
            line, self.pending = self.pending[:newline+1], self.pending[newline+1:]
            if not self.fragment and line in (b'\\.\n', b'\\.\r\n'):
                if self.relation is not None:
                    if self.open:
                        await self._end()
                    if self.rows != self.relation['geometry']['row_count']:
                        raise CopyStreamError('Columnar COPY row count differs from snapshot geometry')
                else:
                    await self.write(line)
                self.copy, self.relation = False, None
                continue
            if self.relation is not None and not self.open:
                await self._begin()
            await self.write(line)
            self.fragment = False
            self.rows += 1
            if self.relation is not None:
                if self.rows > self.relation['geometry']['row_count']:
                    raise CopyStreamError('Columnar COPY exceeds snapshot row count')
                self.batch += 1
                if self.batch >= batch_rows(self.relation):
                    await self._end()

    async def finish(self):
        if self.copy or _trivia(self.pending) != len(self.pending):
            raise CopyStreamError('Archive data section ended before a complete COPY or SQL statement')
        if self.pending:
            await self.write(self.pending)
        if any(r['geometry']['row_count'] and name not in self.seen for name, r in self.relations.items()):
            raise CopyStreamError('Columnar COPY data is missing from the archive')
        self.pending = b''
