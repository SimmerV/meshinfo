"""Physical restore boundaries preserve opaque COPY bytes and fail closed."""
import asyncio
from copy import deepcopy

import pytest

from storage.backup_remote import BackupError, CHUNK_BYTES, validate_restore_metadata
from storage.columnar_copy import CopyBatchStream, CopyStreamError, batch_rows, copy_relation


def metadata(*, schema='public', name='cold', rows=5, stripe=2, group=2):
    return {'schema': 2, 'postgres_major': 18, 'extension': '14.1-1', 'relations': [
        {'schema': schema, 'name': name, 'chunk_group_row_limit': 1000,
         'stripe_row_limit': 150000, 'compression_level': 3, 'compression': 'zstd',
         'geometry': {'row_count': rows, 'max_stripe_rows': stripe,
                      'max_chunk_group_rows': group}}]}


def rewrite(data, value=None, size=31):
    async def run():
        output = []
        async def write(chunk):
            output.append(chunk)
        stream = CopyBatchStream(value or metadata(), write)
        for start in range(0, len(data), size):
            await stream.feed(data[start:start+size])
            # No complete payload row is buffered, even when it exceeds the
            # network chunk budget by orders of magnitude.
            assert len(stream.pending) <= max(CHUNK_BYTES, 1024*1024)
        await stream.finish()
        return b''.join(output)
    return asyncio.run(run())


@pytest.mark.parametrize('size', [1, 2, 3, 7, 31, CHUNK_BYTES])
def test_boundaries_wrap_copy_batches_without_committing_or_changing_values(size):
    rows = [b'1\t\\N\n', b'2\tline\\nnext\\tcolumn\n', b'3\t\\\\.\n',
            '4\t雪 🛰️\n'.encode(), b'5\tlast\n']
    header = b'COPY "public"."cold" ("id", "payload") FROM stdin;\n'
    source = b'\\restrict abc123\n-- Table data\n'+header+b''.join(rows)+b'\\.\n\\unrestrict abc123\n'
    result = rewrite(source, size=size)
    assert result.count(b'SAVEPOINT meshinfo_columnar_restore_batch;') == 6  # SAVEPOINT + RELEASE
    assert result.count(b'RELEASE SAVEPOINT') == 3
    assert b'COMMIT' not in result and b'ROLLBACK' not in result
    assert result.count(header) == 3
    assert all(result.count(row) == 1 for row in rows)
    assert result.count(b'\\.\n') == 4  # three terminators and the escaped literal's suffix
    assert result.startswith(b'\\restrict abc123\n') and result.endswith(b'\\unrestrict abc123\n')


def test_large_copy_value_is_streamed_without_buffering_a_row():
    payload = b'x'*(CHUNK_BYTES*9+17)
    value = metadata(rows=1, stripe=1, group=1)
    source = b'COPY public.cold(payload) FROM stdin;\n'+payload+b'\n\\.\n'
    result = rewrite(source, value, CHUNK_BYTES)
    assert payload+b'\n' in result
    assert result.count(b'RELEASE SAVEPOINT') == 1


def test_quoted_identifiers_with_quotes_unicode_newlines_and_sql_text_are_exact():
    schema, name = '雪\nCOPY', 'x"; SELECT 1; --'
    quote = lambda value: '"'+value.replace('"', '""')+'"'
    header = f'COPY {quote(schema)}.{quote(name)} ("a\n;", "x""y") FROM stdin;\n'.encode()
    assert copy_relation(header[:-1]) == (schema, name)
    result = rewrite(header+b'one\n\\.\n', metadata(schema=schema, name=name, rows=1, stripe=1, group=1), 1)
    assert header in result and result.count(b'RELEASE SAVEPOINT') == 1


def test_sql_literals_comments_and_heap_rows_are_not_mistaken_for_columnar_copy():
    prefix = (b"-- COPY public.cold(x) FROM stdin;\n"
              b"SELECT 'a;\nCOPY public.cold(x) FROM stdin;';\n"
              b"SELECT E'back\\\\slash;';\nSELECT $tag$COPY public.cold(x) FROM stdin;$tag$;\n"
              b'COPY public.heap (x) FROM stdin;\nCOPY public.cold(x) FROM stdin;\n\\.\n')
    result = rewrite(prefix+b'COPY public.cold(x) FROM stdin;\na\nb\nc\nd\ne\n\\.\n')
    assert result.startswith(prefix)
    assert result.count(b'RELEASE SAVEPOINT') == 3


@pytest.mark.parametrize('data', [
    b'COPY public.cold(x) FROM stdin;\na\n',
    b'COPY public.cold(x) FROM stdin;\na\n\\.\n',
    b'COPY public.cold(x) FROM stdin;\na\nb\nc\nd\ne\nf\n\\.\n',
    b'COPY public.cold(x) FROM stdin WITH (FORMAT binary);\n',
    b'COPY cold(x) FROM stdin;\n',
    b'SELECT 1;\n',
    b'SELECT \'unterminated',
])
def test_incomplete_unsupported_or_mismatched_stream_refuses(data):
    with pytest.raises(CopyStreamError):
        rewrite(data)


def test_duplicate_copy_is_rejected_even_if_first_copy_was_valid():
    data = b'COPY public.cold(x) FROM stdin;\na\n\\.\n'
    with pytest.raises(CopyStreamError, match='Duplicate'):
        rewrite(data*2, metadata(rows=1, stripe=1, group=1))


def test_empty_relations_and_changed_options_preserve_observed_upper_bound():
    assert b'RELEASE SAVEPOINT' in rewrite(b'COPY public.cold(x) FROM stdin;\n\\.\n',
        metadata(rows=0, stripe=0, group=0))
    relation = metadata(rows=5000, stripe=3000, group=1000)['relations'][0]
    assert batch_rows(relation) == 3000
    relation['chunk_group_row_limit'] = 10000
    assert batch_rows(relation) == 1000


@pytest.mark.parametrize('geometry', [None, {}, {'row_count': 0, 'max_stripe_rows': 1, 'max_chunk_group_rows': 1},
    {'row_count': 1, 'max_stripe_rows': 1, 'max_chunk_group_rows': 2},
    {'row_count': True, 'max_stripe_rows': 1, 'max_chunk_group_rows': 1}])
def test_geometry_metadata_is_required_bounded_and_consistent(geometry):
    value = metadata()
    value['relations'][0]['geometry'] = geometry
    with pytest.raises(BackupError):
        validate_restore_metadata(value)


def test_legacy_metadata_stays_valid_but_cannot_claim_geometry_preservation():
    value = metadata()
    value['schema'] = 1
    del value['relations'][0]['geometry']
    assert validate_restore_metadata(value) == value
    with pytest.raises(CopyStreamError, match='geometry'):
        CopyBatchStream(value, None)
