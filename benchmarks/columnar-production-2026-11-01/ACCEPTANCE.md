The final source and restored concurrent runs passed the operational criteria
specified before those runs. Both preserve `all_checks_passed: false` and
`latency_gates_passed: false`: the original universal 3×/1-second gates did not
pass. `operational_acceptance_passed: true` is a separate result, not a rewrite
of those failures.

The fixed operational criteria were:

- Identical responses, no read/write errors, and verified cleanup of test writes.
- Indexed work: p95 ≤250 ms and either ≤3× heap or ≤100 ms additional latency.
- Unindexed text searches: p95 ≤10 seconds and ≤3× heap.
- Writer p95 ≤250 ms; first archived page after PostgreSQL buffer reset ≤1 second.

The benchmark classifies common text search as bounded indexed work only when
every measured compressed read uses the bounded payload path, all candidate
counts are known and ≤4096, and the captured query's EXPLAIN confirms canonical
index scans. Mixed, unknown, or native fallback execution retains the unindexed
criteria. This classification and the unchanged thresholds were fixed before
the final instrumented runs. The classifier has tests for missing evidence,
mixed paths, unknown cases, and oversized candidates.

Both runs used 104,480 canonical packets (63,800 archived) and 904,472 reception
rows, with verified canonical-topic helpers. The real archive worker produced
687 stripes of 8–93 rows; the restored database preserved that geometry.
PostgreSQL 18.6/Citus 14.1 ran within one CPU and 384 MiB. Each 60-second run used
two reader workers, four reader connections, and one writer connection, with
paired heap/compressed reads and up to five fixture-hot ingestion commits per
second. Archive windows exclude those writes.

| Query p95, milliseconds | Source heap | Source compressed | Restored heap | Restored compressed |
| --- | ---: | ---: | ---: | ---: |
| Archived page | 22.9 | 45.2 | 24.4 | 62.4 |
| Common payload search | 33.6 | 96.7 | 33.1 | 112.8 |
| Node lookup | 7.3 | 27.4 | 50.9 | 62.3 |
| Literal topic | 15.9 | 28.1 | 10.2 | 20.2 |
| Topic + common payload | 11.3 | 19.8 | 16.6 | 21.4 |
| Topic + absent payload | 11.5 | 17.6 | 14.1 | 25.6 |
| Reconstructed copies | 8.7 | 42.5 | 15.3 | 43.0 |
| Unrestricted absent payload | 1185.4 | 1366.3 | 1278.3 | 1375.6 |
| Reception-topic search | 3220.8 | 2845.7 | 2481.1 | 2597.8 |

Both runs completed 299 writer commits; writer p95 was 47.4/52.5 ms. Response
consistency, error checks, and owned-row cleanup passed. All 14 common-search
samples in each compressed layout used at most 252 candidates; EXPLAIN recorded
canonical PK Index Scan with transaction-local custom-scan suppression. Restored
common search was 3.41× heap, adding 79.7 ms, so it passed the separate operational
criterion while failing the original strict ratio. This is not identical speed.

First archived-page timings after resetting PostgreSQL shared buffers were
105.4 ms on source and 108.0 ms after restore, against the fixed 1000 ms limit.
These probes skipped data preflight queries. OS caches remained warm; host
caches were never dropped. Concurrent runs also used warm caches.

There are only 12–14 observations per query/layout in the concurrent runs, so
nearest-rank p95 equals the observed maximum. These short tests establish bounded
fixture behavior, not production-scale throughput or disk-cold latency. Full
payload/receipt searches without a selective indexed predicate still scan
history and can take seconds; compression does not make all search constant-time.

The evidence files are deliberately labeled by stage:

- `concurrent-source-accepted.json` and `concurrent-restored-accepted.json` contain
  the final timing, path counts, candidate bounds, EXPLAIN evidence, both sets of
  gates, and cleanup checks.
- `read-source-first.json` and `read-restored-first.json` contain the PostgreSQL
  buffer-reset first-page measurements. Later search changes did not alter that
  page path.
- `read-source-complete.json` and `read-restored-complete.json` are the earlier
  twelve-case semantic/timing matrices before the newest-prefix search fix.
  Their `all_checks_passed` means exact response parity, not latency acceptance.
  In particular, the restored common-search warm median was 517.2 ms.
- `read-restored-prefix-compact.json` records that query after the final bounded
  prefix change: 37.1 ms warm median, with unchanged response hash.
- `concurrent-restored-final.json` retains the pre-prefix performance failure.
  `concurrent-restored-compact.json` retains the later diagnostic using the final
  query algorithm but before executed-path classification: it reported
  operational failure under the earlier unconditional search classification.
  Neither report was edited to pass.
- Existing `concurrency-before-indexed-topics.json` and other lifecycle/storage
  reports are preserved unchanged. `acceptance-artifacts.json` records byte sizes
  and SHA-256 hashes for the original nine reports and the later validation
  artifacts listed below.

The JSON contains aggregate measurements, response hashes, synthetic test-write
ownership ranges, generated relation names, and plans stripped of predicates.
No source packets, topic values, query arguments, connection strings, or
credentials are included. Raw fixtures remain private. The reusable runners are
`scripts/benchmark_history_read_plans.py` and
`scripts/benchmark_history_concurrency.py`; both enforce isolated test targets.

## Later live-scale planning checkpoint (2026-11-01)

The fixture results above predate the live September planning diagnosis. They
remain unchanged, including the failed original gates. The later diagnostics
do not replace those load tests or establish final full-history performance.

`live-september-payload-planning-memory.json` and its summary are byte-for-byte
copies of read-only diagnostic reports recorded on 2026-11-01. They compare
custom and generic prepared plans for three fixed September keysets on 7,596
stripes. Payloads, topic values, query parameters and SQL predicates are omitted;
plan nodes retain structural names, costs, row counts, buffers and memory.
Every case returned the same 51-row response digest under both plan modes.

| Payload case | Custom warm planning (ms) | Generic first planning (ms) | Generic reused planning (ms) | Generic reused execution (ms) |
| --- | ---: | ---: | ---: | ---: |
| Page | 17.001 | 25.444 | 0.062 | 1.634 |
| Literal topic | 23.176 | 24.163 | 0.070 | 4.055 |
| Topic plus text search | 29.138 | 29.327 | 0.135 | 10.622 |

First means a new prepared plan, with existing data caches warm. These are
individual component EXPLAIN observations, not p95 values or full HTTP timings.
Generic initial planning allocated 8,985–17,313 KiB versus 32–56 KiB when reusing
the plan. Those are PostgreSQL planning-context measurements, not process peak
RSS or a `work_mem` budget. The largest August month and multiple concurrent
fresh planners were not tested by this diagnostic. New pooled sessions and
plan invalidation repeat initial planning.

The implementation is revision `a1a2fdd64ed36f23ab1107a2919d0f0bf6806560`:
generic planning is confined to already bounded payload reads, with previous
planner settings restored before any broad fallback or connection reuse.
Candidate selection retains custom planning. Exact-head native amd64/arm64
[CI run 41281751780](https://github.com/SimmerV/meshinfo/actions/runs/41281751780)
completed successfully at 06:41:50 UTC. `native-ci-a1a2fdd.json` records the public
API evidence; the older `native-ci-final-implementation.json` remains the
separate earlier `1ce4a0d` result.

Two distinct local runs are preserved as text artifacts:

- `backend-generic-regressions.txt`: 992 passed, 149 skipped, one existing
  `audioop` warning, 25.91 seconds. Skipped cases are not database passes.
- `isolated-generic-reader-regressions.txt`: 49 passed, 8.18 seconds on an
  explicitly configured isolated PostgreSQL cluster. This is separate from the
  native architecture CI matrix.

`generic-planning-provenance.json` binds the diagnostic baseline `8e8b668`,
implementation revision, source-file timestamps, test scope and artifact hashes.
Its live checkpoint is explicitly incomplete: September was converted, while
July verification was still progressing after approximately 2.844 million rows
were copied. Final July/August results, net disk savings and full live-scale
performance belong in a subsequent operator checkpoint.

## Packet-detail and plain PostgreSQL checkpoint (2026-11-01)

Revision `8a70d6a84179f64c6e57b3f1eb951d54c1f2bf4b` addresses a separate live
point-read regression after additional months were compressed. The original
lookup could scan a compressed partition for a missing ID before reaching the
target heap month. Its full diagnostic remains in `live-point-read-diagnosis.json`:

| Fixed statement | Original execution (ms) | Indexed generic execution (ms) | First prepared fetch (ms) | Reused prepared fetch (ms) |
| --- | ---: | ---: | ---: | ---: |
| Existing ID in August heap | 2831.379 | 0.126 | 54.745 | 0.418 |
| Missing ID | 2671.266 | 0.103 | 53.575 | 0.377 |

All variants returned identical digests. These are statement-level observations,
not HTTP latency, and the first-plan cost remains visible. The fix scopes indexed
generic selection to point details and keeps the canonical row and its receipts
in one repeatable-read snapshot. Sender/packet selection retains the exact
newest `(id, created_at)` identity instead of doing a second lookup by ID alone.

`live-query-after-point-fix.json` preserves three sequential HTTP samples for each
of 15 fixed live cases: all returned 200 and matched the supplied baseline digest.
The July/August/September packet-copy medians were 4.726/4.884/5.847 ms. The August
topic/text case's `baseline_status` is null, so it supplies hash agreement but no
successful baseline HTTP timing. `live-query-missing-after-point-fix.json` records
the expected 404 and 38.228/4.253/3.966 ms samples. Caches were not flushed;
ingestion continued normally. Three samples are not a p95 load benchmark, and
August was still heap for this checkpoint.

The following are distinct validation runs, not an additive test total:

- `backend-point-regressions.txt`: 992 passed, 156 skipped, one existing warning,
  22.65 seconds. Seven additional skips are the dedicated plain-server scenarios
  when their explicit connection is not configured.
- `isolated-point-reader-regressions.txt`: 49 actual isolated PostgreSQL reader
  tests passed in 7.65 seconds.
- `plain-reader-point-proof.json`: seven production-method scenarios passed in
  0.83 seconds on a separate PostgreSQL 18.6 server, no preloaded library, no
  Columnar extension/access method/GUC, 128 MiB, half a CPU, no network or ports.
  It covers exact reconstructed copies, missing rows, SQL NULL/malformed/scalar/
  array/JSON-null payloads, reused numeric IDs, sender/packet timestamp ties and
  receipt snapshot consistency. The container, data and credentials were removed.

Exact-head native [CI run 41283075375](https://github.com/SimmerV/meshinfo/actions/runs/41283075375)
passed on amd64 and arm64 at 07:02:44 UTC. Both jobs actually executed and passed
the new plain-server step; it was not skipped or run against an extension-enabled
database. `native-ci-8a70d6a.json` records the public API evidence, while
`point-detail-provenance.json` binds the source-free reports, revision and scope.
Earlier reports and failed gates remain unchanged. These checks do not establish
completed August conversion, final net savings or full-history rollout readiness.

## Node-reader candidate checkpoint (2026-11-01 07:35 UTC)

The node fix in `54f8fd74db109e70e82485703d378ad19edda8a3` was committed and
validated but not deployed at this checkpoint. August copy was still running.
The existing node SQL retains its disjoint sender/recipient branches and exact
pagination; the candidate scopes generic, index-oriented planning to that read,
inside a read-only repeatable-read transaction and ten-second deadline. Ordinary
PostgreSQL remains supported without creating a Columnar setting.

`live-query-during-august-6m.json` preserves 15 fixed HTTP cases measured at
approximately six million August rows copied. All returned 200 and matched the
supplied baseline hashes. July and September had already been compressed; their
node timings failed the latency acceptance gate:

| Node page | Earlier heap median (ms) | During-copy samples (ms) | During-copy median (ms) |
| --- | ---: | --- | ---: |
| July | 12.976 | 295.196, 189.486, 222.641 | 222.641 |
| September | 9.298 | 165.585, 61.416, 282.649 | 165.585 |

`live-query-updated-heap.json` supplies that earlier comparison. Its file timestamp
is 04:22:28 UTC, but its deployed revision was not recorded and is not inferred
from commit timestamps. Both reports are copied unchanged. Each case has three
sequential warm-cache samples, not a p95 or concurrent-load measurement. The
August topic/text baseline status is still unrecorded; its matching response hash
must not be interpreted as a successful baseline HTTP timing.

`live-sender-node-read-diagnosis.json`, recorded at 06:52 UTC, contains the original
read-only planning diagnosis and unchanged generic/custom variants. It predates
the node candidate deployment and includes sender/packet diagnostics as well as
the node cases:

| Statement | Original planning (ms) | Original execution (ms) | Indexed generic execution (ms) | Generic first fetch (ms) | Generic reused fetch (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| July node page | 173.777 | 13.751 | 11.535 | 195.208 | 11.071 |
| Missing July node | 162.159 | 1.350 | 0.168 | 169.794 | 0.387 |

All variants have matching result hashes; the node page returns 51 rows including
its sentinel. First-plan overhead remains visible. These component observations
motivate plan reuse but do not establish post-deployment HTTP performance or
full-history acceptance.

The separate validation runs are preserved without combining their totals:

- `backend-node-regressions.txt`: 1,000 passed, 175 skipped, one existing
  `audioop` deprecation warning, 24.72 seconds. The absolute workspace path in
  that warning was redacted; no test result changed.
- `isolated-node-reader-regressions.txt`: 50 actual PostgreSQL reader tests
  passed in 21.95 seconds, including the larger heap/mixed node fixture.
- `plain-reader-node-proof.json` and `plain-node-reader-regressions.txt`: 25
  actual cases passed in 2.89 seconds on a separate 128 MiB/half-CPU PostgreSQL
  18.6 server with no preload, Columnar extension, access method or GUC. Seven
  original detail scenarios remain; 18 additional node cases cover limits,
  inclusive windows, tied cursors, sender/recipient identity, self-addressed and
  NULL-sender rows, actual SQL failure, deadline and external cancellation.
  The same pooled backend remains usable with its original settings. All test
  schemas, the server, data and private credentials were removed.

Exact-head native amd64 and arm64 [CI run 41285209001](https://github.com/SimmerV/meshinfo/actions/runs/41285209001)
passed at 07:34:33 UTC. Both jobs actually ran and passed the expanded mixed-reader
and no-Columnar node/detail steps. `native-ci-54f8fd7.json` records public API
evidence. `node-reader-provenance.json` binds scope, source timestamps, revision
and hashes; `acceptance-artifacts.json` retains the earlier 23 artifact digests
and adds this checkpoint's eleven artifacts. No earlier failed gate is rewritten.
The node fix's live deployment and post-cutover acceptance remain unmeasured at
this checkpoint.

The subsequent test-only revision `3796e38fb6424f6ffdba424ac2eee242a3a51bfe`
adds stronger assertions that selective sender and recipient reads use their
appropriate indexes. Its production source is identical to `54f8fd7`.
`isolated-node-selective-regression.txt` records the separate actual PostgreSQL
rerun: one test passed in 13.89 seconds. Native amd64 and arm64
[CI run 41285435161](https://github.com/SimmerV/meshinfo/actions/runs/41285435161)
passed at 07:37:44 UTC, including both mixed and plain reader steps.
`native-ci-3796e38.json` preserves that exact-head result separately from the
implementation revision's CI evidence. Neither validation changes the live
deployment status recorded above.
