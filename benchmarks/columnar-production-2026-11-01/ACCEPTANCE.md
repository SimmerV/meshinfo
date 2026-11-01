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

## Deployed verifier and node-reader checkpoint (2026-11-01 08:21 UTC)

The node candidate's earlier undeployed status is historical. Revision
`61d44bdd762f6e89f2b83de13ccdf77ccb602e1a`, including that reader change and the
verifier improvements, was deployed at approximately 08:17 UTC. August's
14,544,219 rows had been copied, but verification and cutover remained unfinished.

The original read-only diagnostic reports remain unchanged:

| Diagnostic | First/current | Off/reused |
| --- | ---: | ---: |
| JIT comparison execution | 473.182 ms | 22.656 ms |
| JIT compilation within current execution | 415.328 ms | none |
| Generic comparison planning | 270.451 ms | 0.030 ms |
| Generic comparison execution | 23.219 ms | 20.433 ms |
| Generic planning allocation | 57,369 KiB | 32 KiB |

`live-august-verify-jit.json` uses one frozen 1,000-row cohort and two EXPLAIN
executions. It does not expose the aggregate's Boolean value, so it is not
presented as a separate equality-result proof. `live-august-verify-generic.json`
uses a different frozen 1,000-row cohort, records an actual true equality result,
and confirms three generic executions with no custom plans. Its index condition
contains parameter placeholders only. The diagnostic covers a nonfirst full
cohort; first/tail/reused-ID/missing/extra cases belong to the integration tests.
The two reports are not a paired full-workload benchmark. Initial planning and
its approximately 56 MiB allocation remain visible.

`live-verification-restart.json` preserves the same job UUID and source/payload/
helper catalog identities across CLI shutdown and backend restart. Copied rows
remain 14,544,219; verified rows advance 1,937,000 → 1,938,000 → 2,012,000; abort
counts remain zero and the source remains frozen. This is a graceful-restart
witness, not a power-loss or crash test.

`live-optimized-verification-observation.json` exports only selected progress and
resource facts from the same job's monitor segments, omitting source/request
values, container identifiers and host process IDs. The 08:10:13–08:13:43 interval
advanced 250,000 verified rows in approximately 210 seconds; the 08:17:12–08:20:42
interval advanced 5,652,000 in approximately 210 seconds. Different ranges,
existing caches, diagnostics and concurrent ingestion prevent treating those
observed rates as a controlled throughput guarantee.

`live-query-during-optimized-verification.json` preserves all 15 sequential HTTP
cases: every status is 200 and every baseline hash matches. July node samples
were 290.914/21.448/24.047 ms, and September's were 127.265/21.807/20.189 ms.
August remained heap; its node samples were 77.043/13.385/258.923 ms. Retaining
these slower samples matters: three requests do not establish p95, and this
report cannot establish final August compressed-query performance. The original
August topic/text baseline HTTP status remains unrecorded.

The separately recorded local runs are:

- `isolated-jit-verification-regressions.txt`: 15 passed, 41 deselected in
  8.24 seconds, covering the preceding scoped-JIT change.
- `isolated-generic-verifier-regressions.txt`: the isolated PostgreSQL validation
  suite passed 210 tests in 86.24 seconds after the exact-bound generic verifier change.
- `backend-verifier-regressions.txt`: 1,000 passed, 191 skipped, one existing
  warning in 25.96 seconds. Its absolute workspace path is redacted; results
  are unchanged. Skips and deselections are not passes, and totals are not added.

Exact-head native amd64/arm64 [CI for `61d44bd`](https://github.com/SimmerV/meshinfo/actions/runs/41288089813)
and [CI for `0dc2128`](https://github.com/SimmerV/meshinfo/actions/runs/41287318063)
report completed success. Both architectures passed backup/restore, checkpoint
lifecycle, mixed readers and plain-PostgreSQL reader steps. The corresponding
`native-ci-*.json` artifacts preserve the API responses' timestamps, which are
approximately one hour ahead of the local host clock at observation. No local
deployment ordering or elapsed time is inferred from those remote timestamps.

`verifier-deployment-provenance.json` binds scope, revisions, source-file times
and hashes. This checkpoint adds eleven artifacts while preserving all 34 prior
artifact digests and their failed gates. Final August verification/cutover, net
filesystem savings and full-history performance acceptance remain subsequent
work; the checkpoint does not assert production readiness.

## Completed live conversion and storage ledger (2026-11-01 08:33 UTC)

The earlier unfinished-August checkpoints are historical. The completed journal
records 2,844,321 July rows, 14,544,219 August rows and 706,383 September rows,
each copied and verified: **18,094,923 total**. August completed at 08:24:54 UTC.
`live-after-august-final.json` records all three packet partitions as Columnar
and no active copy/verification job. The exact row totals independently agree
with the geometry captured by `live-final-snapshot-start.json`; catalog
`estimated_rows` values are approximate and are not used for this count.

`live-final-storage-accounting.json` derives the following ledger from original
inventories. All payload/helper indexes and all Columnar metadata allocations
are included. Shared journal/identity/maintenance relations are conservatively
charged in full once, even where they predate this conversion.

| Component | Bytes |
| --- | ---: |
| Cold canonical table data | 1,837,105,152 |
| Cold canonical indexes | 4,513,300,480 |
| Canonical-topic helper table data | 1,334,812,672 |
| Canonical-topic helper indexes | 2,288,852,992 |
| All Columnar metadata tables/indexes and sequence | 638,230,528 |
| Archive journal, storage identity and maintenance state | 630,784 |
| **Completed archive footprint** | **10,612,932,608** |

`live-before.json` supplies the before-index-packing packet baseline.
The completed jobs' measured source sizes supply the separate post-packing,
pre-conversion baseline. Their distinction preserves the contribution from
earlier index maintenance:

| Baseline | Source bytes | Saved bytes after charging all archive overhead | Saved GiB | Reduction |
| --- | ---: | ---: | ---: | ---: |
| After packing, before conversion | 18,447,892,480 | 7,834,959,872 | 7.296875 | 42.4708% |
| Before packing | 18,552,209,408 | 7,939,276,800 | 7.394028 | 42.7942% |

The source baselines cover canonical packets. Cold reception tables/indexes
remain heap and consume 8,807,284,736 bytes; their shared topic dictionary consumes
376,832 bytes. They are excluded from both sides of the archive-replacement
comparison and remain part of the database. The physical ledger does not claim
a 42% reduction for the entire database.

Observed database size was 29,076,661,951 bytes before this work and
21,150,639,807 bytes in the final main inventory: 7,926,022,144 bytes (7.381683 GiB,
27.2591%) smaller. Ingestion and index maintenance occurred between observations.
The filesystem reports 19,080,392,704 bytes free; editor/build cache cleanup and
test-resource removal also contributed to free space. WAL is separately reported
as 872,415,232 bytes and is outside `pg_database_size`.

The main inventory was captured at 08:30:18 UTC and supplemental metadata at
08:32:59 UTC; overlapping helper/catalog sizes agree. These are live physical
measurements rather than one atomic filesystem snapshot. Supplemental accounting
includes the storage-ID sequence omitted by the main table-only inventory, plus
the journal and shared maintenance state. No metadata bytes are counted twice.

`live-final-snapshot-start.json` records actual read-only
`SnapshotMetadata.start/close`: 266.263 ms to export the snapshot and capture three
Columnar relations, 2.779 ms to close it. The version-2 metadata records Zstandard
level 3, maximum stripe/chunk groups of 93 rows, and the exact verified row total.
No backup file or full-history restore was created by this operation; the earlier
isolated restore proofs retain their separately stated workload scope.

`live-query-final-all-archived.json` preserves 15 fixed cases with three HTTP
samples each. All statuses are 200 and every baseline digest matches. Medians
range from 5.016 to 129.103 ms, but first-request latency remains material:

| Case | All observed samples (ms) |
| --- | --- |
| July page | 883.282, 15.055, 14.133 |
| July node | 1,540.354, 23.025, 22.104 |
| July topic/text | 994.225, 86.849, 80.747 |
| August literal topic | 821.187, 122.619, 109.024 |

The original August topic/text baseline status remains unrecorded. Responses
remain correct in these samples, but neither median values nor hash agreement
erase the slow requests. This is not a concurrent-load p95 test or proof of a
250 ms bound. Final planning-memory/load assessment is pending; completed
conversion and measured savings do not alone establish production readiness.

The later predeclared live load check in `live-final-concurrent-reads.json` is
also retained byte-for-byte. Two workers ran for 60 seconds, each limited to two
requests per second, after one warmup of each fixed case. Normal ingestion
continued; warming every statement on every pooled connection was not assumed.
All **236 responses matched** their baseline, but
`indexed_250ms_p95_gate` is explicitly **false**. July page/copy/node p95 values
were 616.157/642.660/1,590.004 ms; August copy/node values were
533.767/1,438.814 ms. September topic/text also had an 812.235 ms sample.
Each case has 15–16 observations, making the nearest-rank p95 its maximum.
Medians of roughly 7–124 ms do not turn this into a passing result. This evidence
motivates further first-plan latency/memory work; it is not relabeled as accepted.

`live-final-restart-validation.json` records the later clean restart. The backend
stopped gracefully, PostgreSQL restarted, and both services became healthy with
the same container/image/volume identities and no OOM. PostgreSQL shared buffers
were reset; the host page cache remained intact. Normal application startup ran
before the historical request, but no historical-data preflight or monitor
warmed that request after restart. All three response hashes matched. Latencies
were **1,013.425/15.167/14.194 ms**: the first exceeded the predeclared 1,000 ms
limit, so both `first_page_latency_passed` and `all_checks_passed` remain false.
Opaque infrastructure identities and fixed in-container mount destinations are
retained to substantiate continuity; no source messages or credentials are
included. This additional failed result does not replace or relax either prior
latency gate.

## Native planner patch v1 and retained failed load gate (2026-11-01)

The August diagnostic explains why JIT and prepared-plan reuse alone did not
finish the live acceptance work. On 156,390 stripes and 14,544,219 rows, a fresh
page/node plan allocated 200,474/336,923 KiB and took 886.018/1,494.128 ms even
with JIT off. These are PostgreSQL planner-context allocations, not RSS; executor
`work_mem` does not bound them. Generic reuse amortizes cost within a connection
and statement, but new connections and invalidation still need fresh plans.

`live-august-first-reuse-planning-summary.json` and the corrected full report
preserve all four matching response hashes. The earlier valid but partial
`live-august-first-reuse-planning.json` remains unchanged: the initial runner
compared the storage node result's `messages` envelope with the HTTP `packets`
envelope, then stopped. Correct API normalization and envelope handling resolved
that harness mismatch; it was not a SQL/data failure.

The rejected scan-flag experiments are preserved in
`live-august-planner-seq-off.json`, `live-august-planner-seq-index-off.json` and
their summary. All eight response hashes matched, and connection settings were
restored. The seq-off report still has `all_checks_passed=false` because its
structural checker did not understand varchar-to-text casts; it was not rewritten.
The later cast-aware both-off check passed, but that variant increased executed
sender rows from 52 to 912 and warm node execution from 12.263 to 150.430 ms.
Lower planner allocation did not justify changing the chosen execution strategy,
so those flags were not deployed.

Revision `55c2ec3835f679292cc91555eea3d1a434001477` instead patches the pinned
Citus 14.1.0 native code to reuse a temporary hook-local stripe summary while
preserving cost formulas. `planner-native-parity-v3.json` records **289 passing
paired comparisons**: 17 shapes across eight scan-flag combinations and two plan
modes, plus 17 transaction-visibility cases. Initial Columnar fixtures contain
8,180 rows in 121 stripes per server, covering empty/single/many/evolving-column
tables, aliases, self-joins, multiple indexes and scalar arrays. Fixture geometry
and statistics match; estimated plan trees/costs and ordered result hashes match;
both owned schemas were removed. Its retained plan expressions contain only
synthetic test values, never production request values or packets. Timings are
descriptive, and finite fixture parity is not a universal semantic proof.

Two earlier native-harness attempts remain private and are not counted as passes:
v1 called an unavailable setup function and ran zero comparison cases; v2 reached
the comparison/cleanup phase but failed to serialize a bytes-valued catalog
`relkind`, leaving invalid JSON. Supported ALTER TABLE options, explicit text
casting and robust report serialization corrected the harness. Only the complete
valid v3 rerun supplies the 289-case proof. Provenance records the private failed
files' hashes without publishing the truncated file as a valid report.

The separate `isolated-patched-native-regressions.txt` run passed **260 tests in
106.66 seconds**. `patched-native-backup-restore.json` passed the actual
4,530-row/two-Columnar-relation/eight-chunk-group restore checks, including local
and remote receiver, a large COPY row, geometry, stage guards, active statistics
reset with coincident OIDs, and rollback. Native amd64 and arm64
[CI run 41291043803](https://github.com/SimmerV/meshinfo/actions/runs/41291043803)
passed for the exact revision, including the new paired comparison and all prior
backup/lifecycle/mixed/plain-reader steps. Both architectures uploaded their
comparison artifact. Remote API timestamps are internally inconsistent and are
preserved verbatim; exact SHA and job/step conclusions establish status, not
remote timestamp ordering.

The deployed image is identified in `native-planner-patch-provenance.json`, along
with the exact PostgreSQL/source/patch hashes and build files. The actual
`live-native-planner-deployment.json` witness preserves the same data volume and
extension version, healthy services and matching hashes. Its restart-page samples
were **245.396/15.126/13.984 ms**, passing the unchanged 1,000 ms first-page gate.
The earlier 1,013.425 ms failed report remains unchanged.

`live-august-native-planner-fixed.json` and its summary retain the subsequent
same-geometry diagnostic:

| Read | First planning before → v1 (ms) | Allocated planner KiB before → v1 | v1 warm execution (ms) |
| --- | ---: | ---: | ---: |
| Page | 886.018 → 156.970 | 200,474 → 35,161 | 2.296 |
| Node | 1,494.128 → 270.875 | 336,923 → 45,721 | 11.658 |
| Literal topic | 623.032 → 137.595 | 200,474 → 35,161 | 10.039 |
| Topic/text | 731.525 → 124.869 | 229,794 → 35,169 | 25.453 |

All four hashes and bounded index paths agree. The first node/topic/topic-text
executions remained 121.478/54.176/115.350 ms, and the instrumented topic call
took 1,018.807 ms. That call includes extra EXPLAIN and repeated fetches and is
not an HTTP measurement. The single-session diagnostic did not measure concurrent
fresh-planner memory or cold OS caches.

Finally, `live-native-final-concurrent-reads.json` preserves the unchanged declared
60-second/two-worker live test: **240/240 responses matched**, but
`indexed_250ms_p95_gate` remains **false**. August node/topic p95 values were
**308.461/263.389 ms**, with 16 observations per case. The successful restart gate
and lower medians do not override this failed load gate. Further native work is
separate; this v1 checkpoint makes no final readiness claim.
