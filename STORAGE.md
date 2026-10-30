# History retention and storage management

MeshInfo keeps retained history in PostgreSQL. Packet links, per-gateway copy
reconstruction, pagination, and existing API responses remain available without
an archive restore or a second storage service.

## Implementation phases

1. **Retention and visibility.** Add validated, backwards-compatible retention
   configuration, a read-only capacity/expiration preview, public history
   availability, and notices in historical views. Verify that reporting never
   mutates the database, runs within a bounded timeout, and reveals no connection
   credentials or filesystem paths through the public API.
2. **Reliable expiration.** Execute only an explicitly enabled finite policy.
   Coordinate packet/reception boundaries, handle legacy and unpartitioned
   history conservatively, bound locks and work per maintenance cycle, and prove
   retained records and current state survive using an isolated PostgreSQL test
   database. Existing deployments continue to retain everything by default.
3. **Measured storage optimization.** Benchmark representative retained data,
   implement only lossless transformations compatible with ordinary PostgreSQL,
   preserve query behavior and packet reconstruction, and report measured size
   and latency changes. Maintenance must check temporary-space requirements and
   must not rewrite a live large partition merely to produce a benchmark.
4. **Backup coordination.** Separate backup retention from live-history
   retention, make completed backups and failures observable, prevent overlapping
   maintenance, and support bounded-memory off-box backup streaming without a
   full local staging file. Test interrupted transfers, verification, pruning,
   and restore behavior using synthetic data.

Each phase is tested and deployed independently and recorded in a separate
commit. This development deployment retains its local `backups.schedule =
"off"` setting; deployment-specific settings are not committed.

## Policy

`0` months means forever. Positive values count calendar months backwards from
the database's UTC clock. An optional per-family override can keep chat longer
than packet history, for example. The supported history families are `packets`
(canonical messages and gateway receptions together), `telemetry`, `chat`,
`traceroutes`, and `neighbors`. Current node state, current telemetry, ownership,
configuration, and channel definitions are not historical-retention targets.

Age follows server ingestion time (`created_at`), not device timestamps. For
`packets`, the canonical packet's first receipt sets the age of the whole group.
When that packet expires, all of its gateway receptions expire with it, even if
a copy arrived after the cutoff. A later copy does not extend the packet's
lifetime. Other history families use each record's own ingestion time.

Expiration is rounded down to a UTC month boundary, preserving at least the
requested window of canonical packets and other history records. Individual
reception copies follow their packet's lifetime. Depending on the current date,
almost one extra month may remain. A time policy is not a byte quota: higher
ingestion volume still needs more space. Shortening retention permanently removes
expired history; later increasing retention cannot recover it.

Configuration alone never silently changes an existing installation's default:
the default is forever, and automatic expiration requires a separate explicit
enable setting. With `enabled = false`, all finite policies are previews only.

```toml
[storage.retention]
months = 0
enabled = false

[storage.retention.overrides]
# chat = 12
```

Use `python scripts/manage_storage.py report --config config.toml` for a
read-only JSON inventory, candidate partitions, index sizes, and the local backup
filesystem's available space. Inside Compose use
`docker compose exec meshinfo python scripts/manage_storage.py report`.
The command does not assume that PostgreSQL lives on the app's filesystem.
History pages and the Stats page expose the public subset through
`GET /v1/storage/history`; no operator capacity details are public.

## Expiration operation

After previewing, set `enabled = true` and restart the backend. A single
background task attempts a bounded cycle every minute. For a manual cycle use
`python scripts/manage_storage.py expire --apply --config config.toml`; it still
requires an enabled finite policy. This command returns a nonzero exit code for
busy, blocked, partial, or failed work, and can be retried safely.

Each cycle has a 12-second work budget. Packet expiration takes at most two
complete month pairs and at most 5,000 cross-month reception tails per atomic
transaction. Packet locks are acquired without waiting; exclusive work is
bounded to five seconds. Other history families delete up to four 1,000-row
batches each, with short lock waits and statement timeouts. Already committed
batches remain committed if a later batch fails; status reports that progress.
Ingestion and HTTP requests may briefly wait while a packet cohort is removed.

Packet/reception relationships are checked under locks. Bounded overlapping-ID
checks handle normal concurrent inserts at midnight. Ambiguous imported IDs,
populated default partitions, non-monthly or unpaired partitions, missing
indexes, and user dependencies block expiration with an operator-visible reason.
No `CASCADE` or unbounded legacy-table delete is used. Retained packet identities
and reception copies remain intact. An unsupported layout needs operator repair;
enabling retention does not silently migrate it.

Unpartitioned history reuses freed space through ordinary PostgreSQL vacuuming;
row deletion does not guarantee immediate filesystem reclamation. Entire packet
partition drops reclaim their files. We do not run `VACUUM FULL` automatically.

The neighbor-history age index is built concurrently only when finite neighbor
expiration is enabled. If an interrupted build leaves
`idx_neighbor_history_created_at` invalid, inspect it, drop that invalid index
with `DROP INDEX CONCURRENTLY public.idx_neighbor_history_created_at`, and retry.
Maintenance refuses to trust an invalid index.

The operator report includes the last durable result and per-scope reasons.
The public UI exposes only safe status codes and warns about partial, blocked,
failed, busy, or stale maintenance. A configured policy is not a claim that
every expired record has already been removed.

## Operational requirements

- Preview before enabling or shortening a policy. Estimates from PostgreSQL
  catalogs are labeled as estimates, not exact row counts.
- Public availability reports expose policy and time coverage only. Detailed
  sizes and local filesystem capacity belong to the operator command.
- An unavailable database must produce an unavailable report, not an empty
  history or an invented zero-byte estimate.
- Maintenance takes an advisory lock, uses short lock waits, and limits each
  cycle's work. A failed or interrupted operation must be safe to retry.
- Expire a canonical packet and its reception copies atomically, including
  copies received after the cutoff; never leave copies that require an expired
  packet.
- No rate-limit tightening, lossy aggregation, or silent removal of gateway
  copies is a storage optimization.
- Backups can contain history that has expired from the live database; backup
  retention is an independent policy.

## Lossless index packing

Small JSON payloads did not benefit from lowering TOAST targets in our measured
traffic sample. This release does not claim to compress those payloads or create
a separate cold archive. All retained data stays in the same PostgreSQL tables,
with its existing identities, search, pagination, and reception reconstruction.
Lossless index packing reduces overhead; forever retention still grows forever.

Fresh schemas use ascending single-column time indexes. PostgreSQL supports
newest-first results through backward scans; multi-column index order is left
unchanged. Existing indexes are not silently replaced on startup.
See PostgreSQL's [index ordering documentation](https://www.postgresql.org/docs/18/indexes-ordering.html).

`[storage.compression]` is disabled by default. When enabled, an hourly task
attempts one eligible B-tree index on packet/reception months older than
`after_months` (default six). It waits for this process's retention cycle, then
uses the shared maintenance lock. Default limits are 64 MiB per index, a
five-second transaction, 100 ms lock wait, 32 MiB maintenance memory, no parallel
workers, and at least four times the index size plus 1 GiB free headroom.
The configured `database_filesystem` must be on the actual database filesystem;
on separate database hosts, run the manual tool where that disk is accessible.
Custom index tablespaces are excluded from automation. Replication slots or WAL
archiving require a separately budgeted operation and are refused here.

Automatic attempts rotate through eligible index names, wrapping around, so a
repeatedly blocked or slow index cannot prevent others from progressing. The
scheduler saves its position before rebuilding in one private
`compression_cursor` row in `storage_maintenance_status`; cancellation, restart,
and failed attempts preserve that position. Busy attempts leave it unchanged.
This is bounded scheduler metadata, not an attempt log. Manual packing does not
advance the automatic cursor, and public maintenance summaries omit it.

Use `python scripts/compress_storage.py plan` for a catalog-only preview.
For a manual, explicitly named closed index:

```sh
python scripts/compress_storage.py pack-index INDEX_NAME --after-months 0 \
  --apply --database-filesystem /actual/postgresql/filesystem
```

Zero `after_months` includes completed months, never the current month. Index
packing is a short ordinary `REINDEX` transaction: some queries and late writes
can briefly wait. Timeout, cancellation, insufficient headroom, index-definition
changes, or a larger replacement rolls back the operation. No invalid staging
index is left behind. Larger indexes are visible as over-limit candidates and
need an explicit maintenance budget; there is no background full-table rewrite.
See PostgreSQL's [REINDEX documentation](https://www.postgresql.org/docs/18/sql-reindex.html).

Reproduce bounded experiments with `scripts/compress_storage.py benchmark` and
`benchmark-index`, both requiring `--database` naming a separate initialized
test database. Samples are seeded block samples capped by rows and bytes. They
are not a statistically representative prediction of whole-archive savings.
Query measurements are cached observations, not cold-cache or load guarantees.

## Backup retention and streaming

Backups have their own lifetime. `backups.keep_days` controls local verified
snapshots; `remote_keep_days` controls the SSH destination and defaults to
`keep_days`. The newest verified snapshot is always retained, even if older
than that lifetime. Shortening live-history retention does not erase records
inside existing backups.

`schedule = "off"` disables automatic snapshots. Explicit one-off commands are
still available:

```sh
# Host Python 3.11+ and Docker; uses host SSH keys when mode=ssh.
scripts/backup_db.sh --config config.toml

# Container/app PostgreSQL clients; explicit one-off even if schedule is off.
docker compose exec meshinfo python scripts/manage_storage.py backup --apply
```

For direct off-box streaming:

```toml
[backups]
schedule = "daily"
mode = "ssh"
remote_target = "backupuser@backup-host:/srv/backups/this-mesh"
keep_days = 4
remote_keep_days = 30
min_free_mb = 256
timeout_seconds = 21600
```

The SSH destination needs a Unix filesystem, Python 3, and a compatible
`pg_restore` (PostgreSQL 18 for this stack). Provision a dedicated directory,
SSH key, and verified `known_hosts` entry. Connections use batch mode and strict
host-key checking. The receiver is transmitted with each invocation; no service
or installed MeshInfo agent is required remotely. To use the in-app scheduler,
mount the prepared SSH configuration read-only into the backend, for example:

```yaml
services:
  meshinfo:
    volumes:
      - ./backup-ssh:/root/.ssh:ro
```

The host script uses the host's SSH configuration instead. It retains the
`CONFIG_TOML`, `CONTAINER`, `DB_USER`, `DB_NAME`, `BACKUP_DIR`, `KEEP_DAYS`, and
`REMOTE_TARGET` environment overrides; `BACKUP_MODE` explicitly selects a mode.
A legacy nonempty remote target without an explicit mode selects streaming,
which does not also retain a local archive. The host tool rejects an explicit
local mode paired with a remote target rather than silently omit the remote copy.

Snapshots use custom-format `pg_dump`, 256 KiB streaming chunks, bounded error
buffers, and destination free-space checks. Local filesystem operations run
outside the backend event loop; cancellation waits for those operations before
closing or removing files. SSH mode creates no full local staging archive.
The configured deadline covers dump, transfer, and verification; filesystem
operations already in progress must finish safely during cancellation.

Publication requires producer success, a complete SHA-256/size match, and a full
`pg_restore` archive decode. A sidecar manifest records that verification.
Atomic publication never overwrites an existing archive. Expiration runs only
after a new verified snapshot exists. A pruning failure preserves that snapshot
and reports `partial`. Archive decoding is not a substitute for a restore drill.

One monitored PostgreSQL advisory lock coordinates backup, retention, and index
packing. Maintenance owns dedicated PostgreSQL sessions, leaving the application
pool available even when `max_pool_size = 1`. Each maintenance job uses one
additional control session; a backup also opens a `pg_dump` session. Account for
brief overlapping control attempts when sizing PostgreSQL’s connection limit.
Loss of the backup control connection cancels the dump. Host Docker jobs additionally
track and terminate the actual container processes; stopping `docker exec`
alone is insufficient. Scheduled jobs wait for this process's preceding bounded
maintenance. Durable last-success/destination metadata survives restarts and
makes a newly selected destination due immediately. Failures have a one-hour
retry backoff, including across restarts. Operator reports include status,
last-success time, bytes, verification, and cleanup counts; public responses
omit paths and diagnostic details.

Each database owns its backup directory using a durable UUID and a locked owner
marker. A different database cannot adopt or prune that directory. Restoring a
backup restores its identity, supporting disaster recovery into the same backup
destination. For an **independent clone**, select a separate destination and
rotate the identity in the cloned database before enabling its backups:

```sql
UPDATE storage_identity SET instance_id = gen_random_uuid() WHERE singleton;
```

Do not delete an ownership marker to bypass a mismatch. Crash-left recognized
partial files are cleaned only after identity and exclusive-lock checks. Final
archives, foreign files, symlinks, and legacy dumps without the new verification
manifests are preserved. Existing installations should inventory and manually
review legacy dumps after verifying their replacement; they are not silently
deleted during upgrade and will otherwise continue occupying space.

For a restore drill, check the archive against its manifest SHA-256, restore it
into a **new empty test database** with `pg_restore --exit-on-error --no-owner
--no-privileges`, then check records, constraints, sequences, packet links and
reconstructed copies through MeshInfo. Remove only that disposable database
after inspection. The integration tests in `tests/test_backup_integration.py`
provide a repeatable guarded example using tiny synthetic data.

## Verification record

Phase-specific commands, measurements, and deployment checks will be recorded
here as each implementation is completed. Benchmarks use bounded samples or
synthetic datasets in an isolated database, never a full rewrite of live mesh
history.

### Phase 1 — 2026-10-27

- Backend: 487 tests passed; five PostgreSQL integration tests additionally
  passed against `meshinfo_storage_test` (the full suite's other opt-in database
  tests were not enabled in that run).
- Frontend: all 395 tests in 37 files passed with one worker; TypeScript and the
  production Docker build passed. Existing large-chunk build warning remains.
- Rebuilt and deployed backend and frontend. Edge routes, built assets, the
  new history endpoint and existing packet query returned HTTP 200.
- First observed history request: 89 ms; packet request with one result: 22 ms.
  These are smoke measurements, not a load benchmark.
- Catalog inventory: approximately 26.47 GiB of history. Hypothetical one-month
  policy: 22.69 GiB of old paired partition candidates, pending execution checks.
- Actual deployment: forever, expiration disabled, backups off. No live history
  was expired. Approximately 6.5 GiB filesystem space remained after builds.

### Phase 2 — 2026-10-27

- Full backend suite with all PostgreSQL gates enabled: **560 passed**, no
  skipped tests. Destructive tests used only `meshinfo_storage_test`.
- Nineteen real retention scenarios prove atomic reception-tail cleanup,
  interleaved month-boundary IDs, dependency rollback, contention, repeatable
  bounded progress, exact cutoffs, retained packet-copy reconstruction/search
  pagination, and preservation of current node state across all history scopes.
- Fourteen maintenance tests cover durable failures, public redaction,
  single-flight work, and awaited cancellation. Fourteen frontend retention
  tests cover preview, partial/blocked/stale results, and per-scope overrides.
- Backend and frontend production builds passed and both were redeployed.
  Edge history/packet/Stats requests returned HTTP 200. The live policy remains
  forever and disabled; an explicit manual cycle correctly returned `disabled`.
- Approximately 6.4 GiB filesystem space remained. No live history was deleted.

### Phase 3 — 2026-10-27

- Bounded real sample: 2,000 packets across July–October. TOAST targets
  2040/1024/512 all consumed 2,138,112 bytes. No ineffective setting was deployed.
- A 20,000-row append model using those payloads produced an 819,200-byte
  descending timestamp index versus 466,944 bytes ascending (43% smaller).
  Repacking the descending index at fillfactor 100 reduced it to 425,984 bytes
  (48% smaller); returned rows were identical. Cached query medians were
  0.668–0.807 ms across the tested variants.
- Live bounded verification: `mqtt_messages_2026_09_created_at_idx` decreased
  from 28,524,544 to 14,327,808 bytes, saving **14,196,736 bytes (49.77%)** in
  2.597 seconds. Index identity/keys/uniqueness/attachment were preserved; the
  heap was untouched. The newest 100 complete packet tuples compared equal.
  Five-query cached medians were 1.336 ms before and 0.967 ms after. WAL generated
  during the operation was 14,677,088 bytes; other concurrent writes can contribute.
- Eight isolated PostgreSQL packing scenarios verify exact packet/reception
  preservation, cancellation, timeout, space refusal, post-build rollback,
  writer contention, shared maintenance exclusion, and growth between preview
  and lock acquisition. No live history expired.
- Full regression run: 604 passed before the final CLI/redaction/race additions;
  all added/changed focused checks subsequently passed (58 compression checks
  including eight PostgreSQL cases; 10 configuration checks).
- Rebuilt and deployed the backend. Operator preview inspected 77 indexes and
  correctly found no six-month-old candidates on this deployment. Public
  history/search/config checks returned HTTP 200; the filesystem path was
  redacted. Automatic packing remains disabled here; the measured manual index
  rebuild above is the only live packing performed. About 6.3 GiB remained free.

### Phase 4 and final verification — 2026-10-27

- Full backend regression: **746 passed**, with all 47 PostgreSQL/restore cases
  enabled on disposable databases. Final frontend suite: **401 passed** across
  37 files; production frontend build and TypeScript checks passed.
- Actual deployment image: **699 non-database tests passed**, then **45 real
  PostgreSQL tests passed** using that image's Python and asyncpg versions. The
  other two database cases are the host/Docker-pipe restore drills below. The
  runtime checks also corrected a test-order assumption about pre-existing
  synthetic expired rows; production expiration behavior was correct.
- Two complete restore drills passed: local host wrapper produced a 129,077-byte
  archive (2.837-second round trip); the actual receiver over a local Docker
  pipe produced 129,088 bytes (1.792 seconds). Each fixture contained 64 canonical
  packets and 192 reception copies, all history families, and current state.
  Restored raw rows, Unicode payloads, timestamps, IDs, 128 indexes, 111
  constraints, 10 sequences, reconstructed copies and pagination matched.
- The receiver test validates framing, checksums, decoding and restoration;
  an external SSH server/network was not configured or exercised.
- The deployed backend controller additionally completed a 101,908-byte
  temporary snapshot of the isolated database, with durable success status.
  The snapshot was immediately removed; schedule-off stayed off.
- Failure tests cover producer/transport errors, connection loss, timeout,
  repeated cancellation, destination mismatch/change, disk reserve exhaustion,
  orphan cleanup, pruning failure, and publication followed by status failure.
  A real Docker process check demonstrated that stopping `docker exec` leaves
  the container process alive, then verified token-checked cleanup terminates it.
- Backend image rebuilt/deployed with SSH client support. Final backend and
  PostgreSQL health checks were healthy; history, packet, nodes, Stats and Logs
  requests returned HTTP 200. Existing deployment settings remain forever,
  expiration disabled, automatic packing disabled, and scheduled backups off.
- Both task-created test databases and temporary restore artifacts were removed.
  Selected obsolete build cache from this task reclaimed 375.9 MB; current and
  WebAssembly caches and all data volumes were preserved. Approximately 6.3 GiB
  remained free. No live history records were expired.

Validation used PostgreSQL 18 and Python 3.14 in the deployment image. The final
commit includes the existing Docker/development Python 3.14 compatibility pins:
Meshtastic 2.7.11 requires Python below 3.15, so leaving the upstream 3.15 values
would prevent a clean checkout from building the tested deployment. Local
configuration and the development Compose override remain Git-ignored.

### Development-branch follow-up — 2026-10-28 UTC

Read-only live checks compared 300 canonical packet rows across July–October
against PostgreSQL, including three consecutive pages per month. Payloads and
row order matched; no duplicate or omitted rows occurred. Twelve historical
deep links matched their canonical packets and 130 stored reception entries;
the API returned reconstructed copies for every sampled reception. Matching and
absent text searches over bounded two-minute windows completed in 70–139 ms.
Date-bounded query plans pruned unrelated partitions and used the existing time
indexes, including the packed September index. No invalid indexes were present.

The following HTTP timings include the local reverse proxy and API. The 25-row
medians use seven repeated month-end queries; the 200-row medians use five
queries on fixed mid-month dates (October 25 for October). The latter matches
the Logs page size. Each 1,000-row value is a single request on the same date.

| History month | 25 rows, median | 200 rows, median | 1,000 rows |
| --- | ---: | ---: | ---: |
| July | 9.7 ms | 28.6 ms | 150.0 ms |
| August | 8.0 ms | 25.1 ms | 124.5 ms |
| September, packed time index | 8.1 ms | 34.0 ms | 179.1 ms |
| October | 11.6 ms | 28.2 ms | 147.6 ms |

These are bounded serial observations with warm caches, not a cold-cache,
concurrent-load, or unrestricted full-history text-search guarantee. Historical
data remains in PostgreSQL; no separate compressed payload archive was created.
The September index remains 14,327,808 bytes, preserving the earlier measured
49.77% index reduction.

An eleven-sample observation from 03:04:22 through 03:11:52 UTC showed ingestion
advancing at every sample, with no additional deadlocks, conflicts, or temporary
database files. History-status requests took at most 42.2 ms and newest-packet
requests at most 10.4 ms. The existing device-clock and node-field-length
warnings remained; no new history-query or storage-maintenance error appeared.
This verification did not expire live history or enable scheduled backups.

Follow-up review fixed two availability/progress issues before final deployment:
maintenance no longer occupies an application-pool connection, and automatic
packing no longer retries one failing candidate indefinitely ahead of others.
Connection acquisition/closure remains bounded through cancellation, and the
packing cursor is durable before the attempted rebuild. Packet-retention text
also now explicitly describes canonical-packet age and grouped copy expiration.

- Full backend regression: **729 passed**, with 52 opt-in database cases skipped
  in that run. Separately, **13 affected PostgreSQL cases passed**: backup
  control-session isolation/loss, rotation durability/exclusion, and actual
  packing, data preservation, timeout/cancellation rollback, and space refusal.
- The final deployment image passed **130 focused tests**, including those 13
  real PostgreSQL cases. Python 3.14.10 and every installed Python package
  version matched the preceding deployment despite rebuilding dependency layers.
- Frontend: **27 focused tests passed**; full ESLint, TypeScript, and production
  builds passed. The six history routes and the new retention-text bundle were
  checked through the deployed reverse proxy. No browser binary was available,
  so these were HTTP/source/jsdom checks rather than a visual browser session.
- Rebuilt and deployed both application services. Exact source hashes matched
  all four changed backend runtime modules. Both disposable test databases were
  removed after checking that no sessions remained; no test backup files remain.
- Post-deployment checks compared another **800 historical row IDs** across
  July–October exactly. The first 200-row requests took 91.4/30.5/35.0/31.0 ms
  respectively. Seven samples over a further three minutes showed ingestion
  advancing, fresh disabled-retention status, no lingering maintenance sessions,
  and no added deadlocks, conflicts, or temporary database files. Backend and
  PostgreSQL were healthy; approximately **5.4 GiB** remained free. Forever
  retention, disabled automatic packing, and scheduled backups off were preserved.

## Compressed packet-history prototype (2026-10-30)

**Real payload compression is feasible; production conversion is not ready.**
The earlier index packing does not compress historical packet records. This
prototype tests PostgreSQL partitions with the standalone Citus Columnar access
method and Zstandard compression. It adds operator experiments and reports,
not an archive setting, application migration, or automatic conversion worker.
The live application, database image, retention policy, and historical rows were
not changed by this experiment.

The fixture contains **15,890 real canonical packets, 102,698 linked receptions,
and 577 topic dictionary entries**, sampled across July–October. Nine synthetic
packets, two receptions, and two topics exercise boundary cases. October remains
heap storage; the three earlier packet partitions use columnar storage.
Receptions remain heap storage. Everything uses the same local filesystem;
the test databases are temporary, isolated copies, not an external archive.

### Size and query results

All sizes below include the applicable indexes, TOAST storage, and compression
metadata. Shared receptions and topics count once in each hypothetical layout.
GIN leaf indexes were rebuilt after loading, before measurement: pending-list
allocation from fixture COPY must not be mistaken for compression savings.
B-tree definitions, including actual source sort directions and partial
predicates, are preserved. Raw payload TEXT roundtrips exactly; JSONB extras
preserve their decoded values. Storage retained by rollback probes is counted.

| Comparison | Heap | Compressed | Reduction |
| --- | ---: | ---: | ---: |
| Old packet table data, including columnar metadata | 8.73 MiB | 1.01 MiB | 88.5% |
| Old packets with the same supported B-tree indexes | 12.23 MiB | 4.52 MiB | 63.1% |
| Full fixture with equivalent indexes | 41.14 MiB | 33.65 MiB | 18.2% |
| Full fixture with the original cold GIN indexes on the heap baseline | 42.38 MiB | 33.65 MiB | 20.6% |

The last row includes removing unsupported cold GIN indexes. The **18.2%** row
separates that index removal from compression. Uncompressed receptions and the
topic dictionary account for 24.22 MiB, so compressing only old canonical packets
cannot reduce the entire fixture by the packet-data ratio. The preliminary 30.2%
figure, measured before normalizing GIN indexes, is superseded by these results.

Warm medians below compare the original indexed heap with the mixed layout;
the page query fetches 201 rows for a 200-row page plus its continuation marker.

| Existing SQL operation | Heap | Mixed heap/columnar |
| --- | ---: | ---: |
| Recent page | 2.95 ms | 3.21 ms |
| Archived month page | 2.97 ms | 8.66 ms |
| Packet row-ID lookup | 0.65 ms | 2.12 ms |
| Archived node OR filter | 0.90 ms | 10.42 ms |
| Literal topic filter | 2.38 ms | 25.99 ms |
| Archived common payload search | 9.24 ms | 93.16 ms |

These results preserve practical small-sample browsing, but **do not demonstrate
equal query speed**. Node, topic, and payload searches have substantial relative
regressions. The experimental node UNION rewrite was slower on this fixture.

Existing absent-term and reception-only searches are also slow on ordinary
heap storage. All four such direct-query cases failed a two-second threshold in
each of the three layouts: the benchmark deliberately reports twelve failures
and exits with status 2. Longer application-reader checks completed these
archived searches in roughly eight to ten seconds. This existing search issue
must not be attributed solely to compression or silently treated as an empty
successful result.

A separate SQL candidate computes matching reception topic IDs once using
`ANY(ARRAY(SELECT ... FROM mqtt_topics ...))`, instead of repeating the topic
search inside the correlated reception join. Its archived absent/receipt-only
search medians were about 155 ms on compressed data, versus 150 ms on the indexed
heap. These are experimental queries only; application SQL is unchanged. The
report explicitly marks original-query parity unavailable where the original
query exceeded the benchmark timeout; application cases independently check the
original search results and receipt-only witnesses.

### Correctness and operational checks

- All fixture rows match across all three layouts, including payload text,
  nullable fields, timestamps, and reception data. Primary keys reject duplicate
  historical identities; the exact application BEFORE INSERT trigger works on
  both heap and columnar leaves. Probe transactions roll back logically.
- **44 application test cases per layout, 132 total**, pass using the actual
  `PostgresStorage` readers with a ten-second query limit. These cover pagination
  across the archive boundary and tied timestamps, reported versus ingestion
  time, newest packet-ID reuse, literal topic characters, null/scalar/malformed
  payloads, and gateway-copy reconstruction. Independent expected copies check
  Unicode, null overrides, deleted keys, and cross-month receptions. Logged
  warnings/errors fail a case, including methods that otherwise swallow errors.
- A custom-format `pg_dump --no-owner --no-privileges` restores successfully
  with `pg_restore --exit-on-error --single-transaction` into a second isolated
  database. Every fixture row and partition access method matches after restore;
  all 44 application cases pass again against restored compressed history.
  **Storage tuning is not identical:** `chunk_group_row_limit` resets from the
  tested 1,000 to 10,000 on every restored cold partition. Restored page timings
  therefore are not interchangeable with the benchmark timings. The production
  restore gate fails until table options are preserved and applied before data
  loading; changing the setting afterward does not rewrite existing chunks.
- Synthetic lifecycle tests prove lock-timeout refusal leaves the original heap
  intact, DDL rollback restores heap access and rows, committed conversion keeps
  rows, and ordinary B-tree REINDEX preserves them. Native conversion holds an
  exclusive lock that blocks readers until commit; it is not an online migration.
- An UPDATE against the mixed parent with only an ID predicate fails even when
  the matching row is hot. Adding the partition date restriction permits the hot
  UPDATE. This reproduces a real startup-backfill integration blocker.

See the source-free reports in
[`benchmarks/columnar-2026-10-30`](benchmarks/columnar-2026-10-30/):
`benchmark.json`, `application.json`, `application-restored.json`,
`restore-command.json`, and `lifecycle.json`. Query failures remain in the report;
the prototype does not claim an all-green performance gate.

### Reproducing the bounded experiment

`Dockerfile.columnar-prototype` pins the tested PostgreSQL 18.6 image and verifies
the Citus 14.1 package checksum. It is an **amd64 prototype image**, not a
replacement Compose service. Only `citus_columnar` is preloaded; this experiment
does not enable the distributed Citus extension.

Use a new PostgreSQL container with a new data directory on the local disk and
no live database volume or application configuration mounted. The measured
container had one CPU, a 384 MiB memory limit, 32 MiB shared buffers, 4 MiB work
memory, 32 MiB maintenance work memory, JIT disabled, no parallel workers, and
normal WAL/fsync durability. It had no published ports and used its own internal
Docker network. Install `citus_columnar` and `pg_trgm` in a dedicated database
whose name ends in `_columnar_test`. Keep its DSN in an environment variable;
never point the write-capable harnesses at the live database.

```sh
docker build -f Dockerfile.columnar-prototype -t meshinfo-columnar-prototype .

python scripts/sample_columnar_history.py --config config.toml \
  --host SOURCE_HOST --per-partition 500 --output NEW_PRIVATE_FIXTURE_DIRECTORY

python scripts/benchmark_columnar_history.py --dsn-env COLUMNAR_TEST_DSN \
  --fixture-dir PRIVATE_FIXTURE_DIRECTORY --output benchmark.json \
  --keep-schemas --query-timeout-seconds 2 --repeats 3

python scripts/verify_columnar_application.py --dsn-env COLUMNAR_TEST_DSN \
  --fixture-dir PRIVATE_FIXTURE_DIRECTORY --benchmark-report benchmark.json \
  --output application.json

python scripts/verify_columnar_lifecycle.py --dsn-env COLUMNAR_TEST_DSN \
  --output lifecycle.json
```

After restoring into another dedicated `_columnar_test` database, repeat the
application verifier with that target DSN, `--restore-report application.json`,
and a different output file. A nonzero benchmark exit can still produce a valid
failure report; inspect it before running subsequent checks. The application
verifier requires exact-data and constraint gates before accepting its schemas.
It verifies application responses, not storage-option equivalence. Also compare
`SELECT * FROM columnar.options` with the benchmark settings; the recorded
restore experiment failed that separate check.

The exporter uses a read-only repeatable-read snapshot, fixed seeds, short
statement/lock timeouts, a three-minute total budget, private output permissions,
and strict row/byte limits. It refuses existing output directories and removes
its partial files on failure. The first larger sample exceeded the reception
limit and was correctly refused; the measured run reduced packet sampling
instead of raising the caps. Raw fixtures, dump files, credentials, temporary
containers, and test databases must be removed after verification. Only aggregate
reports belong in Git.

### Gates before upstream integration and production conversion

1. Add an independently configured archive age while preserving forever retention
   by default. Archiving keeps history; finite retention deletes it. Support
   mixed partitions in startup, reporting, maintenance, and schema upgrades.
   Parent GIN creation and unpruned startup backfill currently prevent a direct
   switch. Keep receptions in heap until their cross-month deletion semantics
   have a separately verified compression design.
2. Implement a resumable staging/cutover worker with explicit row verification,
   temporary-space and WAL budgets, bounded locks, cancellation, crash recovery,
   and coordination with retention and backups. A large native ALTER rewrite
   can block readers and require substantial free space. This prototype does
   not solve migration of the existing multi-gigabyte partitions on a nearly
   full disk. Whole-month partition sizes may need to be reduced first.
3. Fix and test the existing search-plan issue, preserve indexed topic/node
   access, and benchmark larger histories under concurrent ingestion and cold
   caches. Define acceptable absolute and relative latency limits. Investigate
   query/index changes or separate heap lookup metadata if compressed partitions
   cannot meet them; do not promise identical performance from this sample.
4. Preserve per-partition columnar options through dump/restore, applying them
   before rows are loaded. Validate supported PostgreSQL versions/architectures,
   extension packaging, upgrades, physical crash recovery, and operational
   recovery on the upstream project's deployment configurations before rollout.

The sample is approximately balanced by month, not weighted by production byte
distribution, and it fits in memory. Its largest packet payload is 1,404 bytes;
large-value behavior is untested. The sampler excludes source payloads over
1 MiB. No full-history disk-scale forecast, concurrent-load guarantee, automatic
future/default partition test, or physical crash-recovery claim follows from
these measurements. The mixed layout's hot heap also used about 232 KiB more
than the pure heap copies; the full-layout totals include that difference.

The tested engine's
[versioned columnar documentation](https://github.com/citusdata/citus/blob/v14.1.0/src/backend/columnar/README.md)
describes mixed partitions, B-tree support, unsupported GIN indexes, append-only
restrictions, and space retained by aborted writes. These constraints need
explicit application support; installing the extension alone is insufficient.
