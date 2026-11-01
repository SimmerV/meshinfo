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
traffic sample. Index packing reduces index overhead only; it does not compress
those payloads. The optional Columnar archive described below compresses complete
canonical packet partitions while preserving access through the same table and
API. These are separate controls. Forever retention still grows forever.

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

For a restore drill, keep the archive and its manifest together and use
`scripts/restore_database.py` against a **new empty test database**, as described
in [POSTGRES.md](POSTGRES.md#restore-to-an-empty-database). Then check records,
constraints, sequences, packet links and reconstructed copies through MeshInfo.
Direct `pg_restore` remains suitable for legacy heap-only backups, but does not
restore the captured Columnar settings before data loading. Remove only the
disposable database after inspection.

## Lossless Columnar archive: deployment and recovery

The optional archive converts closed **canonical packet** months from heap to
Citus Columnar within the existing PostgreSQL volume. Recent packets and gateway
receptions remain heap tables. The `mqtt_messages` parent still returns both
forms through ordinary SQL; packet IDs, exact payload text, ingestion times,
pagination and gateway-copy reconstruction keep the same API contract. There is
no archive download or restore step for a historical query. This preserves access,
but does not promise identical query latency for every filter: measure the
queries important to your deployment before converting its larger months.

Archive age and retention lifetime are independent:

| Intent | Archive policy | Retention policy |
| --- | --- | --- |
| Keep everything; compress closed older months | `enabled=true`, chosen `after_months` | `enabled=false`, or every effective family lifetime is `0` |
| Keep six months; compress the older part of that window | For example, `after_months=1` | `enabled=true`, `months=6` |
| Preview without converting or deleting | `enabled=false` | `enabled=false` |

Both ages use complete UTC calendar months and server ingestion time. An archive
age of one month preserves the current month and at least the preceding month's
window in heap storage. Retention that expires a month takes precedence over
compressing it. Compression never extends a finite retention policy and never
makes forever retention fit a fixed disk indefinitely.

### Canonical-topic lookup and query behavior

Each archived month also has a heap partition under `mqtt_archive_topics`.
It stores the exact canonical `(id, created_at, topic)` triples, including NULL
topics, with a primary key, an ordered paging index and a topic trigram index.
Payloads remain in Columnar. This additional table makes literal topic filters
indexable despite Columnar's lack of GIN support. Its heap and index bytes are
part of the archive estimate, actual staging budget and storage inventory;
compression savings must include this cost.

The worker copies and independently verifies those triples before attaching the
lookup in the same transaction as the compressed packet month. Both become
immutable under validated guards. When automatic archiving is enabled, existing
Columnar months missing a lookup are eligible for a resumable lookup-only job,
which takes priority over new conversions. Until a complete lookup is attached,
queries use the canonical packet table for that month. Cancellation removes only
the job's validated detached stage. Finite retention removes an expired month's
lookup with its canonical history in the same transaction; verified restoration
to heap removes the lookup at the atomic reverse switch.

Literal topic filters preserve literal `%`, `_` and backslash characters. They
use complete lookup months together with native hot/uncovered months, under one
repeatable-read snapshot and parent locks that coordinate with conversion and
expiration. Simple topic/date pages select bounded identities before loading
payloads; both identity columns are checked and the requested cursor order is
restored. Free-text and combined filters first inspect a bounded newest-key
prefix, normally at least 128 identities and at most 4,096, plus one overflow
sentinel. Indexed payload filtering proves the requested page when it finds
the requested number of matches plus one; a complete key set also proves a
shorter result. Otherwise the original full query checks all older candidates.
Requests above the probe cap use the full query directly. All predicates apply
before the final page limit, so matches beyond that probe are not omitted.
Node pages use separately indexed sender/recipient
branches and exclude only duplicate self-addressed results.

Free-text search retains its existing ILIKE wildcard behavior and includes
eligible gateway reception topics. Matching dictionary IDs use a bounded array;
more than 4,096 matches switch to an exact dictionary lookup instead of growing
that array without limit. This does not truncate search results. Planner changes
are transaction-local: selective pages use custom plans, and only a bounded
exact-payload fetch disables Columnar custom scans. The full multi-statement
read has one ten-second execution deadline, in addition to the separately
bounded pool acquisition. Broad payload searches can
still scan substantial history. Bounded result pages and statement deadlines do
not imply identical latency to heap storage or a constant memory limit for
arbitrarily large payloads.

### Deploy without starting a conversion

Use a tested application revision containing the archive, query, retention and
backup changes together. The optional database image is PostgreSQL 18.6 plus
Citus 14.1.0, with pinned inputs documented in
[POSTGRES.md](POSTGRES.md#optional-columnar-image-backup-metadata-and-restore).
The native architecture CI job for the exact revision must pass; a Dockerfile or
an amd64 result alone is not arm64 validation. Do not use this overlay to change
the major version of an existing PostgreSQL data directory.

Keep `storage.archive.enabled=false` initially. Keep a verified production backup
and its manifest on an independent destination, and perform a restore drill
before the first production conversion. Build and install the optional image
using the existing data volume. In the example below, add your site's Compose
files before the Columnar overlay and replace the default database/role if needed:

```sh
dc() { docker compose -f docker-compose.yml -f docker-compose.columnar.yml "$@"; }
dc build postgres
dc up -d --wait postgres
dc exec -T postgres psql -U postgres -d meshinfo -v ON_ERROR_STOP=1 \
  -c 'CREATE EXTENSION IF NOT EXISTS citus_columnar'
dc up -d meshinfo
```

The application image must also be the tested revision; the overlay does not
replace or build it. Preserve any additional required preloaded extensions in
the PostgreSQL command. The overlay mounts the database volume read-only inside
the backend at `/var/lib/meshinfo-postgres-space`. Configure that actual volume
for free-space checks, never an unrelated app directory:

```toml
[storage.archive]
enabled = false
after_months = 6
database_filesystem = "/var/lib/meshinfo-postgres-space"
```

For external PostgreSQL, run the operator process where it can measure the actual
data filesystem. Custom data/index/temporary tablespaces, replication slots and
WAL archiving are outside this automatic space budget and are refused. A local
filesystem probe cannot establish an external database's available space.

```sh
dc exec meshinfo python scripts/manage_storage.py report
dc exec meshinfo python scripts/manage_archive.py plan
```

The first command reports capacity, table/index bytes, Columnar metadata, staging
space and job status. The second lists monthly candidates, exclusion reasons and
checkpoints; it is read-only. Catalog row estimates and the later bounded
compression pilot are estimates, not guaranteed savings. Record the pre-migration
inventory and normal ingestion/query timings before proceeding.

### Convert, observe and interrupt safely

Set `storage.archive.enabled=true`, choose `after_months`, and restart the backend
to apply the configuration. The scheduler advances bounded work with the same
advisory lock as backup, retention and index packing. To advance work manually:

```sh
dc exec meshinfo python scripts/manage_archive.py run --apply
dc exec meshinfo python scripts/manage_archive.py run --apply --until-idle --max-cycles 100
```

A single manual cycle can exit nonzero when work remains; its JSON status and
checkpoint distinguish partial progress from a failure. `--until-idle` is an
explicit bounded loop, not a background daemon. Concurrent scheduler/manual
attempts return `busy`; they do not start competing copies.

Before conversion, the tool builds independent topic GIN indexes concurrently
on each heap leaf. Existing indexes continue serving queries until all heap
leaves have validated replacements. Only then does a short transaction remove
the incompatible parent GIN. Preparation handles at most one build per cycle,
with a separate timeout (default 300 seconds) and a measured free-space reserve;
it can take longer than a normal copy cycle. An interrupted managed build is
cleaned or repaired on retry. An unfamiliar index definition is reported for
operator review instead of silently dropped. Archive-disabled heap-only
installations retain normal missing-index startup repair; mixed storage never
recreates an incompatible parent GIN.

The smallest eligible month is attempted first. A bounded pilot estimates the
replacement, retained indexes and WAL budget. It uses the writer's actual
row/byte/WAL limits and seals the same small compression groups with savepoints;
the estimate includes row-scaled Columnar metadata and raw min/max lengths.
Reverse conversion estimates expanded heap/index bytes using the compressed
source's exact catalog row count. Sampling still cannot reserve disk or predict
every later record, so each real batch rechecks the actual filesystem.
The original stays attached and
readable while a detached replacement is built and verified in committed chunks.
Job-specific triggers freeze changes to the old month during conversion; normal
current-month ingestion continues. Default limits are 1,000 rows/8 MiB per copy
batch, a 10-second copy/verification cycle, three-second statements, a 64 MiB WAL
budget and at least 1 GiB free reserve. The executor checks actual free space and
physical staging growth as it advances. It never drops the original to manufacture
space for its replacement.

Small copy batches bound memory, locks and WAL per transaction; they do not bound
the complete replacement's disk footprint. Before the switch, free space must
cover the estimated compressed month, its retained indexes, the canonical-topic
lookup and its indexes, the WAL allowance, and the free-space reserve. The old
month already occupies disk and remains in place until verification succeeds.
Smaller eligible months can finish first and release their original files to
fund a larger conversion. If the remaining month still does not fit, reclaim
unrelated unused files or add capacity before retrying. Lowering the reserve or
deleting parts of the original is not a supported way to make the job fit.
Future ingestion also needs ongoing headroom: retention limits growth, while
keeping all history still requires eventual disk expansion even with compression.

Verification compares every retained column, including text bytes and NULLs,
then checks schema, constraints, indexes, partition bounds and job ownership
again before a short atomic switch. Readers see the complete original or the
complete replacement. Whole-row hashes used by external workload benchmarks
supplement this exact in-database comparison; a row count alone is insufficient.
Planner statistics for the payload stage and topic stage have separate durable
checkpoints. If the second bounded `ANALYZE` times out or is interrupted, the
first successful step does not repeat. Both must complete before the switch.
Validated relation-identity changes after logical restore invalidate these
statistics checkpoints so the restored physical tables are analyzed again.
The original files are reclaimed only at the successful switch. Columnar internal
metadata, B-tree indexes, receptions, staging, WAL and other history still consume
space, so compare total database/filesystem usage as well as compressed table size.

Use `plan` and `report` to inspect `copying`, `verifying`, `ready`, `complete`,
`blocked` or `failed` states and reasons. Cancellation or process termination
leaves committed checkpoints resumable. Repeated interrupted copy batches are
bounded and eventually cancel that attempt, reclaiming its owned stage while
preserving the original. Do not manually remove journal rows, stage tables or
freeze triggers to bypass a refusal. To discard an unfinished conversion safely:

```sh
dc exec meshinfo python scripts/manage_archive.py cancel --job JOB_UUID --apply
```

The container entrypoint replaces its shell with the application process, so
Docker stop signals reach maintenance cancellation and database cleanup. Both
Compose deployments allow 30 seconds before forced termination. Checkpointed
conversion also remains recoverable if that grace period is exhausted.

Setting archive `enabled=false` and restarting the backend cancels an unfinished
forward conversion on the next maintenance attempt. Completed compressed months
remain readable and compressed. A pending explicitly requested heap restoration
continues even while automatic archiving is disabled.

History queries preserve their existing filters and keyset pagination across
both storage methods. A timeout returns HTTP 504 (`history_query_timeout`); an
unavailable query returns HTTP 503 (`history_unavailable`), with retry metadata.
Failures are never reported as an empty page or a missing packet. Test older
packet links, reception-only searches, reused mesh packet IDs, cursor ties and
normal recent ingestion under concurrent load before accepting the deployment.

### Roll back storage or recover from a backup

Keep the Columnar-capable image and compatible application code installed while
any Columnar tables remain. Disabling the archive policy stops conversion; it does
not make a plain PostgreSQL image able to read compressed partitions. The older
`migrate-postgres.sh` SQL-dump workflow is not a Columnar migration procedure;
it now refuses compressed/frozen storage before stopping services or removing
a volume. Use a verified manifest-aware restore for recovery on the same
PostgreSQL major. Major upgrades need a separately tested migration procedure;
the current restore tool refuses a major-version mismatch.

For a deliberate return to heap storage, disable automatic archiving, restart
the backend, cancel any unfinished forward job, and restore one completed month:

```sh
dc exec meshinfo python scripts/manage_archive.py restore \
  --partition mqtt_messages_YYYY_MM --apply --until-idle --max-cycles 100
```

If the cycle limit is reached while that job is still active, advance its
existing checkpoint with `manage_archive.py run --apply --until-idle
--max-cycles 100`. The `restore` command starts a new job and reports `busy` when
one is already active; the scheduler also continues an existing heap-restoration
job while automatic archiving is disabled.

This is the same checkpointed, fully verified copy-and-switch process in reverse.
It requires space for the expanded heap and indexes while the compressed original
remains present. A low-space refusal preserves the source; restoration does not
use destructive in-place rewrites. Repeat only after checking each result. Do not
remove the extension or its image until the operator inventory and PostgreSQL
catalog confirm that no attached or detached Columnar tables remain.

Returning all months to heap does not uninstall `citus_columnar`; the legacy
major-upgrade tool still refuses while that extension is installed. If returning
to an ordinary PostgreSQL image, first verify that no Columnar relations or
unfinished archive guards remain, then remove the unused extension with
`DROP EXTENSION citus_columnar RESTRICT`. Investigate any remaining dependency
instead of using `CASCADE`. Keep the compatible image available for retained
Columnar backups. This changes the table storage format; a PostgreSQL major
upgrade still requires its own verified backup and migration procedure.

Normal local/SSH backups preserve all Columnar relation options, including an
unfinished stage, under the same exported snapshot as the dump. Keep each
`.dump` and `.manifest.json` together and use `scripts/restore_database.py`, which
checks the exact extension version, applies options **before COPY**, and restores
in a single transaction. Version 2 metadata additionally captures observed
physical stripe/chunk row limits. The restore releases bounded COPY savepoints
inside that transaction so short source stripes do not silently become large
ones; failure still rolls back the whole restore. Version 1 manifests remain
compatible but explicitly report that physical geometry and latency can change.
Missing or mismatched metadata refuses the restore.
Restored jobs resolve new object OIDs only after validating names, schema and
job-specific guards; a logical restore cannot blindly reuse old OIDs. Preserve
the original database role for an in-progress job; an ownership change requires
operator review and can be resolved by cancelling the validated unfinished job
and starting a new attempt. See [POSTGRES.md](POSTGRES.md#restore-to-an-empty-database)
for the commands and independent-clone backup identity requirements.

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

**Historical prototype record.** The results and limitations in this section
describe the 2026-10-30 experiment before the archive worker was implemented.
At that point, real payload compression was feasible and production conversion
was not ready. See the [implementation and acceptance status](#columnar-implementation-and-acceptance-status-2026-11-01)
for the subsequent work and its remaining validation gates.

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

### Historical pre-implementation gates

These were the requirements identified by the prototype, not a current list of
unimplemented features. The dated implementation status below distinguishes
completed code and verification from deployment acceptance.

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

## Columnar implementation and acceptance status (2026-11-01)

The four prototype blockers now have implementations and isolated verification.
This entry records implementation evidence, not approval to update an arbitrary
production deployment. Final workload latency, development deployment checks and
native CI for the final revision must be accepted before a production rollout.

| Area | Implemented behavior and verification |
| --- | --- |
| Policy and mixed storage | Archive age is independent of finite retention; both default to preserving existing history. Startup supports heap/Columnar leaves, validates index coverage, and avoids modifying frozen history. Actual rollover checks exercised the current month, two future months and default routing alongside compressed history. |
| Bounded conversion | Committed checkpoints, exact row comparisons, source/stage ownership guards, conservative space/WAL checks, cancellation, reverse conversion and shared maintenance exclusion are implemented. An actual PostgreSQL process kill and restart preserved committed history and allowed checkpoint recovery. |
| Searchable history | Canonical-topic lookup partitions, indexed node branches, combined filters, cursor ordering and reception reconstruction are verified against real heap/mixed fixtures. A 4,098-topic fixture proved dictionary overflow still finds reception-only matches outside the bounded fast-path array. |
| Backup and recovery | Snapshot-bound manifests preserve every Columnar relation, including detached stages. Restore applies exact options before COPY and uses bounded savepoint batches inside one atomic transaction to preserve observed physical grouping limits. Local/remote receiver, rollback, large rows and restored checkpoint continuation have real database coverage. |

The latest full backend regression run recorded **959 passed, 97 skipped**; skipped
database suites require explicit isolated test connections and are not counted
as database passes. A separate execution passed **44 real archive, application,
catalog and legacy-maintenance cases**. Additional focused workload-verifier and
recycling tests passed. These counts describe individual runs rather than a
single combined test total.

The final engine cases also prove that pilot stripes respect the actual copy
budget, wide min/max metadata contributes to space estimates, reverse sizing
does not invert an unrelated compression ratio, and unsupported independent
payload/expression/partial GIN indexes refuse conversion before source freezing.
Process-level entrypoint tests verify direct SIGTERM/SIGINT delivery and cleanup.

The small backup/restore fixture passed with **4,530 rows, two Columnar relations
and eight chunk groups**, including a payload larger than 2 MiB, a detached
stage, escaped identifiers and injected failure proving outer rollback. COPY
rows stream in bounded fragments rather than imposing a small payload-line cap.

The larger workload's final logical restore preserved **63,800 Columnar packet
rows, 687 stripes and 687 chunk groups**. Source and restored groups both ranged
from **8 to 93 rows**. Packet, reception and canonical-topic lookup digests
matched exactly. The custom-format snapshot was **27,089,285 bytes**, took
**6.88 seconds** to create, and restored in **27.84 seconds**; subsequent
`ANALYZE` took **8.62 seconds**. These are isolated workload observations, not a
full-history size forecast or a production recovery-time guarantee.

Physical grouping is part of recovery validation because preserving only
`columnar.options` can regroup many short writes into large stripes and slow
indexed reads. Version 2 manifests preserve row totals and observed maximum
stripe/chunk row counts. They do not promise identical stripe membership for
heterogeneous sources. Version 1 manifests remain readable with an explicit
geometry/latency warning; raw `pg_restore` does not provide these guarantees.

Native amd64 and arm64 image, backup/restore, lifecycle and production-reader
checks passed for implementation revision `dd39d33` in
[the native CI run](https://github.com/SimmerV/meshinfo/actions/runs/41272660081).
The workflow requires an explicitly installed Columnar extension so a missing
extension cannot silently reduce the reader tests to heap-only coverage.

The final load-test acceptance criteria were fixed before the final results:
exact response agreement; no reader or writer errors; complete fixture cleanup;
indexed page, node, topic and packet-copy p95 at most 250 ms, with either at most
three times heap latency or at most 100 ms additional latency; writer p95 at most
250 ms; and the first archive page after restarting PostgreSQL at most one
second. Unindexed broad payload/reception-text searches must stay within the
existing ten-second query budget and three times the equivalent heap latency.
The original experimental three-times/one-second gates remain in the reports,
including failures. Both ratios and absolute differences matter: a 2 ms to
8 ms lookup change and a two-second to eight-second search change need different
operational treatment. PostgreSQL restart measurements leave the shared host's
OS cache intact and are labeled accordingly.

Remaining acceptance work at this checkpoint is to compare the final workload's
forward/restored query timings under the chosen load, inspect the actual
development deployment and its ingestion behavior, and obtain final native CI
results. Compression is lossless and immediately queryable, but query speed
depends on filters, physical grouping, indexes, cache state and history size.
The operator must also retain enough space for complete replacement stages,
ongoing ingestion and recovery. A finite retention window bounds history age;
keeping all history still requires growing storage capacity over time.
