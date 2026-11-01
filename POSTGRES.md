# PostgreSQL Storage

MeshInfo uses PostgreSQL as its storage backend. This document covers configuration, schema, and operational guidance.

## Configuration

Add the following to your `config.toml`:

```toml
[storage.postgres]
enabled = true
host = "postgres"
port = 5432
database = "meshinfo"
username = "postgres"
password = "your_password"
min_pool_size = 1
max_pool_size = 5
```

### Configuration Options

- **`postgres.enabled`**: Must be `true`
- **`postgres.host`**: Database server hostname (use `"localhost"` if not using Docker)
- **`postgres.port`**: Database server port (default: 5432)
- **`postgres.database`**: Database name
- **`postgres.username`**: Database user
- **`postgres.password`**: Database password
- **`postgres.min_pool_size`**: Minimum connection pool size (default: 1)
- **`postgres.max_pool_size`**: Maximum connection pool size (default: 5)

## Database Schema

The schema is automatically created on first run. Key tables:

- **`nodes`**: Core node information (ID, name, hardware, status)
- **`node_positions`**: Historical position data
- **`node_neighborinfo`**: Neighbor relationships (JSONB)
- **`node_telemetry_current`**: Most recent telemetry per node
- **`telemetry`**: Complete telemetry history (JSONB payloads)
- **`chat_channels`**: Chat channel metadata
- **`chat_messages`**: All chat messages
- **`traceroutes`**: Complete traceroute history (JSONB payloads)

## Architecture

### Write Flow

1. MQTT message received
2. Handler reads existing node via `pg_storage.get_node_cached` (LRU + DB)
3. Handler merges incoming fields into the node
4. `data.update_node` writes the merged node to PostgreSQL and refreshes the cache

### Read Flow

1. Application starts
2. PostgreSQL connection established
3. API queries PostgreSQL directly for each request (node lookups hit the LRU cache first)

### Error Handling

- PostgreSQL write failures are logged but **never block** application execution
- Connection pool handles transient network issues
- Failed writes are logged for manual investigation

## Performance Considerations

### Memory tuning

The `postgres` service in both compose files sets a few core memory
parameters via its `command:`. The defaults are deliberately conservative so
the stack runs on a small host, but they are already well above the stock
`postgres` image defaults (`shared_buffers` 128 MB, `work_mem` 4 MB).

Size them to your host: copy `.env.sample` to `.env` next to the compose file
(Docker Compose reads it automatically) and set any of:

| Variable                   | Default | What it does                                  |
|----------------------------|---------|-----------------------------------------------|
| `PG_SHARED_BUFFERS`        | `256MB` | Dedicated PG page cache (real RAM allocation). |
| `PG_WORK_MEM`              | `8MB`   | Per-sort/hash memory; multiplied by concurrency. |
| `PG_MAINTENANCE_WORK_MEM`  | `64MB`  | VACUUM / CREATE INDEX working memory.          |
| `PG_EFFECTIVE_CACHE_SIZE`  | `1GB`   | Planner hint for OS+PG cache (not an allocation). |

Suggested starting points by **total host RAM** (assuming Postgres shares the
box with the rest of the MeshInfo stack — give it less than a dedicated DB
host would get):

| Host RAM | `PG_SHARED_BUFFERS` | `PG_WORK_MEM` | `PG_MAINTENANCE_WORK_MEM` | `PG_EFFECTIVE_CACHE_SIZE` |
|----------|---------------------|---------------|---------------------------|---------------------------|
| 4 GB     | `512MB`             | `16MB`        | `128MB`                   | `2GB`                     |
| 8 GB     | `1GB`               | `32MB`        | `256MB`                   | `4GB`                     |
| 16 GB    | `2GB`               | `64MB`        | `512MB`                   | `10GB`                    |

Example `.env`:

```
PG_SHARED_BUFFERS=512MB
PG_WORK_MEM=16MB
PG_MAINTENANCE_WORK_MEM=128MB
PG_EFFECTIVE_CACHE_SIZE=2GB
```

Recreate the container to apply: `docker compose up -d postgres`.

### Real-time Writes

All writes happen in real-time as data arrives from MQTT, ensuring minimal data loss on crash.

### Connection Pooling

asyncpg connection pooling handles concurrent writes efficiently. Tune `min_pool_size` and `max_pool_size` based on your load.

### Indexing

Key indexes are maintained on node IDs, timestamps, and foreign key relationships for fast queries even with large datasets.

## Monitoring

PostgreSQL operations are logged at INFO and ERROR levels:

```
INFO: PostgreSQL mode: Data will be queried directly from database
ERROR: Failed to write node abc123 to Postgres: connection timeout
```

Monitor these logs to ensure healthy operation.

## Troubleshooting

### Connection Failures

If PostgreSQL connection fails on startup, MeshInfo will log the error and exit. Check:

1. PostgreSQL container/service is running
2. Connection settings in `config.toml` are correct (`host`, `port`, `username`, `password`)
3. Network connectivity between MeshInfo and the database

### Data Inconsistencies

To verify record counts:

```sql
SELECT COUNT(*) FROM nodes;
SELECT COUNT(*) FROM chat_messages;
SELECT COUNT(*) FROM telemetry;
SELECT COUNT(*) FROM traceroutes;
```

### Migrating from JSON Storage

Very old deployments used JSON file storage. The one-time migration script
(`scripts/migrate_json_to_postgres.py`) was removed after the JSON store was
retired — it required the legacy `[paths]` config section that no longer
exists. If you still need it, run the copy from a pre-removal release
(check out a tag from before July 2026) against your legacy deployment,
then upgrade.

## Upgrading PostgreSQL major versions

PostgreSQL major versions (16 → 18, etc.) use **incompatible on-disk storage
formats**. A newer `postgres` container will refuse to start against a data
volume initialised by an older one, failing with:

```
FATAL: database files are incompatible with server
DETAIL: The data directory was initialized by PostgreSQL version 16,
        which is not compatible with this version 18.x.
```

The legacy migration below supports **heap-only installations**. It refuses a
Columnar extension, compressed tables, frozen archive jobs, or an old server it
cannot inspect, before stopping services or removing a volume. Start the original
compatible database image first if it is unavailable. With Columnar history, keep
the current volume and extension-capable image: use the manifest-aware restore
procedure below for same-major recovery. A PostgreSQL major upgrade requires a
separately validated extension/image and migration procedure; the current restore
tool intentionally requires matching major versions. Do not bypass this guard
or use a plain SQL dump as a substitute for captured Columnar options.

When a MeshInfo release bumps the `postgres` image major version, heap-only
installations can run the included migration script once. It dumps the database with a temporary
container of the *old* version, recreates the volume, and restores into the
*new* version — using only official `postgres` images.

```sh
git pull
docker compose pull               # fetch the new postgres image
bash scripts/migrate-postgres.sh  # one-time: dump old → restore new
docker compose up -d
```

For a development stack, point the script at the dev compose file:

```sh
COMPOSE_FILE=docker-compose-dev.yml bash scripts/migrate-postgres.sh
```

Notes:

- The script writes a compressed SQL dump to `backups/` and **verifies it
  before** recreating the volume — that file is your recovery artifact.
- It is a no-op on fresh installs and when the volume is already current, so
  it is safe to run unconditionally as part of an update.
- On Windows, run it from **Git Bash** (bundled with Git) — `bash
  scripts/migrate-postgres.sh`. The script is MSYS-path-safe, so the single
  bash version covers every platform.
- Set `KEEP_VOLUME_BACKUP=1` to also snapshot the old data volume to
  `<volume>_oldpg_backup` for an instant rollback (uses extra disk; delete it
  once the upgrade is verified).
- To roll back from the SQL dump: set the `postgres` image (and volume mount)
  in your compose file back to the old version, remove the data volume so it
  re-initialises empty, `docker compose up -d postgres`, then restore —
  `gzip -dc backups/<dump>.sql.gz | docker compose exec -T postgres psql -U postgres -d meshinfo`.

## Partitioning the mqtt_messages archive

`mqtt_messages` is the raw packet firehose and dominates the database — it
typically holds 98%+ of all rows. Fresh installs since this change get a
**partitioned table** (RANGE partitioned by month on `created_at`), so the
table stays operationally manageable as it grows: each date-range query
prunes to the relevant month(s) instead of scanning everything, and
maintenance runs at partition scale, not table scale. The app
(`ensure_mqtt_partitions`) keeps next month's partition created ahead of
the rollover.

Databases created before partitioning landed need a **one-time conversion**.
The legacy converter refuses compressed or frozen archive storage before stopping
the app and rechecks under the shared maintenance lock before changing tables.
Already partitioned heap installations remain a no-op:


```sh
bash scripts/migrate-mqtt-partitioning.sh
# dev stack:  COMPOSE_FILE=docker-compose-dev.yml bash scripts/migrate-mqtt-partitioning.sh
```

What it does, inside a **single atomic transaction**:

1. Renames the existing table to `mqtt_messages_old`.
2. Creates a new month-partitioned `mqtt_messages` (with the `payload`
   column `lz4`-compressed).
3. Creates one partition per month spanned by the data, copies every row,
   and verifies the row count matches before committing.
4. Rebuilds indexes, re-homes the `id` sequence, reinstalls the trigger.

Any failure rolls the whole thing back — `mqtt_messages` is untouched.
The script stops `meshinfo` for the duration and restarts it on success.

Notes:

- **Requires lz4:** the partitioned `mqtt_messages` `payload` column uses
  `COMPRESSION lz4`, so Postgres must be built with lz4 support (PG 14+ — the
  official `postgres:18` image this stack ships with includes it). A self-hosted
  Postgres compiled without lz4 will reject both the fresh-install schema and the
  migration with `compression method lz4 not supported`.
- **Disk:** the migration needs ~3× the current `mqtt_messages` size free
  on the data volume transiently (new copy + WAL + headroom). The pre-flight
  check aborts with a clear message if there's not enough. Expand the data
  volume first if you're short.
- **Rollback safety:** the original data is kept as `mqtt_messages_old`
  until you drop it. Disk is not reclaimed until then. Once you've
  confirmed the app + Logs page look right, run:
  ```sh
  docker compose exec postgres psql -U postgres -d meshinfo \
    -c 'DROP TABLE mqtt_messages_old;'
  ```
- **Idempotent**: re-running the script when the table is already
  partitioned (or doesn't exist yet) is a no-op.
- **On Windows**, run from Git Bash, like `migrate-postgres.sh`.
- Partitioning does not shrink disk — it makes a huge table manageable.
  Complete-history retention still implies the data volume must be
  allowed to grow.

## Uplink dedup and packet_receptions (#526)

Every gateway that hears a mesh packet uplinks its own copy — measured ~10-11
rows per logical packet, ~91% of archive bytes, with the decoded payload
byte-identical across copies. Since the dedup change, ingest stores **one
canonical `mqtt_messages` row per packet** (the first-heard copy, verbatim) and
records **every uplink copy — including the first — as a `packet_receptions`
row** (~70 B vs ~700 B per copy):

| column | meaning |
| --- | --- |
| `mqtt_row_id` | the canonical `mqtt_messages.id` this copy belongs to |
| `gateway` | uplinking gateway (envelope `sender`, else topic `!suffix`) |
| `topic_id` | interned topic string (`mqtt_topics`) — region paths differ per gateway |
| `rx_rssi` `rx_snr` `rx_time` `hop_limit` `hops_away` `relay_node` `transport` | the per-copy RF envelope |
| `extras` | sparse JSONB patch vs. the reconstruction template (usually NULL) |

Optional `packets` retention treats a canonical packet and its receptions as
one group. The canonical row's `created_at` (first receipt on this server)
determines expiration, not each copy's arrival time or a device timestamp. When
the packet expires, all of its reception copies expire atomically, including
copies received after the cutoff. A later copy does not extend the packet's
lifetime. Other history families use each record's own `created_at`; see the
[retention policy](STORAGE.md#policy).

**Losslessness contract:** each original copy is exactly reconstructible as
`reception_template(canonical, gateway, topic, fields)` + `extras` patch — see
`storage/db/uplink_dedup.py` (`reconstruct_copy`). Copies whose patch cannot be
stored (e.g. `\u0000` escapes, which JSONB rejects) fall back to a verbatim
`mqtt_messages` row, so no path drops data. Packets without a mesh id
(mapreports) are stored verbatim as before.

Dedup is keyed `(from_node_id, packet_id)` within `storage.dedup_window_seconds`
(default 900 s; mesh packet ids recycle, so the window must stay bounded). The
in-memory cache survives restarts via a window-bounded DB lookup on
`idx_mqtt_messages_packet`. Config: `storage.dedup_uplinks` (default `true`).

**Deploying onto a large existing archive:** first startup builds
`idx_mqtt_messages_packet` (partial, `WHERE packet_id IS NOT NULL`) which scans
every partition once — minutes on a multi-10-GB archive, during which ingest
hasn't started yet. To avoid the pause, pre-build it while the old version is
still running (`CREATE INDEX CONCURRENTLY` can't target a partitioned parent,
so use the ON ONLY dance): `CREATE INDEX idx_mqtt_messages_packet ON ONLY
mqtt_messages (from_node_id, packet_id, created_at DESC) WHERE packet_id IS NOT
NULL;`, then per partition `CREATE INDEX CONCURRENTLY <part>_packet_idx ON
<part> (...same...)` + `ALTER INDEX idx_mqtt_messages_packet ATTACH PARTITION
<part>_packet_idx;`. The app's `CREATE INDEX IF NOT EXISTS` then no-ops.

Side effects to expect: `total_mqtt_messages`, the 24h preset split, and
per-node packet counts drop ~10× (they now count logical packets, not copies);
the Logs page shows one row per packet with a "heard N×" chip; the SSE live
stream still emits one `packet` event per gateway copy (all carrying the
canonical `mqtt_row_id`), so the live map's fan-in arcs are unaffected.

### Compacting pre-dedup history

Months ingested with dedup enabled are born compact — compaction only applies
to legacy months from before the dedup deploy. The rewrite is the same lossless
transform as ingest: same window semantics, canonical rows keep their original
ids (permalinks to first-heard copies survive), payload text preserved
byte-for-byte. Every original copy remains reconstructible afterwards —
`GET /v1/packets/{id}?copies=1` returns each gateway's exact original uplink
message rebuilt from its reception row.

This is a **one-time, manual migration** — run it once after deploying dedup,
and it is done forever. It coordinates with the shared storage maintenance lock
and refuses any compressed or frozen archive history before staging or modifying
receptions. Perform legacy dedup compaction before Columnar conversion, or use
the guarded archive controls to cancel unfinished work and restore completed
months to heap first. `--dry-run` performs no table or data writes:


```sh
docker compose exec meshinfo \
  python scripts/compact_mqtt_partitions.py --all --dry-run   # discover + counts only
docker compose exec meshinfo \
  python scripts/compact_mqtt_partitions.py --all [--drop-original]
```

`--all` discovers every eligible pre-dedup month and sweeps them oldest-first;
`--month YYYY_MM` targets one at a time. Originals are kept as
`mqtt_messages_YYYY_MM_precompact` for review — disk is reclaimed when you
`DROP` them, or pass `--drop-original` to drop right after the verified swap.

Expect ~80-85% reduction per month. The script refuses current, mixed, or
already-deduped months. Note: the month a dedup deploy lands in is mixed
forever and can't be compacted — deploy early in a month to minimize it.

### Backups

History defaults to forever; optional retention controls live history. Backup
retention is independent and can preserve records already expired from the live
database. The app schedules snapshots from config.toml after restart:

```toml
[backups]
schedule = "daily"        # off | daily | weekly | monthly
mode = "local"            # local | ssh
keep_days = 4             # local verified snapshot retention
# remote_keep_days = 4    # defaults to keep_days
dir = "backups"           # relative to the repo root, or absolute
remote_target = ""        # mode=ssh: user@host:/dedicated/backup/directory
```

Dumps use compressed `pg_dump -Fc`, full archive decoding and SHA-256 checks,
atomic completion markers, and destination ownership. Expired verified snapshots
are pruned only after a new one succeeds; the newest is always retained. Legacy
dumps without manifests are preserved for manual review. SSH mode streams
directly off-box without a full local staging file. Durable status controls the
schedule across restarts, and all maintenance shares one database advisory lock.

`scripts/backup_db.sh` reads the same section for manual or host-cron runs using
host SSH keys and Docker PostgreSQL clients. Environment variables (`KEEP_DAYS`,
`BACKUP_DIR`, `REMOTE_TARGET`, `CONTAINER`, `DB_USER`, `DB_NAME`) override the
config for one-off runs. Legacy nonempty `remote_target` without an explicit
mode selects direct SSH streaming; it no longer creates an additional local
archive. Explicit `mode=local` plus `remote_target` is rejected by the host tool
to avoid silently omitting the remote copy.

See [STORAGE.md](STORAGE.md) for retention, measured index packing, SSH setup,
restore drills, independent-clone identity, and operational limits.

## Security

- Use strong passwords for PostgreSQL
- Consider SSL/TLS for database connections in production
- Restrict database access via network policies
- Set up regular PostgreSQL backups (e.g., `pg_dump`)
- Keep PostgreSQL updated with security patches

## Support

For issues or questions:
- Open an issue on GitHub
- Join #meshinfo on SacValleyMesh Discord
- Review application logs for detailed error messages

## Optional Columnar image, backup metadata and restore

`Dockerfile.postgres-columnar` supplies PostgreSQL **18.6** with the standalone
`citus_columnar` extension from Citus **14.1.0** (SQL extension version
`14.1-1`). It does not configure a distributed Citus cluster. The normal Compose
file keeps its existing PostgreSQL image; deployments opt into the additional
image with `docker-compose.columnar.yml`:

```bash
docker compose -f docker-compose.yml -f docker-compose.columnar.yml build postgres
docker compose -f docker-compose.yml -f docker-compose.columnar.yml up -d postgres meshinfo
```

Include any site-specific Compose files explicitly, before the Columnar overlay,
when using this command. The overlay preserves the PostgreSQL data volume and
memory tuning variables, and sets `shared_preload_libraries=citus_columnar` for
both new and existing volumes. Archive policy remains a separate opt-in; merely
installing the extension does not convert or delete history. If other extensions
already need preloading, include them in that setting as well.

The backend also receives the existing database volume **read-only** at
`/var/lib/meshinfo-postgres-space`. Set `storage.archive.database_filesystem` to
that path so the archive scheduler probes the database's actual filesystem.
This mount needs no Docker socket and grants no write access to database files.
A host directory unrelated to PostgreSQL does not establish migration headroom;
custom tablespaces need their own explicit support and are refused by the archive
implementation.

The image pins the official multi-architecture PostgreSQL manifest
`sha256:edb0c08dc33befd0262ab3b5cf1c4f3f8cfd8b09e88c05b45adee29d6761ddb6`.
Its architecture-specific Citus inputs are:

| Architecture | Verified upstream input | SHA-256 |
| --- | --- | --- |
| amd64 | [Official Debian trixie PG18 package, 14.1.0.citus-1](https://packagecloud.io/citusdata/community/debian/pool/trixie/main/p/citus/postgresql-18-citus-14.1_14.1.0.citus-1_amd64.deb) | `1e78be38f6c145bac49d622d71def8f1b9a3826d98dffe946e9aed3e0cac1b09` |
| arm64 | [Official Citus v14.1.0 source](https://codeload.github.com/citusdata/citus/tar.gz/refs/tags/v14.1.0), compiled in a separate build stage | `e174ed00efba74aaf5a9da87f770f6a7e11274066cce8449d804bc6b17df6ff8` |

The official trixie repository does not provide a PG18/14.1 arm64 binary package.
The arm64 image therefore builds the same source release using its official
[`columnar` and `install-columnar` targets](https://github.com/citusdata/citus/blob/v14.1.0/Makefile),
which install the standalone AM and generated extension SQL without compiling
the distributed extension. It uses no native-CPU or AVX2 compiler flags and
keeps its compiler/development packages out of the runtime image. Other architectures fail the image build explicitly. The native
`Columnar native restore` CI matrix builds both amd64 and arm64 and exercises
real backup/restore behavior. **An arm64 deployment requires a successful native
arm64 CI run for the exact revision being deployed**; an amd64 test or emulated
build is not evidence of native arm64 runtime compatibility. No arm64 validation
is implied merely by this Dockerfile existing.

Treat PostgreSQL/Citus upgrades as an explicit tested change: update the pinned
input digests, run both native architecture jobs, restore a representative backup
and verify options and query behavior before adopting the new image. Retain an
image capable of restoring each retained backup's exact extension version.
Changing back to a PostgreSQL image without the extension cannot read existing
Columnar tables.

### What a normal backup now preserves

Local and SSH backups still produce ordinary compressed `pg_dump -Fc` archives
and the existing `<archive>.manifest.json`. The manifest now also carries a
bounded, checksummed `restore_metadata` object with the PostgreSQL major version,
exact installed Columnar extension version, and the effective compression,
compression level, stripe row limit and chunk-group row limit of **every**
Columnar table. Version 2 metadata also records the snapshot's exact logical row
count and observed maximum rows per physical stripe and chunk group. Effective
options alone are insufficient: a table configured for 1,000-row chunks can have
192-row physical stripes because its original writes committed in small batches.
Combining those rows into large stripes on restore can substantially slow point
queries even when every byte and option is preserved. The metadata includes
detached or staged archive tables in other schemas.
The backup's read-only metadata session exports a snapshot that `pg_dump` imports,
so options, physical bounds and data describe the same database snapshot while the existing maintenance lock
continues to protect archive/retention operations. There are no writes to Citus
internal catalogs. The backup role must be able to view every Columnar table's
options (the public options view restricts these to table owners and roles with
ownership privileges). Invisible options cause refusal rather than substitution
of default settings.

The archive checksum, destination ownership rules, private permissions, complete
archive decoding, timeout cleanup and local/SSH retention rules remain in force.
The receiver verifies the metadata checksum before publishing the archive and
manifest. Keep the manifest with the archive when moving or downloading backups.
A standalone `pg_restore` does not apply this metadata and may silently recreate
Columnar tables with default geometry; use the restore tool below. New snapshots
request UTF-8 dump encoding and quote all identifiers, including identifiers with
Unicode, embedded quotes or newlines. Payload COPY bytes are streamed opaquely;
no JSON parsing or reserialization occurs during restoration. Geometry-aware
COPY identifiers must be UTF-8, which the new dump command guarantees even for
another convertible source database encoding. Malformed/non-UTF-8 identifiers
in an externally constructed version 2 archive are refused rather than guessed.
Legacy heap data sections are forwarded unchanged. Archive
readability verification remains distinct from a successful restore drill.

### Restore to an empty database

Create a new empty destination database from `template0` on a server with the
required extension binaries and `shared_preload_libraries` setting. Keep the
application disconnected from that database until restoration succeeds. The
restore tool requires an explicit database name; it never drops an existing
database or restores over tables. Linux, PostgreSQL 18 client tools and Python 3.11+
are required; the MeshInfo backend image includes all three. The restore keeps
the verified archive open through `/proc/self/fd` to prevent path substitution. For example, from the
backend container where `/app/backups` is mounted:

```bash
python scripts/restore_database.py backups/meshinfo-pg-<timestamp>-<id>.dump \
  --config config.toml --host replacement-postgres --database meshinfo_restored
python scripts/restore_database.py backups/meshinfo-pg-<timestamp>-<id>.dump \
  --config config.toml --host replacement-postgres --database meshinfo_restored --apply
```

The first invocation validates without loading data. The second verifies the
archive checksum and full decoding, checks the destination, and performs one
transaction in this order:

1. Restore extension/schema definitions and verify the complete Columnar table inventory.
2. Apply and verify every captured Columnar option through supported `ALTER TABLE` APIs.
3. Load the archived rows, then restore indexes, constraints and triggers. For
   version 2 Columnar metadata, split text COPY into bounded batches wrapped in
   savepoints. Releasing a savepoint flushes a Citus stripe without committing
   the outer restore transaction; splitting COPY statements alone does not.
4. Recheck inventory/options, exact Columnar row totals and the recorded physical
   upper bounds, then commit. Any SQL, producer, parser or verification failure
   rolls back everything, including already released savepoints.

The exact extension version must be available; the destination PostgreSQL major
must match backups that carry restore metadata. Missing, mismatched or incomplete
Columnar metadata causes refusal. Existing heap-only custom dumps, including
legacy dumps without manifests, remain supported; they still receive an actual
catalog check rejecting undocumented Columnar tables before data loading.
Older version 1 Columnar manifests also remain data/options compatible, but lack
physical bounds: the restore result explicitly reports
`physical_geometry_preserved=false` and warns that query latency may change.
Make a new version 2 snapshot when physical grouping needs to be retained. A true
flag means the observed maximum stripe/chunk sizes and row totals were preserved;
it does not promise identical file bytes, stripe membership or query timings.
Passwords are passed through the private subprocess environment, not command
arguments or reported errors. Restore metadata includes names/options, never
payload copies or credentials.

For resumable archive jobs, ordinary dump/restore also preserves staging rows,
application job state and source-freeze triggers. The archive executor must
re-resolve object names and validate its durable markers after restore: numeric
PostgreSQL OIDs are not stable across logical restoration. The integration gate
`scripts/verify_columnar_backup_restore.py` uses disposable databases to verify
local/remote-receiver backup, exact restored data, settings **before COPY** via
physical chunk geometry, staged rows/job state, restored guards, and full
rollback after incomplete metadata or a failure after data streaming.
