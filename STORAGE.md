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

Expiration is rounded down to a UTC month boundary, so at least the requested
history is preserved. Depending on the current date, almost one extra month may
remain. A time policy is not a byte quota: higher ingestion volume still needs
more space. Shortening retention permanently removes expired history; later
increasing retention cannot recover it.

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
- Never expire a packet while leaving retained receptions that require it.
- No rate-limit tightening, lossy aggregation, or silent removal of gateway
  copies is a storage optimization.
- Backups can contain history that has expired from the live database; backup
  retention is an independent policy.

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
