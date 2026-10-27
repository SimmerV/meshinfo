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
enable setting. During phase 1 all finite policies are previews only.

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
