#!/usr/bin/env python3
"""Measure compression and explicitly pack closed packet-history indexes.

Examples:
  python scripts/compress_storage.py plan
  python scripts/compress_storage.py benchmark --database meshinfo_compression_test
  python scripts/compress_storage.py benchmark-index --database meshinfo_compression_test
  python scripts/compress_storage.py pack-index mqtt_messages_2026_07_created_at_idx \
      --after-months 0 --apply --database-filesystem /actual/postgres/filesystem

Benchmarks require a separate initialized test database; working tables are
transient. --database-filesystem is an operator assertion: the directory must
reside on the same filesystem as the PostgreSQL index, including its tablespace.
The application host's free space is not inferred to describe PostgreSQL.
A packing transaction may briefly block queries using that index (default 5s
maximum); cancellation rolls the entire rebuild back without staging indexes.
"""

import argparse
import asyncio
import json
from pathlib import Path
import shutil
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import asyncpg

from scripts.manage_storage import load_config
from storage.db.compression import (
    CompressionRefused, MIB, benchmark_compression, benchmark_index_geometry, collect_benchmark_sample,
)
from storage.db.index_maintenance import IndexPackingLimits, pack_index, packing_policy, plan_index_packing
from storage.db.maintenance_status import save_status


def parser():
    result = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    result.add_argument("command", choices=("plan", "benchmark", "benchmark-index", "pack-index"))
    result.add_argument("index", nargs="?")
    result.add_argument("--config", type=Path, default=Path("config.toml"))
    result.add_argument("--host", help="Explicit database hostname override")
    result.add_argument("--database", help="Target database; benchmark requires a separate initialized test DB")
    result.add_argument("--targets", type=int, nargs="+", default=[2040, 1024, 512], help="Benchmark targets")
    result.add_argument("--apply", action="store_true", help="Execute index packing; without this flag, show a read-only plan")
    result.add_argument("--database-filesystem", type=Path, help="Actual PostgreSQL data/tablespace filesystem for headroom checks")
    result.add_argument("--after-months", type=int, help="Override configured minimum closed age")
    result.add_argument("--fillfactor", type=int, help="Override configured index fillfactor")
    result.add_argument("--max-index-mib", type=int, help="Override configured index size limit")
    result.add_argument("--reserve-mib", type=int, help="Override configured reserved free space")
    result.add_argument("--max-seconds", type=float, help="Override configured operation time limit")
    result.add_argument("--sample-rows", type=int, default=2000)
    result.add_argument("--sample-mib", type=int, default=16)
    result.add_argument("--seed", type=int, default=1729)
    return result


async def connect(pg, *, host=None, database=None, readonly=False):
    settings = {"application_name": "meshinfo-compression", "timezone": "UTC"}
    if readonly:
        settings["default_transaction_read_only"] = "on"
    return await asyncpg.connect(host=host or pg["host"], port=pg.get("port", 5432),
                                 user=pg["username"], password=pg.get("password", ""),
                                 database=database or pg["database"], timeout=5,
                                 command_timeout=35, server_settings=settings)


def resolved_packing_settings(args, config):
    """Operator flags override configuration only when explicitly supplied."""
    _, defaults, configured_filesystem = packing_policy(config)
    limits = IndexPackingLimits(
        after_months=args.after_months if args.after_months is not None else defaults.after_months,
        fillfactor=args.fillfactor if args.fillfactor is not None else defaults.fillfactor,
        max_index_bytes=args.max_index_mib * MIB if args.max_index_mib is not None else defaults.max_index_bytes,
        reserve_bytes=args.reserve_mib * MIB if args.reserve_mib is not None else defaults.reserve_bytes,
        max_seconds=args.max_seconds if args.max_seconds is not None else defaults.max_seconds,
    )
    filesystem = args.database_filesystem
    if filesystem is None and configured_filesystem:
        filesystem = Path(configured_filesystem)
    return limits, filesystem


def exit_code(result):
    return int(result.get("status") in {"busy", "blocked", "error", "partial"}
               or result.get("status_persisted") is False)


async def run(args):
    if args.apply and args.command != "pack-index":
        raise ValueError("--apply is only valid with pack-index")
    config = load_config(args.config)
    pg = config["storage"]["postgres"]
    if args.command in ("benchmark", "benchmark-index"):
        if not args.database or args.database == pg["database"]:
            raise ValueError("benchmark requires --database pointing to a separate initialized test database")
        source = await connect(pg, host=args.host, readonly=True)
        try:
            rows, sample = await collect_benchmark_sample(source, max_rows=args.sample_rows,
                                                         max_bytes=args.sample_mib * MIB, seed=args.seed)
        finally:
            await source.close()
        destination = await connect(pg, host=args.host, database=args.database)
        try:
            result = (await benchmark_compression(destination, rows, targets=args.targets)
                      if args.command == "benchmark" else await benchmark_index_geometry(destination, rows))
        finally:
            await destination.close()
        return {"sample": sample, **result}
    apply = args.command == "pack-index" and args.apply
    if args.command == "pack-index" and not args.index:
        raise ValueError("pack-index requires an explicit index name")
    limits, filesystem = resolved_packing_settings(args, config)
    if apply and (filesystem is None or not filesystem.is_dir()):
        raise ValueError("apply requires --database-filesystem pointing to the actual PostgreSQL index filesystem")
    conn = await connect(pg, host=args.host, database=args.database, readonly=not apply)
    try:
        if apply:
            try:
                result = await pack_index(conn, args.index, limits=limits,
                                          free_bytes=lambda: shutil.disk_usage(filesystem).free)
            except CompressionRefused as exc:
                result = {"status": "blocked", "index": args.index, "reason": str(exc)}
            except (OSError, asyncpg.PostgresError, TimeoutError) as exc:
                result = {"status": "error", "index": args.index, "reason": type(exc).__name__}
            try:
                await save_status(conn, "compression", result)
            except (OSError, asyncpg.PostgresError, TimeoutError) as exc:
                # Packing may already have committed. Preserve its actual
                # outcome rather than suggesting an audit failure undid it.
                result = {**result, "operation_status": result["status"], "status": "error",
                          "status_persisted": False, "status_error": type(exc).__name__}
            return result
        plan = await plan_index_packing(conn, limits=limits)
        if args.index:
            plan["indexes"] = [row for row in plan["indexes"] if row["index"] == args.index]
        if filesystem:
            plan["observed_free_bytes"] = shutil.disk_usage(filesystem).free
        return plan
    finally:
        await conn.close()


def main():
    args = parser().parse_args()
    try:
        result = asyncio.run(run(args))
    except (ValueError, OSError, asyncpg.PostgresError, TimeoutError) as exc:
        print(f"Compression operation refused or failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return exit_code(result)


if __name__ == "__main__":
    raise SystemExit(main())
