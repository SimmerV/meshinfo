#!/usr/bin/env python3
"""Operator storage report. Read-only by default; never connects on import."""

import argparse
import asyncio
import json
import shutil
import sys
import tomllib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import asyncpg

from config import DEFAULT_CONFIG, _deep_merge, validate
from storage.db.storage_report import build_storage_report


def load_config(path: Path) -> dict:
    with path.open("rb") as source:
        config = _deep_merge(DEFAULT_CONFIG, tomllib.load(source))
    validate(config)
    return config


def local_capacity(config: dict, config_path: Path) -> dict:
    """Only identify the backup filesystem; a remote DB has a different disk."""
    directory = Path(config.get("backups", {}).get("dir", "backups"))
    if not directory.is_absolute():
        directory = config_path.resolve().parent / directory
    probe = directory
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    try:
        usage = shutil.disk_usage(probe)
    except OSError:
        return {"backup_filesystem": None, "database_filesystem": None}
    return {
        "backup_filesystem": {
            "path": str(directory), "total_bytes": usage.total,
            "free_bytes": usage.free,
            "warning": "low_space" if usage.free < max(1024 ** 3, usage.total // 10) else None,
        },
        "database_filesystem": None,
        "database_filesystem_note": "Database size is known; database-host free space is not inferred from the app host.",
    }


async def report(args: argparse.Namespace) -> dict:
    config = load_config(args.config)
    pg = config["storage"]["postgres"]
    connection = await asyncpg.connect(
        host=pg["host"], port=pg.get("port", 5432),
        user=pg["username"], password=pg.get("password", ""),
        database=args.database or pg["database"], timeout=5, command_timeout=5,
    )
    try:
        result = await build_storage_report(connection, config, enforcement_available=False)
    finally:
        await connection.close()
    result["capacity"] = local_capacity(config, args.config)
    return result


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("command", nargs="?", choices=["report"], default="report")
    result.add_argument("--config", type=Path, default=Path("config.toml"))
    result.add_argument("--database", help="Database override, for example an isolated test database")
    result.add_argument("--json", action="store_true", help="Machine-readable JSON (the default report format)")
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        result = asyncio.run(report(args))
    except (OSError, ValueError, asyncpg.PostgresError, TimeoutError) as exc:
        print(f"Storage report failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
