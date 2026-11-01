#!/usr/bin/env python3
"""Reverse only an identified isolated workload's cold leaves for a fresh trial.

This exercises the real bounded heap-restoration worker. Every packet, reception
and topic dictionary digest must match the completed workload report before and
afterward. Only *_columnar_test databases and report-listed monthly leaves are
accepted. No production connection or source export is opened. The resulting
heap workload is retained for benchmark_archive_workload --resume-report.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import re
import shutil
import sys
import time

import asyncpg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from scripts.benchmark_columnar_history import MQTT_COLUMNS,RECEPTION_COLUMNS,TOPIC_COLUMNS,rows_digest,normalize
from scripts.verify_archive_workload_restore import load_report,source_configuration
from storage.db import archive
from storage.db.storage_report import monthly_partition_bounds

MIB = 1024**2


def require(value,message):
    if not value:
        raise ValueError(message)


async def verify_core(conn,workload,*,all_tables=True):
    expected = workload["fixture"]
    cases = [("packets","public","mqtt_messages",MQTT_COLUMNS,expected["expanded_packet_digest"])]
    if all_tables:
        cases += [
            ("baseline_packets",workload["packet_schemas"]["heap"],"mqtt_messages",MQTT_COLUMNS,expected["expanded_packet_digest"]),
            ("receptions",workload["shared_schema"],"packet_receptions",RECEPTION_COLUMNS,expected["expanded_reception_digest"]),
            ("topics",workload["shared_schema"],"topics",TOPIC_COLUMNS,expected["original_digests"]["topics"]),
        ]
    results = {}
    for label,schema,table,columns,digest in cases:
        results[label] = await rows_digest(conn,schema,table,columns)
        require(results[label] == digest,"Original workload digest changed: "+label)
    return results


async def run(args):
    require(args.apply,"Explicit --apply is required for the isolated heap reversal")
    require(re.fullmatch(r"[A-Z][A-Z0-9_]*",args.dsn_env),"Invalid DSN environment name")
    require(args.database_filesystem.is_dir(),"Verified PostgreSQL filesystem path is required")
    require(1024 <= args.reserve_mib <= 4096 and 60 <= args.max_seconds <= 1800,"Invalid bounded reverse budget")
    workload = load_report(args.workload_report)
    dsn = os.environ.get(args.dsn_env,"")
    _parsed,pg = source_configuration(dsn,workload)
    leaves = [row for row in workload["partition_methods"] if row["amname"] == "columnar"]
    require(1 <= len(leaves) <= 6,"Only1..6 report-listed cold partitions can be recycled")
    require(all(re.fullmatch(r"mqtt_messages_[0-9]{4}_[0-9]{2}",row["relname"]) and
                monthly_partition_bounds(row["bound"]) for row in leaves),"Unexpected workload leaf identity")
    free = lambda: shutil.disk_usage(args.database_filesystem).free
    initial_free = free()
    require(initial_free >= (args.reserve_mib+512)*MIB,"Insufficient headroom for bounded workload reversal")
    config = {"storage":{"archive":{**workload["archive_policy"],"enabled":False,
               "database_filesystem":str(args.database_filesystem),"reserve_mib":args.reserve_mib}}}
    archive.archive_policy(config)
    result = {"all_checks_passed":False,"target_database":pg["database"],"live_storage_untouched":True,
              "isolated_workload_only":True,"free_bytes_before":initial_free,"partitions":[],
              "limits":{"reserve_bytes":args.reserve_mib*MIB,"max_seconds":args.max_seconds}}
    phase = "preflight"
    conn = await asyncpg.connect(dsn,timeout=5,command_timeout=120,server_settings={
        "application_name":"meshinfo-workload-reversal","timezone":"UTC","work_mem":"4MB",
        "maintenance_work_mem":"32MB","max_parallel_workers_per_gather":"0",
        "max_parallel_maintenance_workers":"0","lock_timeout":"500ms","jit":"off"})
    try:
        require(await conn.fetchval("SELECT current_database()") == pg["database"],"Connected workload identity differs")
        require(await conn.fetchval("SELECT pg_database_size(current_database())") <= 640*MIB,
                "Isolated database exceeds the physical workload budget")
        require(not await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM pg_stat_activity
            WHERE datname=current_database() AND pid<>pg_backend_pid() AND backend_type='client backend')"""),
            "Another workload client is active; wait for the isolated DB slot")
        await archive.ensure_archive_schema(conn)
        phase = "verify_original_workload"
        result["before_digests"] = await verify_core(conn,workload)
        active = await conn.fetchrow("SELECT source_name,target_method FROM public.storage_archive_jobs WHERE phase IN ('copying','verifying','ready')")
        require(not active or args.resume and active["target_method"] == "heap" and
                active["source_name"] in {row["relname"] for row in leaves},"Unexpected active job; explicit --resume required for an owned heap reversal")
        if active:
            leaves.sort(key=lambda row: row["relname"] != active["source_name"])
        started = time.monotonic()
        async with asyncio.timeout(args.max_seconds):
            for leaf in leaves:
                name = leaf["relname"]
                phase = "reverse_"+name
                current = await conn.fetchrow(archive._LEAVES_SQL+" AND c.relname=$1",name)
                require(current and current["bound"] == leaf["bound"] and current["method"] in ("heap","columnar"),
                        "Workload partition identity or bounds changed")
                entry = {"partition":name,"cycles":[],"method_before":current["method"]}
                result["partitions"].append(entry)
                step_start = time.monotonic()
                if current["method"] == "columnar":
                    if not active or active["source_name"] != name:
                        requested = await archive.start_restore_archive(conn,name,config,free_bytes=free)
                        entry["request"] = requested
                        require(requested["status"] == "copying","Isolated heap restoration preflight refused")
                    for _ in range(400):
                        outcome = await archive.run_archive_cycle(conn,config,free_bytes=free)
                        entry["cycles"].append(outcome)
                        print(f"Reverse {name}: {outcome['status']}",file=sys.stderr,flush=True)
                        require(outcome["status"] in ("copying","verifying","ready","complete"),
                                "Isolated heap restoration did not progress")
                        if outcome["status"] == "complete":
                            break
                    else:
                        raise ValueError("Isolated heap restoration exceeded its cycle budget")
                entry["seconds"] = round(time.monotonic()-step_start,3)
                require(await conn.fetchval("SELECT a.amname FROM pg_class c JOIN pg_am a ON a.oid=c.relam WHERE c.oid=to_regclass($1)",archive._q(name)) == "heap",
                        "Restored workload partition is not heap")
                require(await archive.archive_topics.attached_partition(conn,*monthly_partition_bounds(leaf["bound"])) is None,
                        "Restored heap still has stale topic lookup coverage")
                entry["packet_digest"] = (await verify_core(conn,workload,all_tables=False))["packets"]
                require(free() >= args.reserve_mib*MIB,"Free space fell below the reserved floor")
                active = None
        phase = "verify_recycled_workload"
        result["after_digests"] = await verify_core(conn,workload)
        require(not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM public.mqtt_archive_topics)"),"Unexpected topic lookup rows remain")
        result["seconds"] = round(time.monotonic()-started,3)
        result["all_checks_passed"] = True
        result["ready_for_fresh_forward_measurement"] = True
    except (Exception,asyncio.CancelledError) as exc:
        result["error"] = {"phase":phase,"category":type(exc).__name__,"sqlstate":getattr(exc,"sqlstate",None),
                           "note":"Only the isolated workload was touched; pending owned work can resume with --resume"}
    finally:
        await conn.close()
        result["free_bytes_after"] = free()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn-env",required=True)
    parser.add_argument("--workload-report",type=Path,required=True)
    parser.add_argument("--database-filesystem",type=Path,required=True)
    parser.add_argument("--output",type=Path,required=True)
    parser.add_argument("--reserve-mib",type=int,default=1024)
    parser.add_argument("--max-seconds",type=int,default=600)
    parser.add_argument("--apply",action="store_true")
    parser.add_argument("--resume",action="store_true")
    args = parser.parse_args()
    try:
        report = asyncio.run(run(args))
    except Exception as exc:
        print("Workload reversal refused: "+type(exc).__name__,file=sys.stderr)
        return 1
    args.output.write_text(json.dumps(report,indent=2,default=normalize)+"\n")
    print(json.dumps({"all_checks_passed":report["all_checks_passed"],"output":str(args.output)}))
    return 0 if report["all_checks_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
