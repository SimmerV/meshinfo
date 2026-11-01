"""Production node pages preserve keysets while reusing bounded read plans."""

import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
from urllib.parse import unquote, urlsplit
from unittest.mock import patch
import uuid

import asyncpg
import pytest

from storage.db.history_queries import HistoryReadError
from storage.db.postgres import PostgresStorage, _encode_cursor


DSN=os.environ.get("MESHINFO_HISTORY_TEST_DSN","")
pytestmark=pytest.mark.skipif(not DSN,reason="MESHINFO_HISTORY_TEST_DSN not set")
JAN=datetime(2020,1,1,tzinfo=timezone.utc)
HOT=datetime(2020,2,1,tzinfo=timezone.utc)
COMMON="c0decafe"
RARE="deadbeef"


def test_node_generic_pages_keep_cardinality_windows_ties_and_cancellation():
    asyncio.run(exercise())


async def exercise():
    database=unquote(urlsplit(DSN).path.lstrip("/"))
    if not database.endswith("_columnar_test"):
        raise ValueError("Node integration tests require an isolated *_columnar_test database")
    writer=await asyncpg.connect(DSN,command_timeout=10,server_settings={"timezone":"UTC",
        "work_mem":"2MB","max_parallel_workers_per_gather":"0","lock_timeout":"500ms"})
    schema="history_nodes_test_"+uuid.uuid4().hex[:12]
    storage=PostgresStorage({"server":{"timezone":"UTC"},"storage":{"postgres":{"enabled":True}}})
    created=False
    try:
        assert await writer.fetchval("SELECT current_database()") == database
        if not await writer.fetchval("SELECT EXISTS(SELECT 1 FROM pg_am WHERE amname='columnar')"):
            pytest.skip("Columnar extension is not installed in isolated database")
        await writer.execute(f'CREATE SCHEMA "{schema}"')
        created=True
        await writer.execute(f'SET search_path="{schema}",public')
        await writer.execute("""CREATE TABLE mqtt_messages(id bigint NOT NULL,topic text,payload text,
            qos integer,retain boolean,timestamp bigint,created_at timestamptz NOT NULL,
            from_node_id varchar(8),to_node_id varchar(8),packet_id bigint,PRIMARY KEY(id,created_at))
            PARTITION BY RANGE(created_at);
            CREATE TABLE packets_cold PARTITION OF mqtt_messages FOR VALUES FROM('2020-01-01') TO('2020-02-01') USING columnar;
            CREATE TABLE packets_hot PARTITION OF mqtt_messages FOR VALUES FROM('2020-02-01') TO('2020-03-01');
            CREATE TABLE packets_legacy PARTITION OF mqtt_messages FOR VALUES FROM('2019-12-01') TO('2020-01-01');
            CREATE INDEX packet_sender ON mqtt_messages(from_node_id,created_at DESC);
            CREATE INDEX packet_recipient ON mqtt_messages(to_node_id,created_at DESC);
            CREATE TABLE packet_receptions(mqtt_row_id bigint,created_at timestamptz);
            CREATE INDEX receipt_lookup ON packet_receptions(mqtt_row_id,created_at)""")
        expected=[]
        batch=[]
        async def add(identity,stamp,sender,recipient,marker):
            expected.append((identity,stamp,sender,recipient,marker))
            batch.append((identity,"node-fixture",json.dumps({"marker":marker}),0,False,0,stamp,sender,recipient,None))
            if len(batch)==93:
                await writer.copy_records_to_table("mqtt_messages",schema_name=schema,records=batch)
                batch.clear()
        # More than50k matches prove the API maximum still leaves its sentinel;
        # thousands of other senders also exercise generic node selectivity.
        for number in range(1,56_052):
            if number<=50_051:
                sender=COMMON if number%13 else None
                recipient=COMMON if number%11==0 or sender is None else "00000002"
            else:
                sender=f"{number:08x}"
                recipient=None
            await add(number,JAN+timedelta(microseconds=number//5),sender,recipient,f"cold-{number}")
        for number in range(1,301):
            await add(70_000+number,HOT+timedelta(microseconds=number//5),
                COMMON if number%2 else None,COMMON,f"hot-{number}")
        await add(10,HOT+timedelta(seconds=1),COMMON,COMMON,"reused-hot")
        await add(10,JAN-timedelta(seconds=1),COMMON,None,"reused-legacy")
        await add(80_001,JAN-timedelta(days=1),RARE,None,"rare-legacy")
        await add(80_002,JAN+timedelta(seconds=1),None,RARE,"rare-cold")
        await add(80_003,HOT+timedelta(seconds=1),RARE,RARE,"rare-hot")
        if batch:await writer.copy_records_to_table("mqtt_messages",schema_name=schema,records=batch)
        await writer.execute("ANALYZE mqtt_messages; ANALYZE packet_receptions")
        storage.pool=await asyncpg.create_pool(DSN,min_size=1,max_size=1,command_timeout=10,
            server_settings={"search_path":f'"{schema}",public',"timezone":"UTC", "work_mem":"2MB",
                "max_parallel_workers_per_gather":"0","default_transaction_read_only":"on"})
        expected.sort(key=lambda row:(row[1],row[0]),reverse=True)
        def oracle(node,start=None,end=None,cursor=None):
            return [row for row in expected if (row[2]==node or row[3]==node)
                    and (start is None or row[1]>=start) and (end is None or row[1]<=end)
                    and (cursor is None or (row[1],row[0])<cursor)]
        cases=[(COMMON,None,None,None),(RARE,None,None,None),("ffffffff",None,None,None),
            (COMMON,JAN,HOT-timedelta(microseconds=1),None),(COMMON,HOT,None,None),
            (COMMON,None,JAN-timedelta(microseconds=1),None),
            (COMMON,None,None,(JAN+timedelta(microseconds=10_000),50_003)),
            (COMMON,None,None,(HOT+timedelta(microseconds=20),70_102)),
            (RARE,JAN,HOT-timedelta(microseconds=1),None)]
        async with storage.pool.acquire() as conn:
            initial=await conn.fetchrow("SELECT current_setting('plan_cache_mode') AS plan,"
                "current_setting('columnar.enable_custom_scan') AS scan")
        for node,start,end,cursor in cases:
            for limit in (1,50,50_000):
                selected=oracle(node,start,end,cursor)
                result=await storage.query_node_mqtt_messages("!"+node,limit=limit,start=start,end=end,
                    before=_encode_cursor(*cursor) if cursor else None)
                page=selected[:limit]
                assert [row["marker"] for row in result["messages"]] == [row[4] for row in page]
                assert result["next_cursor"] == (_encode_cursor(page[-1][1],page[-1][0]) if len(selected)>limit else None)
        # Reused numeric IDs and self-addressed rows each appear once per exact
        # canonical identity. Read several tied timestamp pages end to end.
        cursor=None
        observed=[]
        for _ in range(7):
            result=await storage.query_node_mqtt_messages(COMMON,limit=1,before=cursor)
            observed.extend(row["marker"] for row in result["messages"])
            cursor=result["next_cursor"]
        assert observed == [row[4] for row in oracle(COMMON)[:7]]
        async with storage.pool.acquire() as conn:
            statements=await conn.fetch("SELECT statement,generic_plans,custom_plans FROM pg_prepared_statements "
                "WHERE statement LIKE '%from_node_id=$1%' AND statement LIKE '%UNION ALL%'")
            assert statements and all(row["generic_plans"]>0 and row["custom_plans"]==0 for row in statements)
            name=await conn.fetchval("SELECT name FROM pg_prepared_statements "
                "WHERE statement LIKE '%from_node_id=$1%' AND statement LIKE '%UNION ALL%' "
                "AND statement LIKE '%created_at >= $2%' AND cardinality(parameter_types)=4")
            assert name and name.replace('_','').isalnum()
            async with conn.transaction(readonly=True):
                await conn.execute("SET LOCAL plan_cache_mode=force_generic_plan; SET LOCAL columnar.enable_custom_scan=off")
                for node in (COMMON,RARE,"ffffffff"):
                    quoted=await conn.fetchrow("SELECT quote_literal($1::varchar),quote_literal($2::timestamptz),"
                        "quote_literal($3::timestamptz),quote_literal($4::integer)",node,JAN,HOT-timedelta(microseconds=1),51)
                    plan=json.loads(await conn.fetchval("EXPLAIN (ANALYZE,FORMAT JSON) EXECUTE "
                        +name+"("+','.join(quoted)+")"))[0]["Plan"]
                    def nodes(node):
                        yield node
                        for child in node.get("Plans",[]):yield from nodes(child)
                    cold=[node for node in nodes(plan) if node.get("Relation Name")=="packets_cold"
                          and node.get("Actual Loops",0)>0]
                    assert cold and all(node["Node Type"]=="Index Scan" for node in cold)
                    constrained_keys=set()
                    visited=0
                    for scan in cold:
                        condition=scan.get("Index Cond","")
                        leading=await conn.fetchval("SELECT pg_get_indexdef(c.oid,1,true) "
                            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                            "WHERE n.nspname=$1 AND c.relname=$2",schema,scan["Index Name"])
                        assert leading in {"from_node_id","to_node_id"}
                        # An Index Scan over time/PK followed by node filtering
                        # is not evidence of a selective node lookup. The node
                        # parameter must constrain the leading B-tree key.
                        assert leading in condition and "$1" in condition and "=" in condition
                        constrained_keys.add(leading)
                        visited+=(scan["Actual Rows"]+scan.get("Rows Removed by Filter",0)
                                  +scan.get("Rows Removed by Index Recheck",0))*scan["Actual Loops"]
                    assert constrained_keys == {"from_node_id","to_node_id"}
                    # Common pages may inspect a timestamp tie group and skip
                    # self-addressed recipient duplicates. Rare/absent nodes
                    # must visit only their matching identities, not all56k.
                    assert visited <= (512 if node==COMMON else 1 if node==RARE else 0)
            assert await conn.fetchrow("SELECT current_setting('plan_cache_mode') AS plan,"
                "current_setting('columnar.enable_custom_scan') AS scan") == initial
        await interruptions(storage,initial)
    finally:
        if storage.pool is not None:await storage.pool.close()
        if created:
            await writer.execute("SET search_path=public")
            await writer.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await writer.close()


async def interruptions(storage,initial):
    pool=storage.pool
    for mode in ("error","deadline","cancel"):
        reached=asyncio.Event()
        class WrappedConnection:
            def __init__(self,connection):self.connection=connection
            def __getattr__(self,name):return getattr(self.connection,name)
            async def fetch(self,query,*parameters):
                assert "UNION ALL" in query and "IS DISTINCT FROM" in query
                assert await self.connection.fetchval("SELECT current_setting('plan_cache_mode')") == "force_generic_plan"
                assert await self.connection.fetchval("SELECT current_setting('columnar.enable_custom_scan')") == "off"
                reached.set()
                if mode=="error":raise asyncpg.QueryCanceledError("isolated node cancellation")
                await self.connection.execute("SELECT pg_sleep(2)")
                return await self.connection.fetch(query,*parameters)
        class Acquisition:
            def __init__(self,**options):self.context=pool.acquire(**options)
            async def __aenter__(self):return WrappedConnection(await self.context.__aenter__())
            async def __aexit__(self,*error):return await self.context.__aexit__(*error)
        class Pool:
            acquire=Acquisition
        try:
            storage.pool=Pool()
            if mode=="cancel":
                pending=asyncio.create_task(storage.query_node_mqtt_messages(COMMON))
                await asyncio.wait_for(reached.wait(),timeout=2)
                pending.cancel()
                with pytest.raises(asyncio.CancelledError):await pending
            else:
                with patch("storage.db.history_queries.HISTORY_READ_TIMEOUT_SECONDS",.1 if mode=="deadline" else 10):
                    with pytest.raises(HistoryReadError) as failure:
                        await storage.query_node_mqtt_messages(COMMON)
                assert failure.value.timed_out
            assert reached.is_set()
        finally:
            storage.pool=pool
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT 1") == 1
            assert await conn.fetchrow("SELECT current_setting('plan_cache_mode') AS plan,"
                "current_setting('columnar.enable_custom_scan') AS scan") == initial
