"""Packet-history query fragments and explicit read-failure contracts.

The search expression preserves the API's existing ILIKE wildcard behavior.
Only the topic dictionary lookup is moved out of the per-packet reception
subquery, so PostgreSQL can evaluate that lookup once for the entire statement.
"""

from datetime import datetime, timezone
import re

HISTORY_ACQUIRE_TIMEOUT_SECONDS = 3
TOPIC_MATCH_ARRAY_LIMIT = 4096


class HistoryReadError(RuntimeError):
    """A failed read must never look like an empty archive or a missing packet.

    Public messages and attributes contain no SQL, request text, or source
    payload. The original exception remains available through exception chaining
    for server-side diagnostics.
    """

    def __init__(self, operation: str, *, timed_out: bool = False):
        super().__init__("Packet history query timed out." if timed_out
                         else "Packet history could not be read.")
        self.operation = operation
        self.timed_out = timed_out


class HistoryUnavailableError(HistoryReadError):
    """Packet history storage is disabled or its connection pool is unavailable."""


def read_failure(operation: str, cause: Exception) -> HistoryReadError:
    """Classify driver deadlines and PostgreSQL statement cancellation safely."""
    return HistoryReadError(
        operation,
        timed_out=isinstance(cause, TimeoutError) or getattr(cause, "sqlstate", None) == "57014",
    )


def search_condition(parameter: int, lookback_seconds: int) -> str:
    """Match canonical text or any eligible reception's topic, without row fanout.

    packet_page_sql materializes matching dictionary IDs once. The empty-array
    guard skips receipt probes altogether when no dictionary topics match,
    while nonempty indexed lookups retain their canonical ID and time bounds.
    EXISTS keeps a packet unique even when several receipts match. An empty topic
    array, nullable packet text, and SQL wildcard characters retain their prior
    meanings.
    """
    if type(parameter) is not int or parameter < 1:
        raise ValueError("Search parameter must be a positive integer")
    if type(lookback_seconds) is not int or lookback_seconds < 1:
        raise ValueError("Reception lookback must be a positive integer")
    term = f"${parameter}"
    return (
        f"(topic ILIKE '%' || {term} || '%'"
        f" OR payload ILIKE '%' || {term} || '%'"
        " OR ((SELECT cardinality(ids)>0 FROM packet_topic_matches)"
        " AND EXISTS (SELECT 1 FROM packet_receptions pr"
        " WHERE pr.mqtt_row_id = mqtt_messages.id"
        " AND pr.created_at >= mqtt_messages.created_at"
        " AND pr.created_at < mqtt_messages.created_at"
        f" + interval '{lookback_seconds} seconds'"
        " AND CASE WHEN (SELECT overflow FROM packet_topic_matches) THEN EXISTS ("
        "SELECT 1 FROM mqtt_topics matched_topic WHERE matched_topic.id=pr.topic_id "
        f"AND matched_topic.topic ILIKE '%' || {term} || '%') "
        "ELSE pr.topic_id = ANY((SELECT ids FROM packet_topic_matches)::integer[]) END)))"
    )


def literal_topic_pattern(topic: str) -> str:
    """Escape only the literal-topic filter; free-text search remains ILIKE."""
    return topic.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def page_limit(limit: int) -> int:
    """Apply the same explicit row ceiling as the public packet endpoints."""
    if type(limit) is not int or not 1 <= limit <= 50_000:
        raise ValueError("Packet page limit must be an integer between 1 and 50000")
    return limit


async def fetch_packet_page(connection, query, *parameters, custom_plan=False):
    """Keep selective topic filters from inheriting a generic broad-scan plan.

    A pooled connection may see both a common preset and a rare literal topic.
    PostgreSQL's generic prepared plan cannot cost their different selectivity.
    Transaction-local planning leaves other users of the connection unchanged,
    including when the read fails or is cancelled. This requires no extension.
    """
    if not custom_plan:
        return await connection.fetch(query, *parameters)
    async with connection.transaction(readonly=True):
        await connection.execute("SET LOCAL plan_cache_mode = force_custom_plan")
        return await connection.fetch(query, *parameters)


def topic_coverage_intervals(bounds):
    """Parse only complete UTC monthly bounds emitted by PostgreSQL catalogs."""
    intervals = []
    for expression in bounds:
        match = re.fullmatch(r"FOR VALUES FROM \('([^']+)'\) TO \('([^']+)'\)", expression)
        if match is None:
            raise ValueError("Unsupported canonical-topic partition bounds")
        parsed = [datetime.fromisoformat(value) for value in match.groups()]
        if any(value.tzinfo is None for value in parsed):
            raise ValueError("Canonical-topic lookup bounds require timezone offsets")
        # pg_get_expr renders timestamptz constants in the session timezone;
        # their UTC instants, rather than printed local dates, define coverage.
        start, end = (value.astimezone(timezone.utc) for value in parsed)
        if any(value.day != 1
               or any((value.hour, value.minute, value.second, value.microsecond)) for value in (start, end)):
            raise ValueError("Canonical-topic lookup bounds must be UTC month boundaries")
        following = start.replace(year=start.year+1, month=1) if start.month == 12 else start.replace(month=start.month+1)
        if end != following:
            raise ValueError("Canonical-topic lookup partitions must cover exactly one month")
        intervals.append((start, end))
    merged = []
    for start, end in sorted(set(intervals)):
        if merged and start == merged[-1][1]:
            merged[-1] = (merged[-1][0], end)
        else:
            merged.append((start, end))
    return merged


def topic_candidate_sql(conditions, limit_parameter, intervals, *, bounded=True):
    """Use verified cold metadata and native hot/uncovered canonical filters."""
    if not intervals:
        raise ValueError("Verified canonical-topic coverage is required")
    exclusions, values = [], []
    parameter = limit_parameter + 1
    for start, end in intervals:
        exclusions.append(f"(created_at >= ${parameter}::timestamptz AND created_at < ${parameter+1}::timestamptz)")
        values.extend((start, end))
        parameter += 2
    where = " AND ".join(conditions) if conditions else "true"
    covered = " OR ".join(exclusions)
    sql = (
        "SELECT id,created_at FROM ("
        f"SELECT id,created_at FROM mqtt_archive_topics WHERE ({where}) AND ({covered}) "
        "UNION ALL "
        f"SELECT id,created_at FROM mqtt_messages WHERE ({where}) AND NOT ({covered})"
        ") candidates"
    )
    if bounded:
        sql += f" ORDER BY created_at DESC,id DESC LIMIT ${limit_parameter}::integer"
    return sql, values


async def fetch_topic_packet_page(connection, conditions, parameters, limit_parameter, reception_count_sql,
                                  *, candidate_conditions=None, search_parameter=None,
                                  indexed_payload=True, window_start=None, window_end=None):
    """Read literal-topic pages through complete cold metadata when available.

    Existing compressed leaves without a verified helper remain in the native
    fallback. Parent locks protect the coverage/key/payload sequence against
    archive cutover, reverse conversion and retention. Matching keys are bounded
    before payload fetch; a bound array gives the custom planner real selectivity
    instead of an unknown materialized-subquery cardinality.
    """
    direct = packet_page_sql(conditions, limit_parameter, reception_count_sql, search_parameter=search_parameter)
    helper = await connection.fetchval("SELECT to_regclass('mqtt_archive_topics')::oid")
    if helper is None:
        return await fetch_packet_page(connection, direct, *parameters, custom_plan=True)
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        await connection.execute("SET LOCAL plan_cache_mode = force_custom_plan")
        await connection.execute("LOCK TABLE ONLY mqtt_messages, ONLY mqtt_archive_topics IN ACCESS SHARE MODE")
        rows = await connection.fetch("""SELECT pg_get_expr(helper.relpartbound,helper.oid) AS bounds
            FROM pg_inherits membership JOIN pg_class helper ON helper.oid=membership.inhrelid
            WHERE membership.inhparent='mqtt_archive_topics'::regclass
            AND EXISTS (SELECT 1 FROM pg_inherits canonical_membership
                JOIN pg_class canonical ON canonical.oid=canonical_membership.inhrelid
                JOIN pg_am method ON method.oid=canonical.relam
                WHERE canonical_membership.inhparent='mqtt_messages'::regclass AND method.amname='columnar'
                AND pg_get_expr(canonical.relpartbound,canonical.oid)=pg_get_expr(helper.relpartbound,helper.oid))""")
        intervals = topic_coverage_intervals(row["bounds"] for row in rows)
        if window_start is not None:
            intervals = [(start, end) for start, end in intervals if end > window_start]
        if window_end is not None:
            intervals = [(start, end) for start, end in intervals if start <= window_end]
        if not intervals:
            return await connection.fetch(direct, *parameters)
        if candidate_conditions is not None and candidate_conditions != conditions:
            # Keep the helper relation visible to the planner rather than
            # aggregating an unbounded ID array. A selective literal topic can
            # use nested PK lookups; broad matches can use a scan/hash join.
            # Payload and reported-time predicates precede the final LIMIT.
            keys_sql, coverage_parameters = topic_candidate_sql(
                candidate_conditions, limit_parameter, intervals, bounded=False)
            prefix = "WITH " + topic_match_cte(search_parameter) + " " if search_parameter else ""
            columns = ",".join("mqtt_messages."+name for name in
                ("id","topic","payload","qos","retain","timestamp","created_at"))
            query = (f"{prefix}SELECT {columns},{reception_count_sql} AS reception_count "
                f"FROM mqtt_messages JOIN ({keys_sql}) topic_candidates USING(id,created_at) "
                "WHERE " + " AND ".join(conditions) +
                f" ORDER BY mqtt_messages.created_at DESC,mqtt_messages.id DESC LIMIT ${limit_parameter}::integer")
            return await connection.fetch(query, *parameters, *coverage_parameters)
        keys_sql, coverage_parameters = topic_candidate_sql(conditions, limit_parameter, intervals)
        keys = await connection.fetch(keys_sql, *parameters, *coverage_parameters)
        if not keys:
            return []
        if indexed_payload:
            # Only this bounded exact-ID projection needs index scans. Keeping
            # custom scans enabled for broad predicates avoids expensive full
            # index walks. The local setting disappears on transaction exit.
            await connection.execute("SET LOCAL columnar.enable_custom_scan = off")
        query, payload_parameters = packet_payload_sql(keys, reception_count_sql)
        return ordered_packet_rows(keys, await connection.fetch(query, *payload_parameters))


def packet_payload_sql(keys, reception_count_sql):
    """Fetch known identities in physical key order, then restore page order."""
    if not keys or len(keys) > 50_001:
        raise ValueError("Payload lookup requires one bounded page of identities")
    ids, stamps = [row["id"] for row in keys], [row["created_at"] for row in keys]
    parameters = [ids, stamps, min(stamps), max(stamps)]
    query = (
        f"SELECT id,topic,payload,qos,retain,timestamp,created_at,{reception_count_sql} AS reception_count "
        "FROM mqtt_messages WHERE id=ANY($1::bigint[]) "
        "AND created_at >= $3::timestamptz AND created_at <= $4::timestamptz "
        "AND EXISTS (SELECT 1 FROM unnest($1::bigint[],$2::timestamptz[]) AS wanted(id,created_at) "
        "WHERE wanted.id=mqtt_messages.id AND wanted.created_at=mqtt_messages.created_at) "
        "ORDER BY id,created_at"
    )
    return query, parameters


def ordered_packet_rows(keys, rows):
    by_key = {(row["id"], row["created_at"]): row for row in rows}
    if len(rows) != len(keys) or len(by_key) != len(rows) or set(by_key) != {(row["id"], row["created_at"]) for row in keys}:
        # A stale/corrupt helper must never silently turn into an empty page.
        raise HistoryReadError("canonical_topic_lookup")
    return [by_key[(row["id"], row["created_at"])] for row in keys]


def topic_match_cte(search_parameter):
    if type(search_parameter) is not int or search_parameter < 1:
        raise ValueError("Search parameter must be a positive integer")
    return ("packet_topic_matches AS MATERIALIZED ("
            "SELECT coalesce(array_agg(id),ARRAY[]::integer[]) AS ids,"
            f"count(*)>{TOPIC_MATCH_ARRAY_LIMIT} AS overflow FROM ("
            f"SELECT id FROM mqtt_topics WHERE topic ILIKE '%' || ${search_parameter} || '%' "
            f"LIMIT {TOPIC_MATCH_ARRAY_LIMIT+1}) bounded_topics)")


def packet_page_sql(conditions, limit_parameter: int, reception_count_sql: str,
                    *, strategy: str = "direct", search_parameter: int | None = None,
                    keys_only: bool = False) -> str:
    """Build equivalent direct and bounded-key packet-page plans.

    Conditions are internal SQL fragments with separately bound parameters.
    The materialized strategy selects at most limit+1 identities before loading
    their payloads and reception counts. Both identity columns participate in
    the join, preserving partition routing and timestamp-tie pagination.
    """
    if type(limit_parameter) is not int or limit_parameter < 1:
        raise ValueError("Limit parameter must be a positive integer")
    if strategy not in {"direct", "candidates", "candidate_array", "node_branches", "node_branch_array", "node_rows"}:
        raise ValueError("Unknown packet page strategy")
    if keys_only and strategy not in {"direct", "node_rows"}:
        raise ValueError("Key-only selection requires a direct or node-row plan")
    common_tables = []
    if search_parameter is not None:
        common_tables.append(topic_match_cte(search_parameter))
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    columns = (("id", "created_at") if keys_only else
               ("id", "topic", "payload", "qos", "retain", "timestamp", "created_at"))
    reception_projection = "" if keys_only else f", {reception_count_sql} AS reception_count"
    if strategy == "direct":
        prefix = "WITH " + ",".join(common_tables) + " " if common_tables else ""
        return (
            f"{prefix}SELECT {','.join(columns)}{reception_projection} "
            f"FROM mqtt_messages {where} "
            f"ORDER BY created_at DESC, id DESC LIMIT ${limit_parameter}::integer"
        )
    ordering = f"ORDER BY created_at DESC,id DESC LIMIT ${limit_parameter}::integer"
    if strategy in {"node_branches", "node_branch_array", "node_rows"}:
        if not conditions or conditions[0] != "(from_node_id = $1 OR to_node_id = $1)":
            raise ValueError("Node branch strategy requires the node reader predicate")
        # Each independently indexed arm contributes at most the requested page.
        # Excluding self-addressed rows from the recipient arm permits UNION ALL
        # without payload-wide deduplication or duplicated packet identities.
        remaining = " AND " + " AND ".join(conditions[1:]) if conditions[1:] else ""
        if strategy == "node_rows":
            prefix = "WITH " + ",".join(common_tables) + " " if common_tables else ""
            projection = ",".join(columns)
            rows = (
                f"(SELECT {projection} FROM mqtt_messages WHERE from_node_id=$1{remaining} {ordering}) "
                "UNION ALL "
                f"(SELECT {projection} FROM mqtt_messages "
                f"WHERE to_node_id=$1 AND from_node_id IS DISTINCT FROM $1{remaining} {ordering})"
            )
            return (f"{prefix}SELECT {projection}{reception_projection} "
                    f"FROM ({rows}) mqtt_messages {ordering}")
        keys = (
            f"(SELECT id,created_at FROM mqtt_messages WHERE from_node_id=$1{remaining} {ordering}) "
            "UNION ALL "
            "(SELECT id,created_at FROM mqtt_messages "
            f"WHERE to_node_id=$1 AND from_node_id IS DISTINCT FROM $1{remaining} {ordering}) "
            f"{ordering}"
        )
    else:
        keys = f"SELECT id,created_at FROM mqtt_messages {where} {ordering}"
    common_tables.append(f"packet_candidates AS MATERIALIZED ({keys})")
    prefix = "WITH " + ",".join(common_tables) + " "
    projection = ",".join(f"mqtt_messages.{name}" for name in columns)
    if strategy in {"candidate_array", "node_branch_array"}:
        return (
            prefix +
            f"SELECT {projection}, {reception_count_sql} AS reception_count FROM mqtt_messages "
            "WHERE mqtt_messages.created_at >= (SELECT min(created_at) FROM packet_candidates) "
            "AND mqtt_messages.created_at <= (SELECT max(created_at) FROM packet_candidates) "
            "AND mqtt_messages.id = ANY(ARRAY(SELECT id FROM packet_candidates)) "
            "AND EXISTS (SELECT 1 FROM packet_candidates candidate "
            "WHERE candidate.id=mqtt_messages.id AND candidate.created_at=mqtt_messages.created_at) "
            "ORDER BY mqtt_messages.created_at DESC,mqtt_messages.id DESC"
        )
    return (
        prefix +
        f"SELECT {projection}, {reception_count_sql} AS reception_count "
        "FROM packet_candidates candidate JOIN mqtt_messages "
        "ON mqtt_messages.id=candidate.id AND mqtt_messages.created_at=candidate.created_at "
        "ORDER BY mqtt_messages.created_at DESC,mqtt_messages.id DESC"
    )
