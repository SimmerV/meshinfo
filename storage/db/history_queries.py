"""Packet-history query fragments and explicit read-failure contracts.

The search expression preserves the API's existing ILIKE wildcard behavior.
Only the topic dictionary lookup is moved out of the per-packet reception
subquery, so PostgreSQL can evaluate that lookup once for the entire statement.
"""


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

    ARRAY(SELECT ...) is uncorrelated: matching dictionary IDs are selected once,
    while the indexed receipt lookup retains its canonical ID and time bounds.
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
        " OR EXISTS (SELECT 1 FROM packet_receptions pr"
        " WHERE pr.mqtt_row_id = mqtt_messages.id"
        " AND pr.created_at >= mqtt_messages.created_at"
        " AND pr.created_at < mqtt_messages.created_at"
        f" + interval '{lookback_seconds} seconds'"
        " AND pr.topic_id = ANY(ARRAY(SELECT id FROM mqtt_topics"
        f" WHERE topic ILIKE '%' || {term} || '%'))))"
    )


def literal_topic_pattern(topic: str) -> str:
    """Escape only the literal-topic filter; free-text search remains ILIKE."""
    return topic.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def page_limit(limit: int) -> int:
    """Apply the same explicit row ceiling as the public packet endpoints."""
    if type(limit) is not int or not 1 <= limit <= 50_000:
        raise ValueError("Packet page limit must be an integer between 1 and 50000")
    return limit


def packet_page_sql(conditions, limit_parameter: int, reception_count_sql: str,
                    *, strategy: str = "direct") -> str:
    """Build equivalent direct and bounded-key packet-page plans.

    Conditions are internal SQL fragments with separately bound parameters.
    The materialized strategy selects at most limit+1 identities before loading
    their payloads and reception counts. Both identity columns participate in
    the join, preserving partition routing and timestamp-tie pagination.
    """
    if type(limit_parameter) is not int or limit_parameter < 1:
        raise ValueError("Limit parameter must be a positive integer")
    if strategy not in {"direct", "candidates", "candidate_array", "node_branches", "node_branch_array"}:
        raise ValueError("Unknown packet page strategy")
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    columns = ("id", "topic", "payload", "qos", "retain", "timestamp", "created_at")
    if strategy == "direct":
        return (
            f"SELECT {','.join(columns)}, {reception_count_sql} AS reception_count "
            f"FROM mqtt_messages {where} "
            f"ORDER BY created_at DESC, id DESC LIMIT ${limit_parameter}::integer"
        )
    ordering = f"ORDER BY created_at DESC,id DESC LIMIT ${limit_parameter}::integer"
    if strategy in {"node_branches", "node_branch_array"}:
        if not conditions or conditions[0] != "(from_node_id = $1 OR to_node_id = $1)":
            raise ValueError("Node branch strategy requires the node reader predicate")
        # Each independently indexed arm contributes at most the requested page.
        # Excluding self-addressed rows from the recipient arm permits UNION ALL
        # without payload-wide deduplication or duplicated packet identities.
        remaining = " AND " + " AND ".join(conditions[1:]) if conditions[1:] else ""
        keys = (
            f"(SELECT id,created_at FROM mqtt_messages WHERE from_node_id=$1{remaining} {ordering}) "
            "UNION ALL "
            "(SELECT id,created_at FROM mqtt_messages "
            f"WHERE to_node_id=$1 AND from_node_id IS DISTINCT FROM $1{remaining} {ordering}) "
            f"{ordering}"
        )
    else:
        keys = f"SELECT id,created_at FROM mqtt_messages {where} {ordering}"
    projection = ",".join(f"mqtt_messages.{name}" for name in columns)
    if strategy in {"candidate_array", "node_branch_array"}:
        return (
            f"WITH packet_candidates AS MATERIALIZED ({keys}) "
            f"SELECT {projection}, {reception_count_sql} AS reception_count FROM mqtt_messages "
            "WHERE mqtt_messages.created_at >= (SELECT min(created_at) FROM packet_candidates) "
            "AND mqtt_messages.created_at <= (SELECT max(created_at) FROM packet_candidates) "
            "AND mqtt_messages.id = ANY(ARRAY(SELECT id FROM packet_candidates)) "
            "AND EXISTS (SELECT 1 FROM packet_candidates candidate "
            "WHERE candidate.id=mqtt_messages.id AND candidate.created_at=mqtt_messages.created_at) "
            "ORDER BY mqtt_messages.created_at DESC,mqtt_messages.id DESC"
        )
    return (
        f"WITH packet_candidates AS MATERIALIZED ({keys}) "
        f"SELECT {projection}, {reception_count_sql} AS reception_count "
        "FROM packet_candidates candidate JOIN mqtt_messages "
        "ON mqtt_messages.id=candidate.id AND mqtt_messages.created_at=candidate.created_at "
        "ORDER BY mqtt_messages.created_at DESC,mqtt_messages.id DESC"
    )
