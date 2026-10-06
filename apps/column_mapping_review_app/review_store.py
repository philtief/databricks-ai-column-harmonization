# generated, edit src/harmonization/review_store.py
"""Data access for the Lakebase Postgres review store."""

from __future__ import annotations

import json
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row, tuple_row

SCHEMA = "harmonization_review"
ACTIONS = {"APPROVE", "CORRECT", "REJECT", "RESET"}

_QUEUE_COLUMNS = (
    "source_system",
    "local_column_name",
    "local_data_type",
    "local_sample_values",
    "proposed_global_column_name",
    "proposed_match_type",
    "mapping_rationale",
    "confidence",
    "mandatory_flag",
    "mapping_version",
    "published_at",
    "updated_at",
)

_AI_UPDATES = (
    "local_data_type = excluded.local_data_type",
    "local_sample_values = excluded.local_sample_values",
    "proposed_global_column_name = excluded.proposed_global_column_name",
    "proposed_match_type = excluded.proposed_match_type",
    "mapping_rationale = excluded.mapping_rationale",
    "confidence = excluded.confidence",
    "mandatory_flag = excluded.mandatory_flag",
    "mapping_version = excluded.mapping_version",
    "published_at = excluded.published_at",
    "updated_at = excluded.updated_at",
)

_ENSURE_SCHEMA_SQL = sql.SQL("""
CREATE SCHEMA IF NOT EXISTS {schema};
CREATE TABLE IF NOT EXISTS {schema}.review_queue (
    source_system text,
    local_column_name text,
    local_data_type text,
    local_sample_values jsonb,
    proposed_global_column_name text,
    proposed_match_type text,
    mapping_rationale text,
    confidence text,
    mandatory_flag boolean,
    review_status text NOT NULL DEFAULT 'PENDING',
    final_global_column_name text,
    final_match_type text,
    reviewed_by text,
    reviewed_at timestamptz,
    review_comment text,
    mapping_version text,
    published_at timestamptz,
    updated_at timestamptz,
    PRIMARY KEY (source_system, local_column_name)
);
CREATE TABLE IF NOT EXISTS {schema}.review_audit (
    audit_id bigserial PRIMARY KEY,
    source_system text,
    local_column_name text,
    old_status text,
    new_status text,
    old_global_column_name text,
    new_global_column_name text,
    action_by text,
    action_at timestamptz DEFAULT now(),
    action_comment text,
    action_source text
);
CREATE INDEX IF NOT EXISTS review_queue_review_status_idx
    ON {schema}.review_queue (review_status);
""")


def ensure_schema(conn: psycopg.Connection) -> None:
    """Create the review schema, tables, and status index if needed."""
    statement = _ENSURE_SCHEMA_SQL.format(schema=sql.Identifier(SCHEMA))
    with conn.cursor() as cursor:
        cursor.execute(statement)
    conn.commit()


def upsert_queue(conn: psycopg.Connection, rows: list[dict[str, Any]]) -> int:
    """Insert queue rows and refresh AI fields only for pending rows."""
    if not rows:
        return 0

    columns = sql.SQL(", ").join(map(sql.Identifier, _QUEUE_COLUMNS))
    value_placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in _QUEUE_COLUMNS)
    assignments = sql.SQL(", ").join(map(sql.SQL, _AI_UPDATES))
    statement = sql.SQL("""
INSERT INTO {schema}.review_queue ({columns})
VALUES ({values})
ON CONFLICT (source_system, local_column_name) DO UPDATE SET
    {assignments}
WHERE {schema}.review_queue.review_status = 'PENDING'
""").format(
        schema=sql.Identifier(SCHEMA),
        columns=columns,
        values=value_placeholders,
        assignments=assignments,
    )

    serialized_rows = []
    for row in rows:
        values = [row.get(column) for column in _QUEUE_COLUMNS]
        if values[3] is not None:
            values[3] = json.dumps(values[3])
        serialized_rows.append(values)

    with conn.cursor() as cursor:
        cursor.executemany(statement, serialized_rows)
        updated_count = cursor.rowcount
    conn.commit()
    return updated_count


def _fetch(conn: psycopg.Connection, where_sql: sql.Composable, params: list[Any]) -> list[dict[str, Any]]:
    statement = sql.SQL("""
SELECT * FROM {schema}.review_queue {where}
ORDER BY source_system, local_column_name
""").format(schema=sql.Identifier(SCHEMA), where=where_sql)
    with conn.cursor() as cursor:
        cursor.execute(statement, params)
        return cursor.fetchall()


def fetch_queue(
    conn: psycopg.Connection,
    source_system: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    """Return queue rows, optionally filtered by source system and status."""
    conditions = []
    params = []
    if source_system is not None:
        conditions.append(sql.SQL("source_system = %s"))
        params.append(source_system)
    if status is not None:
        conditions.append(sql.SQL("review_status = %s"))
        params.append(status)
    where_sql = sql.SQL("WHERE ") + sql.SQL(" AND ").join(conditions) if conditions else sql.SQL("")
    return _fetch(conn, where_sql, params)


def fetch_decisions(conn: psycopg.Connection, source_system: str) -> list[dict[str, Any]]:
    """Return non-pending review decisions for one source system."""
    return _fetch(conn, sql.SQL("WHERE source_system = %s AND review_status <> 'PENDING'"), [source_system])


def status_summary(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """Return review counts by source system, status, and mandatory flag."""
    statement = sql.SQL("""
SELECT source_system, review_status, mandatory_flag, count(*) AS count
FROM {schema}.review_queue
GROUP BY source_system, review_status, mandatory_flag
ORDER BY source_system, review_status, mandatory_flag
""").format(schema=sql.Identifier(SCHEMA))
    with conn.cursor() as cursor:
        cursor.execute(statement)
        return cursor.fetchall()


def record_decision(
    conn: psycopg.Connection,
    source_system: str,
    local_column_name: str,
    action: str,
    user: str,
    final_global: str | None = None,
    final_match_type: str | None = None,
    comment: str = "",
    source: str = "DATABRICKS_APP",
) -> bool:
    """Apply one review action and write its audit record atomically."""
    if action not in ACTIONS:
        raise ValueError(f"Invalid action: {action}")
    if action == "REJECT" and not comment.strip():
        raise ValueError("REJECT requires a non-empty comment")
    if action == "CORRECT" and final_global is None:
        raise ValueError("CORRECT requires final_global")

    if action == "APPROVE":
        new_status = "APPROVED"
    elif action == "CORRECT":
        new_status = "CORRECTED"
        new_global = final_global
        new_match_type = final_match_type
    elif action == "REJECT":
        new_status = "REJECTED"
        new_global = None
        new_match_type = None
    else:
        new_status = "PENDING"
        new_global = None
        new_match_type = None

    select_statement = sql.SQL("""
SELECT review_status, proposed_global_column_name, proposed_match_type, final_global_column_name
FROM {schema}.review_queue
WHERE source_system = %s AND local_column_name = %s
FOR UPDATE
""").format(schema=sql.Identifier(SCHEMA))
    update_statement = sql.SQL("""
UPDATE {schema}.review_queue
SET review_status = %s,
    final_global_column_name = %s,
    final_match_type = %s,
    reviewed_by = %s,
    reviewed_at = now(),
    review_comment = %s,
    updated_at = now()
WHERE source_system = %s AND local_column_name = %s
""").format(schema=sql.Identifier(SCHEMA))
    audit_statement = sql.SQL("""
INSERT INTO {schema}.review_audit (
    source_system, local_column_name, old_status, new_status,
    old_global_column_name, new_global_column_name, action_by,
    action_comment, action_source
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""").format(schema=sql.Identifier(SCHEMA))

    # tuple_row: callers often open the connection with dict_row, and unpacking a dict yields its keys.
    with conn.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(select_statement, [source_system, local_column_name])
        existing = cursor.fetchone()
        if existing is None:
            conn.rollback()
            return False

        old_status, proposed_global, proposed_match_type, old_final_global = existing
        old_global = old_final_global if old_final_global is not None else proposed_global
        if action == "APPROVE":
            new_global = proposed_global
            new_match_type = proposed_match_type
        cursor.execute(
            update_statement,
            [
                new_status,
                new_global,
                new_match_type,
                user,
                comment,
                source_system,
                local_column_name,
            ],
        )
        cursor.execute(
            audit_statement,
            [
                source_system,
                local_column_name,
                old_status,
                new_status,
                old_global,
                new_global,
                user,
                comment,
                source,
            ],
        )
    conn.commit()
    return True


def connect(
    workspace_client: Any,
    endpoint_path: str,
    host: str | None = None,
    dbname: str = "databricks_postgres",
    user: str | None = None,
) -> psycopg.Connection:
    """Open a TLS connection using a Databricks-generated credential."""
    resolved_host = host
    if resolved_host is None:
        endpoint = workspace_client.postgres.get_endpoint(name=endpoint_path)
        resolved_host = endpoint.status.hosts.host
    resolved_user = user or workspace_client.current_user.me().user_name
    credential = workspace_client.postgres.generate_database_credential(endpoint=endpoint_path)
    return psycopg.connect(
        host=resolved_host,
        dbname=dbname,
        user=resolved_user,
        password=credential.token,
        sslmode="require",
        autocommit=False,
        row_factory=dict_row,
    )
