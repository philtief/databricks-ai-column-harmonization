"""Tests for the Lakebase Postgres review store."""

from __future__ import annotations

import json
import shutil
import socket
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from psycopg.rows import dict_row

from harmonization.review_store import (
    ACTIONS,
    SCHEMA,
    connect,
    ensure_schema,
    fetch_decisions,
    fetch_queue,
    record_decision,
    status_summary,
    upsert_queue,
)

POSTGRES_BIN = Path("/opt/homebrew/opt/postgresql@17/bin")
INITDB = POSTGRES_BIN / "initdb" if (POSTGRES_BIN / "initdb").exists() else shutil.which("initdb")
PG_CTL = POSTGRES_BIN / "pg_ctl" if (POSTGRES_BIN / "pg_ctl").exists() else shutil.which("pg_ctl")


def _queue_row(source_system: str, local_column_name: str, **overrides):
    row = {
        "source_system": source_system,
        "local_column_name": local_column_name,
        "local_data_type": "STRING",
        "local_sample_values": ["a", "b"],
        "proposed_global_column_name": "gross_written_premium",
        "proposed_match_type": "EXACT",
        "mapping_rationale": "Direct semantic match",
        "confidence": "HIGH",
        "mandatory_flag": True,
        "mapping_version": "v1",
        "published_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-01-01T00:00:00Z",
    }
    row.update(overrides)
    return row


@pytest.fixture(scope="session")
def postgres_connection(tmp_path_factory):
    if INITDB is None or PG_CTL is None:
        pytest.skip("PostgreSQL initdb and pg_ctl are not available")

    root = tmp_path_factory.mktemp("review-store-postgres")
    data_dir = root / "data"
    # Unix socket paths are limited to ~104 bytes on macOS; pytest tmp paths are longer.
    socket_dir = Path(tempfile.mkdtemp(prefix="rs-", dir="/tmp"))
    logfile = root / "postgres.log"
    init_result = subprocess.run(
        [str(INITDB), "-D", str(data_dir), "--username=postgres"],
        check=False,
        capture_output=True,
        text=True,
    )
    if init_result.returncode != 0:
        pytest.skip(f"PostgreSQL initdb failed: {init_result.stderr.strip()}")

    with socket.socket() as probe_socket:
        probe_socket.bind(("127.0.0.1", 0))
        port = probe_socket.getsockname()[1]

    start_result = subprocess.run(
        [
            str(PG_CTL),
            "-D",
            str(data_dir),
            "-l",
            str(logfile),
            "-w",
            "-t",
            "30",
            "start",
            "-o",
            f"-p {port} -k {socket_dir} -c listen_addresses=127.0.0.1",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if start_result.returncode != 0:
        pytest.skip(f"PostgreSQL pg_ctl failed: {start_result.stderr.strip()}")

    admin_connection = None
    test_connection = None
    try:
        connection_info = {
            "host": "127.0.0.1",
            "port": port,
            "user": "postgres",
            "dbname": "postgres",
        }
        admin_connection = psycopg.connect(**connection_info, autocommit=True)
        with admin_connection.cursor() as cursor:
            cursor.execute("CREATE DATABASE review_store_test")
        test_connection = psycopg.connect(
            **{**connection_info, "dbname": "review_store_test"},
            autocommit=False,
            row_factory=dict_row,
        )
        yield test_connection
    finally:
        if test_connection is not None:
            test_connection.close()
        if admin_connection is not None:
            admin_connection.close()
        subprocess.run([str(PG_CTL), "-D", str(data_dir), "stop", "-m", "immediate"], check=False, capture_output=True)


class TestConnect:
    def test_uses_default_host_and_user(self):
        client = MagicMock()
        client.postgres.get_endpoint.return_value.status.hosts.host = "lakebase.example"
        client.current_user.me().user_name = "halvard@example.com"
        client.postgres.generate_database_credential.return_value.token = "credential"
        with patch("harmonization.review_store.psycopg.connect") as connect_mock:
            connect(client, "endpoint/path")
        connect_mock.assert_called_once_with(
            host="lakebase.example",
            dbname="databricks_postgres",
            user="halvard@example.com",
            password="credential",
            sslmode="require",
            autocommit=False,
            row_factory=dict_row,
        )
        client.postgres.get_endpoint.assert_called_once_with(name="endpoint/path")
        client.postgres.generate_database_credential.assert_called_once_with(endpoint="endpoint/path")

    def test_uses_explicit_host_and_user(self):
        client = MagicMock()
        with patch("harmonization.review_store.psycopg.connect") as connect_mock:
            connect(client, "endpoint/path", host="explicit.example", dbname="review", user="app")
        assert connect_mock.call_args.kwargs["host"] == "explicit.example"
        assert connect_mock.call_args.kwargs["dbname"] == "review"
        assert connect_mock.call_args.kwargs["user"] == "app"
        client.postgres.get_endpoint.assert_not_called()


class TestStoreHelpers:
    def test_actions_match_contract(self):
        assert {"APPROVE", "CORRECT", "REJECT", "RESET"} == ACTIONS

    def test_ensure_schema_commits(self):
        connection = MagicMock()
        ensure_schema(connection)
        connection.cursor.assert_called_once()
        connection.commit.assert_called_once()

    def test_upsert_serializes_sample_values(self):
        connection = MagicMock()
        connection.encoding = "utf-8"
        connection.pgconn._encoding = "utf-8"
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.rowcount = 1
        row = _queue_row("SOURCE", "premium", local_sample_values={"premium": 1})
        assert upsert_queue(connection, [row]) == 1
        statement, rows = cursor.executemany.call_args[0]
        assert "ON CONFLICT (source_system, local_column_name)" in statement.as_string(None)
        assert rows[0][3] == json.dumps({"premium": 1})
        connection.commit.assert_called_once()

    def test_upsert_accepts_empty_rows(self):
        connection = MagicMock()
        assert upsert_queue(connection, []) == 0
        connection.cursor.assert_not_called()

    def test_fetch_queue_applies_filters(self):
        connection = MagicMock()
        connection.encoding = "utf-8"
        connection.pgconn._encoding = "utf-8"
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [{"source_system": "SOURCE"}]
        rows = fetch_queue(connection, "SOURCE", "PENDING")
        statement = cursor.execute.call_args[0][0].as_string(None)
        assert "WHERE source_system = %s AND review_status = %s" in statement
        assert rows == [{"source_system": "SOURCE"}]

    def test_status_summary_groups_rows(self):
        connection = MagicMock()
        connection.encoding = "utf-8"
        connection.pgconn._encoding = "utf-8"
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [{"source_system": "SOURCE", "count": 2}]
        rows = status_summary(connection)
        statement = cursor.execute.call_args[0][0].as_string(None)
        assert "GROUP BY source_system, review_status, mandatory_flag" in statement
        assert rows[0]["count"] == 2

    def test_record_decision_validates_before_locking(self):
        connection = MagicMock()
        with pytest.raises(ValueError, match="Invalid action"):
            record_decision(connection, "SOURCE", "premium", "EDIT", "steward")
        with pytest.raises(ValueError, match="REJECT requires"):
            record_decision(connection, "SOURCE", "premium", "REJECT", "steward")
        with pytest.raises(ValueError, match="CORRECT requires"):
            record_decision(connection, "SOURCE", "premium", "CORRECT", "steward")
        connection.cursor.assert_not_called()


class TestReviewStore:
    def test_ensure_schema_is_idempotent(self, postgres_connection):
        ensure_schema(postgres_connection)
        ensure_schema(postgres_connection)
        with postgres_connection.cursor() as cursor:
            cursor.execute(
                """SELECT 1 FROM information_schema.tables
                   WHERE table_schema = %s AND table_name = 'review_queue'""",
                [SCHEMA],
            )
            assert cursor.fetchone() is not None

    def test_ensure_schema_grants_app_role(self, postgres_connection):
        with postgres_connection.cursor() as cursor:
            cursor.execute("DROP ROLE IF EXISTS app_sp_test")
            cursor.execute("CREATE ROLE app_sp_test")
        ensure_schema(postgres_connection, grant_to="app_sp_test")
        with postgres_connection.cursor() as cursor:
            cursor.execute(
                "SELECT has_table_privilege('app_sp_test', %s, 'INSERT') AS can_insert,"
                " has_schema_privilege('app_sp_test', 'harmonization_review', 'CREATE') AS can_create,"
                " has_sequence_privilege('app_sp_test', %s, 'USAGE') AS can_use_seq",
                [f"{SCHEMA}.review_audit", f"{SCHEMA}.review_audit_audit_id_seq"],
            )
            assert cursor.fetchone() == {"can_insert": True, "can_create": True, "can_use_seq": True}
        # The app connects as that role and calls ensure_schema on startup: it must not need ownership.
        with postgres_connection.cursor() as cursor:
            cursor.execute("SET ROLE app_sp_test")
        try:
            ensure_schema(postgres_connection)
        finally:
            with postgres_connection.cursor() as cursor:
                cursor.execute("RESET ROLE")
            postgres_connection.commit()

    def test_upsert_inserts_and_updates_pending_row(self, postgres_connection):
        ensure_schema(postgres_connection)
        source = "INSERT_UPDATE"
        assert upsert_queue(postgres_connection, [_queue_row(source, "premium")]) == 1
        updated = _queue_row(
            source,
            "premium",
            local_data_type="DECIMAL",
            local_sample_values=["1.0", "2.0"],
            proposed_global_column_name="gwp_updated",
            proposed_match_type="SEMANTIC",
            mapping_rationale="Updated",
            confidence="MEDIUM",
            mandatory_flag=False,
            mapping_version="v2",
        )
        assert upsert_queue(postgres_connection, [updated]) == 1
        fetched = fetch_queue(postgres_connection, source, "PENDING")
        assert len(fetched) == 1
        assert fetched[0]["local_data_type"] == "DECIMAL"
        assert fetched[0]["local_sample_values"] == ["1.0", "2.0"]
        assert fetched[0]["proposed_global_column_name"] == "gwp_updated"
        assert fetched[0]["proposed_match_type"] == "SEMANTIC"
        assert fetched[0]["mandatory_flag"] is False
        assert fetched[0]["mapping_version"] == "v2"

    def test_upsert_does_not_overwrite_approved_row(self, postgres_connection):
        ensure_schema(postgres_connection)
        source = "APPROVED_PROTECTED"
        upsert_queue(postgres_connection, [_queue_row(source, "premium")])
        assert record_decision(postgres_connection, source, "premium", "APPROVE", "steward")
        upsert_queue(
            postgres_connection,
            [_queue_row(source, "premium", proposed_global_column_name="changed", confidence="LOW")],
        )
        fetched = fetch_queue(postgres_connection, source, "APPROVED")
        assert (
            upsert_queue(
                postgres_connection,
                [_queue_row(source, "premium", proposed_global_column_name="changed", confidence="LOW")],
            )
            == 0
        )
        assert fetched[0]["proposed_global_column_name"] == "gross_written_premium"
        assert fetched[0]["final_global_column_name"] == "gross_written_premium"
        assert fetched[0]["final_match_type"] == "EXACT"
        assert fetched[0]["confidence"] == "HIGH"

    def test_record_decision_actions_and_audit(self, postgres_connection):
        ensure_schema(postgres_connection)
        # (action, kwargs, new status, final global, final match type)
        cases = [
            ("APPROVE", {}, "APPROVED", "gross_written_premium", "EXACT"),
            (
                "CORRECT",
                {"final_global": "net_written_premium_eur", "final_match_type": "SEMANTIC_TRANSLATION"},
                "CORRECTED",
                "net_written_premium_eur",
                "SEMANTIC_TRANSLATION",
            ),
            ("REJECT", {"comment": "No target"}, "REJECTED", None, None),
        ]
        for index, (action, kwargs, new_status, final_global, final_match_type) in enumerate(cases):
            source = f"ACTIONS_{index}"
            upsert_queue(postgres_connection, [_queue_row(source, "premium")])
            assert record_decision(postgres_connection, source, "premium", action, "steward", **kwargs)
            decision = fetch_decisions(postgres_connection, source)[0]
            assert decision["review_status"] == new_status
            assert decision["final_global_column_name"] == final_global
            assert decision["final_match_type"] == final_match_type
            audit = _audit_for(postgres_connection, source, "premium")
            assert audit["old_status"] == "PENDING"
            assert audit["new_status"] == new_status
            # Before any decision, the "old" target is the AI proposal.
            assert audit["old_global_column_name"] == "gross_written_premium"
            assert audit["new_global_column_name"] == final_global
            assert audit["action_by"] == "steward"

    def test_reset_after_correct_audits_the_corrected_target(self, postgres_connection):
        ensure_schema(postgres_connection)
        source = "RESET_AFTER_CORRECT"
        upsert_queue(postgres_connection, [_queue_row(source, "premium")])
        record_decision(
            postgres_connection, source, "premium", "CORRECT", "steward", final_global="net_written_premium_eur"
        )
        assert record_decision(postgres_connection, source, "premium", "RESET", "steward")
        assert fetch_queue(postgres_connection, source, "PENDING")[0]["final_global_column_name"] is None
        audit = _audit_for(postgres_connection, source, "premium")
        assert audit["old_status"] == "CORRECTED"
        assert audit["old_global_column_name"] == "net_written_premium_eur"

        source = "ACTIONS_RESET"
        upsert_queue(postgres_connection, [_queue_row(source, "premium")])
        assert record_decision(postgres_connection, source, "premium", "APPROVE", "steward")
        assert record_decision(postgres_connection, source, "premium", "RESET", "admin", source="SCRIPT")
        assert fetch_queue(postgres_connection, source, "PENDING")[0]["final_global_column_name"] is None
        audit = _audit_for(postgres_connection, source, "premium")
        assert audit["old_status"] == "APPROVED"
        assert audit["new_status"] == "PENDING"
        assert audit["action_source"] == "SCRIPT"

    def test_record_decision_validation_errors(self, postgres_connection):
        ensure_schema(postgres_connection)
        upsert_queue(postgres_connection, [_queue_row("VALIDATION", "premium")])
        with pytest.raises(ValueError, match="Invalid action"):
            record_decision(postgres_connection, "VALIDATION", "premium", "EDIT", "steward")
        with pytest.raises(ValueError, match="REJECT requires"):
            record_decision(postgres_connection, "VALIDATION", "premium", "REJECT", "steward")
        with pytest.raises(ValueError, match="CORRECT requires"):
            record_decision(postgres_connection, "VALIDATION", "premium", "CORRECT", "steward")

    def test_record_decision_missing_row_returns_false(self, postgres_connection):
        ensure_schema(postgres_connection)
        assert not record_decision(postgres_connection, "MISSING", "premium", "APPROVE", "steward")

    def test_fetch_decisions_excludes_pending(self, postgres_connection):
        ensure_schema(postgres_connection)
        source = "FETCH_DECISIONS"
        upsert_queue(postgres_connection, [_queue_row(source, "pending"), _queue_row(source, "approved")])
        record_decision(postgres_connection, source, "approved", "APPROVE", "steward")
        locals_ = {row["local_column_name"] for row in fetch_decisions(postgres_connection, source)}
        assert locals_ == {"approved"}

    def test_status_summary_counts(self, postgres_connection):
        ensure_schema(postgres_connection)
        source = "SUMMARY"
        upsert_queue(
            postgres_connection,
            [
                _queue_row(source, "mandatory_pending", mandatory_flag=True),
                _queue_row(source, "optional_pending", mandatory_flag=False),
                _queue_row(source, "approved", mandatory_flag=True),
            ],
        )
        record_decision(postgres_connection, source, "approved", "APPROVE", "steward")
        rows = [row for row in status_summary(postgres_connection) if row["source_system"] == source]
        assert {
            ("PENDING", True, 1),
            ("PENDING", False, 1),
            ("APPROVED", True, 1),
        } == {(row["review_status"], row["mandatory_flag"], row["count"]) for row in rows}


def _audit_for(conn, source_system: str, local_column_name: str):
    with conn.cursor() as cursor:
        cursor.execute(
            f"""SELECT * FROM {SCHEMA}.review_audit
                WHERE source_system = %s AND local_column_name = %s
                ORDER BY audit_id DESC LIMIT 1""",
            [source_system, local_column_name],
        )
        return cursor.fetchone()
