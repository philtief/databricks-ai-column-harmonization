"""Tests for the Streamlit review app helper functions."""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.service import dashboards as genie

ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "apps" / "column_mapping_review_app"
APP_PATH = APP_DIR / "app.py"
SOURCE_STORE = ROOT / "src" / "harmonization" / "review_store.py"
APP_STORE = APP_DIR / "review_store.py"


class OperationalError(Exception):
    """Test stand-in for psycopg.OperationalError."""


@pytest.fixture(autouse=True)
def mock_streamlit_and_sdk():
    """Mock Streamlit and SDK imports so app helpers load without app runtime."""
    mock_st = MagicMock()
    mock_st.cache_resource = lambda **kwargs: lambda function: function
    mock_st.cache_data = lambda **kwargs: lambda function: function
    mock_st.context = MagicMock()
    mock_st.context.headers = {}

    mock_sdk = MagicMock()
    mock_state = MagicMock()
    mock_state.SUCCEEDED = "SUCCEEDED"
    mock_state.FAILED = "FAILED"
    mock_state.CANCELED = "CANCELED"
    mock_state.CLOSED = "CLOSED"
    mock_sdk.service.sql.StatementState = mock_state

    mock_psycopg = MagicMock()
    mock_psycopg.OperationalError = OperationalError
    mock_review_store = MagicMock()
    mock_review_store.psycopg = mock_psycopg

    mock_pd = MagicMock()
    mock_pd.DataFrame = MagicMock
    mock_pd.notna = MagicMock(return_value=True)

    modules = {
        "streamlit": mock_st,
        "pandas": mock_pd,
        "databricks": MagicMock(),
        "databricks.sdk": mock_sdk,
        "databricks.sdk.service": MagicMock(),
        "databricks.sdk.service.sql": mock_sdk.service.sql,
        "psycopg": mock_psycopg,
        "psycopg.rows": MagicMock(),
        "psycopg.sql": MagicMock(),
        "review_store": mock_review_store,
    }
    sys.path.insert(0, str(APP_DIR))
    with patch.dict(sys.modules, modules):
        sys.modules.pop("app", None)
        yield mock_st, mock_review_store
    sys.path.remove(str(APP_DIR))
    sys.modules.pop("app", None)


def _import_app():
    """Import app.py with the app folder available for its local review_store import."""
    spec = importlib.util.spec_from_file_location("app", APP_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestCountryAndPublishRules:
    def test_country_mapping(self):
        app = _import_app()
        assert app.COUNTRY_SOURCES == {"ES": "ES_PROPERTY_RAW", "IT": "IT_PROPERTY_RAW"}

    def test_publish_disabled_while_mandatory_column_is_pending(self):
        app = _import_app()
        rows = [
            {"mandatory_flag": True, "review_status": "PENDING"},
            {"mandatory_flag": False, "review_status": "PENDING"},
        ]
        assert app.publish_is_ready(rows) is False

    def test_publish_enabled_when_mandatory_columns_are_resolved(self):
        app = _import_app()
        rows = [
            {"mandatory_flag": True, "review_status": "APPROVED", "final_global_column_name": "region"},
            {"mandatory_flag": True, "review_status": "CORRECTED", "final_global_column_name": "currency"},
            {"mandatory_flag": False, "review_status": "PENDING"},
        ]
        assert app.publish_is_ready(rows) is True

    def test_publish_disabled_when_mandatory_column_has_no_target(self):
        app = _import_app()
        for final in (None, "NO_MATCH"):
            rows = [{"mandatory_flag": True, "review_status": "REJECTED", "final_global_column_name": final}]
            assert app.publish_is_ready(rows) is False


class TestReviewActions:
    def test_record_action_calls_store_with_app_source(self):
        app = _import_app()
        connection = MagicMock()
        app.review_store.record_decision = MagicMock(return_value=True)

        result = app._record_review_action(
            connection,
            "ES_PROPERTY_RAW",
            "prima_bruta",
            "CORRECT",
            "analyst@example.com",
            final_global="gross_written_premium_eur",
            final_match_type="SEMANTIC_TRANSLATION",
            comment="Verified against source",
        )

        assert result is True
        app.review_store.record_decision.assert_called_once_with(
            connection,
            "ES_PROPERTY_RAW",
            "prima_bruta",
            "CORRECT",
            "analyst@example.com",
            final_global="gross_written_premium_eur",
            final_match_type="SEMANTIC_TRANSLATION",
            comment="Verified against source",
            source="DATABRICKS_APP",
        )

    def test_connect_retries_operational_error_twice(self):
        app = _import_app()
        connection = MagicMock()
        app.review_store.connect = MagicMock(side_effect=[OperationalError(), OperationalError(), connection])

        with (
            patch.object(app, "WorkspaceClient", return_value=MagicMock()),
            patch.object(app.time, "sleep") as sleep,
            patch.dict(
                os.environ,
                {
                    "LAKEBASE_ENDPOINT": "endpoint",
                    "PGHOST": "host",
                    "PGDATABASE": "database",
                    "PGUSER": "user",
                },
            ),
        ):
            result = app.connect_review_store()

        assert result is connection
        assert app.review_store.connect.call_count == 3
        assert [call.args[0] for call in sleep.call_args_list] == [1, 2]


class TestGenieHelpers:
    # Real SDK objects (imported before the autouse fixture mocks databricks.*) pin the response shape.
    def test_extract_text_only_message(self):
        app = _import_app()
        message = genie.GenieMessage(
            space_id="s",
            conversation_id="c",
            content="What is GWP?",  # the user's question, never the answer
            message_id="m",
            attachments=[genie.GenieAttachment(attachment_id="t1", text=genie.TextAttachment(content="GWP was 1.2m."))],
        )
        assert app.extract_genie_answer(message) == {"text": "GWP was 1.2m.", "sql": "", "attachment_id": None}

    def test_extract_sql_attachment(self):
        app = _import_app()
        message = genie.GenieMessage(
            space_id="s",
            conversation_id="c",
            content="Loss ratio by country?",
            message_id="m",
            attachments=[
                genie.GenieAttachment(
                    attachment_id="q1",
                    query=genie.GenieQueryAttachment(query="SELECT 1", description="Loss ratio per country."),
                )
            ],
        )
        assert app.extract_genie_answer(message) == {
            "text": "Loss ratio per country.",
            "sql": "SELECT 1",
            "attachment_id": "q1",
        }

    def test_extract_empty_message(self):
        app = _import_app()
        message = genie.GenieMessage(space_id="s", conversation_id="c", content="hi", message_id="m")
        assert app.extract_genie_answer(message) == {"text": "", "sql": "", "attachment_id": None}

    def test_ask_genie_follow_up_uses_same_conversation(self):
        app = _import_app()
        workspace = MagicMock()
        message = MagicMock()
        workspace.genie.create_message_and_wait.return_value = message

        result = app.ask_genie(workspace, "What is loss ratio?", "conversation-1")

        assert result == (message, "conversation-1")
        workspace.genie.create_message_and_wait.assert_called_once_with(
            space_id=app.GENIE_SPACE_ID,
            conversation_id="conversation-1",
            content="What is loss ratio?",
        )


class TestCompatibility:
    def test_match_type_options(self):
        app = _import_app()
        assert "DIRECT" in app.MATCH_TYPE_OPTIONS
        assert "NO_MATCH" in app.MATCH_TYPE_OPTIONS


class TestModuleSync:
    def test_app_review_store_matches_source(self):
        header = "# generated, edit src/harmonization/review_store.py\n"
        generated = APP_STORE.read_text()
        assert generated.startswith(header)
        assert generated[len(header) :] == SOURCE_STORE.read_text()
