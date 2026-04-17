"""Tests for the Streamlit review app helper functions.

These tests import only the pure-Python helpers from app.py by mocking
streamlit and databricks.sdk at import time.
"""

import sys
from unittest.mock import MagicMock, patch
import pytest


@pytest.fixture(autouse=True)
def mock_streamlit_and_sdk():
    """Mock streamlit and databricks.sdk so app.py can be imported without them installed."""
    mock_st = MagicMock()
    mock_st.cache_resource = lambda **kwargs: lambda f: f
    mock_st.cache_data = lambda **kwargs: lambda f: f

    mock_sdk = MagicMock()
    mock_state_enum = MagicMock()
    mock_state_enum.SUCCEEDED = "SUCCEEDED"
    mock_state_enum.FAILED = "FAILED"
    mock_state_enum.CANCELED = "CANCELED"
    mock_state_enum.CLOSED = "CLOSED"
    mock_sdk.service.sql.StatementState = mock_state_enum

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
    }

    with patch.dict(sys.modules, modules):
        # Remove cached import if any
        if "apps.column_mapping_review_app.app" in sys.modules:
            del sys.modules["apps.column_mapping_review_app.app"]
        yield mock_st, mock_sdk


def _import_app():
    """Import app module after mocks are in place."""
    sys.path.insert(0, ".")
    try:
        # Import the module directly
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "app",
            "apps/column_mapping_review_app/app.py",
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    finally:
        sys.path.pop(0)


class TestEscapeFunction:
    def test_escapes_single_quotes(self):
        app = _import_app()
        assert app._esc("O'Brien") == "O''Brien"

    def test_none_returns_empty(self):
        app = _import_app()
        assert app._esc(None) == ""

    def test_no_special_chars(self):
        app = _import_app()
        assert app._esc("hello") == "hello"

    def test_empty_string(self):
        app = _import_app()
        assert app._esc("") == ""


class TestConfidenceBadge:
    def test_high_confidence(self):
        app = _import_app()
        result = app.confidence_pill("HIGH")
        assert "HIGH" in result

    def test_medium_confidence(self):
        app = _import_app()
        result = app.confidence_pill("MEDIUM")
        assert "MEDIUM" in result

    def test_low_confidence(self):
        app = _import_app()
        result = app.confidence_pill("LOW")
        assert "LOW" in result

    def test_unknown_confidence(self):
        app = _import_app()
        result = app.confidence_pill("UNKNOWN")
        assert "UNKNOWN" in result


class TestConstants:
    def test_mandatory_columns_is_list(self):
        app = _import_app()
        assert isinstance(app.MANDATORY_COLUMNS, list)

    def test_match_type_options(self):
        app = _import_app()
        assert "DIRECT" in app.MATCH_TYPE_OPTIONS
        assert "NO_MATCH" in app.MATCH_TYPE_OPTIONS
