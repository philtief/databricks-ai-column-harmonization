"""
Column Mapping Review App
Streamlit application for reviewing AI-proposed column mappings in Databricks Apps.
Connects to Unity Catalog via environment variables (CATALOG_NAME, SCHEMA_NAME).
"""

import contextlib
import os
import time

import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CATALOG = os.environ.get("CATALOG_NAME", "pt_catalog")
SCHEMA = os.environ.get("SCHEMA_NAME", "harmonizing_agent")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
WORKFLOW_JOB_ID = os.environ.get("WORKFLOW_JOB_ID", "")

CANDIDATES_TABLE = f"{CATALOG}.{SCHEMA}.column_mapping_candidates"
AUDIT_TABLE = f"{CATALOG}.{SCHEMA}.column_mapping_audit"
GLOBAL_COLUMNS_TABLE = f"{CATALOG}.{SCHEMA}.global_target_columns"

# Mandatory columns are derived from the database at runtime (see _load_mandatory_columns_from_db).
# During unit tests (no DB connection), this list stays empty.
MANDATORY_COLUMNS: list[str] = []


def _load_mandatory_columns_from_db(ws_client, warehouse_id: str) -> list[str]:
    """Load mandatory columns by querying column_mapping_candidates where mandatory_flag = TRUE."""
    sql = (
        f"SELECT DISTINCT local_column_name FROM {CANDIDATES_TABLE}"
        " WHERE mandatory_flag = TRUE ORDER BY local_column_name"
    )
    try:
        resp = ws_client.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=sql, wait_timeout="30s"
        )
        if resp.status and resp.status.state == StatementState.SUCCEEDED and resp.result and resp.result.data_array:
            return [row[0] for row in resp.result.data_array if row[0]]
    except Exception:
        pass
    return []


MATCH_TYPE_OPTIONS = ["DIRECT", "SEMANTIC_TRANSLATION", "DERIVED", "NO_MATCH"]

# ---------------------------------------------------------------------------
# Theme constants
# ---------------------------------------------------------------------------
PRIMARY_BLUE = "#003781"
SECONDARY_BLUE = "#0078DA"
DARK_BLUE = "#003D63"
TEXT_PRIMARY = "#1A1A2E"
TEXT_SECONDARY = "#414141"
BG_LIGHT = "#F5F5F5"
BG_WHITE = "#FFFFFF"
BORDER_COLOR = "#E0E2E6"
SUCCESS_GREEN = "#10A251"
WARNING_ORANGE = "#E15200"
ERROR_RED = "#DC3149"
TEAL_ACCENT = "#00908D"


# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------
def inject_custom_css():
    st.markdown(
        f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@300;400;500;600;700&display=swap');

        /* --- Global typography --- */
        html, body, [class*="css"] {{
            font-family: 'Source Sans 3', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            color: {TEXT_PRIMARY};
        }}

        /* --- Page background --- */
        .stApp {{
            background-color: {BG_LIGHT};
        }}

        /* --- Sidebar --- */
        section[data-testid="stSidebar"] {{
            background-color: {PRIMARY_BLUE};
            border-right: none;
        }}
        section[data-testid="stSidebar"] * {{
            color: #FFFFFF !important;
        }}
        section[data-testid="stSidebar"] .stRadio label {{
            color: rgba(255, 255, 255, 0.85) !important;
            font-size: 0.95rem;
            font-weight: 400;
            padding: 6px 0;
        }}
        section[data-testid="stSidebar"] .stRadio label:hover {{
            color: #FFFFFF !important;
        }}
        section[data-testid="stSidebar"] .stRadio label[data-checked="true"],
        section[data-testid="stSidebar"] .stRadio [aria-checked="true"] + label {{
            color: #FFFFFF !important;
            font-weight: 600;
        }}
        section[data-testid="stSidebar"] hr {{
            border-color: rgba(255, 255, 255, 0.2);
        }}
        section[data-testid="stSidebar"] .stMarkdown p {{
            color: rgba(255, 255, 255, 0.9) !important;
        }}
        section[data-testid="stSidebar"] code {{
            color: rgba(255, 255, 255, 0.95) !important;
            background-color: rgba(255, 255, 255, 0.15) !important;
        }}

        /* --- Headers --- */
        h1 {{
            color: {PRIMARY_BLUE} !important;
            font-weight: 700 !important;
            font-size: 1.85rem !important;
            letter-spacing: -0.01em;
            border-bottom: 3px solid {PRIMARY_BLUE};
            padding-bottom: 0.5rem;
            margin-bottom: 1.5rem !important;
        }}
        h2 {{
            color: {DARK_BLUE} !important;
            font-weight: 600 !important;
            font-size: 1.35rem !important;
        }}
        h3 {{
            color: {TEXT_PRIMARY} !important;
            font-weight: 600 !important;
            font-size: 1.1rem !important;
        }}

        /* --- Metric cards --- */
        div[data-testid="stMetric"] {{
            background-color: {BG_WHITE};
            border: 1px solid {BORDER_COLOR};
            border-radius: 8px;
            padding: 16px 20px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04);
        }}
        div[data-testid="stMetric"] label {{
            color: {TEXT_SECONDARY} !important;
            font-size: 0.8rem !important;
            font-weight: 500 !important;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }}
        div[data-testid="stMetric"] [data-testid="stMetricValue"] {{
            color: {PRIMARY_BLUE} !important;
            font-size: 1.75rem !important;
            font-weight: 700 !important;
        }}

        /* --- Data tables --- */
        .stDataFrame {{
            border: 1px solid {BORDER_COLOR};
            border-radius: 8px;
            overflow: hidden;
        }}
        .stDataFrame thead th {{
            background-color: {PRIMARY_BLUE} !important;
            color: #FFFFFF !important;
            font-weight: 600 !important;
            font-size: 0.82rem !important;
            text-transform: uppercase;
            letter-spacing: 0.03em;
        }}

        /* --- Buttons --- */
        .stButton > button[kind="primary"],
        .stButton > button[data-testid="stBaseButton-primary"] {{
            background-color: {PRIMARY_BLUE} !important;
            border: none !important;
            border-radius: 6px !important;
            font-weight: 600 !important;
            font-size: 0.9rem !important;
            padding: 0.5rem 1.5rem !important;
            letter-spacing: 0.02em;
            transition: background-color 0.2s ease;
        }}
        .stButton > button[kind="primary"]:hover,
        .stButton > button[data-testid="stBaseButton-primary"]:hover {{
            background-color: {SECONDARY_BLUE} !important;
        }}
        .stButton > button[kind="secondary"],
        .stButton > button[data-testid="stBaseButton-secondary"] {{
            background-color: transparent !important;
            border: 1.5px solid {PRIMARY_BLUE} !important;
            color: {PRIMARY_BLUE} !important;
            border-radius: 6px !important;
            font-weight: 600 !important;
            transition: all 0.2s ease;
        }}
        .stButton > button[kind="secondary"]:hover,
        .stButton > button[data-testid="stBaseButton-secondary"]:hover {{
            background-color: {PRIMARY_BLUE} !important;
            color: #FFFFFF !important;
        }}

        /* --- Expanders --- */
        details[data-testid="stExpander"] {{
            background-color: {BG_WHITE};
            border: 1px solid {BORDER_COLOR};
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04);
        }}
        details[data-testid="stExpander"] summary {{
            font-weight: 600;
            color: {PRIMARY_BLUE};
        }}

        /* --- Forms --- */
        .stForm {{
            background-color: {BG_WHITE};
            border: 1px solid {BORDER_COLOR};
            border-radius: 8px;
            padding: 1rem;
        }}

        /* --- Alerts --- */
        .stAlert [data-testid="stAlertContentSuccess"] {{
            color: {SUCCESS_GREEN};
        }}
        .stAlert [data-testid="stAlertContentError"] {{
            color: {ERROR_RED};
        }}

        /* --- Selectbox / inputs --- */
        .stSelectbox > div > div,
        .stTextInput > div > div > input {{
            border-radius: 6px !important;
            border-color: {BORDER_COLOR} !important;
        }}
        .stSelectbox > div > div:focus-within,
        .stTextInput > div > div > input:focus {{
            border-color: {PRIMARY_BLUE} !important;
            box-shadow: 0 0 0 1px {PRIMARY_BLUE} !important;
        }}

        /* --- Dividers --- */
        hr {{
            border-color: {BORDER_COLOR};
        }}

        /* --- Bar chart --- */
        .stBarChart {{
            background-color: {BG_WHITE};
            border: 1px solid {BORDER_COLOR};
            border-radius: 8px;
            padding: 8px;
        }}

        /* --- Radio (main area) --- */
        .stRadio > div {{
            gap: 0.25rem;
        }}

        /* --- Hide Streamlit branding --- */
        #MainMenu {{visibility: hidden;}}
        footer {{visibility: hidden;}}
        header {{visibility: hidden;}}
    </style>
    """,
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# HTML helpers for professional badges
# ---------------------------------------------------------------------------


def confidence_pill(val: str) -> str:
    """Return an HTML confidence pill."""
    v = str(val).upper()
    colors = {
        "HIGH": (SUCCESS_GREEN, "#E8F5E9"),
        "MEDIUM": (WARNING_ORANGE, "#FFF3E0"),
        "LOW": (ERROR_RED, "#FFEBEE"),
    }
    fg, bg = colors.get(v, (TEXT_SECONDARY, BG_LIGHT))
    return (
        f'<span style="display:inline-block; padding:2px 10px; border-radius:12px; '
        f"font-size:0.78rem; font-weight:600; color:{fg}; background:{bg}; "
        f'border:1px solid {fg}22;">{v}</span>'
    )


def status_indicator(status: str) -> str:
    """Return a clean status label with colored dot."""
    s = str(status).upper()
    color_map = {
        "APPROVED": SUCCESS_GREEN,
        "CORRECTED": TEAL_ACCENT,
        "REJECTED": ERROR_RED,
        "PENDING": WARNING_ORANGE,
        "NOT_FOUND": TEXT_SECONDARY,
    }
    color = color_map.get(s, TEXT_SECONDARY)
    return (
        f'<span style="display:inline-flex; align-items:center; gap:6px; font-size:0.9rem;">'
        f'<span style="display:inline-block; width:8px; height:8px; border-radius:50%; '
        f'background:{color};"></span>'
        f'<span style="font-weight:500; color:{TEXT_PRIMARY};">{s}</span></span>'
    )


# ---------------------------------------------------------------------------
# Databricks SDK client (cached for the session)
# ---------------------------------------------------------------------------


@st.cache_resource(show_spinner=False)
def get_workspace_client() -> WorkspaceClient:
    """Return a WorkspaceClient. On Databricks Apps, auth is automatic."""
    return WorkspaceClient()


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------


def run_sql(statement: str) -> pd.DataFrame:
    """Execute a SELECT statement and return the results as a DataFrame.

    Polls StatementExecutionAPI until the query reaches a terminal state.
    Returns an empty DataFrame on any error.
    """
    if not WAREHOUSE_ID:
        st.error("DATABRICKS_WAREHOUSE_ID environment variable is not set. Please configure it in app.yaml.")
        return pd.DataFrame()

    try:
        client = get_workspace_client()
        response = client.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=WAREHOUSE_ID,
            wait_timeout="30s",
        )

        # Poll until terminal state
        max_polls = 60
        poll_interval = 2
        for _ in range(max_polls):
            state = response.status.state
            if state == StatementState.SUCCEEDED:
                break
            if state in (
                StatementState.FAILED,
                StatementState.CANCELED,
                StatementState.CLOSED,
            ):
                error_msg = getattr(response.status, "error", None)
                detail = getattr(error_msg, "message", str(error_msg)) if error_msg else "Unknown error"
                st.error(f"SQL execution failed: {detail}")
                return pd.DataFrame()
            time.sleep(poll_interval)
            response = client.statement_execution.get_statement(statement_id=response.statement_id)
        else:
            st.error("SQL query timed out after polling.")
            return pd.DataFrame()

        # Build DataFrame from result
        result = response.result
        if result is None or result.data_array is None:
            return pd.DataFrame()

        schema = response.manifest.schema
        columns = [col.name for col in schema.columns]
        rows = result.data_array
        return pd.DataFrame(rows, columns=columns)

    except Exception as exc:
        st.error(f"SQL error: {exc}")
        return pd.DataFrame()


def execute_dml(statement: str) -> int:
    """Execute an UPDATE/INSERT statement and return the affected row count.

    Returns -1 on error.
    """
    if not WAREHOUSE_ID:
        st.error("DATABRICKS_WAREHOUSE_ID environment variable is not set.")
        return -1

    try:
        client = get_workspace_client()
        response = client.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=WAREHOUSE_ID,
            wait_timeout="30s",
        )

        max_polls = 60
        poll_interval = 2
        for _ in range(max_polls):
            state = response.status.state
            if state == StatementState.SUCCEEDED:
                break
            if state in (
                StatementState.FAILED,
                StatementState.CANCELED,
                StatementState.CLOSED,
            ):
                error_msg = getattr(response.status, "error", None)
                detail = getattr(error_msg, "message", str(error_msg)) if error_msg else "Unknown error"
                st.error(f"DML execution failed: {detail}")
                return -1
            time.sleep(poll_interval)
            response = client.statement_execution.get_statement(statement_id=response.statement_id)
        else:
            st.error("DML query timed out after polling.")
            return -1

        # Attempt to read affected rows from result set if available
        result = response.result
        if result and result.data_array:
            try:
                return int(result.data_array[0][0])
            except (IndexError, TypeError, ValueError):
                pass
        return 0

    except Exception as exc:
        st.error(f"DML error: {exc}")
        return -1


def _esc(value: str) -> str:
    """Escape a string for use in a SQL single-quoted literal."""
    if value is None:
        return ""
    return str(value).replace("'", "''")


# ---------------------------------------------------------------------------
# Database write helpers
# ---------------------------------------------------------------------------


def _approve_mapping(local_column_name: str, user: str, comment: str = "") -> bool:
    """Approve an AI-proposed mapping."""
    col = _esc(local_column_name)
    usr = _esc(user)
    cmt = _esc(comment)

    audit_sql = f"""
    INSERT INTO {AUDIT_TABLE}
        (audit_id, local_column_name, old_review_status, new_review_status,
         old_global_column_name, new_global_column_name,
         old_match_type, new_match_type,
         action_by, action_at, action_comment, action_source)
    SELECT
        bigint(unix_micros(current_timestamp())),
        local_column_name, review_status, 'APPROVED',
        final_global_column_name, proposed_global_column_name,
        final_match_type, proposed_match_type,
        '{usr}', current_timestamp(), '{cmt}', 'DATABRICKS_APP'
    FROM {CANDIDATES_TABLE}
    WHERE local_column_name = '{col}' AND review_status = 'PENDING'
    """
    if execute_dml(audit_sql) == -1:
        return False

    update_sql = f"""
    UPDATE {CANDIDATES_TABLE}
    SET
        review_status            = 'APPROVED',
        final_global_column_name = proposed_global_column_name,
        final_match_type         = proposed_match_type,
        app_decision_source      = 'DATABRICKS_APP',
        review_comment           = '{cmt}',
        reviewed_by              = '{usr}',
        reviewed_at              = current_timestamp(),
        updated_at               = current_timestamp()
    WHERE local_column_name = '{col}'
      AND review_status = 'PENDING'
    """
    return execute_dml(update_sql) != -1


def _reject_mapping(local_column_name: str, user: str, comment: str) -> bool:
    """Reject a proposed mapping."""
    col = _esc(local_column_name)
    usr = _esc(user)
    cmt = _esc(comment)

    audit_sql = f"""
    INSERT INTO {AUDIT_TABLE}
        (audit_id, local_column_name, old_review_status, new_review_status,
         old_global_column_name, new_global_column_name,
         old_match_type, new_match_type,
         action_by, action_at, action_comment, action_source)
    SELECT
        bigint(unix_micros(current_timestamp())),
        local_column_name, review_status, 'REJECTED',
        final_global_column_name, final_global_column_name,
        final_match_type, 'NO_MATCH',
        '{usr}', current_timestamp(), '{cmt}', 'DATABRICKS_APP'
    FROM {CANDIDATES_TABLE}
    WHERE local_column_name = '{col}' AND review_status = 'PENDING'
    """
    if execute_dml(audit_sql) == -1:
        return False

    update_sql = f"""
    UPDATE {CANDIDATES_TABLE}
    SET
        review_status       = 'REJECTED',
        final_match_type    = 'NO_MATCH',
        app_decision_source = 'DATABRICKS_APP',
        review_comment      = '{cmt}',
        reviewed_by         = '{usr}',
        reviewed_at         = current_timestamp(),
        updated_at          = current_timestamp()
    WHERE local_column_name = '{col}'
      AND review_status = 'PENDING'
    """
    return execute_dml(update_sql) != -1


def _correct_mapping(
    local_column_name: str,
    new_global_col: str,
    new_match_type: str,
    user: str,
    comment: str,
) -> bool:
    """Override the AI proposal with a corrected mapping."""
    col = _esc(local_column_name)
    new_col = _esc(new_global_col)
    new_mt = _esc(new_match_type)
    usr = _esc(user)
    cmt = _esc(comment)

    audit_sql = f"""
    INSERT INTO {AUDIT_TABLE}
        (audit_id, local_column_name, old_review_status, new_review_status,
         old_global_column_name, new_global_column_name,
         old_match_type, new_match_type,
         action_by, action_at, action_comment, action_source)
    SELECT
        bigint(unix_micros(current_timestamp())),
        local_column_name, review_status, 'CORRECTED',
        final_global_column_name, '{new_col}',
        final_match_type, '{new_mt}',
        '{usr}', current_timestamp(), '{cmt}', 'DATABRICKS_APP'
    FROM {CANDIDATES_TABLE}
    WHERE local_column_name = '{col}'
    """
    if execute_dml(audit_sql) == -1:
        return False

    update_sql = f"""
    UPDATE {CANDIDATES_TABLE}
    SET
        review_status            = 'CORRECTED',
        final_global_column_name = '{new_col}',
        final_match_type         = '{new_mt}',
        app_decision_source      = 'DATABRICKS_APP',
        review_comment           = '{cmt}',
        reviewed_by              = '{usr}',
        reviewed_at              = current_timestamp(),
        updated_at               = current_timestamp()
    WHERE local_column_name = '{col}'
    """
    return execute_dml(update_sql) != -1


def _reset_to_pending(local_column_name: str, user: str, from_status: str = "REJECTED") -> bool:
    """Reset a mapping back to PENDING for re-review."""
    col = _esc(local_column_name)
    usr = _esc(user)
    fs = _esc(from_status)

    audit_sql = f"""
    INSERT INTO {AUDIT_TABLE}
        (audit_id, local_column_name, old_review_status, new_review_status,
         old_global_column_name, new_global_column_name,
         old_match_type, new_match_type,
         action_by, action_at, action_comment, action_source)
    SELECT
        bigint(unix_micros(current_timestamp())),
        local_column_name, review_status, 'PENDING',
        final_global_column_name, NULL,
        final_match_type, NULL,
        '{usr}', current_timestamp(), 'Reset to pending for re-review', 'DATABRICKS_APP'
    FROM {CANDIDATES_TABLE}
    WHERE local_column_name = '{col}' AND review_status = '{fs}'
    """
    if execute_dml(audit_sql) == -1:
        return False

    update_sql = f"""
    UPDATE {CANDIDATES_TABLE}
    SET
        review_status            = 'PENDING',
        final_global_column_name = NULL,
        final_match_type         = NULL,
        app_decision_source      = NULL,
        review_comment           = NULL,
        reviewed_by              = NULL,
        reviewed_at              = NULL,
        updated_at               = current_timestamp()
    WHERE local_column_name = '{col}'
      AND review_status = '{fs}'
    """
    return execute_dml(update_sql) != -1


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------


@st.cache_data(ttl=30, show_spinner=False)
def load_candidates() -> pd.DataFrame:
    """Load all rows from column_mapping_candidates."""
    return run_sql(f"SELECT * FROM {CANDIDATES_TABLE}")


@st.cache_data(ttl=30, show_spinner=False)
def load_global_columns() -> list[str]:
    """Load the list of available global target column names."""
    df = run_sql(f"SELECT global_column_name FROM {GLOBAL_COLUMNS_TABLE} ORDER BY global_column_name")
    if df.empty or "global_column_name" not in df.columns:
        return []
    return df["global_column_name"].tolist()


@st.cache_data(ttl=30, show_spinner=False)
def load_status_summary() -> pd.DataFrame:
    """Load count by review_status for the sidebar summary."""
    return run_sql(f"""
        SELECT review_status, COUNT(*) AS count
        FROM {CANDIDATES_TABLE}
        GROUP BY review_status
        ORDER BY review_status
        """)


def invalidate_caches():
    """Clear all cached data to force a fresh reload."""
    load_candidates.clear()
    load_global_columns.clear()
    load_status_summary.clear()


# ---------------------------------------------------------------------------
# User identity
# ---------------------------------------------------------------------------


def get_current_user() -> str:
    """Resolve the current user from request headers or environment."""
    try:
        # Streamlit >= 1.37 exposes request headers
        user = st.context.headers.get("X-Forwarded-User", "")
        if user:
            return user
    except AttributeError:
        pass
    return os.environ.get("DATABRICKS_APP_CURRENT_USER_NAME", "app-service-principal")


# ---------------------------------------------------------------------------
# Page renderers
# ---------------------------------------------------------------------------


def page_pending_review(user: str):
    st.header("Pending Review Queue")

    df_all = load_candidates()

    if df_all.empty:
        st.info("No proposals found. Run the AI mapping task first to populate the candidates table.")
        return

    if "review_status" not in df_all.columns:
        st.warning("Unexpected table schema — 'review_status' column not found.")
        return

    df_pending = df_all[df_all["review_status"] == "PENDING"].copy()

    # --- Summary metrics ---
    total_pending = len(df_pending)
    mandatory_pending = (
        df_pending[df_pending["local_column_name"].isin(MANDATORY_COLUMNS)]
        if "local_column_name" in df_pending.columns
        else pd.DataFrame()
    )
    mandatory_pending_count = len(mandatory_pending)

    high_conf = 0
    ai_errors = 0
    if "confidence" in df_pending.columns:
        high_conf = int((df_pending["confidence"].str.upper() == "HIGH").sum())
    if "ai_error_status" in df_pending.columns:
        ai_errors = int(df_pending["ai_error_status"].notna().sum())

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Pending", total_pending)
    col2.metric("Mandatory Pending", mandatory_pending_count)
    col3.metric("High Confidence", high_conf)
    col4.metric("AI Errors", ai_errors)

    if df_pending.empty:
        st.success("All mappings have been reviewed.")
        return

    # --- Display columns ---
    display_cols = [
        c
        for c in [
            "local_column_name",
            "local_data_type",
            "local_sample_values",
            "proposed_global_column_name",
            "proposed_match_type",
            "mapping_rationale",
            "confidence",
            "mandatory_flag",
            "ai_error_status",
        ]
        if c in df_pending.columns
    ]

    display_df = df_pending[display_cols].copy()

    # Normalise sample values to comma-separated string for display
    if "local_sample_values" in display_df.columns:
        display_df["local_sample_values"] = display_df["local_sample_values"].apply(
            lambda v: ", ".join(v) if isinstance(v, list) else str(v) if pd.notna(v) else ""
        )

    column_config = {}
    if "mandatory_flag" in display_df.columns:
        column_config["mandatory_flag"] = st.column_config.CheckboxColumn(
            "Mandatory", help="Whether this column is mandatory for publish readiness"
        )
    if "confidence" in display_df.columns:
        column_config["confidence"] = st.column_config.TextColumn("Confidence")
    if "mapping_rationale" in display_df.columns:
        column_config["mapping_rationale"] = st.column_config.TextColumn("Rationale", width="large")
    if "local_sample_values" in display_df.columns:
        column_config["local_sample_values"] = st.column_config.TextColumn("Sample Values", width="medium")

    st.dataframe(display_df, use_container_width=True, column_config=column_config)

    # --- Review section ---
    with st.expander("Review a mapping", expanded=True):
        column_names = df_pending["local_column_name"].tolist() if "local_column_name" in df_pending.columns else []
        if not column_names:
            st.info("No pending columns to review.")
            return

        selected_col = st.selectbox(
            "Select column to review",
            column_names,
            key="pending_selected_col",
        )

        if selected_col:
            row = df_pending[df_pending["local_column_name"] == selected_col].iloc[0]

            st.subheader(f"Details: `{selected_col}`")
            detail_col1, detail_col2 = st.columns(2)
            with detail_col1:
                st.markdown(f"**Data Type:** {row.get('local_data_type', 'N/A')}")
                st.markdown(f"**Sample Values:** {row.get('local_sample_values', 'N/A')}")
                mandatory = "Yes" if row.get("mandatory_flag") else "No"
                st.markdown(f"**Mandatory:** {mandatory}")
                st.markdown(f"**AI Error:** {row.get('ai_error_status', 'None')}")
            with detail_col2:
                st.markdown(f"**Proposed Global Column:** `{row.get('proposed_global_column_name', 'N/A')}`")
                st.markdown(f"**Proposed Match Type:** {row.get('proposed_match_type', 'N/A')}")
                conf = str(row.get("confidence", "N/A"))
                st.markdown(f"**Confidence:** {conf}", unsafe_allow_html=True)
                st.markdown(f"**Rationale:** {row.get('mapping_rationale', 'N/A')}")

            st.divider()
            action_col1, action_col2, action_col3 = st.columns(3)

            # ---- APPROVE ----
            with action_col1:
                approve_comment = st.text_input(
                    "Approve comment (optional)",
                    key=f"approve_comment_{selected_col}",
                )
                if st.button("Approve", key=f"approve_{selected_col}", type="primary"):
                    with st.spinner("Approving..."):
                        ok = _approve_mapping(selected_col, user, approve_comment)
                    if ok:
                        st.success(f"Approved: `{selected_col}`")
                        invalidate_caches()
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error("Approval failed. Check the error above.")

            # ---- REJECT ----
            with action_col2:
                reject_comment = st.text_input(
                    "Rejection reason (required)",
                    key=f"reject_comment_{selected_col}",
                )
                if st.button("Reject", key=f"reject_{selected_col}"):
                    if not reject_comment.strip():
                        st.warning("A rejection reason is required.")
                    else:
                        with st.spinner("Rejecting..."):
                            ok = _reject_mapping(selected_col, user, reject_comment)
                        if ok:
                            st.success(f"Rejected: `{selected_col}`")
                            invalidate_caches()
                            time.sleep(0.5)
                            st.rerun()
                        else:
                            st.error("Rejection failed. Check the error above.")

            # ---- CORRECT ----
            with action_col3:
                st.markdown("**Correct Mapping**")
                with st.form(key=f"correct_form_{selected_col}"):
                    global_cols = load_global_columns()
                    if not global_cols:
                        global_cols = ["(no global columns available)"]

                    new_global_col = st.selectbox(
                        "New global column",
                        global_cols,
                        key=f"new_global_{selected_col}",
                    )
                    new_match_type = st.selectbox(
                        "New match type",
                        MATCH_TYPE_OPTIONS,
                        key=f"new_match_{selected_col}",
                    )
                    correct_comment = st.text_input(
                        "Comment (required)",
                        key=f"correct_comment_{selected_col}",
                    )
                    submitted = st.form_submit_button("Submit Correction")

                if submitted:
                    if not correct_comment.strip():
                        st.warning("A comment is required for corrections.")
                    elif new_global_col == "(no global columns available)":
                        st.warning("Global columns list is empty. Cannot submit correction.")
                    else:
                        with st.spinner("Submitting correction..."):
                            ok = _correct_mapping(
                                selected_col,
                                new_global_col,
                                new_match_type,
                                user,
                                correct_comment,
                            )
                        if ok:
                            st.success(f"Corrected: `{selected_col}`")
                            invalidate_caches()
                            time.sleep(0.5)
                            st.rerun()
                        else:
                            st.error("Correction failed. Check the error above.")


def page_approved_mappings(user: str):
    st.header("Approved & Corrected Mappings")

    df_all = load_candidates()

    if df_all.empty:
        st.info("No proposals found. Run the AI mapping task first.")
        return

    if "review_status" not in df_all.columns:
        st.warning("Unexpected table schema.")
        return

    df = df_all[df_all["review_status"].isin(["APPROVED", "CORRECTED"])].copy()
    st.markdown(f"**{len(df)} mapping(s) approved or corrected**")

    if df.empty:
        st.info("No approved or corrected mappings yet.")
        return

    display_cols = [
        c
        for c in [
            "local_column_name",
            "final_global_column_name",
            "final_match_type",
            "review_status",
            "reviewed_by",
            "reviewed_at",
            "review_comment",
            "app_decision_source",
        ]
        if c in df.columns
    ]

    st.dataframe(df[display_cols], use_container_width=True)

    st.divider()
    st.subheader("Move back to Pending")

    col_names = df["local_column_name"].tolist() if "local_column_name" in df.columns else []
    if not col_names:
        return

    unapprove_col = st.selectbox("Select column to move back to pending", col_names, key="unapprove_col")
    if unapprove_col:
        current_status = df.loc[df["local_column_name"] == unapprove_col, "review_status"].iloc[0]
        if st.button("Reset to Pending", key="unapprove_btn"):
            with st.spinner("Resetting..."):
                ok = _reset_to_pending(unapprove_col, user, from_status=current_status)
            if ok:
                st.success(f"Reset to PENDING: `{unapprove_col}`")
                invalidate_caches()
                time.sleep(0.5)
                st.rerun()
            else:
                st.error("Reset failed. Check the error above.")


def page_rejected_mappings(user: str):
    st.header("Rejected Mappings")

    df_all = load_candidates()

    if df_all.empty:
        st.info("No proposals found. Run the AI mapping task first.")
        return

    if "review_status" not in df_all.columns:
        st.warning("Unexpected table schema.")
        return

    df = df_all[df_all["review_status"] == "REJECTED"].copy()
    st.markdown(f"**{len(df)} mapping(s) rejected**")

    if df.empty:
        st.info("No rejected mappings.")
        return

    display_cols = [
        c
        for c in [
            "local_column_name",
            "proposed_global_column_name",
            "final_global_column_name",
            "final_match_type",
            "reviewed_by",
            "reviewed_at",
            "review_comment",
            "app_decision_source",
        ]
        if c in df.columns
    ]

    st.dataframe(df[display_cols], use_container_width=True)

    st.divider()
    st.subheader("Reconsider a rejection")

    col_names = df["local_column_name"].tolist() if "local_column_name" in df.columns else []
    if not col_names:
        return

    reconsider_col = st.selectbox("Select column to reconsider", col_names, key="reconsider_col")
    if st.button("Reset to Pending", key="reconsider_btn"):
        with st.spinner("Resetting..."):
            ok = _reset_to_pending(reconsider_col, user)
        if ok:
            st.success(f"Reset to PENDING: `{reconsider_col}`")
            invalidate_caches()
            time.sleep(0.5)
            st.rerun()
        else:
            st.error("Reset failed. Check the error above.")


def page_dashboard():
    st.header("Review Dashboard")

    df_all = load_candidates()

    if df_all.empty:
        st.info("No proposals found. Run the AI mapping task first.")
        return

    if "review_status" not in df_all.columns:
        st.warning("Unexpected table schema.")
        return

    total = len(df_all)
    approved = int((df_all["review_status"].isin(["APPROVED", "CORRECTED"])).sum())
    rejected = int((df_all["review_status"] == "REJECTED").sum())
    pending = int((df_all["review_status"] == "PENDING").sum())
    coverage_pct = round(approved / total * 100, 1) if total > 0 else 0.0

    # --- Top metrics ---
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Columns", total)
    c2.metric("Approved / Corrected", approved)
    c3.metric("Rejected", rejected)
    c4.metric("Pending", pending)

    st.metric("Coverage", f"{coverage_pct}%", help="(Approved + Corrected) / Total")

    st.divider()

    # --- Mandatory status ---
    st.subheader("Mandatory Columns Status")
    if "local_column_name" in df_all.columns:
        mandatory_df = df_all[df_all["local_column_name"].isin(MANDATORY_COLUMNS)].copy()
        mandatory_approved = int((mandatory_df["review_status"].isin(["APPROVED", "CORRECTED"])).sum())
        mandatory_pending_count = int((mandatory_df["review_status"].isin(["PENDING", "REJECTED"])).sum())
        mc1, mc2 = st.columns(2)
        mc1.metric("Mandatory Resolved", f"{mandatory_approved} / {len(MANDATORY_COLUMNS)}")
        mc2.metric("Mandatory Still Pending/Rejected", mandatory_pending_count)
    else:
        st.info("Column 'local_column_name' not found in table.")

    st.divider()

    # --- Status distribution bar chart ---
    st.subheader("Count by Review Status")
    status_counts = (
        df_all["review_status"]
        .value_counts()
        .reset_index()
        .rename(columns={"index": "review_status", "review_status": "count", "count": "count"})
    )
    # Handle both pandas versions
    if "review_status" in status_counts.columns and "count" in status_counts.columns:
        chart_df = status_counts.set_index("review_status")
    else:
        chart_df = df_all["review_status"].value_counts().to_frame(name="count")

    st.bar_chart(chart_df)

    st.divider()

    # --- Confidence distribution ---
    if "confidence" in df_all.columns:
        st.subheader("Confidence Distribution by Review Status")
        conf_dist = (
            df_all.groupby(["confidence", "review_status"])
            .size()
            .reset_index(name="count")
            .pivot(index="confidence", columns="review_status", values="count")
            .fillna(0)
            .astype(int)
        )
        st.dataframe(conf_dist, use_container_width=True)


def page_publish_readiness():
    st.header("Publish Readiness")

    df_all = load_candidates()

    if df_all.empty:
        st.info("No proposals found. Run the AI mapping task first to populate the candidates table.")
        return

    if "review_status" not in df_all.columns or "local_column_name" not in df_all.columns:
        st.warning("Unexpected table schema — required columns not found.")
        return

    # Build a status lookup
    status_lookup = dict(zip(df_all["local_column_name"], df_all["review_status"], strict=False))

    # Evaluate mandatory columns
    all_mandatory_resolved = True
    mandatory_statuses = []
    for col in MANDATORY_COLUMNS:
        status = status_lookup.get(col, "NOT_FOUND")
        resolved = status in ("APPROVED", "CORRECTED")
        if not resolved:
            all_mandatory_resolved = False
        mandatory_statuses.append({"column": col, "status": status, "resolved": resolved})

    # --- Banner ---
    if all_mandatory_resolved:
        st.success(
            "READY TO PUBLISH\n\n"
            f"All {len(MANDATORY_COLUMNS)} mandatory columns have been approved or corrected. "
            "You can now re-run the workflow from task **column_mapping_review_gate**."
        )
    else:
        unresolved = sum(1 for m in mandatory_statuses if not m["resolved"])
        st.error(f"BLOCKED\n\n{unresolved} mandatory column(s) are not yet resolved (must be APPROVED or CORRECTED).")

    st.divider()

    # --- Mandatory columns detail ---
    st.subheader("Mandatory Columns")
    for item in mandatory_statuses:
        status = item["status"]
        col_name = item["column"]
        st.markdown(f"{status_indicator(status)} &nbsp; `{col_name}`", unsafe_allow_html=True)

    st.divider()

    # --- Non-mandatory summary ---
    st.subheader("Non-Mandatory Columns")
    non_mandatory_df = df_all[~df_all["local_column_name"].isin(MANDATORY_COLUMNS)]
    total_nm = len(non_mandatory_df)
    if total_nm > 0:
        nm_counts = non_mandatory_df["review_status"].value_counts()
        nm_approved = int(nm_counts.get("APPROVED", 0)) + int(nm_counts.get("CORRECTED", 0))
        nm_pending = int(nm_counts.get("PENDING", 0))
        nm_rejected = int(nm_counts.get("REJECTED", 0))
        nm_col1, nm_col2, nm_col3 = st.columns(3)
        nm_col1.metric("Resolved", nm_approved)
        nm_col2.metric("Pending", nm_pending)
        nm_col3.metric("Rejected", nm_rejected)

        status_order = {"PENDING": 0, "REJECTED": 1, "APPROVED": 2, "CORRECTED": 2}
        nm_sorted = non_mandatory_df.sort_values(
            by=["review_status", "local_column_name"],
            key=lambda s: s.map(status_order) if s.name == "review_status" else s,
        )
        for _, row in nm_sorted.iterrows():
            status = row["review_status"]
            col_name = row["local_column_name"]
            target = row.get("final_global_column_name") or row.get("proposed_global_column_name") or "—"
            label = f"{status_indicator(status)} &nbsp; `{col_name}` &nbsp;→&nbsp; `{target}`"
            st.markdown(label, unsafe_allow_html=True)
    else:
        st.info("No non-mandatory columns found.")

    st.divider()

    # --- Workflow trigger ---
    st.subheader("Trigger Harmonization Workflow")

    if not WORKFLOW_JOB_ID:
        st.warning("WORKFLOW_JOB_ID environment variable is not set. Cannot trigger workflow.")
    elif not all_mandatory_resolved:
        st.info(
            "All mandatory columns must be approved or corrected before the workflow can be triggered. "
            f"{sum(1 for m in mandatory_statuses if not m['resolved'])} column(s) still pending."
        )
    else:
        st.success(
            "All mandatory columns are resolved. You can now publish the harmonized output "
            "by triggering the workflow below."
        )

        if st.button("Trigger Harmonization Workflow", type="primary", key="trigger_workflow_btn"):
            with st.spinner("Triggering workflow..."):
                try:
                    client = get_workspace_client()
                    run = client.jobs.run_now(job_id=int(WORKFLOW_JOB_ID))
                    run_id = run.run_id
                    host = client.config.host.rstrip("/")
                    run_url = f"{host}/#job/{WORKFLOW_JOB_ID}/run/{run_id}"
                    st.session_state["last_triggered_run_id"] = run_id
                    st.session_state["last_triggered_run_url"] = run_url
                except Exception as exc:
                    st.error(f"Failed to trigger workflow: {exc}")

        if "last_triggered_run_id" in st.session_state:
            run_id = st.session_state["last_triggered_run_id"]
            run_url = st.session_state.get("last_triggered_run_url", "")
            st.success(f"Workflow triggered — Run ID: `{run_id}`")
            if run_url:
                st.markdown(f"[Open run in Databricks]({run_url})")


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------


def main():
    global MANDATORY_COLUMNS
    st.set_page_config(
        page_title="Column Mapping Review",
        page_icon="data:image/svg+xml,"
        "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'>"
        "<rect fill='%23003781' rx='4' width='24' height='24'/>"
        "<path fill='white' d='M7 8h10v2H7zm0 3h10v2H7zm0 3h7v2H7z'/></svg>",
        layout="wide",
    )

    inject_custom_css()

    # Load mandatory columns from database (once per session)
    if not MANDATORY_COLUMNS and WAREHOUSE_ID:
        with contextlib.suppress(Exception):
            MANDATORY_COLUMNS = _load_mandatory_columns_from_db(get_workspace_client(), WAREHOUSE_ID)

    user = get_current_user()

    # --- Sidebar ---
    with st.sidebar:
        st.markdown(
            """
            <div style="padding: 0 0 12px 0;">
                <div style="font-size: 1.4rem; font-weight: 700; letter-spacing: -0.01em;
                            color: #FFFFFF; line-height: 1.2;">
                    Column Mapping<br/>Review
                </div>
                <div style="font-size: 0.78rem; color: rgba(255,255,255,0.6);
                            margin-top: 4px; letter-spacing: 0.03em; text-transform: uppercase;">
                    Data Harmonization
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div style="font-size:0.85rem; color:rgba(255,255,255,0.7);">'
            f'<span style="font-weight:600; color:#FFFFFF;">User:</span> '
            f'<span style="color:rgba(255,255,255,0.9);">{user}</span></div>',
            unsafe_allow_html=True,
        )
        st.divider()

        # Status summary
        st.markdown("**Review Status**")
        try:
            summary_df = load_status_summary()
            if not summary_df.empty and "review_status" in summary_df.columns and "count" in summary_df.columns:
                for _, row in summary_df.iterrows():
                    st.markdown(
                        f"{status_indicator(row['review_status'])} &nbsp; **{row['count']}**",
                        unsafe_allow_html=True,
                    )
            else:
                st.caption("No data available yet.")
        except Exception:
            st.caption("Could not load status summary.")

        st.divider()

        page = st.radio(
            "Navigate",
            [
                "Pending Review Queue",
                "Approved Mappings",
                "Rejected Mappings",
                "Review Dashboard",
                "Publish Readiness",
            ],
            key="nav_page",
        )

    # --- Main area ---
    if page == "Pending Review Queue":
        page_pending_review(user)
    elif page == "Approved Mappings":
        page_approved_mappings(user)
    elif page == "Rejected Mappings":
        page_rejected_mappings(user)
    elif page == "Review Dashboard":
        page_dashboard()
    elif page == "Publish Readiness":
        page_publish_readiness()


if __name__ == "__main__":
    main()
