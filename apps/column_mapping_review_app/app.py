"""
Column Mapping Review App
Streamlit application for reviewing AI-proposed column mappings in Databricks Apps.
Review state is served from Lakebase; analytics reads remain on Unity Catalog.
"""

from __future__ import annotations

import contextlib
import os
import time
from typing import Any

import pandas as pd
import review_store
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

CATALOG = os.environ.get("CATALOG_NAME", "agent_marketplace_catalog")
SCHEMA = os.environ.get("SCHEMA_NAME", "halvard_harmonization")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
PUBLISH_JOB_ID = os.environ.get("PUBLISH_JOB_ID", "")
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")

GLOBAL_COLUMNS_TABLE = f"{CATALOG}.{SCHEMA}.global_target_columns"
HARMONIZED_TABLE = f"{CATALOG}.{SCHEMA}.harmonized_property_monthly"
METRIC_VIEW_TABLE = f"{CATALOG}.{SCHEMA}.mv_group_property_kpis"
METRIC_VIEW_QUERY = f"""
SELECT `Country`,
       MEASURE(`Gross Written Premium`) AS gwp,
       MEASURE(`Loss Ratio`) AS loss_ratio,
       MEASURE(`Combined Ratio`) AS combined_ratio
FROM {METRIC_VIEW_TABLE}
GROUP BY ALL
"""
EVALUATION_QUERY = f"""
SELECT source_system, metric_name, metric_value
FROM (
    SELECT source_system, metric_name, metric_value,
           ROW_NUMBER() OVER (
               PARTITION BY source_system, metric_name ORDER BY evaluated_at DESC
           ) AS metric_rank
    FROM {CATALOG}.{SCHEMA}.mapping_eval_results
    WHERE slice = 'ALL'
)
WHERE metric_rank = 1
"""

COUNTRY_SOURCES = {"ES": "ES_PROPERTY_RAW", "IT": "IT_PROPERTY_RAW"}
RESOLVED_STATUSES = {"APPROVED", "CORRECTED"}
MATCH_TYPE_OPTIONS = ["DIRECT", "SEMANTIC_TRANSLATION", "DERIVED", "NO_MATCH"]
MANDATORY_COLUMNS: list[str] = []
SAMPLE_QUESTIONS = [
    "What was gross written premium by country in the latest period?",
    "Which country has the highest loss ratio?",
    "How many claims were reported by country and month?",
    "Show combined ratio by distribution channel.",
]

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


def inject_custom_css():
    st.markdown(
        f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@300;400;500;600;700&display=swap');
        html, body, [class*="css"] {{
            font-family: 'Source Sans 3', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            color: {TEXT_PRIMARY};
        }}
        .stApp {{ background-color: {BG_LIGHT}; }}
        section[data-testid="stSidebar"] {{
            background-color: {PRIMARY_BLUE};
            border-right: none;
        }}
        section[data-testid="stSidebar"] * {{ color: #FFFFFF !important; }}
        section[data-testid="stSidebar"] .stRadio label {{
            color: rgba(255, 255, 255, 0.85) !important;
            font-size: 0.95rem;
            font-weight: 400;
            padding: 6px 0;
        }}
        section[data-testid="stSidebar"] .stRadio label:hover {{ color: #FFFFFF !important; }}
        section[data-testid="stSidebar"] .stRadio label[data-checked="true"],
        section[data-testid="stSidebar"] .stRadio [aria-checked="true"] + label {{
            color: #FFFFFF !important;
            font-weight: 600;
        }}
        section[data-testid="stSidebar"] hr {{ border-color: rgba(255, 255, 255, 0.2); }}
        section[data-testid="stSidebar"] .stMarkdown p {{ color: rgba(255, 255, 255, 0.9) !important; }}
        section[data-testid="stSidebar"] code {{
            color: rgba(255, 255, 255, 0.95) !important;
            background-color: rgba(255, 255, 255, 0.15) !important;
        }}
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
        h3 {{ color: {TEXT_PRIMARY} !important; font-weight: 600 !important; font-size: 1.1rem !important; }}
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
        details[data-testid="stExpander"] {{
            background-color: {BG_WHITE};
            border: 1px solid {BORDER_COLOR};
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04);
        }}
        details[data-testid="stExpander"] summary {{ font-weight: 600; color: {PRIMARY_BLUE}; }}
        .stForm {{
            background-color: {BG_WHITE};
            border: 1px solid {BORDER_COLOR};
            border-radius: 8px;
            padding: 1rem;
        }}
        .stAlert [data-testid="stAlertContentSuccess"] {{ color: {SUCCESS_GREEN}; }}
        .stAlert [data-testid="stAlertContentError"] {{ color: {ERROR_RED}; }}
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
        hr {{ border-color: {BORDER_COLOR}; }}
        .stBarChart {{
            background-color: {BG_WHITE};
            border: 1px solid {BORDER_COLOR};
            border-radius: 8px;
            padding: 8px;
        }}
        .stRadio > div {{ gap: 0.25rem; }}
        #MainMenu {{ visibility: hidden; }}
        footer {{ visibility: hidden; }}
        header {{ visibility: hidden; }}
    </style>
    """,
        unsafe_allow_html=True,
    )


def status_indicator(status: str) -> str:
    """Return a clean status label with colored dot."""
    normalized = str(status).upper()
    color_map = {
        "APPROVED": SUCCESS_GREEN,
        "CORRECTED": TEAL_ACCENT,
        "REJECTED": ERROR_RED,
        "PENDING": WARNING_ORANGE,
        "NOT_FOUND": TEXT_SECONDARY,
    }
    color = color_map.get(normalized, TEXT_SECONDARY)
    return (
        '<span style="display:inline-flex; align-items:center; gap:6px; font-size:0.9rem;">'
        '<span style="display:inline-block; width:8px; height:8px; border-radius:50%; '
        f'background:{color};"></span>'
        f'<span style="font-weight:500; color:{TEXT_PRIMARY};">{normalized}</span></span>'
    )


@st.cache_resource(show_spinner=False)
def get_workspace_client() -> WorkspaceClient:
    """Return a WorkspaceClient. On Databricks Apps, auth is automatic."""
    return WorkspaceClient()


def connect_review_store():
    """Open a Lakebase connection, retrying while a scale-to-zero endpoint wakes."""
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            return review_store.connect(
                WorkspaceClient(),
                os.environ["LAKEBASE_ENDPOINT"],
                host=os.environ["PGHOST"],
                dbname=os.environ["PGDATABASE"],
                user=os.environ["PGUSER"],
            )
        except review_store.psycopg.OperationalError as exc:
            last_error = exc
            if attempt == 2:
                raise
            time.sleep(2**attempt)
    raise last_error


@st.cache_resource(show_spinner=False)
def get_review_connection():
    """Return the cached Lakebase connection and create the review schema once."""
    connection = connect_review_store()
    try:
        review_store.ensure_schema(connection)
    except review_store.psycopg.OperationalError:
        with contextlib.suppress(Exception):
            connection.close()
        get_review_connection.clear()
        connection = connect_review_store()
        review_store.ensure_schema(connection)
    return connection


def call_review_store(store_function, *args, **kwargs):
    """Call a review-store function, refreshing an expired cached connection."""
    for attempt in range(2):
        connection = get_review_connection()
        try:
            return store_function(connection, *args, **kwargs)
        except review_store.psycopg.OperationalError:
            if attempt == 1:
                raise
            with contextlib.suppress(Exception):
                connection.close()
            get_review_connection.clear()
    raise RuntimeError("Unreachable review-store retry state")


def run_sql(statement: str, quiet: bool = False) -> pd.DataFrame:
    """Execute a SELECT statement and return the results as a DataFrame."""
    if not WAREHOUSE_ID:
        if not quiet:
            st.error("DATABRICKS_WAREHOUSE_ID environment variable is not set.")
        return pd.DataFrame()
    try:
        client = get_workspace_client()
        response = client.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=WAREHOUSE_ID,
            wait_timeout="30s",
        )
        for _ in range(60):
            state = response.status.state
            if state == StatementState.SUCCEEDED:
                break
            if state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
                error = getattr(response.status, "error", None)
                detail = getattr(error, "message", str(error)) if error else "Unknown error"
                if not quiet:
                    st.error(f"SQL execution failed: {detail}")
                return pd.DataFrame()
            time.sleep(2)
            response = client.statement_execution.get_statement(statement_id=response.statement_id)
        else:
            if not quiet:
                st.error("SQL query timed out after polling.")
            return pd.DataFrame()

        result = response.result
        if result is None or result.data_array is None:
            return pd.DataFrame()
        columns = [column.name for column in response.manifest.schema.columns]
        return pd.DataFrame(result.data_array, columns=columns)
    except Exception as exc:
        if not quiet:
            st.error(f"SQL error: {exc}")
        return pd.DataFrame()


def load_optional_sql(statement: str) -> pd.DataFrame:
    """Load analytics data without showing an error when a table is absent."""
    return run_sql(statement, quiet=True)


@st.cache_data(ttl=15, show_spinner=False)
def load_queue(source_system: str, status: str | None = None) -> list[dict[str, Any]]:
    """Load review rows from Lakebase for one source system."""
    return call_review_store(review_store.fetch_queue, source_system=source_system, status=status)


@st.cache_data(ttl=15, show_spinner=False)
def load_review_summary() -> list[dict[str, Any]]:
    """Load review counts from Lakebase."""
    return call_review_store(review_store.status_summary)


@st.cache_data(ttl=120, show_spinner=False)
def load_global_columns() -> list[str]:
    """Load global target columns from Unity Catalog."""
    frame = run_sql(f"SELECT global_column_name FROM {GLOBAL_COLUMNS_TABLE} ORDER BY global_column_name")
    if frame.empty or "global_column_name" not in frame.columns:
        return []
    return frame["global_column_name"].tolist()


def invalidate_review_caches():
    """Clear cached review data after a decision."""
    load_queue.clear()
    load_review_summary.clear()


def _record_review_action(
    conn,
    source_system: str,
    local_column_name: str,
    action: str,
    user: str,
    final_global: str | None = None,
    final_match_type: str | None = None,
    comment: str = "",
):
    """Apply one review action through the tested Lakebase store."""
    return review_store.record_decision(
        conn,
        source_system,
        local_column_name,
        action,
        user,
        final_global=final_global,
        final_match_type=final_match_type,
        comment=comment,
        source="DATABRICKS_APP",
    )


def record_review_decision(
    source_system: str,
    local_column_name: str,
    action: str,
    user: str,
    final_global: str | None = None,
    final_match_type: str | None = None,
    comment: str = "",
) -> bool:
    """Apply one review action with automatic connection refresh."""
    return call_review_store(
        _record_review_action,
        source_system,
        local_column_name,
        action,
        user,
        final_global=final_global,
        final_match_type=final_match_type,
        comment=comment,
    )


def filter_status_summary(summary_rows: list[dict[str, Any]], source_system: str) -> list[dict[str, Any]]:
    """Return summary rows for one source system."""
    return [row for row in summary_rows if row.get("source_system") == source_system]


def status_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Return counts by review status."""
    counts = {"APPROVED": 0, "CORRECTED": 0, "REJECTED": 0, "PENDING": 0}
    for row in rows:
        status = str(row.get("review_status", "")).upper()
        counts[status] = counts.get(status, 0) + 1
    return counts


def mandatory_pending_count(rows: list[dict[str, Any]]) -> int:
    """Count mandatory columns still pending."""
    return sum(
        1 for row in rows if bool(row.get("mandatory_flag")) and str(row.get("review_status", "")).upper() == "PENDING"
    )


def publish_is_ready(rows: list[dict[str, Any]]) -> bool:
    """Same rule as the review gate (notebook 06): every mandatory column is reviewed and has a target."""
    return all(
        row["review_status"] != "PENDING"
        and (row.get("final_global_column_name") or "").upper() not in ("", "NO_MATCH")
        for row in rows
        if row.get("mandatory_flag")
    )


def rows_with_status(rows: list[dict[str, Any]], statuses: set[str]) -> list[dict[str, Any]]:
    """Return rows in one or more review statuses."""
    return [row for row in rows if str(row.get("review_status", "")).upper() in statuses]


def coerce_float(value: Any) -> float | None:
    """Convert an SDK or SQL scalar to a finite numeric value."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def evaluation_metrics(rows: list[dict[str, Any]], source_system: str) -> dict[str, float]:
    """Return the latest numeric evaluation metrics for one source system."""
    metrics: dict[str, float] = {}
    for row in rows:
        if row.get("source_system") != source_system:
            continue
        value = coerce_float(row.get("metric_value"))
        if value is not None:
            metrics[str(row.get("metric_name"))] = value
    return metrics


def get_current_user() -> str:
    """Resolve the current user from request headers or environment."""
    try:
        user = st.context.headers.get("X-Forwarded-User", "")
        if user:
            return user
    except AttributeError:
        pass
    return os.environ.get("DATABRICKS_APP_CURRENT_USER_NAME", "app-service-principal")


def sample_display_value(value: Any) -> str:
    """Render a sample-value field as text."""
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return str(value) if value is not None else ""


def render_review_table(rows: list[dict[str, Any]]):
    """Render queue rows with the existing table styling."""
    display = pd.DataFrame(rows)
    if display.empty:
        st.info("No mappings to display.")
        return
    if "local_sample_values" in display.columns:
        display["local_sample_values"] = display["local_sample_values"].map(sample_display_value)
    st.dataframe(display, use_container_width=True)


def page_group_overview(country: str):
    st.header("Group Overview")
    source_system = COUNTRY_SOURCES[country]
    summary = filter_status_summary(load_review_summary(), source_system)
    counts = status_counts(summary)
    mandatory_pending = sum(
        int(row.get("count", 0))
        for row in summary
        if bool(row.get("mandatory_flag")) and str(row.get("review_status", "")).upper() == "PENDING"
    )
    harmonized = load_optional_sql(
        f"SELECT source_country, COUNT(*) AS row_count FROM {HARMONIZED_TABLE} GROUP BY source_country"
    )
    kpis = load_optional_sql(METRIC_VIEW_QUERY)
    evaluations = load_optional_sql(EVALUATION_QUERY)

    harmonized_row_count = 0
    if not harmonized.empty and {"source_country", "row_count"}.issubset(harmonized.columns):
        matches = harmonized[harmonized["source_country"] == country]
        harmonized_row_count = int(matches["row_count"].sum()) if not matches.empty else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Harmonized Rows", harmonized_row_count)
    col2.metric("Approved / Corrected", counts["APPROVED"] + counts["CORRECTED"])
    col3.metric("Rejected", counts["REJECTED"])
    col4.metric("Pending", counts["PENDING"])
    st.metric("Mandatory Pending", mandatory_pending)

    if kpis.empty:
        st.info("Group KPIs are not available yet. Publish harmonized data to create the metric view input.")
    else:
        country_kpis = kpis[kpis["Country"] == country] if "Country" in kpis.columns else kpis
        values = country_kpis.iloc[0] if not country_kpis.empty else pd.Series(dtype=object)
        kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
        kpi_col1.metric("Gross Written Premium", values.get("gwp", "—"))
        kpi_col2.metric("Loss Ratio", values.get("loss_ratio", "—"))
        kpi_col3.metric("Combined Ratio", values.get("combined_ratio", "—"))

    latest = evaluation_metrics(evaluations, source_system) if not evaluations.empty else {}
    if latest:
        eval_col1, eval_col2 = st.columns(2)
        eval_col1.metric("Mapping Accuracy", latest.get("accuracy", "—"))
        eval_col2.metric("Auto-Accept Rate", latest.get("auto_accept_rate", "—"))
    else:
        st.info("Mapping evaluation results are not available yet.")

    if summary:
        chart_data = pd.DataFrame(summary).groupby("review_status")["count"].sum().to_frame()
        st.subheader("Review Progress")
        st.bar_chart(chart_data)
    else:
        st.info("No review state is available for this country yet.")


def page_pending_review(country: str, user: str):
    st.header("Pending Review")
    source_system = COUNTRY_SOURCES[country]
    rows = rows_with_status(load_queue(source_system), {"PENDING"})
    if not rows:
        st.info("No pending mappings.")
        return
    render_review_table(rows)
    selected_name = st.selectbox("Select column to review", [row["local_column_name"] for row in rows])
    row = next(item for item in rows if item["local_column_name"] == selected_name)

    detail_col1, detail_col2 = st.columns(2)
    with detail_col1:
        st.markdown(f"**Data Type:** {row.get('local_data_type', 'N/A')}")
        st.markdown(f"**Sample Values:** {sample_display_value(row.get('local_sample_values'))}")
        st.markdown(f"**Mandatory:** {'Yes' if row.get('mandatory_flag') else 'No'}")
    with detail_col2:
        st.markdown(f"**Proposed Global Column:** `{row.get('proposed_global_column_name', 'N/A')}`")
        st.markdown(f"**Proposed Match Type:** {row.get('proposed_match_type', 'N/A')}")
        st.markdown(f"**Confidence:** {row.get('confidence', 'N/A')}")
        st.markdown(f"**Rationale:** {row.get('mapping_rationale', 'N/A')}")

    action_col1, action_col2, action_col3 = st.columns(3)
    with action_col1:
        approve_comment = st.text_input("Approve comment (optional)", key=f"approve_{selected_name}")
        if st.button("Approve", key=f"approve_btn_{selected_name}", type="primary"):
            if record_review_decision(source_system, selected_name, "APPROVE", user, comment=approve_comment):
                st.success(f"Approved: `{selected_name}`")
                invalidate_review_caches()
                st.rerun()
            else:
                st.error("Mapping not found.")
    with action_col2:
        reject_comment = st.text_input("Rejection reason (required)", key=f"reject_{selected_name}")
        if st.button("Reject", key=f"reject_btn_{selected_name}"):
            if not reject_comment.strip():
                st.warning("A rejection reason is required.")
            elif record_review_decision(source_system, selected_name, "REJECT", user, comment=reject_comment):
                st.success(f"Rejected: `{selected_name}`")
                invalidate_review_caches()
                st.rerun()
    with action_col3:
        global_columns = load_global_columns() or ["(no global columns available)"]
        with st.form(key=f"correct_{selected_name}"):
            target = st.selectbox("New global column", global_columns, key=f"target_{selected_name}")
            match_type = st.selectbox("New match type", MATCH_TYPE_OPTIONS, key=f"match_{selected_name}")
            correction_comment = st.text_input("Comment", key=f"correct_comment_{selected_name}")
            submitted = st.form_submit_button("Submit Correction")
        if submitted:
            if target == "(no global columns available)":
                st.warning("Global columns list is empty.")
            elif record_review_decision(
                source_system,
                selected_name,
                "CORRECT",
                user,
                final_global=target,
                final_match_type=match_type,
                comment=correction_comment,
            ):
                st.success(f"Corrected: `{selected_name}`")
                invalidate_review_caches()
                st.rerun()


def page_approved_mappings(country: str, user: str):
    st.header("Approved Mappings")
    source_system = COUNTRY_SOURCES[country]
    rows = rows_with_status(load_queue(source_system), RESOLVED_STATUSES)
    if not rows:
        st.info("No approved or corrected mappings yet.")
        return
    st.markdown(f"**{len(rows)} mapping(s) approved or corrected**")
    render_review_table(rows)
    selected_name = st.selectbox("Select column to reset", [row["local_column_name"] for row in rows])
    if st.button("Reset to Pending") and record_review_decision(source_system, selected_name, "RESET", user):
        st.success(f"Reset to PENDING: `{selected_name}`")
        invalidate_review_caches()
        st.rerun()


def page_rejected_mappings(country: str, user: str):
    st.header("Rejected Mappings")
    source_system = COUNTRY_SOURCES[country]
    rows = rows_with_status(load_queue(source_system), {"REJECTED"})
    if not rows:
        st.info("No rejected mappings.")
        return
    render_review_table(rows)
    selected_name = st.selectbox("Select column to reconsider", [row["local_column_name"] for row in rows])
    comment = st.text_input("Optional comment", key=f"reconsider_{selected_name}")
    if st.button("Reset to Pending") and record_review_decision(
        source_system, selected_name, "RESET", user, comment=comment
    ):
        st.success(f"Reset to PENDING: `{selected_name}`")
        invalidate_review_caches()
        st.rerun()


def page_review_dashboard(country: str):
    st.header("Review Dashboard")
    rows = load_queue(COUNTRY_SOURCES[country])
    if not rows:
        st.info("No review state for this country yet.")
        return
    counts = status_counts(rows)
    total = len(rows)
    resolved = counts["APPROVED"] + counts["CORRECTED"]
    coverage = round(resolved / total * 100, 1) if total else 0.0
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Columns", total)
    col2.metric("Approved / Corrected", resolved)
    col3.metric("Rejected", counts["REJECTED"])
    col4.metric("Pending", counts["PENDING"])
    st.metric("Coverage", f"{coverage}%")
    mandatory_rows = [row for row in rows if bool(row.get("mandatory_flag"))]
    mandatory_resolved = len(rows_with_status(mandatory_rows, RESOLVED_STATUSES))
    mandatory_unresolved = len(mandatory_rows) - mandatory_resolved
    mandatory_col1, mandatory_col2 = st.columns(2)
    mandatory_col1.metric("Mandatory Resolved", f"{mandatory_resolved} / {len(mandatory_rows)}")
    mandatory_col2.metric("Mandatory Unresolved", mandatory_unresolved)

    st.divider()
    st.subheader("Count by Review Status")
    chart_data = pd.DataFrame(rows)["review_status"].value_counts().to_frame(name="count")
    st.bar_chart(chart_data)
    if "confidence" in rows[0]:
        confidence_distribution = (
            pd.DataFrame(rows)
            .groupby(["confidence", "review_status"])
            .size()
            .reset_index(name="count")
            .pivot(index="confidence", columns="review_status", values="count")
            .fillna(0)
            .astype(int)
        )
        st.subheader("Confidence Distribution by Review Status")
        st.dataframe(confidence_distribution, use_container_width=True)


def page_publish_readiness(country: str):
    st.header("Publish Readiness")
    source_system = COUNTRY_SOURCES[country]
    rows = load_queue(source_system)
    if not rows:
        st.info("No review state for this country yet.")
        return
    mandatory_pending = mandatory_pending_count(rows)
    ready = publish_is_ready(rows)
    if ready:
        st.success(f"READY TO PUBLISH\n\nAll mandatory mappings for {country} are resolved.")
    else:
        st.error(f"BLOCKED\n\n{mandatory_pending} mandatory column(s) are still PENDING.")

    st.subheader("Mandatory Columns")
    for row in rows:
        if bool(row.get("mandatory_flag")):
            st.markdown(
                f"{status_indicator(row.get('review_status', 'PENDING'))} &nbsp; `{row.get('local_column_name')}`",
                unsafe_allow_html=True,
            )

    st.subheader("Non-Mandatory Columns")
    non_mandatory_rows = [row for row in rows if not bool(row.get("mandatory_flag"))]
    if non_mandatory_rows:
        non_mandatory_counts = status_counts(non_mandatory_rows)
        non_mandatory_col1, non_mandatory_col2, non_mandatory_col3 = st.columns(3)
        non_mandatory_col1.metric("Resolved", non_mandatory_counts["APPROVED"] + non_mandatory_counts["CORRECTED"])
        non_mandatory_col2.metric("Pending", non_mandatory_counts["PENDING"])
        non_mandatory_col3.metric("Rejected", non_mandatory_counts["REJECTED"])
        for row in non_mandatory_rows:
            target = row.get("final_global_column_name") or row.get("proposed_global_column_name") or "—"
            st.markdown(
                f"{status_indicator(row.get('review_status', 'PENDING'))} &nbsp; "
                f"`{row.get('local_column_name')}` &nbsp;→&nbsp; `{target}`",
                unsafe_allow_html=True,
            )
    else:
        st.info("No non-mandatory columns found.")

    st.divider()
    st.subheader(f"Publish harmonized data for {country}")
    if not PUBLISH_JOB_ID:
        st.warning("PUBLISH_JOB_ID environment variable is not set.")
    elif not ready:
        st.info(f"Resolve mandatory PENDING columns before publishing {country}.")
    elif st.button(f"Publish harmonized data for {country}", type="primary"):
        try:
            client = get_workspace_client()
            run = client.jobs.run_now(
                job_id=int(PUBLISH_JOB_ID),
                job_parameters={"source_country": country},
            )
            run_id = run.run_id
            run_url = f"{client.config.host.rstrip('/')}/#job/{PUBLISH_JOB_ID}/run/{run_id}"
            st.session_state["publish_run_id"] = run_id
            st.session_state["publish_run_url"] = run_url
        except Exception as exc:
            st.error(f"Failed to start publish job: {exc}")

    run_id = st.session_state.get("publish_run_id")
    if run_id:
        st.success(f"Publish job started — Run ID: `{run_id}`")
        if st.button("Refresh run state"):
            try:
                run = get_workspace_client().jobs.get_run(run_id=run_id)
                state = run.state.result_state or run.state.life_cycle_state
                st.metric("Run State", str(state))
            except Exception as exc:
                st.error(f"Failed to read run state: {exc}")
        if st.session_state.get("publish_run_url"):
            st.markdown(f"[Open run in Databricks]({st.session_state['publish_run_url']})")


def extract_genie_answer(message: Any) -> dict[str, Any]:
    """Text answer, generated SQL and query attachment ID from a Genie message.

    ``message.content`` is the user's question; Genie's answer lives in the attachments.
    """
    msg = message.as_dict() if hasattr(message, "as_dict") else message
    answer = {"text": "", "sql": "", "attachment_id": None}
    for attachment in msg.get("attachments") or []:
        if attachment.get("text") and not answer["text"]:
            answer["text"] = attachment["text"].get("content", "")
        query = attachment.get("query")
        if query and not answer["sql"]:
            answer["sql"] = query.get("query", "")
            answer["attachment_id"] = attachment.get("attachment_id")
            answer["text"] = answer["text"] or query.get("description", "")
    return answer


def genie_attachment_frame(ws, answer: dict[str, Any], message: Any) -> pd.DataFrame:
    """Fetch the rows of the query attachment as a DataFrame."""
    if not answer.get("attachment_id"):
        return pd.DataFrame()
    statement = ws.genie.get_message_attachment_query_result(
        space_id=GENIE_SPACE_ID,
        conversation_id=message.conversation_id,
        message_id=message.message_id or message.id,
        attachment_id=answer["attachment_id"],
    ).statement_response
    if statement is None or statement.result is None or not statement.result.data_array:
        return pd.DataFrame()
    columns = [column.name for column in statement.manifest.schema.columns]
    return pd.DataFrame(statement.result.data_array, columns=columns)


def ask_genie(ws, question: str, conversation_id: str | None):
    """Start or continue a Genie conversation and return its message and conversation ID."""
    if conversation_id:
        message = ws.genie.create_message_and_wait(
            space_id=GENIE_SPACE_ID,
            conversation_id=conversation_id,
            content=question,
        )
    else:
        message = ws.genie.start_conversation_and_wait(space_id=GENIE_SPACE_ID, content=question)
        conversation_id = message.conversation_id
    return message, conversation_id


def page_ask_genie():
    st.header("Ask the group data")
    if not GENIE_SPACE_ID:
        st.warning("GENIE_SPACE_ID environment variable is not set.")
        return

    question = st.text_input(
        "Ask a question about group property KPIs",
        value=st.session_state.get("genie_question", ""),
        key="genie_input",
    )
    question_cols = st.columns(4)
    for index, sample in enumerate(SAMPLE_QUESTIONS):
        if question_cols[index % len(question_cols)].button(sample, key=f"sample_{index}"):
            st.session_state["genie_question"] = sample
            st.rerun()

    if not question:
        return
    ws = get_workspace_client()
    try:
        with st.spinner("Asking Genie..."):
            message, conversation_id = ask_genie(ws, question, st.session_state.get("genie_conversation_id"))
        st.session_state["genie_conversation_id"] = conversation_id
        answer = extract_genie_answer(message)
        st.markdown(answer["text"] or "Genie did not return a text answer.")
        if answer["sql"]:
            with st.expander("Generated SQL"):
                st.code(answer["sql"], language="sql")
        result = genie_attachment_frame(ws, answer, message)
        if not result.empty:
            st.dataframe(result, use_container_width=True)
    except Exception as exc:
        st.error(f"Genie request failed: {exc}")


def main():
    st.set_page_config(
        page_title="Column Mapping Review",
        page_icon="data:image/svg+xml,"
        "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'>"
        "<rect fill='%23003781' rx='4' width='24' height='24'/>"
        "<path fill='white' d='M7 8h10v2H7zm0 3h10v2H7zm0 3h7v2H7z'/></svg>",
        layout="wide",
    )
    inject_custom_css()

    try:
        get_review_connection()
    except Exception as exc:
        st.error(f"Could not connect to Lakebase: {exc}")
        return

    user = get_current_user()
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
        country = st.selectbox("Country", list(COUNTRY_SOURCES), key="selected_country")
        st.divider()
        st.markdown("**Review Status**")
        try:
            summary = filter_status_summary(load_review_summary(), COUNTRY_SOURCES[country])
            for row in summary:
                st.markdown(
                    f"{status_indicator(row['review_status'])} &nbsp; **{row['count']}**",
                    unsafe_allow_html=True,
                )
            if not summary:
                st.caption("No data available yet.")
        except Exception:
            st.caption("Could not load status summary.")
        st.divider()
        page = st.radio(
            "Navigate",
            [
                "Group Overview",
                "Pending Review Queue",
                "Approved Mappings",
                "Rejected Mappings",
                "Review Dashboard",
                "Publish Readiness",
                "Ask the group data",
            ],
            key="nav_page",
        )

    if page == "Group Overview":
        page_group_overview(country)
    elif page == "Pending Review Queue":
        page_pending_review(country, user)
    elif page == "Approved Mappings":
        page_approved_mappings(country, user)
    elif page == "Rejected Mappings":
        page_rejected_mappings(country, user)
    elif page == "Review Dashboard":
        page_review_dashboard(country)
    elif page == "Publish Readiness":
        page_publish_readiness(country)
    elif page == "Ask the group data":
        page_ask_genie()


if __name__ == "__main__":
    main()
