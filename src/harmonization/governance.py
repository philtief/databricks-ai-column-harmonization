"""Pure SQL builders for Unity Catalog governance."""

from __future__ import annotations

from pathlib import Path
from typing import Any

HARMONIZED_TABLE = "harmonized_property_monthly"
METRIC_VIEW_NAME = "mv_group_property_kpis"
ROW_FILTER_NAME = "country_row_filter"
ADMIN_GROUP = "halvard-group-actuarial"
STEWARD_GROUP_PREFIX = "halvard-steward-"
CONTROL_TABLES = (
    "source_column_inventory",
    "column_mapping_candidates",
    "column_mapping_dictionary",
    "column_mapping_audit",
    "value_mapping_candidates",
    "value_mapping_dictionary",
    "global_target_columns",
    "workflow_run_metrics",
    "ai_mapping_usage_metrics",
    "data_quality_results",
    "mapping_eval_results",
)


def quote_ident(name: str) -> str:
    """Return a backtick-quoted SQL identifier."""
    return f"`{name.replace('`', '``')}`"


def sql_str(value: str) -> str:
    """Return a single-quoted Spark SQL string literal.

    Spark joins adjacent literals, so 'it''s' reads as "its"; backslash escapes keep the quote.
    """
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _tags(tags: dict[str, Any]) -> str:
    if not tags:
        raise ValueError("tags must contain at least one tag")
    return ", ".join(f"{sql_str(str(key))} = {sql_str(str(value))}" for key, value in tags.items())


def table_tags_sql(fqn: str, tags: dict[str, Any]) -> str:
    """Build an ALTER TABLE statement that sets table tags."""
    return f"ALTER TABLE {fqn} SET TAGS ({_tags(tags)})"


def column_tags_sql(fqn: str, column: str, tags: dict[str, Any]) -> str:
    """Build an ALTER TABLE statement that sets one column's tags."""
    return f"ALTER TABLE {fqn} ALTER COLUMN {quote_ident(column)} SET TAGS ({_tags(tags)})"


def table_comment_sql(fqn: str, comment: str) -> str:
    """Build a SQL table comment statement."""
    return f"COMMENT ON TABLE {fqn} IS {sql_str(comment)}"


def row_filter_function_sql(
    db: str,
    privileged_principals: list[str],
    group_prefix: str = STEWARD_GROUP_PREFIX,
    admin_group: str = ADMIN_GROUP,
) -> str:
    """Build the per-country row-filter function definition."""
    principal_sql = ", ".join(sql_str(principal) for principal in privileged_principals)
    current_user_clause = f" OR current_user() IN ({principal_sql})" if privileged_principals else ""
    return (
        f"CREATE OR REPLACE FUNCTION {db}.{ROW_FILTER_NAME}(source_country STRING)\n"
        f"RETURN is_account_group_member({sql_str(admin_group)})\n"
        f" OR is_account_group_member(concat({sql_str(group_prefix)}, lower(source_country)))"
        f"{current_user_clause}"
    )


def apply_row_filter_sql(fqn: str, db: str) -> str:
    """Build the statement that attaches the country row filter to a table."""
    return f"ALTER TABLE {fqn} SET ROW FILTER {db}.{ROW_FILTER_NAME} ON (source_country)"


def grant_sql(privilege: str, securable_type: str, fqn: str, principal: str) -> str:
    """Build a Unity Catalog GRANT statement."""
    return f"GRANT {privilege} ON {securable_type} {fqn} TO {quote_ident(principal)}"


def metric_view_sql(db: str, yaml_text: str) -> str:
    """Build the metric-view CREATE statement."""
    return f"CREATE OR REPLACE VIEW {db}.{METRIC_VIEW_NAME} WITH METRICS LANGUAGE YAML AS $$\n{yaml_text.rstrip()}\n$$"


def _bronze_table_names(config: dict[str, Any]) -> list[str]:
    sources = config.get("sources")
    if isinstance(sources, dict):
        return [source["source_table"] for source in sources.values()]
    return [config["source_context"]["source_table"]]


def _financial_columns(config: dict[str, Any]) -> list[str]:
    return [
        column["name"]
        for column in config["target_model"]["columns"]
        if column.get("semantic_group") in {"premium", "claims", "expenses"} and column.get("type") == "DOUBLE"
    ]


def _unique_principals(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def governance_plan(
    catalog: str,
    schema: str,
    config: dict[str, Any],
    privileged_principals: list[str],
    app_sp: str,
) -> list[tuple[str, str]]:
    """Build the ordered governance statements to execute in Unity Catalog."""
    catalog_ident = quote_ident(catalog)
    schema_ident = quote_ident(schema)
    db = f"{catalog_ident}.{schema_ident}"
    harmonized = f"{db}.{quote_ident(HARMONIZED_TABLE)}"
    plan: list[tuple[str, str]] = []

    for table_name in _bronze_table_names(config):
        bronze = f"{db}.{quote_ident(table_name)}"
        plan.append(("Comment bronze table", table_comment_sql(bronze, "Raw source input for column harmonization")))
        plan.append(("Tag bronze table", table_tags_sql(bronze, {"layer": "bronze"})))

    gold_tags = {
        "layer": "gold",
        "domain": "property_insurance",
        "data_owner": "group_actuarial",
        "classification": "confidential",
    }
    plan.append(
        ("Comment harmonized table", table_comment_sql(harmonized, "Gold harmonized property insurance reporting data"))
    )
    plan.append(("Tag harmonized table", table_tags_sql(harmonized, gold_tags)))

    for column in _financial_columns(config):
        plan.append(
            (
                f"Tag financial column {column}",
                column_tags_sql(harmonized, column, {"kpi_type": "financial"}),
            )
        )

    for table_name in CONTROL_TABLES:
        control = f"{db}.{quote_ident(table_name)}"
        plan.append(("Comment control table", table_comment_sql(control, "Column harmonization control table")))
        plan.append(("Tag control table", table_tags_sql(control, {"layer": "control"})))

    principals = _unique_principals([ADMIN_GROUP, *privileged_principals])
    plan.append(("Create country row-filter function", row_filter_function_sql(db, privileged_principals)))
    plan.append(("Apply country row filter to harmonized table", apply_row_filter_sql(harmonized, db)))

    if app_sp:
        plan.append(("Grant app catalog usage", grant_sql("USE CATALOG", "CATALOG", catalog_ident, app_sp)))
        plan.append(("Grant app schema usage", grant_sql("USE SCHEMA", "SCHEMA", db, app_sp)))
        plan.append(("Grant app schema read", grant_sql("SELECT", "SCHEMA", db, app_sp)))
        plan.append(("Grant app harmonized access", grant_sql("SELECT", "TABLE", harmonized, app_sp)))
    for principal in principals:
        plan.append(("Grant group harmonized access", grant_sql("SELECT", "TABLE", harmonized, principal)))

    metric_view_path = Path(__file__).resolve().parents[2] / "sql" / "mv_group_property_kpis.yaml"
    metric_yaml = metric_view_path.read_text().replace("{catalog}", catalog).replace("{schema}", schema)
    plan.append(("Create metric view", metric_view_sql(db, metric_yaml)))

    return plan
