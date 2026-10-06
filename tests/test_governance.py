"""Tests for harmonization.governance SQL builders."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from harmonization.config import load_config
from harmonization.governance import (
    METRIC_VIEW_NAME,
    apply_row_filter_sql,
    column_tags_sql,
    governance_plan,
    grant_sql,
    metric_view_sql,
    quote_ident,
    row_filter_function_sql,
    sql_str,
    table_comment_sql,
    table_tags_sql,
)


@pytest.fixture
def config() -> dict:
    return {
        "sources": {
            "ES": {"source_table": "bronze_property_monthly_es"},
            "IT": {"source_table": "bronze_property_monthly_it"},
        },
        "target_model": {
            "table_name": "harmonized_property_monthly",
            "columns": [
                {"name": "reporting_month", "type": "INT", "semantic_group": "time"},
                {"name": "gross_written_premium_eur", "type": "DOUBLE", "semantic_group": "premium"},
                {"name": "claims_reported_count", "type": "INT", "semantic_group": "claims"},
                {"name": "commissions_eur", "type": "DOUBLE", "semantic_group": "expenses"},
                {"name": "customer_segment", "type": "STRING", "semantic_group": "customer"},
            ],
        },
    }


class TestQuoting:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("table", "`table`"),
            ("ta`ble", "`ta``ble`"),
            ("'; DROP TABLE x; --", "`'; DROP TABLE x; --`"),
        ],
    )
    def test_quote_ident_escapes_backticks(self, value, expected):
        assert quote_ident(value) == expected

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("value", "'value'"),
            # Spark joins adjacent literals ('o''clock' reads as "oclock"), so quotes need backslash escapes.
            ("o'clock", r"'o\'clock'"),
            ("'; DROP TABLE x; --", r"'\'; DROP TABLE x; --'"),
            ("a\\b", r"'a\\b'"),
        ],
    )
    def test_sql_str_escapes_single_quotes(self, value, expected):
        assert sql_str(value) == expected


class TestBuilders:
    def test_table_tags_sql(self):
        sql = table_tags_sql("cat.schema.table", {"classification": "confidential", "layer": "gold"})
        assert sql == "ALTER TABLE cat.schema.table SET TAGS ('classification' = 'confidential', 'layer' = 'gold')"

    def test_column_tags_sql(self):
        sql = column_tags_sql("cat.schema.table", "gross_premium", {"kpi_type": "financial"})
        assert sql == "ALTER TABLE cat.schema.table ALTER COLUMN `gross_premium` SET TAGS ('kpi_type' = 'financial')"

    def test_row_filter_function_sql_with_principals(self):
        sql = row_filter_function_sql("cat.schema", ["steward-one", "group@example.com"])
        assert sql.startswith("CREATE OR REPLACE FUNCTION cat.schema.country_row_filter(source_country STRING)\n")
        assert "is_account_group_member('halvard-group-actuarial')" in sql
        assert "concat('halvard-steward-', lower(source_country))" in sql
        assert "current_user() IN ('steward-one', 'group@example.com')" in sql

    def test_row_filter_function_sql_without_principals(self):
        sql = row_filter_function_sql("cat.schema", [])
        assert "current_user() IN" not in sql
        assert sql.endswith("concat('halvard-steward-', lower(source_country)))")

    def test_row_filter_function_sql_accepts_overrides(self):
        sql = row_filter_function_sql("cat.schema", [], group_prefix="acme-steward-", admin_group="acme-admin")
        assert "is_account_group_member('acme-admin')" in sql
        assert "concat('acme-steward-', lower(source_country))" in sql

    def test_apply_row_filter_sql(self):
        sql = apply_row_filter_sql("cat.schema.table", "cat.schema")
        assert sql == "ALTER TABLE cat.schema.table SET ROW FILTER cat.schema.country_row_filter ON (source_country)"

    def test_grant_sql(self):
        sql = grant_sql("SELECT", "TABLE", "cat.schema.table", "halvard-group-actuarial")
        assert sql == "GRANT SELECT ON TABLE cat.schema.table TO `halvard-group-actuarial`"

    def test_metric_view_sql(self):
        sql = metric_view_sql("cat.schema", "version: 1.1\n")
        assert sql == (
            "CREATE OR REPLACE VIEW cat.schema.mv_group_property_kpis "
            "WITH METRICS LANGUAGE YAML AS $$\nversion: 1.1\n$$"
        )

    def test_table_comment_sql(self):
        assert table_comment_sql("cat.schema.table", "Gold table") == (
            "COMMENT ON TABLE cat.schema.table IS 'Gold table'"
        )


class TestGovernancePlan:
    def test_plan_is_ordered_and_complete(self, config):
        plan = governance_plan("cat", "schema", config, ["halvard-group-actuarial"], "app-sp@example.com")
        descriptions = [description for description, _ in plan]
        sqls = [sql for _, sql in plan]
        joined_sqls = "\n".join(sqls)

        assert "Create country row-filter function" in descriptions
        assert descriptions.index("Create country row-filter function") < descriptions.index(
            "Apply country row filter to harmonized table"
        )
        assert descriptions[-1] == "Create metric view"
        assert sqls[-1].startswith(
            f"CREATE OR REPLACE VIEW `cat`.`schema`.{METRIC_VIEW_NAME} WITH METRICS LANGUAGE YAML"
        )

        assert "COMMENT ON TABLE `cat`.`schema`.`bronze_property_monthly_es`" in joined_sqls
        assert "SET TAGS ('layer' = 'bronze')" in joined_sqls
        assert "COMMENT ON TABLE `cat`.`schema`.`harmonized_property_monthly`" in joined_sqls
        assert "SET TAGS ('layer' = 'gold', 'domain' = 'finance', 'line_of_business' = 'property'" in joined_sqls
        assert "SET TAGS ('component' = 'control')" in joined_sqls
        assert "ALTER TABLE `cat`.`schema`.`mapping_eval_results`" in joined_sqls
        assert "ALTER TABLE `cat`.`schema`.`gross_written_premium_eur`" not in joined_sqls
        assert (
            "ALTER TABLE `cat`.`schema`.`harmonized_property_monthly` ALTER COLUMN `gross_written_premium_eur` "
            "SET TAGS ('kpi_type' = 'financial')" in joined_sqls
        )
        assert (
            "ALTER TABLE `cat`.`schema`.`harmonized_property_monthly` ALTER COLUMN `commissions_eur` "
            "SET TAGS ('kpi_type' = 'financial')" in joined_sqls
        )
        assert (
            "GRANT SELECT ON TABLE `cat`.`schema`.`harmonized_property_monthly` TO `app-sp@example.com`" in joined_sqls
        )
        assert "GRANT SELECT ON SCHEMA `cat`.`schema` TO `app-sp@example.com`" in joined_sqls

    def test_financial_tags_cover_config_groups(self, config):
        plan = governance_plan("cat", "schema", config, [], "")
        joined = "\n".join(sql for _, sql in plan)
        for column in ("gross_written_premium_eur", "commissions_eur"):
            assert f"ALTER COLUMN `{column}`" in joined
        # Counts are not monetary amounts, so they are not tagged financial.
        for column in ("claims_reported_count", "customer_segment"):
            assert f"ALTER COLUMN `{column}`" not in joined

    def test_no_app_grants_when_app_sp_is_empty(self, config):
        plan = governance_plan("cat", "schema", config, [], "")
        assert not any("TO `app-sp@example.com`" in sql for _, sql in plan)


class TestMetricViewYaml:
    def test_yaml_parses_and_uses_valid_columns(self, config):
        path = Path(__file__).resolve().parents[1] / "sql" / "mv_group_property_kpis.yaml"
        payload = yaml.safe_load(path.read_text())

        assert payload["version"] == 1.1
        assert payload["source"] == "{catalog}.{schema}.harmonized_property_monthly"

        dimension_exprs = [dimension["expr"] for dimension in payload["dimensions"]]
        measure_exprs = [measure["expr"] for measure in payload["measures"]]
        dimension_names = [dimension["name"] for dimension in payload["dimensions"]]
        measure_names = [measure["name"] for measure in payload["measures"]]
        expected_dimensions = [
            "Country",
            "Reporting Period",
            "Distribution Channel",
            "Customer Segment",
            "Risk Zone",
            "Region",
        ]
        expected_measures = [
            "Gross Written Premium",
            "Net Written Premium",
            "Gross Claims Incurred",
            "Claims Reported",
            "Claims Paid",
            "Commissions",
            "Management Expenses",
            "Loss Ratio",
            "Expense Ratio",
            "Combined Ratio",
            "Policies In Force Change",
        ]
        assert dimension_names == expected_dimensions
        assert measure_names == expected_measures
        assert dimension_exprs[0] == "source_country"
        assert dimension_exprs[1] == "make_date(reporting_year, reporting_month, 1)"
        assert all(dimension.get("comment") for dimension in payload["dimensions"])
        assert all(measure.get("comment") for measure in payload["measures"])
        config_columns = {item["name"] for item in load_config()["target_model"]["columns"]}
        assert {
            "reporting_year",
            "reporting_month",
            "distribution_channel",
            "customer_segment",
            "risk_zone",
            "region",
        } <= config_columns
        assert measure_exprs[-1] == ("SUM(new_policies_count + renewed_policies_count - cancelled_policies_count)")
        assert "NULLIF(SUM(gross_written_premium_eur), 0)" in measure_exprs[7]

    def test_metric_view_references_harmonized_table(self):
        path = Path(__file__).resolve().parents[1] / "sql" / "mv_group_property_kpis.yaml"
        assert "harmonized_property_monthly" in path.read_text()
