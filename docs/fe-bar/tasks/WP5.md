# WP5: Unity Catalog governance + metric view

## Goal
Make the governance visible and checkable: tags, comments, a per-country row filter, grants, and a UC
metric view for the group KPIs that Genie and the app use.

## Files
- NEW `src/harmonization/governance.py`: pure SQL builders (return strings, no Spark).
  - `quote_ident(name)` (backticks, escape backticks), `sql_str(value)` (single quotes, escaped).
  - `table_tags_sql(fqn, tags: dict) -> str` → `ALTER TABLE <fqn> SET TAGS ('k' = 'v', ...)`
  - `column_tags_sql(fqn, column, tags) -> str` → `ALTER TABLE <fqn> ALTER COLUMN <col> SET TAGS (...)`
  - `row_filter_function_sql(db, privileged_principals: list[str], group_prefix="halvard-steward-",
    admin_group="halvard-group-actuarial") -> str` → `CREATE OR REPLACE FUNCTION <db>.country_row_filter(source_country STRING)
    RETURN is_account_group_member('<admin_group>') OR is_account_group_member(concat('<prefix>', lower(source_country)))
    OR current_user() IN (<privileged...>)`. With an empty list, the IN clause is omitted.
  - `apply_row_filter_sql(fqn, db) -> str` → `ALTER TABLE <fqn> SET ROW FILTER <db>.country_row_filter ON (source_country)`
  - `grant_sql(privilege, securable_type, fqn, principal) -> str` (principal quoted with backticks)
  - `metric_view_sql(db, yaml_text) -> str` → `CREATE OR REPLACE VIEW <db>.mv_group_property_kpis WITH METRICS LANGUAGE YAML AS $$\n<yaml>\n$$`
  - `governance_plan(catalog, schema, config, privileged_principals, app_sp) -> list[tuple[str, str]]`
    → ordered (description, sql) for everything the notebook runs. Table tags: bronze tables (`layer=bronze`),
    `harmonized_property_monthly` (`layer=gold`, `domain=property_insurance`, `data_owner=group_actuarial`,
    `classification=confidential`), control tables (`layer=control`). Column tags: every target-model column
    in `semantic_group` premium, claims or expenses gets `kpi_type=financial`.
- NEW `sql/mv_group_property_kpis.yaml`: metric view YAML `version: 1.1`, `source:` placeholder
  `{catalog}.{schema}.harmonized_property_monthly` (the notebook formats it). Dimensions: Country
  (`source_country`), Reporting Month (use the target model's period column, check its name in the config),
  Distribution Channel, Customer Segment, Risk Zone, Region. Measures: Gross Written Premium
  (SUM gross_written_premium_eur), Net Written Premium, Gross Claims Incurred, Claims Reported, Claims Paid,
  Commissions, Management Expenses, Loss Ratio (`SUM(gross_claims_incurred_eur) / NULLIF(SUM(gross_written_premium_eur), 0)`),
  Expense Ratio ((commissions + management) / GWP), Combined Ratio (loss + expense), Policies In Force Change
  (new + renewed - cancelled). Add a short `comment` for each, written for a business reader.
- NEW `notebooks/12_apply_governance.py`: widgets catalog_name, schema_name, privileged_principals
  (comma-separated; default empty), app_service_principal (default empty). `%run ./_shared_utils`; config via
  `load_harmonization_config()`. Runs each statement from `governance_plan`; grants to groups that do not
  exist are caught and reported as SKIPPED (never silently). The current user is always added to the
  privileged principals. Then prints evidence: `SHOW GRANTS ON SCHEMA`, tags from
  `<catalog>.information_schema.table_tags` and `column_tags` filtered to the schema, `DESCRIBE TABLE EXTENDED`
  rows for the row filter, the function definition (`DESCRIBE FUNCTION EXTENDED`), and a test
  `SELECT source_country, count(*) FROM harmonized GROUP BY 1`. Exits with a JSON summary (statements ok/skipped/failed).
  Fails the task (raise) if any non-grant statement fails.
- NEW `tests/test_governance.py`

## Tests
Quoting and escaping (including injection-like input), every builder, an empty privileged list,
governance_plan ordering (function before row filter, metric view last) and contents for a sample config,
and that the YAML file parses with `yaml.safe_load` and names valid harmonized columns from the config.
