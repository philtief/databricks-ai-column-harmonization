# validate_and_monitor (job halvard_publish_harmonized, run 288885881518368, task run 225833631506391, SUCCESS)

```python
%run ./_shared_utils
```

```python
import datetime as _dt
import json
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from harmonization.config import get_mandatory_columns
from harmonization.validation import (
    check_constraint,
    check_mapping_coverage,
    check_mapping_version,
    check_null_count,
    check_row_parity,
)
from harmonization.value_mapping import categorical_targets

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "your_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "harmonizing_agent", "Schema Name")
dbutils.widgets.text("source_country", "ES", "Source Country")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
source_country = dbutils.widgets.get("source_country").strip()

DB = f"`{catalog_name}`.`{schema_name}`"
_cfg = load_harmonization_config()
_refs = get_table_refs(_cfg, DB, source_country)
RAW_TABLE = _refs["raw_table"]
HARM_TABLE = _refs["harm_table"]
DICT_TABLE = _refs["dict_table"]
CAND_TABLE = _refs["cand_table"]
DQ_TABLE = _refs["dq_table"]
USAGE_TABLE = _refs["usage_table"]
OPS_TABLE = _refs["ops_table"]
SOURCE_SYSTEM = _refs["source_system"]
MANDATORY_COLUMNS = get_mandatory_columns(_cfg, source_country)
CATEGORICAL_TARGETS = categorical_targets(_cfg)
REQUIRED_TARGET_COLUMNS = [column["name"] for column in _cfg["target_model"]["columns"] if column.get("required")]
DQ_RULES = _cfg.get("data_quality_rules", []) or []

print(
    f"Config: {DB}, country={source_country}, required_target_cols={len(REQUIRED_TARGET_COLUMNS)}, "
    f"custom_rules={len(DQ_RULES)}"
)

RUN_ID = str(uuid4())
_start = _dt.datetime.utcnow()
```

Output:
```text
Config: `agent_marketplace_catalog`.`halvard_harmonization`, country=ES, required_target_cols=21, custom_rules=3

/home/spark-f7cacc41-5b9f-4449-8d7f-00/.ipykernel/66/command-8770032077284742-521443706:49: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
  _start = _dt.datetime.utcnow()
```

```python
raw_df = spark.table(RAW_TABLE)
harm_df = spark.table(HARM_TABLE).where(F.col("source_country") == source_country)
raw_count = raw_df.count()
harm_count = harm_df.count()
harm_columns = {field.name for field in harm_df.schema.fields}

print(f"Raw rows: {raw_count:,}, country harmonized rows: {harm_count:,}")
```

Output:
```text
Raw rows: 10,000, country harmonized rows: 10,000
```

```python
now = _dt.datetime.utcnow()
dq_results = []


def record(result):
    name, status, metric, details = result
    emoji = "PASS" if status == "PASSED" else ("WARN" if status == "WARNING" else "FAIL")
    print(f"  [{emoji}] {name}: {status} | metric={metric} | {details}")
    dq_results.append((RUN_ID, SOURCE_SYSTEM, name, status, float(metric), now, details))


record(check_row_parity(raw_count, harm_count))

active_dict_columns = {
    row["local_column_name"]
    for row in spark.sql(f"""
        SELECT DISTINCT local_column_name
        FROM {DICT_TABLE}
        WHERE source_system = '{SOURCE_SYSTEM}' AND active_flag = TRUE
    """).collect()
}
record(check_mapping_coverage(MANDATORY_COLUMNS, active_dict_columns))

for target_column in REQUIRED_TARGET_COLUMNS:
    if target_column not in harm_columns:
        record((f"not_null_{target_column}", "FAILED", 0, f"target column '{target_column}' is missing"))
        continue
    null_count = harm_df.where(F.col(target_column).isNull()).count()
    record(check_null_count(target_column, null_count))

for global_column, allowed_values in sorted(CATEGORICAL_TARGETS.items()):
    if global_column not in harm_columns:
        record((f"values_in_allowed_list_{global_column}", "WARNING", len(allowed_values), "column is missing"))
        continue
    invalid_count = harm_df.where(F.col(global_column).isNotNull() & ~F.col(global_column).isin(allowed_values)).count()
    record(
        check_constraint(
            f"values_in_allowed_list_{global_column}",
            invalid_count,
            f"{global_column} values are in the configured allow-list",
            severity="WARNING",
        )
    )

for rule in DQ_RULES:
    rule_name = rule["name"]
    predicate = rule["predicate"]
    description = rule.get("description", predicate)
    severity = (rule.get("severity") or "FAILED").upper()
    try:
        violations = harm_df.where(F.expr(predicate)).count()
    except Exception as error:
        record((rule_name, "FAILED", 0, f"predicate failed to evaluate: {error}"))
        continue
    record(check_constraint(rule_name, violations, description, severity=severity))

if "mapping_status" in harm_columns:
    bad_status = harm_df.where(
        ~F.col("mapping_status").isin("APPROVED_COLUMN_MAPPING", "APPROVED_COLUMN_AND_VALUE_MAPPING")
    ).count()
    record(check_mapping_version(bad_status))

print(f"Total checks: {len(dq_results)}")
```

Output:
```text
[PASS] row_count_parity: PASSED | metric=10000 | raw=10,000, harmonized=10,000

/home/spark-f7cacc41-5b9f-4449-8d7f-00/.ipykernel/66/command-8770032077284746-735787411:1: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
  now = _dt.datetime.utcnow()

  [PASS] column_mapping_coverage: PASSED | metric=100.0 | 14/14 mandatory columns have active dict entries
  [PASS] not_null_record_id: PASSED | metric=0 | 0 null record_id values
  [PASS] not_null_reporting_year: PASSED | metric=0 | 0 null reporting_year values
  [PASS] not_null_reporting_month: PASSED | metric=0 | 0 null reporting_month values
  [PASS] not_null_policy_number: PASSED | metric=0 | 0 null policy_number values
  [PASS] not_null_risk_type: PASSED | metric=0 | 0 null risk_type values
  [PASS] not_null_region: PASSED | metric=0 | 0 null region values
  [PASS] not_null_distribution_channel: PASSED | metric=0 | 0 null distribution_channel values
  [PASS] not_null_net_written_premium_eur: PASSED | metric=0 | 0 null net_written_premium_eur values
  [PASS] not_null_gross_written_premium_eur: PASSED | metric=0 | 0 null gross_written_premium_eur values
  [PASS] not_null_new_policies_count: PASSED | metric=0 | 0 null new_policies_count values
  [PASS] not_null_renewed_policies_count: PASSED | metric=0 | 0 null renewed_policies_count values
  [PASS] not_null_cancelled_policies_count: PASSED | metric=0 | 0 null cancelled_policies_count values
  [PASS] not_null_claims_reported_count: PASSED | metric=0 | 0 null claims_reported_count values
  [PASS] not_null_claims_paid_count: PASSED | metric=0 | 0 null claims_paid_count values
  [PASS] not_null_gross_claims_incurred_eur: PASSED | metric=0 | 0 null gross_claims_incurred_eur values
  [PASS] not_null_management_expenses_eur: PASSED | metric=0 | 0 null management_expenses_eur values
  [PASS] not_null_commissions_eur: PASSED | metric=0 | 0 null commissions_eur values
  [PASS] not_null_customer_segment: PASSED | metric=0 | 0 null customer_segment values
  [PASS] not_null_risk_zone: PASSED | metric=0 | 0 null risk_zone values
  [PASS] not_null_primary_coverage: PASSED | metric=0 | 0 null primary_coverage values
  [PASS] not_null_currency: PASSED | metric=0 | 0 null currency values
  [PASS] values_in_allowed_list_customer_segment: PASSED | metric=0 | customer_segment values are in the configured allow-list: 0 violations
  [PASS] values_in_allowed_list_distribution_channel: PASSED | metric=0 | distribution_channel values are in the configured allow-list: 0 violations
  [PASS] values_in_allowed_list_risk_type: PASSED | metric=0 | risk_type values are in the configured allow-list: 0 violations
  [PASS] values_in_allowed_list_risk_zone: PASSED | metric=0 | risk_zone values are in the configured allow-list: 0 violations
  [PASS] numeric_premium_gross_gte_net: PASSED | metric=0 | gross_written_premium_eur >= net_written_premium_eur: 0 violations
  [PASS] numeric_claims_paid_lte_reported: PASSED | metric=0 | claims_paid_count <= claims_reported_count: 0 violations
  [PASS] loss_ratio_in_range: PASSED | metric=0 | 0 <= loss_ratio <= 2: 0 violations
  [PASS] column_mapping_version_present: PASSED | metric=0 | 0 rows without expected mapping_status flag
Total checks: 31
```

```python
dq_schema = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("source_system", StringType(), False),
        StructField("check_name", StringType(), True),
        StructField("check_status", StringType(), True),
        StructField("metric_value", DoubleType(), True),
        StructField("recorded_at", TimestampType(), True),
        StructField("details", StringType(), True),
    ]
)
dq_df = spark.createDataFrame(dq_results, schema=dq_schema)
dq_df.write.format("delta").mode("append").saveAsTable(DQ_TABLE)
print(f"Written {len(dq_results)} DQ results to {DQ_TABLE}")
```

Output:
```text
Written 31 DQ results to `agent_marketplace_catalog`.`halvard_harmonization`.`data_quality_results`
```

```python
display(
    spark.sql(f"""
SELECT task_name, task_status, row_count, started_at, finished_at,
       ROUND((unix_timestamp(finished_at) - unix_timestamp(started_at)) / 60.0, 1) AS duration_min,
       message
FROM {OPS_TABLE}
ORDER BY started_at DESC
LIMIT 20
""")
)

display(
    spark.sql(f"""
SELECT mapping_type, source_field_or_column, candidate_rows, success_rows,
       failed_rows, low_confidence_rows, estimated_prompt_units,
       estimated_response_units, estimated_cost_eur, recorded_at
FROM {USAGE_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
ORDER BY recorded_at DESC
LIMIT 10
""")
)

display(
    spark.sql(f"""
SELECT review_status, mandatory_flag, COUNT(*) AS count
FROM {CAND_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
GROUP BY review_status, mandatory_flag
ORDER BY mandatory_flag DESC, review_status
""")
)

display(
    spark.sql(f"""
SELECT local_column_name, global_column_name, match_type, mapping_version, active_flag
FROM {DICT_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
ORDER BY local_column_name
""")
)

existing_required = [column for column in REQUIRED_TARGET_COLUMNS if column in harm_columns]
required_cols_sql = (
    ",\n  ".join(f"COUNT(DISTINCT `{column}`) AS distinct_{column}" for column in existing_required)
    or "1 AS placeholder"
)
display(
    spark.sql(f"""
SELECT COUNT(*) AS row_count,
       {required_cols_sql},
       MAX(harmonization_timestamp) AS latest_harmonization
FROM {HARM_TABLE}
WHERE source_country = '{source_country}'
""")
)
```

```python
spark.sql(f"""
CREATE OR REPLACE VIEW {DB}.`vw_latest_column_mapping_summary` AS
SELECT
  c.source_system,
  c.local_column_name,
  c.local_data_type,
  c.proposed_global_column_name,
  COALESCE(d.global_column_name, c.proposed_global_column_name) AS active_global_column_name,
  c.proposed_match_type,
  COALESCE(d.match_type, c.proposed_match_type) AS active_match_type,
  c.confidence,
  c.review_status,
  c.mandatory_flag,
  c.reviewed_by,
  c.reviewed_at,
  CASE WHEN d.local_column_name IS NOT NULL AND d.active_flag = TRUE THEN TRUE ELSE FALSE END AS in_active_dictionary,
  d.mapping_version
FROM {CAND_TABLE} c
LEFT JOIN {DICT_TABLE} d
  ON c.source_system = d.source_system
  AND c.local_column_name = d.local_column_name
  AND d.active_flag = TRUE
WHERE c.source_system = '{SOURCE_SYSTEM}'
ORDER BY c.mandatory_flag DESC, c.local_column_name
""")

print("View vw_latest_column_mapping_summary created.")
```

Output:
```text
View vw_latest_column_mapping_summary created.
```

```python
passed = sum(result[3] == "PASSED" for result in dq_results)
warned = sum(result[3] == "WARNING" for result in dq_results)
failed = sum(result[3] == "FAILED" for result in dq_results)
overall_status = "SUCCEEDED" if failed == 0 else "FAILED"

log_run_metric(
    spark,
    OPS_TABLE,
    RUN_ID,
    "validate_and_monitor",
    overall_status,
    _start,
    harm_count,
    f"DQ: {passed} PASSED, {warned} WARNING, {failed} FAILED out of {len(dq_results)} checks. "
    f"Harmonized rows: {harm_count:,}.",
)

summary = {
    "source_country": source_country,
    "source_system": SOURCE_SYSTEM,
    "raw_rows": raw_count,
    "harmonized_rows": harm_count,
    "checks": len(dq_results),
    "passed": passed,
    "warnings": warned,
    "failed": failed,
    "overall_status": overall_status,
}

if failed > 0:
    raise Exception(
        f"DATA QUALITY FAILED: {failed} critical check(s) failed for {SOURCE_SYSTEM}. "
        f"Review data_quality_results (run_id={RUN_ID})."
    )

print(f"DQ: {passed} PASSED, {warned} WARNING, {failed} FAILED. Harmonized rows: {harm_count:,}")
dbutils.notebook.exit(json.dumps(summary))
```

Output:
```text
{"source_country": "ES", "source_system": "ES_PROPERTY_RAW", "raw_rows": 10000, "harmonized_rows": 10000, "checks": 31, "passed": 31, "warnings": 0, "failed": 0, "overall_status": "SUCCEEDED"}
```
