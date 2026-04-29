# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Validate and Monitor
# MAGIC
# MAGIC Runs data quality checks on the harmonized output table, produces a
# MAGIC monitoring summary, and raises an exception if critical checks fail.
# MAGIC
# MAGIC **Always-on checks (no config needed):**
# MAGIC - `row_count_parity` — raw row count must equal harmonized row count
# MAGIC - `column_mapping_coverage` — all mandatory source columns must have active dict entries
# MAGIC - `column_mapping_version_present` — every row must carry a recognised `mapping_status`
# MAGIC - `not_null_<col>` — for each `required: true` column in `target_model.columns`
# MAGIC
# MAGIC **Custom checks (config-driven):**
# MAGIC - `data_quality_rules:` in `harmonization_config.yaml`. Each rule lists a
# MAGIC   `name`, `description`, SQL `predicate` (rows matching the predicate
# MAGIC   are *violations*), and `severity` (`FAILED` or `WARNING`).

# COMMAND ----------

# MAGIC %run ./_shared_utils

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "pt_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "harmonizing_agent", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()

DB = f"`{catalog_name}`.`{schema_name}`"
_cfg = load_harmonization_config()
_refs = get_table_refs(_cfg, DB)
RAW_TABLE = _refs["raw_table"]
HARM_TABLE = _refs["harm_table"]
DICT_TABLE = _refs["dict_table"]
CAND_TABLE = _refs["cand_table"]
DQ_TABLE = _refs["dq_table"]
USAGE_TABLE = _refs["usage_table"]
OPS_TABLE = _refs["ops_table"]
SOURCE_SYSTEM = _refs["source_system"]
MANDATORY_COLUMNS = _cfg["mandatory_source_columns"]

# Required target columns get an automatic not-null check.
REQUIRED_TARGET_COLUMNS = [c["name"] for c in _cfg["target_model"]["columns"] if c.get("required")]

# Custom DQ rules from the YAML config.
DQ_RULES = _cfg.get("data_quality_rules", []) or []

print(f"Config: {DB}, required_target_cols={len(REQUIRED_TARGET_COLUMNS)}, custom_rules={len(DQ_RULES)}")

# COMMAND ----------

# MAGIC %md ## Imports

# COMMAND ----------

import datetime as _dt
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from harmonization.validation import (
    check_constraint,
    check_mapping_coverage,
    check_mapping_version,
    check_null_count,
    check_row_parity,
)

RUN_ID = str(uuid4())
_start = _dt.datetime.utcnow()

# COMMAND ----------

# MAGIC %md ## Load Tables

# COMMAND ----------

raw_df = spark.table(RAW_TABLE)
harm_df = spark.table(HARM_TABLE)

raw_count = raw_df.count()
harm_count = harm_df.count()

print(f"Raw rows: {raw_count:,}, harmonized rows: {harm_count:,}")

# COMMAND ----------

# MAGIC %md ## Run Data Quality Checks

# COMMAND ----------

_now = _dt.datetime.utcnow()
dq_results = []


def record(result):
    name, status, metric, details = result
    emoji = "PASS" if status == "PASSED" else ("WARN" if status == "WARNING" else "FAIL")
    print(f"  [{emoji}] {name}: {status} | metric={metric} | {details}")
    dq_results.append((RUN_ID, name, status, float(metric), _now, details))


# --- Row parity --------------------------------------------------------------
record(check_row_parity(raw_count, harm_count))

# --- Mapping coverage --------------------------------------------------------
active_dict_cols = {
    row["local_column_name"]
    for row in spark.sql(f"""
        SELECT DISTINCT local_column_name
        FROM {DICT_TABLE}
        WHERE source_system = '{SOURCE_SYSTEM}' AND active_flag = TRUE
    """).collect()
}
record(check_mapping_coverage(MANDATORY_COLUMNS, active_dict_cols))

# --- Not-null checks for every required target column ----------------------
harm_columns = {f.name for f in harm_df.schema.fields}
for col in REQUIRED_TARGET_COLUMNS:
    if col not in harm_columns:
        # Required target column missing entirely from the harmonized output
        record((f"not_null_{col}", "FAILED", 0, f"target column '{col}' is missing from harmonized table"))
        continue
    null_count = harm_df.where(F.col(col).isNull()).count()
    record(check_null_count(col, null_count))

# --- Custom rules from config -----------------------------------------------
for rule in DQ_RULES:
    rule_name = rule["name"]
    predicate = rule["predicate"]
    description = rule.get("description", predicate)
    severity = (rule.get("severity") or "FAILED").upper()
    try:
        violations = harm_df.where(F.expr(predicate)).count()
    except Exception as e:
        record((rule_name, "FAILED", 0, f"predicate failed to evaluate: {e}"))
        continue
    record(check_constraint(rule_name, violations, description, severity=severity))

# --- Mapping version flag check ---------------------------------------------
if "mapping_status" in harm_columns:
    bad_status = harm_df.where(
        ~F.col("mapping_status").isin("APPROVED_COLUMN_MAPPING", "APPROVED_COLUMN_AND_VALUE_MAPPING")
    ).count()
    record(check_mapping_version(bad_status))

print(f"\n  Total checks: {len(dq_results)}")

# COMMAND ----------

# MAGIC %md ## Write DQ Results to data_quality_results

# COMMAND ----------

dq_schema = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("check_name", StringType(), True),
        StructField("check_status", StringType(), True),
        StructField("metric_value", DoubleType(), True),
        StructField("recorded_at", TimestampType(), True),
        StructField("details", StringType(), True),
    ]
)

dq_df = spark.createDataFrame(dq_results, schema=dq_schema)
dq_df.write.format("delta").mode("append").saveAsTable(DQ_TABLE)

print(f"Written {len(dq_results)} DQ check results to {DQ_TABLE}")

# COMMAND ----------

# MAGIC %md ## Monitoring Summary

# COMMAND ----------

# Workflow run metrics (latest per task)
display(
    spark.sql(f"""
SELECT
  task_name,
  task_status,
  row_count,
  started_at,
  finished_at,
  ROUND((unix_timestamp(finished_at) - unix_timestamp(started_at)) / 60.0, 1) AS duration_min,
  message
FROM {OPS_TABLE}
ORDER BY started_at DESC
LIMIT 20
""")
)

# AI usage metrics
display(
    spark.sql(f"""
SELECT
  mapping_type,
  source_field_or_column,
  candidate_rows,
  success_rows,
  failed_rows,
  low_confidence_rows,
  estimated_prompt_units,
  estimated_response_units,
  estimated_cost_eur,
  recorded_at
FROM {USAGE_TABLE}
ORDER BY recorded_at DESC
LIMIT 10
""")
)

# Column mapping candidates by review_status
display(
    spark.sql(f"""
SELECT
  review_status,
  mandatory_flag,
  COUNT(*) AS count
FROM {CAND_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
GROUP BY review_status, mandatory_flag
ORDER BY mandatory_flag DESC, review_status
""")
)

# Column mapping dictionary coverage
display(
    spark.sql(f"""
SELECT
  d.local_column_name,
  d.global_column_name,
  d.match_type,
  d.mapping_version,
  d.active_flag
FROM {DICT_TABLE} d
WHERE d.source_system = '{SOURCE_SYSTEM}'
ORDER BY d.local_column_name
""")
)

# Harmonized output stats — generic counts across all required target columns
_required_cols_sql = (
    ",\n  ".join(f"COUNT(DISTINCT `{c}`) AS distinct_{c}" for c in REQUIRED_TARGET_COLUMNS) or "1 AS placeholder"
)

display(
    spark.sql(f"""
SELECT
  COUNT(*) AS row_count,
  {_required_cols_sql},
  MAX(harmonization_timestamp) AS latest_harmonization
FROM {HARM_TABLE}
""")
)

# COMMAND ----------

# MAGIC %md ## Create Monitoring View

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {DB}.`vw_latest_column_mapping_summary` AS
SELECT
  c.local_column_name,
  c.local_data_type,
  c.proposed_global_column_name,
  COALESCE(d.global_column_name, c.proposed_global_column_name) AS active_global_column_name,
  c.proposed_match_type,
  COALESCE(d.match_type, c.proposed_match_type)                 AS active_match_type,
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
display(spark.sql(f"SELECT * FROM {DB}.`vw_latest_column_mapping_summary`"))

# COMMAND ----------

# MAGIC %md ## Log to workflow_run_metrics

# COMMAND ----------

passed = sum(1 for r in dq_results if r[2] == "PASSED")
warned = sum(1 for r in dq_results if r[2] == "WARNING")
failed = sum(1 for r in dq_results if r[2] == "FAILED")
total = len(dq_results)

overall_status = "SUCCEEDED" if failed == 0 else "FAILED"

log_run_metric(
    spark,
    OPS_TABLE,
    RUN_ID,
    "validate_and_monitor",
    overall_status,
    _start,
    harm_count,
    f"DQ: {passed} PASSED, {warned} WARNING, {failed} FAILED out of {total} checks. Harmonized rows: {harm_count:,}.",
)

# COMMAND ----------

# MAGIC %md ## Raise Exception on Critical Failures

# COMMAND ----------

if failed > 0:
    failed_checks = [r[1] for r in dq_results if r[2] == "FAILED"]
    raise Exception(
        f"DATA QUALITY FAILED: {failed} critical check(s) failed: {failed_checks}. "
        f"Review data_quality_results table (run_id={RUN_ID}) for details."
    )

print(f"DQ: {passed} PASSED, {warned} WARNING, {failed} FAILED. Harmonized rows: {harm_count:,}")
