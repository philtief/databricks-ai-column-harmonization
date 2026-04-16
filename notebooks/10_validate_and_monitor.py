# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Validate and Monitor
# MAGIC
# MAGIC Runs data quality checks on the harmonized `property_insurance_monthly` table,
# MAGIC produces a monitoring summary, and raises an exception if critical checks fail.
# MAGIC
# MAGIC **Data Quality Checks:**
# MAGIC 1. `row_count_parity` — raw count must equal harmonized count
# MAGIC 2. `column_mapping_coverage` — mandatory columns must all have active dict entries
# MAGIC 3. `not_null_record_id` — record_id must not be null
# MAGIC 4. `not_null_reporting_year` — reporting_year must not be null
# MAGIC 5. `not_null_reporting_month` — reporting_month must not be null
# MAGIC 6. `not_null_policy_number` — policy_number must not be null
# MAGIC 7. `numeric_premium_gross_gte_net` — gross_written_premium_eur >= net_written_premium_eur
# MAGIC 8. `numeric_claims_paid_lte_reported` — claims_paid_count <= claims_reported_count
# MAGIC 9. `loss_ratio_in_range` — 0 <= loss_ratio <= 2
# MAGIC 10. `column_mapping_version_present` — all rows have mapping_status = APPROVED_COLUMN_MAPPING*

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "pt_catalog",        "Catalog Name")
dbutils.widgets.text("schema_name",  "harmonizing_agent", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name  = dbutils.widgets.get("schema_name").strip()

DB           = f"`{catalog_name}`.`{schema_name}`"
RAW_TABLE    = f"{DB}.`property_insurance_monthly_raw`"
HARM_TABLE   = f"{DB}.`property_insurance_monthly`"
DICT_TABLE   = f"{DB}.`column_mapping_dictionary_es`"
CAND_TABLE   = f"{DB}.`column_mapping_candidates_es`"
DQ_TABLE     = f"{DB}.`data_quality_results`"
USAGE_TABLE  = f"{DB}.`ai_mapping_usage_metrics`"
OPS_TABLE    = f"{DB}.`workflow_run_metrics`"
SOURCE_SYSTEM = "ES_PROPERTY_RAW"

MANDATORY_COLUMNS = [
    "id_registro", "anio", "mes", "codigo_poliza", "tipo_riesgo",
    "provincia", "canal_distribucion", "prima_neta", "prima_bruta",
    "num_siniestros_declarados", "num_siniestros_pagados",
    "segmento_cliente", "cobertura_principal", "moneda",
]

print("STEP 1 — Parameters loaded")
print(f"  catalog_name : {catalog_name}")
print(f"  schema_name  : {schema_name}")

# COMMAND ----------

# MAGIC %md ## STEP 2 — Imports

# COMMAND ----------

import datetime as _dt
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, TimestampType
)

RUN_ID = str(uuid4())
_start = _dt.datetime.utcnow()

print(f"STEP 2 — Imports done. RUN_ID = {RUN_ID}")

# COMMAND ----------

# MAGIC %md ## STEP 3 — Load Tables

# COMMAND ----------

print("STEP 3 — Loading tables ...")

raw_df  = spark.table(RAW_TABLE)
harm_df = spark.table(HARM_TABLE)

raw_count  = raw_df.count()
harm_count = harm_df.count()

print(f"  Raw table rows        : {raw_count:,}")
print(f"  Harmonized table rows : {harm_count:,}")

# COMMAND ----------

# MAGIC %md ## STEP 4 — Data Quality Checks

# COMMAND ----------

print("STEP 4 — Running data quality checks ...")

_now = _dt.datetime.utcnow()
dq_results = []

def record_check(check_name, status, metric_value, details):
    emoji = "PASS" if status == "PASSED" else ("WARN" if status == "WARNING" else "FAIL")
    print(f"  [{emoji}] {check_name}: {status} | metric={metric_value} | {details}")
    dq_results.append((RUN_ID, check_name, status, float(metric_value), _now, details))

# --- CHECK 1: row_count_parity ---
if raw_count == harm_count:
    record_check("row_count_parity", "PASSED", harm_count,
                 f"raw={raw_count:,}, harmonized={harm_count:,}")
else:
    record_check("row_count_parity", "FAILED", abs(raw_count - harm_count),
                 f"MISMATCH: raw={raw_count:,}, harmonized={harm_count:,}")

# --- CHECK 2: column_mapping_coverage ---
active_dict_cols = set(
    row["local_column_name"]
    for row in spark.sql(f"""
        SELECT DISTINCT local_column_name
        FROM {DICT_TABLE}
        WHERE source_system = '{SOURCE_SYSTEM}' AND active_flag = TRUE
    """).collect()
)
mandatory_covered = [c for c in MANDATORY_COLUMNS if c in active_dict_cols]
coverage_pct = len(mandatory_covered) / len(MANDATORY_COLUMNS) * 100
status = "PASSED" if coverage_pct >= 100.0 else "WARNING"
record_check("column_mapping_coverage", status, round(coverage_pct, 1),
             f"{len(mandatory_covered)}/{len(MANDATORY_COLUMNS)} mandatory columns have active dict entries")

# --- CHECK 3: not_null_record_id ---
null_record_id = harm_df.where(F.col("record_id").isNull()).count()
status = "PASSED" if null_record_id == 0 else "FAILED"
record_check("not_null_record_id", status, null_record_id,
             f"{null_record_id} null record_id values")

# --- CHECK 4: not_null_reporting_year ---
null_year = harm_df.where(F.col("reporting_year").isNull()).count()
status = "PASSED" if null_year == 0 else "FAILED"
record_check("not_null_reporting_year", status, null_year,
             f"{null_year} null reporting_year values")

# --- CHECK 5: not_null_reporting_month ---
null_month = harm_df.where(F.col("reporting_month").isNull()).count()
status = "PASSED" if null_month == 0 else "FAILED"
record_check("not_null_reporting_month", status, null_month,
             f"{null_month} null reporting_month values")

# --- CHECK 6: not_null_policy_number ---
null_policy = harm_df.where(F.col("policy_number").isNull()).count()
status = "PASSED" if null_policy == 0 else "FAILED"
record_check("not_null_policy_number", status, null_policy,
             f"{null_policy} null policy_number values")

# --- CHECK 7: numeric_premium_gross_gte_net ---
premium_violations = harm_df.where(
    F.col("gross_written_premium_eur").isNotNull() &
    F.col("net_written_premium_eur").isNotNull() &
    (F.col("gross_written_premium_eur") < F.col("net_written_premium_eur"))
).count()
status = "PASSED" if premium_violations == 0 else "FAILED"
record_check("numeric_premium_gross_gte_net", status, premium_violations,
             f"{premium_violations} rows where gross_written_premium_eur < net_written_premium_eur")

# --- CHECK 8: numeric_claims_paid_lte_reported ---
claims_violations = harm_df.where(
    F.col("claims_paid_count").isNotNull() &
    F.col("claims_reported_count").isNotNull() &
    (F.col("claims_paid_count") > F.col("claims_reported_count"))
).count()
status = "PASSED" if claims_violations == 0 else "FAILED"
record_check("numeric_claims_paid_lte_reported", status, claims_violations,
             f"{claims_violations} rows where claims_paid_count > claims_reported_count")

# --- CHECK 9: loss_ratio_in_range ---
harm_cols = [f.name for f in harm_df.schema.fields]
if "loss_ratio" in harm_cols:
    lr_violations = harm_df.where(
        F.col("loss_ratio").isNotNull() &
        ((F.col("loss_ratio") < 0.0) | (F.col("loss_ratio") > 2.0))
    ).count()
    status = "WARNING" if lr_violations > 0 else "PASSED"
    record_check("loss_ratio_in_range", status, lr_violations,
                 f"{lr_violations} rows where loss_ratio is outside [0, 2]")
else:
    record_check("loss_ratio_in_range", "WARNING", 0,
                 "loss_ratio column not present in harmonized table (computed column not yet applied)")

# --- CHECK 10: column_mapping_version_present ---
bad_status = harm_df.where(
    ~F.col("mapping_status").isin("APPROVED_COLUMN_MAPPING", "APPROVED_COLUMN_AND_VALUE_MAPPING")
).count()
status = "WARNING" if bad_status > 0 else "PASSED"
record_check("column_mapping_version_present", status, bad_status,
             f"{bad_status} rows without expected mapping_status flag")

print(f"\n  Total checks: {len(dq_results)}")

# COMMAND ----------

# MAGIC %md ## STEP 5 — Write DQ Results to data_quality_results

# COMMAND ----------

print(f"STEP 5 — Writing DQ results to {DQ_TABLE} ...")

dq_schema = StructType([
    StructField("run_id",       StringType(),    False),
    StructField("check_name",   StringType(),    True),
    StructField("check_status", StringType(),    True),
    StructField("metric_value", DoubleType(),    True),
    StructField("recorded_at",  TimestampType(), True),
    StructField("details",      StringType(),    True),
])

dq_df = spark.createDataFrame(dq_results, schema=dq_schema)
dq_df.write.format("delta").mode("append").saveAsTable(DQ_TABLE)

print(f"  Written {len(dq_results)} DQ check results.")

# COMMAND ----------

# MAGIC %md ## STEP 6 — Monitoring Summary

# COMMAND ----------

print("STEP 6 — Monitoring Summary")

# Workflow run metrics (latest per task)
print("\n  Recent workflow runs (latest per task):")
display(spark.sql(f"""
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
"""))

# AI usage metrics
print("\n  AI mapping usage (latest run):")
display(spark.sql(f"""
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
"""))

# Column mapping candidates by review_status
print("\n  Column mapping candidates by status:")
display(spark.sql(f"""
SELECT
  review_status,
  mandatory_flag,
  COUNT(*) AS count
FROM {CAND_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
GROUP BY review_status, mandatory_flag
ORDER BY mandatory_flag DESC, review_status
"""))

# Column mapping dictionary coverage
print("\n  Column mapping dictionary coverage:")
display(spark.sql(f"""
SELECT
  d.local_column_name,
  d.global_column_name,
  d.match_type,
  d.mapping_version,
  d.active_flag
FROM {DICT_TABLE} d
WHERE d.source_system = '{SOURCE_SYSTEM}'
ORDER BY d.local_column_name
"""))

# Harmonized output stats
print("\n  Harmonized output statistics:")
display(spark.sql(f"""
SELECT
  COUNT(*)                                    AS row_count,
  ROUND(SUM(gross_written_premium_eur), 2)    AS total_gwp_eur,
  ROUND(AVG(gross_written_premium_eur), 2)    AS avg_gwp_eur,
  ROUND(AVG(net_written_premium_eur), 2)      AS avg_nwp_eur,
  ROUND(AVG(CAST(NULL AS DOUBLE)), 4)         AS avg_loss_ratio,  -- placeholder; loss_ratio not in this table
  MIN(reporting_year)                          AS min_year,
  MAX(reporting_year)                          AS max_year,
  COUNT(DISTINCT currency)                     AS distinct_currencies,
  COUNT(DISTINCT source_country)               AS distinct_source_countries,
  MAX(harmonization_timestamp)                 AS latest_harmonization
FROM {HARM_TABLE}
"""))

# COMMAND ----------

# MAGIC %md ## STEP 7 — Create Monitoring View

# COMMAND ----------

print("STEP 7 — Creating vw_latest_column_mapping_summary ...")

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

print("  View vw_latest_column_mapping_summary created.")
display(spark.sql(f"SELECT * FROM {DB}.`vw_latest_column_mapping_summary`"))

# COMMAND ----------

# MAGIC %md ## STEP 8 — Compact DQ Summary Box

# COMMAND ----------

passed  = sum(1 for r in dq_results if r[2] == "PASSED")
warned  = sum(1 for r in dq_results if r[2] == "WARNING")
failed  = sum(1 for r in dq_results if r[2] == "FAILED")
total   = len(dq_results)

print()
print("=" * 60)
print("  DATA QUALITY SUMMARY")
print("=" * 60)
print(f"  Total checks : {total}")
print(f"  PASSED       : {passed}")
print(f"  WARNING      : {warned}")
print(f"  FAILED       : {failed}")
print()
for r in dq_results:
    icon = "OK  " if r[2] == "PASSED" else ("WARN" if r[2] == "WARNING" else "FAIL")
    print(f"  [{icon}] {r[1]:<45s} {r[3]}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md ## STEP 9 — Log to workflow_run_metrics

# COMMAND ----------

_end = _dt.datetime.utcnow()

log_schema = StructType([
    StructField("run_id",        StringType(),    False),
    StructField("workflow_name", StringType(),    True),
    StructField("task_name",     StringType(),    True),
    StructField("task_status",   StringType(),    True),
    StructField("started_at",    TimestampType(), True),
    StructField("finished_at",   TimestampType(), True),
    StructField("row_count",     LongType(),      True),
    StructField("message",       StringType(),    True),
])

overall_status = "SUCCEEDED" if failed == 0 else "FAILED"

log_df = spark.createDataFrame([(
    RUN_ID,
    "PT_ES_Column_Mapping_To_Global_Model",
    "validate_and_monitor",
    overall_status,
    _start,
    _end,
    harm_count,
    f"DQ: {passed} PASSED, {warned} WARNING, {failed} FAILED out of {total} checks. Harmonized rows: {harm_count:,}.",
)], schema=log_schema)

log_df.write.format("delta").mode("append").saveAsTable(OPS_TABLE)

print(f"STEP 9 — Logged run record '{overall_status}'. RUN_ID={RUN_ID}")

# COMMAND ----------

# MAGIC %md ## STEP 10 — Raise Exception on Critical Failures

# COMMAND ----------

if failed > 0:
    failed_checks = [r[1] for r in dq_results if r[2] == "FAILED"]
    raise Exception(
        f"DATA QUALITY FAILED: {failed} critical check(s) failed: {failed_checks}. "
        f"Review data_quality_results table (run_id={RUN_ID}) for details."
    )

print()
print("=" * 60)
print("  10_validate_and_monitor COMPLETE")
print(f"  DQ: {passed} PASSED, {warned} WARNING, {failed} FAILED")
print(f"  Harmonized rows: {harm_count:,}")
print("=" * 60)
