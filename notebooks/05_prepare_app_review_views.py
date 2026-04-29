# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Prepare App Review Views
# MAGIC
# MAGIC Refreshes the review views used by the Column Mapping Review Databricks App
# MAGIC and prints a summary of current mapping proposals ready for review.
# MAGIC
# MAGIC After this task completes, business users should open the Databricks App to
# MAGIC review and approve/correct/reject the AI-proposed column mappings.

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

print(f"Config: {DB}")

# COMMAND ----------

# MAGIC %md ## Imports

# COMMAND ----------

import datetime as _dt
from uuid import uuid4

RUN_ID = str(uuid4())
_start = _dt.datetime.utcnow()

# COMMAND ----------

# MAGIC %md ## Recreate All 5 Review Views

# COMMAND ----------

results = []


def execute_ddl(label, sql):
    try:
        spark.sql(sql)
        results.append(("OK", label))
        print(f"  [OK]  {label}")
    except Exception as e:
        results.append(("ERROR", label, str(e)))
        print(f"  [ERR] {label}: {e}")
        raise


execute_ddl(
    "vw_pending_column_mappings",
    f"""
CREATE OR REPLACE VIEW {DB}.`vw_pending_column_mappings` AS
SELECT
  candidate_id,
  source_system,
  local_column_name,
  local_data_type,
  local_sample_values,
  proposed_global_column_name,
  proposed_global_data_type,
  proposed_match_type,
  mapping_rationale,
  confidence,
  ai_error_status,
  review_status,
  mandatory_flag,
  created_at
FROM {DB}.`column_mapping_candidates`
WHERE review_status = 'PENDING'
ORDER BY mandatory_flag DESC, confidence DESC, local_column_name
""",
)

execute_ddl(
    "vw_mapping_review_summary",
    f"""
CREATE OR REPLACE VIEW {DB}.`vw_mapping_review_summary` AS
SELECT
  review_status,
  mandatory_flag,
  confidence,
  COUNT(*) AS count,
  SUM(CASE WHEN ai_error_status IS NOT NULL THEN 1 ELSE 0 END) AS ai_error_count
FROM {DB}.`column_mapping_candidates`
GROUP BY review_status, mandatory_flag, confidence
""",
)

execute_ddl(
    "vw_publish_readiness",
    f"""
CREATE OR REPLACE VIEW {DB}.`vw_publish_readiness` AS
WITH mandatory AS (
  SELECT local_column_name AS mandatory_column_name,
         review_status,
         final_global_column_name,
         final_match_type,
         reviewed_by,
         reviewed_at,
         app_decision_source,
         CASE WHEN review_status IN ('APPROVED','CORRECTED') THEN TRUE ELSE FALSE END AS is_ready
  FROM {DB}.`column_mapping_candidates`
  WHERE mandatory_flag = TRUE
)
SELECT * FROM mandatory
ORDER BY is_ready ASC, mandatory_column_name
""",
)

execute_ddl(
    "vw_column_mapping_low_conf",
    f"""
CREATE OR REPLACE VIEW {DB}.`vw_column_mapping_low_conf` AS
SELECT
  candidate_id,
  source_system,
  local_column_name,
  local_data_type,
  proposed_global_column_name,
  proposed_match_type,
  confidence,
  ai_error_status,
  review_status,
  mandatory_flag,
  mapping_rationale
FROM {DB}.`column_mapping_candidates`
WHERE UPPER(confidence) = 'LOW' OR ai_error_status IS NOT NULL
ORDER BY mandatory_flag DESC, local_column_name
""",
)

execute_ddl(
    "vw_column_mapping_coverage",
    f"""
CREATE OR REPLACE VIEW {DB}.`vw_column_mapping_coverage` AS
SELECT
  g.global_column_name,
  g.global_data_type,
  g.semantic_group,
  g.required_flag,
  g.business_definition,
  c.local_column_name,
  c.proposed_match_type,
  c.confidence,
  c.review_status,
  CASE
    WHEN c.review_status IN ('APPROVED','CORRECTED') THEN 'MAPPED'
    WHEN c.review_status = 'REJECTED'               THEN 'REJECTED'
    WHEN c.review_status = 'PENDING'                THEN 'PENDING'
    WHEN c.local_column_name IS NULL                THEN 'UNMAPPED'
    ELSE 'UNKNOWN'
  END AS coverage_status
FROM {DB}.`global_target_columns` g
LEFT JOIN {DB}.`column_mapping_candidates` c
  ON g.global_column_name = COALESCE(c.final_global_column_name, c.proposed_global_column_name)
ORDER BY g.semantic_group, g.global_column_name
""",
)

print(
    f"\nViews recreated: {sum(1 for r in results if r[0] == 'OK')} OK, {sum(1 for r in results if r[0] == 'ERROR')} ERROR"
)

# COMMAND ----------

# MAGIC %md ## Current Proposal Status Summary

# COMMAND ----------

summary_df = spark.sql(f"""
SELECT
  review_status,
  mandatory_flag,
  confidence,
  COUNT(*) AS count,
  SUM(CASE WHEN ai_error_status IS NOT NULL THEN 1 ELSE 0 END) AS ai_error_count
FROM {DB}.`column_mapping_candidates`
GROUP BY review_status, mandatory_flag, confidence
ORDER BY mandatory_flag DESC, review_status, confidence
""")

display(summary_df)

# COMMAND ----------

# MAGIC %md ## All Candidates with Current Review Status

# COMMAND ----------

all_candidates_df = spark.sql(f"""
SELECT
  local_column_name,
  local_data_type,
  proposed_global_column_name,
  proposed_match_type,
  confidence,
  ai_error_status,
  review_status,
  mandatory_flag,
  app_decision_source,
  reviewed_by,
  reviewed_at
FROM {DB}.`column_mapping_candidates`
ORDER BY mandatory_flag DESC, review_status, local_column_name
""")

display(all_candidates_df)

total_count = all_candidates_df.count()
pending_count = spark.sql(
    f"SELECT COUNT(*) AS cnt FROM {DB}.`column_mapping_candidates` WHERE review_status = 'PENDING'"
).collect()[0]["cnt"]
approved_count = spark.sql(
    f"SELECT COUNT(*) AS cnt FROM {DB}.`column_mapping_candidates` WHERE review_status IN ('APPROVED','CORRECTED')"
).collect()[0]["cnt"]
mandatory_pending = spark.sql(
    f"SELECT COUNT(*) AS cnt FROM {DB}.`column_mapping_candidates` WHERE mandatory_flag = TRUE AND review_status = 'PENDING'"
).collect()[0]["cnt"]

print(
    f"Total: {total_count}, Pending: {pending_count}, Approved/Corrected: {approved_count}, Mandatory pending: {mandatory_pending}"
)

# COMMAND ----------

# MAGIC %md ## Publish Readiness (Mandatory Columns)

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {DB}.`vw_publish_readiness`"))

# COMMAND ----------

# MAGIC %md ## Log to workflow_run_metrics

# COMMAND ----------

log_run_metric(
    spark,
    f"{DB}.`workflow_run_metrics`",
    RUN_ID,
    "prepare_app_review_views",
    "SUCCEEDED",
    _start,
    total_count,
    f"Refreshed 5 review views. Total candidates: {total_count}. Pending: {pending_count}. Mandatory pending: {mandatory_pending}.",
)
