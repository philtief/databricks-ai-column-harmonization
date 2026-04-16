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

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "pt_catalog",        "Catalog Name")
dbutils.widgets.text("schema_name",  "harmonizing_agent", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name  = dbutils.widgets.get("schema_name").strip()
DB = f"`{catalog_name}`.`{schema_name}`"

print("STEP 1 — Parameters loaded")
print(f"  catalog_name : {catalog_name}")
print(f"  schema_name  : {schema_name}")
print(f"  DB prefix    : {DB}")

# COMMAND ----------

import datetime as _dt
from uuid import uuid4
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

RUN_ID = str(uuid4())
_start = _dt.datetime.utcnow()

print(f"STEP 2 — Imports done. RUN_ID = {RUN_ID}")

# COMMAND ----------

# MAGIC %md ## STEP 3 — Recreate All 5 Review Views

# COMMAND ----------

print("STEP 3 — Recreating all 5 review views (CREATE OR REPLACE) ...")

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

execute_ddl("vw_pending_column_mappings", f"""
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
FROM {DB}.`column_mapping_candidates_es`
WHERE review_status = 'PENDING'
ORDER BY mandatory_flag DESC, confidence DESC, local_column_name
""")

execute_ddl("vw_mapping_review_summary", f"""
CREATE OR REPLACE VIEW {DB}.`vw_mapping_review_summary` AS
SELECT
  review_status,
  mandatory_flag,
  confidence,
  COUNT(*) AS count,
  SUM(CASE WHEN ai_error_status IS NOT NULL THEN 1 ELSE 0 END) AS ai_error_count
FROM {DB}.`column_mapping_candidates_es`
GROUP BY review_status, mandatory_flag, confidence
""")

execute_ddl("vw_publish_readiness", f"""
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
  FROM {DB}.`column_mapping_candidates_es`
  WHERE mandatory_flag = TRUE
)
SELECT * FROM mandatory
ORDER BY is_ready ASC, mandatory_column_name
""")

execute_ddl("vw_column_mapping_low_conf_es", f"""
CREATE OR REPLACE VIEW {DB}.`vw_column_mapping_low_conf_es` AS
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
FROM {DB}.`column_mapping_candidates_es`
WHERE UPPER(confidence) = 'LOW' OR ai_error_status IS NOT NULL
ORDER BY mandatory_flag DESC, local_column_name
""")

execute_ddl("vw_column_mapping_coverage_es", f"""
CREATE OR REPLACE VIEW {DB}.`vw_column_mapping_coverage_es` AS
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
LEFT JOIN {DB}.`column_mapping_candidates_es` c
  ON g.global_column_name = COALESCE(c.final_global_column_name, c.proposed_global_column_name)
ORDER BY g.semantic_group, g.global_column_name
""")

print(f"\n  Views recreated: {sum(1 for r in results if r[0] == 'OK')} OK, {sum(1 for r in results if r[0] == 'ERROR')} ERROR")

# COMMAND ----------

# MAGIC %md ## STEP 4 — Current Proposal Status Summary

# COMMAND ----------

print("STEP 4 — Current mapping proposal status:")

summary_df = spark.sql(f"""
SELECT
  review_status,
  mandatory_flag,
  confidence,
  COUNT(*) AS count,
  SUM(CASE WHEN ai_error_status IS NOT NULL THEN 1 ELSE 0 END) AS ai_error_count
FROM {DB}.`column_mapping_candidates_es`
GROUP BY review_status, mandatory_flag, confidence
ORDER BY mandatory_flag DESC, review_status, confidence
""")

display(summary_df)

# COMMAND ----------

# MAGIC %md ## STEP 5 — All Candidates with Current Review Status

# COMMAND ----------

print("STEP 5 — All column mapping candidates with current review status:")

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
FROM {DB}.`column_mapping_candidates_es`
ORDER BY mandatory_flag DESC, review_status, local_column_name
""")

display(all_candidates_df)

total_count   = all_candidates_df.count()
pending_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {DB}.`column_mapping_candidates_es` WHERE review_status = 'PENDING'").collect()[0]["cnt"]
approved_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {DB}.`column_mapping_candidates_es` WHERE review_status IN ('APPROVED','CORRECTED')").collect()[0]["cnt"]
mandatory_pending = spark.sql(f"SELECT COUNT(*) AS cnt FROM {DB}.`column_mapping_candidates_es` WHERE mandatory_flag = TRUE AND review_status = 'PENDING'").collect()[0]["cnt"]

print(f"\n  Total candidates        : {total_count}")
print(f"  Pending review          : {pending_count}")
print(f"  Approved / Corrected    : {approved_count}")
print(f"  Mandatory still pending : {mandatory_pending}")

# COMMAND ----------

# MAGIC %md ## STEP 6 — Publish Readiness (Mandatory Columns)

# COMMAND ----------

print("STEP 6 — Mandatory column publish readiness:")

display(spark.sql(f"SELECT * FROM {DB}.`vw_publish_readiness`"))

# COMMAND ----------

# MAGIC %md ## STEP 7 — Instructions for Databricks App Review

# COMMAND ----------

print("=" * 70)
print("  NEXT ACTION: Review Mappings in the Databricks App")
print("=" * 70)
print()
print("  1. Open the Column Mapping Review Databricks App.")
print("  2. Review each PENDING mapping — APPROVE, CORRECT, or REJECT.")
print("  3. Pay particular attention to the 14 mandatory columns:")
print("       id_registro, anio, mes, codigo_poliza, tipo_riesgo,")
print("       provincia, canal_distribucion, prima_neta, prima_bruta,")
print("       num_siniestros_declarados, num_siniestros_pagados,")
print("       segmento_cliente, cobertura_principal, moneda")
print()
print("  4. Once all 14 mandatory columns are APPROVED or CORRECTED,")
print("     re-run the workflow from task 06_column_mapping_review_gate.")
print()

try:
    app_url = dbutils.secrets.get(scope="pt_harmonization", key="app_url")
    print(f"  Databricks App URL: {app_url}")
except Exception:
    print("  Databricks App URL: Open the Databricks App from the Apps section")
    print("  of your Databricks workspace.")

print()
print("  Review mappings in the Databricks App before re-running the workflow")
print("  from column_mapping_review_gate.")
print("=" * 70)

# COMMAND ----------

# MAGIC %md ## STEP 8 — Log to workflow_run_metrics

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

log_df = spark.createDataFrame([(
    RUN_ID,
    "PT_ES_Column_Mapping_To_Global_Model",
    "prepare_app_review_views",
    "SUCCEEDED",
    _start,
    _end,
    total_count,
    f"Refreshed 5 review views. Total candidates: {total_count}. Pending: {pending_count}. Mandatory pending: {mandatory_pending}.",
)], schema=log_schema)

log_df.write.format("delta").mode("append").saveAsTable(f"{DB}.`workflow_run_metrics`")

print(f"STEP 8 — Logged run record. RUN_ID={RUN_ID}")
print()
print("=" * 60)
print("  05_prepare_app_review_views COMPLETE")
print(f"  Total candidates   : {total_count}")
print(f"  Pending review     : {pending_count}")
print(f"  Mandatory pending  : {mandatory_pending}")
print("=" * 60)
