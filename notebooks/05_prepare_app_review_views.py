# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Prepare App Review Views
# MAGIC
# MAGIC Refreshes the review views, pushes this country's candidates to the Lakebase
# MAGIC review queue, and prints the operational status summary.

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]>=3.2" "databricks-sdk>=0.81" -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_shared_utils

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

import json

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "your_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "harmonizing_agent", "Schema Name")
dbutils.widgets.text("source_country", "ES", "Source Country")
dbutils.widgets.text("mapping_version", "v1", "Mapping Version")
dbutils.widgets.text("lakebase_endpoint", "", "Lakebase Endpoint")
dbutils.widgets.text("app_service_principal", "", "App service principal (Lakebase grant)")
dbutils.widgets.text("app_name", "", "App name (resolves the app service principal)")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
source_country = dbutils.widgets.get("source_country").strip()
mapping_version = dbutils.widgets.get("mapping_version").strip()
lakebase_endpoint = dbutils.widgets.get("lakebase_endpoint").strip()
app_service_principal = dbutils.widgets.get("app_service_principal").strip()
app_name = dbutils.widgets.get("app_name").strip()
if not app_service_principal and app_name:
    # The jobs get the app name, not its SP id: the app references the publish job, so a direct reference is a cycle.
    from databricks.sdk import WorkspaceClient

    app_service_principal = WorkspaceClient().apps.get(app_name).service_principal_client_id

if not lakebase_endpoint:
    raise ValueError("lakebase_endpoint must not be empty")

DB = f"`{catalog_name}`.`{schema_name}`"
_cfg = load_harmonization_config()
_refs = get_table_refs(_cfg, DB, source_country)
CAND_TABLE = _refs["cand_table"]
SOURCE_SYSTEM = _refs["source_system"]
SOURCE_TABLE = _refs["source_table_name"]
OPS_TABLE = _refs["ops_table"]

print(f"Config: {DB}, country={source_country}, endpoint={lakebase_endpoint}")

# COMMAND ----------

# MAGIC %md ## Imports

# COMMAND ----------

from uuid import uuid4

from databricks.sdk import WorkspaceClient

from harmonization.review_store import connect, ensure_schema, status_summary, upsert_queue

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
  source_system,
  review_status,
  mandatory_flag,
  confidence,
  COUNT(*) AS count,
  SUM(CASE WHEN ai_error_status IS NOT NULL THEN 1 ELSE 0 END) AS ai_error_count
FROM {DB}.`column_mapping_candidates`
GROUP BY source_system, review_status, mandatory_flag, confidence
""",
)

execute_ddl(
    "vw_publish_readiness",
    f"""
CREATE OR REPLACE VIEW {DB}.`vw_publish_readiness` AS
WITH mandatory AS (
  SELECT source_system,
         local_column_name AS mandatory_column_name,
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
ORDER BY is_ready ASC, source_system, mandatory_column_name
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
  c.source_system,
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

print(f"Views recreated: {sum(result[0] == 'OK' for result in results)} OK")

# COMMAND ----------

# MAGIC %md ## Push Review Queue to Lakebase

# COMMAND ----------

now = _dt.datetime.utcnow()
candidate_rows = spark.sql(f"""
SELECT local_column_name, local_data_type, local_sample_values,
       proposed_global_column_name, proposed_match_type, mapping_rationale,
       confidence, mandatory_flag
FROM {CAND_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
ORDER BY local_column_name
""").collect()

queue_rows = [
    {
        "source_system": SOURCE_SYSTEM,
        "local_column_name": row["local_column_name"],
        "local_data_type": row["local_data_type"],
        "local_sample_values": list(row["local_sample_values"] or []),
        "proposed_global_column_name": row["proposed_global_column_name"],
        "proposed_match_type": row["proposed_match_type"],
        "mapping_rationale": row["mapping_rationale"],
        "confidence": row["confidence"],
        "mandatory_flag": bool(row["mandatory_flag"]),
        "mapping_version": mapping_version,
        "published_at": now,
        "updated_at": now,
    }
    for row in candidate_rows
]

workspace_client = WorkspaceClient()
conn = connect(workspace_client, lakebase_endpoint)
try:
    ensure_schema(conn, grant_to=app_service_principal or None)
    rows_pushed = upsert_queue(conn, queue_rows)
    conn.commit()
    lakebase_status = status_summary(conn)
finally:
    conn.close()

print(f"Queue rows pushed: {rows_pushed}")
print("Status summary:")
for status_row in lakebase_status:
    print(
        f"  {status_row['source_system']} {status_row['review_status']} "
        f"mandatory={status_row['mandatory_flag']} count={status_row['count']}"
    )

# COMMAND ----------

# MAGIC %md ## Current Proposal Status Summary

# COMMAND ----------

summary_df = spark.sql(f"""
SELECT review_status, mandatory_flag, confidence, COUNT(*) AS count,
       SUM(CASE WHEN ai_error_status IS NOT NULL THEN 1 ELSE 0 END) AS ai_error_count
FROM {CAND_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
GROUP BY review_status, mandatory_flag, confidence
ORDER BY mandatory_flag DESC, review_status, confidence
""")
display(summary_df)

total_count = len(candidate_rows)
pending_count = spark.sql(f"""
SELECT COUNT(*) AS cnt
FROM {CAND_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}' AND review_status = 'PENDING'
""").collect()[0]["cnt"]
approved_count = spark.sql(f"""
SELECT COUNT(*) AS cnt
FROM {CAND_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}' AND review_status IN ('APPROVED','CORRECTED')
""").collect()[0]["cnt"]
mandatory_pending = spark.sql(f"""
SELECT COUNT(*) AS cnt
FROM {CAND_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}' AND mandatory_flag = TRUE AND review_status = 'PENDING'
""").collect()[0]["cnt"]

print(
    f"Total: {total_count}, Pending: {pending_count}, "
    f"Approved/Corrected: {approved_count}, Mandatory pending: {mandatory_pending}"
)

# COMMAND ----------

# MAGIC %md ## Publish Readiness

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {DB}.`vw_publish_readiness` WHERE source_system = '{SOURCE_SYSTEM}'"))

# COMMAND ----------

# MAGIC %md ## Log and Exit

# COMMAND ----------

log_run_metric(
    spark,
    OPS_TABLE,
    RUN_ID,
    "prepare_app_review_views",
    "SUCCEEDED",
    _start,
    rows_pushed,
    f"Refreshed 5 review views and pushed {rows_pushed} queue rows for {SOURCE_SYSTEM}.",
)

summary = {
    "source_country": source_country,
    "source_system": SOURCE_SYSTEM,
    "source_table": SOURCE_TABLE,
    "candidates": total_count,
    "pending": pending_count,
    "approved_or_corrected": approved_count,
    "mandatory_pending": mandatory_pending,
    "rows_pushed": rows_pushed,
}
dbutils.notebook.exit(json.dumps(summary))
