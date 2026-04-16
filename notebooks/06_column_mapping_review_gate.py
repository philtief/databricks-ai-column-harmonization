# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Column Mapping Review Gate
# MAGIC
# MAGIC **This notebook is a workflow gate.**
# MAGIC
# MAGIC It checks that all mandatory source columns have been reviewed (APPROVED or CORRECTED)
# MAGIC via the Column Mapping Review Databricks App. If any mandatory column is still
# MAGIC PENDING or REJECTED without a corrected target, the notebook raises an exception
# MAGIC and the workflow stops.
# MAGIC
# MAGIC Mandatory columns are loaded from `config/harmonization_config.yaml`.
# MAGIC
# MAGIC **To unblock:** Review pending mappings using the Column Mapping Review Databricks App,
# MAGIC then re-run this task.

# COMMAND ----------

# MAGIC %run ./_shared_utils

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "pt_catalog",        "Catalog Name")
dbutils.widgets.text("schema_name",  "harmonizing_agent", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name  = dbutils.widgets.get("schema_name").strip()

DB            = f"`{catalog_name}`.`{schema_name}`"
_cfg          = load_harmonization_config()
_refs         = get_table_refs(_cfg, DB)
CAND_TABLE    = _refs["cand_table"]
OPS_TABLE     = _refs["ops_table"]
SOURCE_SYSTEM = _refs["source_system"]
MANDATORY_COLUMNS = _cfg["mandatory_source_columns"] if _cfg else [
    "id_registro", "anio", "mes", "codigo_poliza", "tipo_riesgo",
    "provincia", "canal_distribucion", "prima_neta", "prima_bruta",
    "num_siniestros_declarados", "num_siniestros_pagados",
    "segmento_cliente", "cobertura_principal", "moneda",
]

print(f"Config: {DB}")

# COMMAND ----------

# MAGIC %md ## Imports

# COMMAND ----------

import datetime as _dt
from uuid import uuid4
from pyspark.sql import functions as F

RUN_ID = str(uuid4())
_start = _dt.datetime.utcnow()

# COMMAND ----------

# MAGIC %md ## Overall Status Summary

# COMMAND ----------

all_candidates_df = spark.sql(f"""
SELECT
  review_status,
  mandatory_flag,
  COUNT(*) AS count
FROM {CAND_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
GROUP BY review_status, mandatory_flag
ORDER BY mandatory_flag DESC, review_status
""")

display(all_candidates_df)

# COMMAND ----------

# MAGIC %md ## Check Blocking Conditions

# COMMAND ----------

# Condition A: Mandatory columns that are still PENDING
blocking_pending_df = spark.sql(f"""
SELECT
  local_column_name,
  review_status,
  proposed_global_column_name,
  confidence,
  ai_error_status,
  mandatory_flag
FROM {CAND_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
  AND mandatory_flag = TRUE
  AND review_status = 'PENDING'
ORDER BY local_column_name
""")

pending_blocking = blocking_pending_df.collect()

# Condition B: Mandatory columns that are REJECTED with no final target set
blocking_rejected_df = spark.sql(f"""
SELECT
  local_column_name,
  review_status,
  proposed_global_column_name,
  final_global_column_name,
  confidence,
  ai_error_status,
  mandatory_flag
FROM {CAND_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
  AND mandatory_flag = TRUE
  AND review_status = 'REJECTED'
  AND (final_global_column_name IS NULL OR final_global_column_name = '')
ORDER BY local_column_name
""")

rejected_blocking = blocking_rejected_df.collect()

# Also check: mandatory columns that don't appear in candidates at all (not yet inventoried/proposed)
found_mandatory_cols = set(
    row["local_column_name"]
    for row in spark.sql(f"""
        SELECT DISTINCT local_column_name
        FROM {CAND_TABLE}
        WHERE source_system = '{SOURCE_SYSTEM}'
          AND mandatory_flag = TRUE
    """).collect()
)

missing_mandatory = [c for c in MANDATORY_COLUMNS if c not in found_mandatory_cols]

print(f"Mandatory PENDING: {len(pending_blocking)}, REJECTED no target: {len(rejected_blocking)}, Missing: {len(missing_mandatory)}")

# COMMAND ----------

# MAGIC %md ## Diagnostic Table

# COMMAND ----------

mandatory_status_df = spark.sql(f"""
SELECT
  local_column_name,
  review_status,
  proposed_global_column_name,
  COALESCE(final_global_column_name, proposed_global_column_name) AS resolved_global_column,
  proposed_match_type,
  confidence,
  ai_error_status,
  reviewed_by,
  reviewed_at
FROM {CAND_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
  AND mandatory_flag = TRUE
ORDER BY review_status, local_column_name
""")

display(mandatory_status_df)

# COMMAND ----------

# MAGIC %md ## Gate Decision

# COMMAND ----------

gate_status = "PASSED"
blocking_count = len(pending_blocking) + len(rejected_blocking) + len(missing_mandatory)

if blocking_count > 0:
    gate_status = "BLOCKED"

print(f"Gate decision: {gate_status}")

if gate_status == "BLOCKED":
    print(f"  BLOCKING: {len(pending_blocking)} PENDING, {len(rejected_blocking)} REJECTED no target, {len(missing_mandatory)} missing")
else:
    print("  All mandatory columns have an approved or corrected mapping.")

# COMMAND ----------

# MAGIC %md ## Log Gate Result to workflow_run_metrics

# COMMAND ----------

log_run_metric(spark, OPS_TABLE, RUN_ID, "column_mapping_review_gate", gate_status, _start, blocking_count,
               f"Gate {gate_status}. Blocking issues: {blocking_count}. Pending: {len(pending_blocking)}, Rejected-no-target: {len(rejected_blocking)}, Missing: {len(missing_mandatory)}.")

# COMMAND ----------

# MAGIC %md ## Raise Exception if Blocked

# COMMAND ----------

if gate_status == "BLOCKED":
    raise Exception(
        f"COLUMN MAPPING GATE BLOCKED: {blocking_count} mandatory column(s) are still PENDING or unresolved. "
        f"Review pending mappings using the Column Mapping Review Databricks App, "
        f"then re-run this task. "
        f"Pending: {[r['local_column_name'] for r in pending_blocking]}. "
        f"Rejected-no-target: {[r['local_column_name'] for r in rejected_blocking]}. "
        f"Missing from candidates: {missing_mandatory}."
    )

print("Gate PASSED. Proceeding to 07_build_column_mapping_dictionary.")
