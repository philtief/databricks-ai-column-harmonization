# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Column Mapping Review Gate
# MAGIC
# MAGIC **This notebook is a workflow gate.**
# MAGIC
# MAGIC It checks that all mandatory source columns have been reviewed (APPROVED or CORRECTED)
# MAGIC by the Spain local entity via the Column Mapping Review Databricks App. If any mandatory
# MAGIC column is still PENDING or REJECTED without a corrected target, the notebook raises an
# MAGIC exception and the workflow stops.
# MAGIC
# MAGIC **Mandatory columns (14):**
# MAGIC `id_registro, anio, mes, codigo_poliza, tipo_riesgo, provincia, canal_distribucion,
# MAGIC prima_neta, prima_bruta, num_siniestros_declarados, num_siniestros_pagados,
# MAGIC segmento_cliente, cobertura_principal, moneda`
# MAGIC
# MAGIC **To unblock:** Review pending mappings using the Column Mapping Review Databricks App,
# MAGIC then re-run this task.

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "pt_catalog",        "Catalog Name")
dbutils.widgets.text("schema_name",  "harmonizing_agent", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name  = dbutils.widgets.get("schema_name").strip()

DB            = f"`{catalog_name}`.`{schema_name}`"
CAND_TABLE    = f"{DB}.`column_mapping_candidates_es`"
OPS_TABLE     = f"{DB}.`workflow_run_metrics`"
SOURCE_SYSTEM = "ES_PROPERTY_RAW"

MANDATORY_COLUMNS = [
    "id_registro", "anio", "mes", "codigo_poliza", "tipo_riesgo",
    "provincia", "canal_distribucion", "prima_neta", "prima_bruta",
    "num_siniestros_declarados", "num_siniestros_pagados",
    "segmento_cliente", "cobertura_principal", "moneda",
]

print("STEP 1 — Parameters loaded")
print(f"  catalog_name      : {catalog_name}")
print(f"  schema_name       : {schema_name}")
print(f"  candidates table  : {CAND_TABLE}")
print(f"  mandatory columns : {len(MANDATORY_COLUMNS)}")

# COMMAND ----------

# MAGIC %md ## STEP 2 — Imports

# COMMAND ----------

import datetime as _dt
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

RUN_ID = str(uuid4())
_start = _dt.datetime.utcnow()

print(f"STEP 2 — Imports done. RUN_ID = {RUN_ID}")

# COMMAND ----------

# MAGIC %md ## STEP 3 — Overall Status Summary

# COMMAND ----------

print("STEP 3 — Overall column mapping candidate status summary:")

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

# MAGIC %md ## STEP 4 — Check Blocking Conditions

# COMMAND ----------

print("STEP 4 — Checking blocking conditions for mandatory columns ...")

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

print(f"  Mandatory PENDING (blocking): {len(pending_blocking)}")
print(f"  Mandatory REJECTED with no target (blocking): {len(rejected_blocking)}")
print(f"  Mandatory columns missing from candidates: {len(missing_mandatory)}")

# COMMAND ----------

# MAGIC %md ## STEP 5 — Print Diagnostic Table

# COMMAND ----------

print("STEP 5 — Full mandatory column status:")

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

# MAGIC %md ## STEP 6 — Gate Decision

# COMMAND ----------

gate_status = "PASSED"
blocking_count = len(pending_blocking) + len(rejected_blocking) + len(missing_mandatory)

if blocking_count > 0:
    gate_status = "BLOCKED"

print(f"STEP 6 — Gate decision: {gate_status}")

if gate_status == "BLOCKED":
    print()
    print("  BLOCKING ISSUES:")
    if pending_blocking:
        print(f"  [{len(pending_blocking)}] Mandatory columns still PENDING review:")
        for row in pending_blocking:
            print(f"      - {row['local_column_name']} (proposed: {row['proposed_global_column_name']}, confidence: {row['confidence']})")
    if rejected_blocking:
        print(f"  [{len(rejected_blocking)}] Mandatory columns REJECTED with no final mapping target:")
        for row in rejected_blocking:
            print(f"      - {row['local_column_name']}")
    if missing_mandatory:
        print(f"  [{len(missing_mandatory)}] Mandatory columns not found in candidates at all:")
        for col in missing_mandatory:
            print(f"      - {col}")
else:
    print()
    print("  All 14 mandatory columns have an approved or corrected mapping.")
    print("  Workflow may proceed to build_column_mapping_dictionary.")

# COMMAND ----------

# MAGIC %md ## STEP 7 — Log Gate Result to workflow_run_metrics

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
    "column_mapping_review_gate",
    gate_status,
    _start,
    _end,
    blocking_count,
    f"Gate {gate_status}. Blocking issues: {blocking_count}. Pending: {len(pending_blocking)}, Rejected-no-target: {len(rejected_blocking)}, Missing: {len(missing_mandatory)}.",
)], schema=log_schema)

log_df.write.format("delta").mode("append").saveAsTable(OPS_TABLE)

print(f"STEP 7 — Logged gate result '{gate_status}' to {OPS_TABLE}. RUN_ID={RUN_ID}")

# COMMAND ----------

# MAGIC %md ## STEP 8 — Raise Exception if Blocked

# COMMAND ----------

if gate_status == "BLOCKED":
    raise Exception(
        f"COLUMN MAPPING GATE BLOCKED: {blocking_count} mandatory column(s) are still PENDING or unresolved. "
        f"Spain local entity must review pending mappings using the Column Mapping Review Databricks App, "
        f"then re-run this task. "
        f"Pending: {[r['local_column_name'] for r in pending_blocking]}. "
        f"Rejected-no-target: {[r['local_column_name'] for r in rejected_blocking]}. "
        f"Missing from candidates: {missing_mandatory}."
    )

print()
print("=" * 60)
print("  06_column_mapping_review_gate PASSED")
print("  All mandatory columns are approved. Proceeding to")
print("  07_build_column_mapping_dictionary.")
print("=" * 60)
