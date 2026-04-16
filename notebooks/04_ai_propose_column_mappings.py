# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — AI Propose Column Mappings
# MAGIC
# MAGIC Uses `ai_query` to propose a global English target column for each Spanish source column
# MAGIC that does not yet have an approved mapping. Results are written to
# MAGIC `column_mapping_candidates_es` with `review_status = 'PENDING'`.
# MAGIC
# MAGIC **AI endpoint:** `{ai_endpoint}` (default: databricks-gpt-5-2)
# MAGIC
# MAGIC The AI call is a single vectorized SQL statement — no Python loop per column.

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name",    "pt_catalog",          "Catalog Name")
dbutils.widgets.text("schema_name",     "harmonizing_agent",   "Schema Name")
dbutils.widgets.text("ai_endpoint",     "databricks-gpt-5-2",  "AI Endpoint")
dbutils.widgets.text("mapping_version", "v1",                  "Mapping Version")

catalog_name    = dbutils.widgets.get("catalog_name").strip()
schema_name     = dbutils.widgets.get("schema_name").strip()
ai_endpoint     = dbutils.widgets.get("ai_endpoint").strip()
mapping_version = dbutils.widgets.get("mapping_version").strip()

DB             = f"`{catalog_name}`.`{schema_name}`"
INV_TABLE      = f"{DB}.`source_column_inventory_es`"
GTC_TABLE      = f"{DB}.`global_target_columns`"
DICT_TABLE     = f"{DB}.`column_mapping_dictionary_es`"
CAND_TABLE     = f"{DB}.`column_mapping_candidates_es`"
USAGE_TABLE    = f"{DB}.`ai_mapping_usage_metrics`"
OPS_TABLE      = f"{DB}.`workflow_run_metrics`"
SOURCE_SYSTEM  = "ES_PROPERTY_RAW"

# Mandatory columns that must be reviewed before the workflow can proceed
MANDATORY_COLUMNS = {
    "id_registro", "anio", "mes", "codigo_poliza", "tipo_riesgo",
    "provincia", "canal_distribucion", "prima_neta", "prima_bruta",
    "num_siniestros_declarados", "num_siniestros_pagados",
    "segmento_cliente", "cobertura_principal", "moneda",
}

print("STEP 1 — Parameters loaded")
print(f"  catalog_name    : {catalog_name}")
print(f"  schema_name     : {schema_name}")
print(f"  ai_endpoint     : {ai_endpoint}")
print(f"  mapping_version : {mapping_version}")
print(f"  mandatory cols  : {len(MANDATORY_COLUMNS)}")

# COMMAND ----------

# MAGIC %md ## STEP 2 — Imports

# COMMAND ----------

import datetime as _dt
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, BooleanType, TimestampType, ArrayType
)

RUN_ID = str(uuid4())
_start = _dt.datetime.utcnow()

print(f"STEP 2 — Imports done. RUN_ID = {RUN_ID}")

# COMMAND ----------

# MAGIC %md ## STEP 3 — Load Global Target Columns List

# COMMAND ----------

print(f"STEP 3 — Loading global target columns from {GTC_TABLE} ...")

global_cols_list = [
    row["global_column_name"]
    for row in spark.table(GTC_TABLE).select("global_column_name").collect()
]

# Add NO_MATCH as a valid return value
global_cols_list_with_no_match = global_cols_list + ["NO_MATCH"]
global_cols_str = ", ".join(global_cols_list_with_no_match)

print(f"  Found {len(global_cols_list)} global target columns.")
print(f"  Global cols: {global_cols_str[:200]}...")

# COMMAND ----------

# MAGIC %md ## STEP 4 — Determine Pending Source Columns

# COMMAND ----------

print("STEP 4 — Determining which source columns still need mapping proposals ...")

# Source columns that already have an active approved mapping can be skipped
already_approved = spark.sql(f"""
    SELECT DISTINCT local_column_name
    FROM {DICT_TABLE}
    WHERE source_system = '{SOURCE_SYSTEM}'
      AND active_flag = TRUE
""").select("local_column_name").collect()

approved_set = {row["local_column_name"] for row in already_approved}
print(f"  Columns with active approved mapping: {len(approved_set)} -> {sorted(approved_set)[:5]}...")

# Load all source columns from inventory for this source system
all_inv_df = spark.table(INV_TABLE).where(F.col("source_system") == SOURCE_SYSTEM)
total_source_cols = all_inv_df.count()

# Filter to only columns that are not yet approved
if approved_set:
    pending_df = all_inv_df.where(~F.col("local_column_name").isin(approved_set))
else:
    pending_df = all_inv_df

pending_count = pending_df.count()
print(f"  Total source columns        : {total_source_cols}")
print(f"  Already approved (skipped)  : {len(approved_set)}")
print(f"  Pending AI mapping          : {pending_count}")

if pending_count == 0:
    print("  No columns require new AI proposals. All are already approved.")
    dbutils.notebook.exit("No pending columns — all mappings already approved.")

# COMMAND ----------

# MAGIC %md ## STEP 5 — Register Pending Columns as Temp View

# COMMAND ----------

print("STEP 5 — Registering pending columns as temp view ...")

pending_df.createOrReplaceTempView("source_cols_pending_mapping")
print(f"  Temp view 'source_cols_pending_mapping' created with {pending_count} rows.")

# COMMAND ----------

# MAGIC %md ## STEP 6 — Run AI Mapping (Single Vectorized SQL Call)

# COMMAND ----------

print(f"STEP 6 — Running ai_query on {pending_count} source columns via {ai_endpoint} ...")

ai_sql = f"""
SELECT
  source_system,
  source_table,
  local_column_name,
  local_data_type,
  sample_values,
  array_join(sample_values, '; ') AS sample_str,
  ai_query(
    '{ai_endpoint}',
    CONCAT(
      'You are a data harmonization expert. ',
      'Map this Spanish property insurance source column to the best matching global English target column. ',
      'Source column name: "', local_column_name, '". ',
      'Data type: ', local_data_type, '. ',
      'Sample values: ', array_join(sample_values, '; '), '. ',
      'Context: Spain property insurance monthly reporting. Source system: ES_PROPERTY_RAW. ',
      'Valid global target columns: {global_cols_str}. ',
      'Rules: prefer DIRECT for exact or near-exact matches, SEMANTIC_TRANSLATION for conceptual equivalents, DERIVED if computed from other fields, NO_MATCH if no safe mapping exists (e.g. technical metadata columns). ',
      'Return ONLY a JSON object with no additional text: {{\"global_column_name\":\"...\",\"match_type\":\"DIRECT|SEMANTIC_TRANSLATION|DERIVED|NO_MATCH\",\"rationale\":\"...\",\"confidence\":\"HIGH|MEDIUM|LOW\"}}'
    )
  ) AS ai_result
FROM source_cols_pending_mapping
"""

raw_ai_df = spark.sql(ai_sql)
print("  AI query executed.")

# COMMAND ----------

# MAGIC %md ## STEP 7 — Parse AI Responses

# COMMAND ----------

print("STEP 7 — Parsing AI JSON responses ...")

ai_response_schema = StructType([
    StructField("global_column_name", StringType(), True),
    StructField("match_type",         StringType(), True),
    StructField("rationale",          StringType(), True),
    StructField("confidence",         StringType(), True),
])

parsed_df = raw_ai_df.withColumn(
    "ai_parsed",
    F.from_json(F.col("ai_result"), ai_response_schema)
).select(
    F.col("source_system"),
    F.col("source_table"),
    F.col("local_column_name"),
    F.col("local_data_type"),
    F.col("sample_values"),
    F.col("ai_parsed.global_column_name").alias("proposed_global_column_name"),
    F.col("ai_parsed.match_type").alias("proposed_match_type"),
    F.col("ai_parsed.rationale").alias("mapping_rationale"),
    F.col("ai_parsed.confidence").alias("confidence"),
    F.when(
        F.col("ai_result").isNull() | F.col("ai_parsed.global_column_name").isNull(),
        F.lit("AI_ERROR")
    ).otherwise(F.lit(None).cast(StringType())).alias("ai_error_status"),
)

print("  JSON parsing complete.")

# COMMAND ----------

# MAGIC %md ## STEP 8 — Add Mandatory Flag, Timestamps, Candidate ID

# COMMAND ----------

print("STEP 8 — Adding mandatory_flag, timestamps, candidate_id ...")

_now = _dt.datetime.utcnow()

mandatory_col_list = list(MANDATORY_COLUMNS)

enriched_df = (
    parsed_df
    .withColumn("review_status",            F.lit("PENDING"))
    .withColumn("final_global_column_name", F.lit(None).cast(StringType()))
    .withColumn("final_match_type",         F.lit(None).cast(StringType()))
    .withColumn("mandatory_flag",           F.col("local_column_name").isin(mandatory_col_list))
    .withColumn("reviewed_by",              F.lit(None).cast(StringType()))
    .withColumn("reviewed_at",              F.lit(None).cast(TimestampType()))
    .withColumn("review_comment",           F.lit(None).cast(StringType()))
    .withColumn("app_decision_source",      F.lit(None).cast(StringType()))
    .withColumn("created_at",              F.lit(_now).cast(TimestampType()))
    .withColumn("updated_at",              F.lit(_now).cast(TimestampType()))
    .withColumn("proposed_global_data_type", F.lit(None).cast(StringType()))
    # candidate_id: use a row_number over source columns as surrogate
    .withColumn(
        "candidate_id",
        F.monotonically_increasing_id()
    )
    # Rename sample_values to local_sample_values for target schema
    .withColumnRenamed("sample_values", "local_sample_values")
)

print(f"  Enriched rows: {enriched_df.count()}")
enriched_df.createOrReplaceTempView("_candidates_staged")

# COMMAND ----------

# MAGIC %md ## STEP 9 — MERGE into column_mapping_candidates_es

# COMMAND ----------

print(f"STEP 9 — Merging into {CAND_TABLE} ...")

spark.sql(f"""
MERGE INTO {CAND_TABLE} AS tgt
USING _candidates_staged AS src
ON tgt.source_system = src.source_system
   AND tgt.local_column_name = src.local_column_name
WHEN MATCHED AND tgt.review_status = 'PENDING' THEN UPDATE SET
  tgt.local_data_type              = src.local_data_type,
  tgt.local_sample_values          = src.local_sample_values,
  tgt.proposed_global_column_name  = src.proposed_global_column_name,
  tgt.proposed_global_data_type    = src.proposed_global_data_type,
  tgt.proposed_match_type          = src.proposed_match_type,
  tgt.mapping_rationale            = src.mapping_rationale,
  tgt.confidence                   = src.confidence,
  tgt.ai_error_status              = src.ai_error_status,
  tgt.mandatory_flag               = src.mandatory_flag,
  tgt.updated_at                   = src.updated_at,
  tgt.app_decision_source          = src.app_decision_source
WHEN NOT MATCHED THEN INSERT (
  candidate_id, source_system, source_table, local_column_name, local_data_type,
  local_sample_values, proposed_global_column_name, proposed_global_data_type,
  proposed_match_type, mapping_rationale, confidence, ai_error_status,
  review_status, final_global_column_name, final_match_type, mandatory_flag,
  reviewed_by, reviewed_at, review_comment, app_decision_source, created_at, updated_at
) VALUES (
  src.candidate_id, src.source_system, src.source_table, src.local_column_name, src.local_data_type,
  src.local_sample_values, src.proposed_global_column_name, src.proposed_global_data_type,
  src.proposed_match_type, src.mapping_rationale, src.confidence, src.ai_error_status,
  src.review_status, src.final_global_column_name, src.final_match_type, src.mandatory_flag,
  src.reviewed_by, src.reviewed_at, src.review_comment, src.app_decision_source, src.created_at, src.updated_at
)
""")

cand_count = spark.table(CAND_TABLE).count()
print(f"  Merge complete. Total candidates in table: {cand_count}")

# COMMAND ----------

# MAGIC %md ## STEP 10 — Show Results

# COMMAND ----------

print("STEP 10 — Candidate summary:")

summary_df = spark.sql(f"""
SELECT
  review_status,
  mandatory_flag,
  confidence,
  ai_error_status,
  COUNT(*) AS count
FROM {CAND_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
GROUP BY review_status, mandatory_flag, confidence, ai_error_status
ORDER BY mandatory_flag DESC, review_status, confidence
""")
display(summary_df)

print()
print("Proposed mappings (new candidates):")
display(
    spark.table(CAND_TABLE)
    .where(F.col("source_system") == SOURCE_SYSTEM)
    .where(F.col("review_status") == "PENDING")
    .select(
        "local_column_name", "local_data_type",
        "proposed_global_column_name", "proposed_match_type",
        "confidence", "ai_error_status", "mandatory_flag", "mapping_rationale"
    )
    .orderBy(F.col("mandatory_flag").desc(), "local_column_name")
)

# COMMAND ----------

# MAGIC %md ## STEP 11 — Log to ai_mapping_usage_metrics

# COMMAND ----------

print("STEP 11 — Logging AI usage metrics ...")

_end = _dt.datetime.utcnow()

success_count = (
    spark.table(CAND_TABLE)
    .where(F.col("source_system") == SOURCE_SYSTEM)
    .where(F.col("ai_error_status").isNull())
    .where(F.col("review_status") == "PENDING")
    .count()
)
error_count = (
    spark.table(CAND_TABLE)
    .where(F.col("source_system") == SOURCE_SYSTEM)
    .where(F.col("ai_error_status").isNotNull())
    .count()
)
low_conf_count = (
    spark.table(CAND_TABLE)
    .where(F.col("source_system") == SOURCE_SYSTEM)
    .where(F.upper(F.col("confidence")) == "LOW")
    .count()
)

# Rough token estimates: ~200 prompt tokens + ~80 response tokens per column
EST_PROMPT_TOKENS_PER_COL   = 200.0
EST_RESPONSE_TOKENS_PER_COL = 80.0
EST_EUR_PER_1K_TOKENS        = 0.002

est_prompt   = pending_count * EST_PROMPT_TOKENS_PER_COL
est_response = pending_count * EST_RESPONSE_TOKENS_PER_COL
est_cost_eur = ((est_prompt + est_response) / 1000.0) * EST_EUR_PER_1K_TOKENS

usage_schema = StructType([
    StructField("run_id",                   StringType(),    False),
    StructField("mapping_type",             StringType(),    True),
    StructField("source_field_or_column",   StringType(),    True),
    StructField("candidate_rows",           LongType(),      True),
    StructField("success_rows",             LongType(),      True),
    StructField("failed_rows",              LongType(),      True),
    StructField("low_confidence_rows",      LongType(),      True),
    StructField("estimated_prompt_units",   StructField("estimated_prompt_units",   StringType(), True).dataType if False else __import__("pyspark.sql.types", fromlist=["DoubleType"]).DoubleType(), True),
    StructField("estimated_response_units", __import__("pyspark.sql.types", fromlist=["DoubleType"]).DoubleType(), True),
    StructField("estimated_cost_eur",       __import__("pyspark.sql.types", fromlist=["DoubleType"]).DoubleType(), True),
    StructField("recorded_at",              TimestampType(), True),
])

from pyspark.sql.types import DoubleType

usage_schema2 = StructType([
    StructField("run_id",                   StringType(),    False),
    StructField("mapping_type",             StringType(),    True),
    StructField("source_field_or_column",   StringType(),    True),
    StructField("candidate_rows",           LongType(),      True),
    StructField("success_rows",             LongType(),      True),
    StructField("failed_rows",              LongType(),      True),
    StructField("low_confidence_rows",      LongType(),      True),
    StructField("estimated_prompt_units",   DoubleType(),    True),
    StructField("estimated_response_units", DoubleType(),    True),
    StructField("estimated_cost_eur",       DoubleType(),    True),
    StructField("recorded_at",              TimestampType(), True),
])

usage_df = spark.createDataFrame([(
    RUN_ID,
    "COLUMN",
    SOURCE_SYSTEM,
    pending_count,
    success_count,
    error_count,
    low_conf_count,
    est_prompt,
    est_response,
    est_cost_eur,
    _end,
)], schema=usage_schema2)

usage_df.write.format("delta").mode("append").saveAsTable(USAGE_TABLE)

print(f"  Candidates submitted  : {pending_count}")
print(f"  Successful responses  : {success_count}")
print(f"  AI errors             : {error_count}")
print(f"  Low confidence        : {low_conf_count}")
print(f"  Est. prompt tokens    : {est_prompt:,.0f}")
print(f"  Est. response tokens  : {est_response:,.0f}")
print(f"  Est. cost EUR         : {est_cost_eur:.4f}")

# COMMAND ----------

# MAGIC %md ## STEP 12 — Log to workflow_run_metrics

# COMMAND ----------

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
    "ai_propose_column_mappings",
    "SUCCEEDED",
    _start,
    _end,
    cand_count,
    f"AI proposed mappings for {pending_count} source columns. {success_count} successful, {error_count} errors. Total candidates: {cand_count}.",
)], schema=log_schema)

log_df.write.format("delta").mode("append").saveAsTable(OPS_TABLE)

print(f"STEP 12 — Logged run record. RUN_ID={RUN_ID}")
print()
print("=" * 60)
print("  04_ai_propose_column_mappings COMPLETE")
print(f"  Columns submitted to AI  : {pending_count}")
print(f"  Total candidates in table: {cand_count}")
print(f"  AI errors                : {error_count}")
print()
print("  NEXT STEP: Spain local entity must review pending mappings using the")
print("  Column Mapping Review Databricks App. Once all 14 mandatory columns")
print("  are approved in the app, re-run task 06_column_mapping_review_gate.")
print("=" * 60)
