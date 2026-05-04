# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — AI Propose Column Mappings
# MAGIC
# MAGIC Uses `ai_query` to propose a global target column for each source column
# MAGIC that does not yet have an approved mapping. Results are written to
# MAGIC `column_mapping_candidates` with `review_status = 'PENDING'`.
# MAGIC
# MAGIC **AI endpoint:** `{ai_endpoint}` (default: databricks-gpt-5-2)
# MAGIC
# MAGIC The AI call is a single vectorized SQL statement — no Python loop per column.

# COMMAND ----------

# MAGIC %run ./_shared_utils

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "your_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "harmonizing_agent", "Schema Name")
dbutils.widgets.text("ai_endpoint", "", "AI Endpoint (override; blank = use config)")
dbutils.widgets.text("mapping_version", "v1", "Mapping Version")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
ai_endpoint_override = dbutils.widgets.get("ai_endpoint").strip()
mapping_version = dbutils.widgets.get("mapping_version").strip()

DB = f"`{catalog_name}`.`{schema_name}`"
_cfg = load_harmonization_config()
_refs = get_table_refs(_cfg, DB)
INV_TABLE = _refs["inv_table"]
GTC_TABLE = _refs["gtc_table"]
DICT_TABLE = _refs["dict_table"]
CAND_TABLE = _refs["cand_table"]
USAGE_TABLE = _refs["usage_table"]
OPS_TABLE = _refs["ops_table"]

SOURCE_SYSTEM = _refs["source_system"]

MANDATORY_COLUMNS = set(_cfg["mandatory_source_columns"])

AI_CONTEXT = _cfg["source_context"]["description"].strip()

# All LLM knobs (endpoint, prompt template, vocabularies, cost) come from the
# `ai:` block of harmonization_config.yaml. The widget can override the endpoint.
from dataclasses import replace as _dc_replace

from harmonization.llm import build_ai_query_sql, estimate_cost, load_llm_config

llm_config = load_llm_config(_cfg)
if ai_endpoint_override:
    llm_config = _dc_replace(llm_config, endpoint=ai_endpoint_override)

print(f"Config: {DB}, ai_endpoint={llm_config.endpoint}, mandatory_cols={len(MANDATORY_COLUMNS)}")

# COMMAND ----------

# MAGIC %md ## Imports

# COMMAND ----------

from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

RUN_ID = str(uuid4())
_start = _dt.datetime.utcnow()

# COMMAND ----------

# MAGIC %md ## Load Global Target Columns

# COMMAND ----------

global_cols_list = [row["global_column_name"] for row in spark.table(GTC_TABLE).select("global_column_name").collect()]

global_cols_list_with_no_match = global_cols_list + ["NO_MATCH"]
global_cols_str = ", ".join(global_cols_list_with_no_match)

print(f"Global target columns: {len(global_cols_list)}")

# COMMAND ----------

# MAGIC %md ## Determine Pending Source Columns

# COMMAND ----------

# Source columns that already have an active approved mapping can be skipped
already_approved = (
    spark.sql(f"""
    SELECT DISTINCT local_column_name
    FROM {DICT_TABLE}
    WHERE source_system = '{SOURCE_SYSTEM}'
      AND active_flag = TRUE
""")
    .select("local_column_name")
    .collect()
)

approved_set = {row["local_column_name"] for row in already_approved}

all_inv_df = spark.table(INV_TABLE).where(F.col("source_system") == SOURCE_SYSTEM)
total_source_cols = all_inv_df.count()

if approved_set:
    pending_df = all_inv_df.where(~F.col("local_column_name").isin(approved_set))
else:
    pending_df = all_inv_df

pending_count = pending_df.count()
print(f"Source columns: {total_source_cols} total, {len(approved_set)} approved, {pending_count} pending")

if pending_count == 0:
    print("No columns require new AI proposals. All are already approved.")
    dbutils.notebook.exit("No pending columns — all mappings already approved.")

# COMMAND ----------

# MAGIC %md ## Register Pending Columns as Temp View

# COMMAND ----------

pending_df.createOrReplaceTempView("source_cols_pending_mapping")

# COMMAND ----------

# MAGIC %md ## Run AI Mapping (Single Vectorized SQL Call)

# COMMAND ----------

ai_sql = build_ai_query_sql(
    source_view="source_cols_pending_mapping",
    llm_config=llm_config,
    ai_context=AI_CONTEXT,
    target_columns=global_cols_list,
)

raw_ai_df = spark.sql(ai_sql)
print(f"AI query executed on {pending_count} columns via {llm_config.endpoint}.")

# COMMAND ----------

# MAGIC %md ## Parse AI Responses

# COMMAND ----------

ai_response_schema = StructType(
    [
        StructField("global_column_name", StringType(), True),
        StructField("match_type", StringType(), True),
        StructField("rationale", StringType(), True),
        StructField("confidence", StringType(), True),
    ]
)

parsed_df = raw_ai_df.withColumn("ai_parsed", F.from_json(F.col("ai_result"), ai_response_schema)).select(
    F.col("source_system"),
    F.col("source_table"),
    F.col("local_column_name"),
    F.col("local_data_type"),
    F.col("sample_values"),
    F.col("ai_parsed.global_column_name").alias("proposed_global_column_name"),
    F.col("ai_parsed.match_type").alias("proposed_match_type"),
    F.col("ai_parsed.rationale").alias("mapping_rationale"),
    F.col("ai_parsed.confidence").alias("confidence"),
    F.when(F.col("ai_result").isNull() | F.col("ai_parsed.global_column_name").isNull(), F.lit("AI_ERROR"))
    .otherwise(F.lit(None).cast(StringType()))
    .alias("ai_error_status"),
)

# COMMAND ----------

# MAGIC %md ## Add Mandatory Flag, Timestamps, Candidate ID

# COMMAND ----------

_now = _dt.datetime.utcnow()
mandatory_col_list = list(MANDATORY_COLUMNS)

enriched_df = (
    parsed_df.withColumn("review_status", F.lit("PENDING"))
    .withColumn("final_global_column_name", F.lit(None).cast(StringType()))
    .withColumn("final_match_type", F.lit(None).cast(StringType()))
    .withColumn("mandatory_flag", F.col("local_column_name").isin(mandatory_col_list))
    .withColumn("reviewed_by", F.lit(None).cast(StringType()))
    .withColumn("reviewed_at", F.lit(None).cast(TimestampType()))
    .withColumn("review_comment", F.lit(None).cast(StringType()))
    .withColumn("app_decision_source", F.lit(None).cast(StringType()))
    .withColumn("created_at", F.lit(_now).cast(TimestampType()))
    .withColumn("updated_at", F.lit(_now).cast(TimestampType()))
    .withColumn("proposed_global_data_type", F.lit(None).cast(StringType()))
    .withColumn("candidate_id", F.monotonically_increasing_id())
    .withColumnRenamed("sample_values", "local_sample_values")
)

print(f"Enriched rows: {enriched_df.count()}")
enriched_df.createOrReplaceTempView("_candidates_staged")

# COMMAND ----------

# MAGIC %md ## MERGE into column_mapping_candidates

# COMMAND ----------

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
print(f"Merge complete. Total candidates: {cand_count}")

# COMMAND ----------

# MAGIC %md ## Results

# COMMAND ----------

summary_df = spark.sql(f"""
SELECT review_status, mandatory_flag, confidence, ai_error_status, COUNT(*) AS count
FROM {CAND_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
GROUP BY review_status, mandatory_flag, confidence, ai_error_status
ORDER BY mandatory_flag DESC, review_status, confidence
""")
display(summary_df)

display(
    spark.table(CAND_TABLE)
    .where(F.col("source_system") == SOURCE_SYSTEM)
    .where(F.col("review_status") == "PENDING")
    .select(
        "local_column_name",
        "local_data_type",
        "proposed_global_column_name",
        "proposed_match_type",
        "confidence",
        "ai_error_status",
        "mandatory_flag",
        "mapping_rationale",
    )
    .orderBy(F.col("mandatory_flag").desc(), "local_column_name")
)

# COMMAND ----------

# MAGIC %md ## Log AI Usage Metrics

# COMMAND ----------

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

# Token cost estimates come from llm_config (sourced from harmonization_config.yaml).
_cost_est = estimate_cost(pending_count, llm_config)
est_prompt = _cost_est["prompt_tokens"]
est_response = _cost_est["response_tokens"]
est_cost_eur = _cost_est["cost"]

usage_schema = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("mapping_type", StringType(), True),
        StructField("source_field_or_column", StringType(), True),
        StructField("candidate_rows", LongType(), True),
        StructField("success_rows", LongType(), True),
        StructField("failed_rows", LongType(), True),
        StructField("low_confidence_rows", LongType(), True),
        StructField("estimated_prompt_units", DoubleType(), True),
        StructField("estimated_response_units", DoubleType(), True),
        StructField("estimated_cost_eur", DoubleType(), True),
        StructField("recorded_at", TimestampType(), True),
    ]
)

usage_df = spark.createDataFrame(
    [
        (
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
        )
    ],
    schema=usage_schema,
)

usage_df.write.format("delta").mode("append").saveAsTable(USAGE_TABLE)

print(
    f"AI usage: {success_count} success, {error_count} errors, {low_conf_count} low confidence, est. cost {est_cost_eur:.4f}"
)

# COMMAND ----------

# MAGIC %md ## Log to workflow_run_metrics

# COMMAND ----------

log_run_metric(
    spark,
    OPS_TABLE,
    RUN_ID,
    "ai_propose_column_mappings",
    "SUCCEEDED",
    _start,
    cand_count,
    f"AI proposed mappings for {pending_count} columns. {success_count} success, {error_count} errors.",
)
