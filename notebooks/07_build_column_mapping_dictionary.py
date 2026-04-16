# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Build Column Mapping Dictionary
# MAGIC
# MAGIC Promotes all APPROVED and CORRECTED candidates from `column_mapping_candidates_es`
# MAGIC into the production `column_mapping_dictionary_es`.
# MAGIC
# MAGIC - APPROVED rows use `final_global_column_name` if set, otherwise `proposed_global_column_name`.
# MAGIC - CORRECTED rows use `final_global_column_name` (required for CORRECTED status).
# MAGIC - Entries resolving to `NO_MATCH` or NULL are excluded from the dictionary.
# MAGIC - Dictionary entries no longer in the approved set are deactivated.

# COMMAND ----------

# MAGIC %run ./_shared_utils

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name",    "pt_catalog",        "Catalog Name")
dbutils.widgets.text("schema_name",     "harmonizing_agent", "Schema Name")
dbutils.widgets.text("mapping_version", "v1",                "Mapping Version")

catalog_name    = dbutils.widgets.get("catalog_name").strip()
schema_name     = dbutils.widgets.get("schema_name").strip()
mapping_version = dbutils.widgets.get("mapping_version").strip()

DB            = f"`{catalog_name}`.`{schema_name}`"
CAND_TABLE    = f"{DB}.`column_mapping_candidates_es`"
DICT_TABLE    = f"{DB}.`column_mapping_dictionary_es`"
OPS_TABLE     = f"{DB}.`workflow_run_metrics`"
SOURCE_SYSTEM = "ES_PROPERTY_RAW"
SOURCE_TABLE  = "property_insurance_monthly_raw"

print(f"Config: {DB}, version={mapping_version}")

# COMMAND ----------

# MAGIC %md ## Imports

# COMMAND ----------

import datetime as _dt
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

RUN_ID = str(uuid4())
_start = _dt.datetime.utcnow()

# COMMAND ----------

# MAGIC %md ## Load Approved and Corrected Candidates

# COMMAND ----------

approved_df = spark.sql(f"""
SELECT
  source_system,
  source_table,
  local_column_name,
  local_data_type,
  CASE
    WHEN review_status = 'CORRECTED' THEN final_global_column_name
    WHEN review_status = 'APPROVED'  THEN COALESCE(final_global_column_name, proposed_global_column_name)
    ELSE NULL
  END AS global_column_name,
  CASE
    WHEN review_status = 'CORRECTED' THEN COALESCE(final_match_type, proposed_match_type)
    WHEN review_status = 'APPROVED'  THEN COALESCE(final_match_type, proposed_match_type)
    ELSE NULL
  END AS match_type,
  review_status,
  reviewed_by,
  reviewed_at,
  review_comment
FROM {CAND_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
  AND review_status IN ('APPROVED', 'CORRECTED')
""")

approved_count = approved_df.count()
print(f"Found {approved_count} APPROVED/CORRECTED candidates")

# COMMAND ----------

# MAGIC %md ## Exclude NO_MATCH and Null Resolutions

# COMMAND ----------

valid_mappings_df = approved_df.where(
    F.col("global_column_name").isNotNull() &
    (F.upper(F.col("global_column_name")) != "NO_MATCH") &
    (F.col("global_column_name") != "")
)

excluded_count = approved_count - valid_mappings_df.count()
valid_count = valid_mappings_df.count()

print(f"Valid mappings: {valid_count}, Excluded (NO_MATCH/null): {excluded_count}")

# COMMAND ----------

# MAGIC %md ## Print REJECTED columns

# COMMAND ----------

rejected_df = spark.sql(f"""
SELECT
  local_column_name,
  proposed_global_column_name,
  review_status,
  review_comment,
  reviewed_by,
  reviewed_at
FROM {CAND_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
  AND review_status = 'REJECTED'
ORDER BY local_column_name
""")

rejected_count = rejected_df.count()
if rejected_count > 0:
    display(rejected_df)
    print(f"{rejected_count} column(s) rejected")
else:
    print("No rejected columns.")

# COMMAND ----------

# MAGIC %md ## Prepare Dictionary Rows

# COMMAND ----------

_now = _dt.datetime.utcnow()

dict_staged_df = (
    valid_mappings_df
    .withColumn("approved_by",      F.coalesce(F.col("reviewed_by"), F.lit("system")))
    .withColumn("approved_at",      F.coalesce(F.col("reviewed_at"), F.lit(_now).cast(TimestampType())))
    .withColumn("mapping_version",  F.lit(mapping_version))
    .withColumn("active_flag",      F.lit(True))
    .withColumn("mapping_comment",  F.col("review_comment"))
    .withColumn("created_at",       F.lit(_now).cast(TimestampType()))
    .withColumn("updated_at",       F.lit(_now).cast(TimestampType()))
    .select(
        "source_system", "source_table", "local_column_name",
        "global_column_name", "match_type",
        "approved_by", "approved_at", "mapping_version",
        "active_flag", "mapping_comment", "created_at", "updated_at"
    )
)

dict_staged_df.createOrReplaceTempView("_dict_staged")
print(f"Dictionary rows ready: {dict_staged_df.count()}")

# COMMAND ----------

# MAGIC %md ## MERGE into column_mapping_dictionary_es

# COMMAND ----------

spark.sql(f"""
MERGE INTO {DICT_TABLE} AS tgt
USING _dict_staged AS src
ON tgt.source_system = src.source_system
   AND tgt.local_column_name = src.local_column_name
WHEN MATCHED THEN UPDATE SET
  tgt.global_column_name = src.global_column_name,
  tgt.match_type         = src.match_type,
  tgt.approved_by        = src.approved_by,
  tgt.approved_at        = src.approved_at,
  tgt.mapping_version    = src.mapping_version,
  tgt.active_flag        = TRUE,
  tgt.mapping_comment    = src.mapping_comment,
  tgt.updated_at         = src.updated_at
WHEN NOT MATCHED THEN INSERT (
  source_system, source_table, local_column_name, global_column_name, match_type,
  approved_by, approved_at, mapping_version, active_flag, mapping_comment, created_at, updated_at
) VALUES (
  src.source_system, src.source_table, src.local_column_name, src.global_column_name, src.match_type,
  src.approved_by, src.approved_at, src.mapping_version, src.active_flag, src.mapping_comment, src.created_at, src.updated_at
)
""")

# COMMAND ----------

# MAGIC %md ## Deactivate Stale Dictionary Entries

# COMMAND ----------

valid_local_cols = [row["local_column_name"] for row in dict_staged_df.select("local_column_name").collect()]
valid_cols_str = ", ".join(f"'{c}'" for c in valid_local_cols)

if valid_local_cols:
    deactivate_sql = f"""
    UPDATE {DICT_TABLE}
    SET active_flag = FALSE, updated_at = '{_now.isoformat()}'
    WHERE source_system = '{SOURCE_SYSTEM}'
      AND active_flag = TRUE
      AND local_column_name NOT IN ({valid_cols_str})
    """
    spark.sql(deactivate_sql)
    print("Stale entries deactivated (if any).")
else:
    print("No valid local columns -- nothing to deactivate.")

# COMMAND ----------

# MAGIC %md ## Verify Dictionary

# COMMAND ----------

dict_final_df = spark.sql(f"""
SELECT
  local_column_name,
  global_column_name,
  match_type,
  mapping_version,
  active_flag,
  approved_by,
  approved_at,
  mapping_comment
FROM {DICT_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
  AND active_flag = TRUE
ORDER BY local_column_name
""")

active_count = dict_final_df.count()
display(dict_final_df)

print(f"Active dictionary entries: {active_count}")

# COMMAND ----------

# MAGIC %md ## Log to workflow_run_metrics

# COMMAND ----------

log_run_metric(spark, OPS_TABLE, RUN_ID, "build_column_mapping_dictionary", "SUCCEEDED", _start, active_count,
               f"Dictionary built with {active_count} active entries. Approved: {approved_count}, Excluded (NO_MATCH): {excluded_count}, Rejected: {rejected_count}. Version: {mapping_version}.")
