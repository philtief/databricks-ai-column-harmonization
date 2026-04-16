# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Optional Value Mapping
# MAGIC
# MAGIC **SECONDARY OPTIONAL STEP — Value Translation**
# MAGIC
# MAGIC This notebook applies categorical value translations (Spanish values → English values)
# MAGIC to the already-harmonized `property_insurance_monthly` table, **if and only if** approved
# MAGIC entries exist in `value_mapping_dictionary_es`.
# MAGIC
# MAGIC If the dictionary is empty, the notebook exits gracefully without modifying the
# MAGIC harmonized table. The harmonized table is fully usable with column mapping alone.
# MAGIC
# MAGIC **Value translation is NOT the primary feature.** The primary feature is column-level
# MAGIC schema mapping (notebooks 03-08).

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name",    "pt_catalog",         "Catalog Name")
dbutils.widgets.text("schema_name",     "harmonizing_agent",  "Schema Name")
dbutils.widgets.text("ai_endpoint",     "databricks-gpt-5-2", "AI Endpoint")
dbutils.widgets.text("mapping_version", "v1",                 "Mapping Version")

catalog_name    = dbutils.widgets.get("catalog_name").strip()
schema_name     = dbutils.widgets.get("schema_name").strip()
ai_endpoint     = dbutils.widgets.get("ai_endpoint").strip()
mapping_version = dbutils.widgets.get("mapping_version").strip()

DB           = f"`{catalog_name}`.`{schema_name}`"
HARM_TABLE   = f"{DB}.`property_insurance_monthly`"
VDICT_TABLE  = f"{DB}.`value_mapping_dictionary_es`"
VCAND_TABLE  = f"{DB}.`value_mapping_candidates_es`"
OPS_TABLE    = f"{DB}.`workflow_run_metrics`"

# Semantic fields eligible for value translation
SEMANTIC_FIELDS = [
    "risk_type",
    "distribution_channel",
    "customer_segment",
    "risk_zone",
    "primary_coverage",
]

print("STEP 1 — Parameters loaded")
print(f"  catalog_name    : {catalog_name}")
print(f"  schema_name     : {schema_name}")
print(f"  mapping_version : {mapping_version}")
print(f"  harm_table      : {HARM_TABLE}")
print(f"  vdict_table     : {VDICT_TABLE}")

# COMMAND ----------

# MAGIC %md ## STEP 2 — Imports

# COMMAND ----------

import datetime as _dt
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, TimestampType
)

RUN_ID = str(uuid4())
_start = _dt.datetime.utcnow()

print(f"STEP 2 — Imports done. RUN_ID = {RUN_ID}")

# COMMAND ----------

# MAGIC %md ## STEP 3 — Check for Active Value Mapping Entries

# COMMAND ----------

print(f"STEP 3 — Checking {VDICT_TABLE} for active value mappings ...")

active_vdict_count = spark.sql(f"""
SELECT COUNT(*) AS cnt
FROM {VDICT_TABLE}
WHERE active_flag = TRUE
""").collect()[0]["cnt"]

print(f"  Active value mapping entries: {active_vdict_count}")

# COMMAND ----------

# MAGIC %md ## STEP 4 — Branch: Empty Dictionary (Skip)

# COMMAND ----------

if active_vdict_count == 0:
    print()
    print("  No approved value mappings found in value_mapping_dictionary_es.")
    print("  Column mapping only — values remain as sourced from Spain.")
    print("  The harmonized table is complete and usable as-is.")
    print()
    print("  To enable value translation:")
    print("    1. Run the helper section below to populate value_mapping_candidates_es")
    print("    2. Review and approve candidates using Databricks SQL Editor")
    print("    3. Re-run this task")

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
        "optional_value_mapping",
        "INFO",
        _start,
        _end,
        0,
        "Skipped: no active entries in value_mapping_dictionary_es. Values remain as sourced. Column mapping is complete.",
    )], schema=log_schema)

    log_df.write.format("delta").mode("append").saveAsTable(OPS_TABLE)

    print(f"\n  Logged INFO record. RUN_ID={RUN_ID}")
    print()
    print("=" * 60)
    print("  09_optional_value_mapping SKIPPED (no active value dict)")
    print("=" * 60)

    dbutils.notebook.exit("Skipped: no active value mappings.")

# COMMAND ----------

# MAGIC %md ## STEP 5 — Apply Value Translations (Dictionary Not Empty)

# COMMAND ----------

print(f"STEP 5 — Applying value translations from {VDICT_TABLE} ...")

harm_df = spark.table(HARM_TABLE)
updates_applied = []

for field in SEMANTIC_FIELDS:
    # Check if this field has any active value mappings
    field_mappings = spark.sql(f"""
        SELECT raw_value, harmonized_value
        FROM {VDICT_TABLE}
        WHERE source_field = '{field}'
          AND active_flag = TRUE
    """).collect()

    if not field_mappings:
        print(f"  [{field}] No active value mappings — field values unchanged.")
        continue

    print(f"  [{field}] Applying {len(field_mappings)} value translation(s) ...")

    # Build a mapping expression using CASE WHEN
    # For each raw_value -> harmonized_value pair, build a when clause
    mapping_expr = None
    for row in field_mappings:
        raw_val  = row["raw_value"]
        harm_val = row["harmonized_value"]
        condition = F.col(field) == F.lit(raw_val)
        if mapping_expr is None:
            mapping_expr = F.when(condition, F.lit(harm_val))
        else:
            mapping_expr = mapping_expr.when(condition, F.lit(harm_val))

    if mapping_expr is not None:
        # Keep original value where no mapping found
        mapping_expr = mapping_expr.otherwise(F.col(field))
        harm_df = harm_df.withColumn(field, mapping_expr)
        updates_applied.append(field)
        print(f"    Applied {len(field_mappings)} translations for '{field}'.")

print(f"\n  Fields with value translations applied: {updates_applied}")

# COMMAND ----------

# MAGIC %md ## STEP 6 — Re-write Harmonized Table

# COMMAND ----------

if updates_applied:
    print(f"STEP 6 — Re-writing {HARM_TABLE} with value translations applied ...")

    # Update mapping_status to reflect value mapping was also applied
    harm_df = harm_df.withColumn(
        "mapping_status",
        F.lit("APPROVED_COLUMN_AND_VALUE_MAPPING")
    )

    (
        harm_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(HARM_TABLE)
    )

    final_count = spark.table(HARM_TABLE).count()
    print(f"  Re-written rows: {final_count:,}")
    print(f"  mapping_status updated to 'APPROVED_COLUMN_AND_VALUE_MAPPING'")

    display(spark.table(HARM_TABLE).select(*SEMANTIC_FIELDS).distinct().orderBy(SEMANTIC_FIELDS[0]).limit(20))
else:
    final_count = spark.table(HARM_TABLE).count()
    print("STEP 6 — No value translations were applied. Table unchanged.")

# COMMAND ----------

# MAGIC %md ## STEP 7 — Log to workflow_run_metrics

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
    "optional_value_mapping",
    "SUCCEEDED",
    _start,
    _end,
    final_count,
    f"Value translations applied to {len(updates_applied)} field(s): {updates_applied}. Active dict entries: {active_vdict_count}.",
)], schema=log_schema)

log_df.write.format("delta").mode("append").saveAsTable(OPS_TABLE)

print(f"STEP 7 — Logged run record. RUN_ID={RUN_ID}")
print()
print("=" * 60)
print("  09_optional_value_mapping COMPLETE")
print(f"  Fields updated     : {updates_applied}")
print(f"  Active dict entries: {active_vdict_count}")
print(f"  Harmonized rows    : {final_count:,}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Helper: Populate value_mapping_candidates_es for Future Review
# MAGIC
# MAGIC Run the cells below **manually** if the team wants to enable value translation.
# MAGIC These cells do NOT run automatically as part of the workflow.

# COMMAND ----------

# MAGIC %md ### [MANUAL] Inspect Distinct Categorical Values in Harmonized Table

# COMMAND ----------

# This cell is for manual/exploratory use only.
# Uncomment and run to see distinct values in categorical fields.

# harm_df_inspect = spark.table(HARM_TABLE)
#
# for field in SEMANTIC_FIELDS:
#     print(f"\nDistinct values for '{field}':")
#     distinct_vals = (
#         harm_df_inspect
#         .select(field)
#         .where(F.col(field).isNotNull())
#         .distinct()
#         .orderBy(field)
#         .collect()
#     )
#     for row in distinct_vals:
#         print(f"  - {row[field]}")

print("Helper cells are commented out. Uncomment to inspect distinct categorical values.")

# COMMAND ----------

# MAGIC %md ### [MANUAL] Stage Distinct Values as Pending Candidates

# COMMAND ----------

# This cell is for manual/exploratory use only.
# Uncomment and run to populate value_mapping_candidates_es with pending candidates.
# Do NOT call AI here — AI calling for value mapping is a separate future step.

# import datetime as _dt_helper
# _now_helper = _dt_helper.datetime.utcnow()
#
# harm_df_for_cands = spark.table(HARM_TABLE)
#
# for field in SEMANTIC_FIELDS:
#     distinct_vals = (
#         harm_df_for_cands
#         .select(field)
#         .where(F.col(field).isNotNull())
#         .distinct()
#         .collect()
#     )
#
#     cand_rows = []
#     for i, row in enumerate(distinct_vals):
#         raw_val = str(row[field])
#         cand_rows.append((
#             None,         # candidate_id (use monotonically_increasing_id later)
#             field,
#             raw_val,
#             None,         # proposed_harmonized_value — not set yet
#             None,         # proposed_description
#             None,         # confidence
#             None,         # ai_error_status
#             "PENDING",
#             None,         # final_harmonized_value
#             None,         # reviewed_by
#             None,         # reviewed_at
#             None,         # review_comment
#             _now_helper,
#             _now_helper,
#         ))
#
#     if cand_rows:
#         vcand_schema = StructType([...])  # Use full schema matching value_mapping_candidates_es
#         vcand_df = spark.createDataFrame(cand_rows, schema=vcand_schema)
#         vcand_df.write.format("delta").mode("append").saveAsTable(VCAND_TABLE)
#         print(f"  Staged {len(cand_rows)} pending candidates for field '{field}'.")

print("Helper staging cell is commented out. Uncomment to stage value mapping candidates.")
