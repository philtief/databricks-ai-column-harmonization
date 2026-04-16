# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Optional Value Mapping
# MAGIC
# MAGIC **SECONDARY OPTIONAL STEP — Value Translation**
# MAGIC
# MAGIC This notebook applies categorical value translations (source values to harmonized values)
# MAGIC to the already-harmonized target table, **if and only if** approved entries exist in
# MAGIC `value_mapping_dictionary`.
# MAGIC
# MAGIC If the dictionary is empty, the notebook exits gracefully without modifying the
# MAGIC harmonized table. The harmonized table is fully usable with column mapping alone.
# MAGIC
# MAGIC **Value translation is NOT the primary feature.** The primary feature is column-level
# MAGIC schema mapping (notebooks 03-08).

# COMMAND ----------

# MAGIC %run ./_shared_utils

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
_cfg         = load_harmonization_config()
_refs        = get_table_refs(_cfg, DB)
HARM_TABLE   = _refs["harm_table"]
VDICT_TABLE  = _refs["vdict_table"]
VCAND_TABLE  = _refs["vcand_table"]
OPS_TABLE    = _refs["ops_table"]
SEMANTIC_FIELDS = _cfg["semantic_fields"] if _cfg else [
    "risk_type",
    "distribution_channel",
    "customer_segment",
    "risk_zone",
    "primary_coverage",
]

print(f"Config: {DB}")

# COMMAND ----------

# MAGIC %md ## STEP 2 — Imports

# COMMAND ----------

import datetime as _dt
from uuid import uuid4
from pyspark.sql import functions as F

RUN_ID = str(uuid4())
_start = _dt.datetime.utcnow()

# COMMAND ----------

# MAGIC %md ## STEP 3 — Check for Active Value Mapping Entries

# COMMAND ----------

active_vdict_count = spark.sql(f"""
SELECT COUNT(*) AS cnt
FROM {VDICT_TABLE}
WHERE active_flag = TRUE
""").collect()[0]["cnt"]

print(f"Active value mapping entries: {active_vdict_count}")

# COMMAND ----------

# MAGIC %md ## STEP 4 — Branch: Empty Dictionary (Skip)

# COMMAND ----------

if active_vdict_count == 0:
    print("No approved value mappings found. Column mapping only — values remain as sourced.")

    log_run_metric(
        spark, OPS_TABLE, RUN_ID,
        "optional_value_mapping", "INFO", _start, 0,
        "Skipped: no active entries in value_mapping_dictionary. Values remain as sourced. Column mapping is complete."
    )

    dbutils.notebook.exit("Skipped: no active value mappings.")

# COMMAND ----------

# MAGIC %md ## STEP 5 — Apply Value Translations (Dictionary Not Empty)

# COMMAND ----------

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

print(f"Fields with value translations applied: {updates_applied}")

# COMMAND ----------

# MAGIC %md ## STEP 6 — Re-write Harmonized Table

# COMMAND ----------

if updates_applied:
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
    print(f"Written {final_count:,} rows with value translations applied")

    display(spark.table(HARM_TABLE).select(*SEMANTIC_FIELDS).distinct().orderBy(SEMANTIC_FIELDS[0]).limit(20))
else:
    final_count = spark.table(HARM_TABLE).count()
    print("No value translations were applied. Table unchanged.")

# COMMAND ----------

# MAGIC %md ## STEP 7 — Log to workflow_run_metrics

# COMMAND ----------

log_run_metric(
    spark, OPS_TABLE, RUN_ID,
    "optional_value_mapping", "SUCCEEDED", _start, final_count,
    f"Value translations applied to {len(updates_applied)} field(s): {updates_applied}. Active dict entries: {active_vdict_count}."
)
