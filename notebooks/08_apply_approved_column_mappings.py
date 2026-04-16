# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Apply Approved Column Mappings
# MAGIC
# MAGIC **CORE TRANSFORMATION NOTEBOOK**
# MAGIC
# MAGIC Reads the approved column mapping dictionary and uses it to dynamically rename
# MAGIC Spanish source columns to global English column names. Then adds pipeline metadata
# MAGIC columns and writes the result to `property_insurance_monthly`.
# MAGIC
# MAGIC **No column renames are hardcoded in this notebook.**
# MAGIC All renames are driven entirely by `column_mapping_dictionary_es`.

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name",    "pt_catalog",        "Catalog Name")
dbutils.widgets.text("schema_name",     "harmonizing_agent", "Schema Name")
dbutils.widgets.text("source_country",  "Spain",             "Source Country")
dbutils.widgets.text("mapping_version", "v1",                "Mapping Version")

catalog_name    = dbutils.widgets.get("catalog_name").strip()
schema_name     = dbutils.widgets.get("schema_name").strip()
source_country  = dbutils.widgets.get("source_country").strip()
mapping_version = dbutils.widgets.get("mapping_version").strip()

DB             = f"`{catalog_name}`.`{schema_name}`"
RAW_TABLE      = f"{DB}.`property_insurance_monthly_raw`"
DICT_TABLE     = f"{DB}.`column_mapping_dictionary_es`"
HARM_TABLE     = f"{DB}.`property_insurance_monthly`"
OPS_TABLE      = f"{DB}.`workflow_run_metrics`"
SOURCE_SYSTEM  = "ES_PROPERTY_RAW"

print("STEP 1 — Parameters loaded")
print(f"  catalog_name    : {catalog_name}")
print(f"  schema_name     : {schema_name}")
print(f"  source_country  : {source_country}")
print(f"  mapping_version : {mapping_version}")
print(f"  raw_table       : {RAW_TABLE}")
print(f"  harm_table      : {HARM_TABLE}")

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

# MAGIC %md ## STEP 3 — Load Approved Column Mapping Dictionary

# COMMAND ----------

print(f"STEP 3 — Loading active column mapping dictionary from {DICT_TABLE} ...")

dict_df = spark.sql(f"""
SELECT
  local_column_name,
  global_column_name,
  match_type,
  mapping_version
FROM {DICT_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
  AND active_flag = TRUE
  AND UPPER(global_column_name) != 'NO_MATCH'
  AND global_column_name IS NOT NULL
  AND global_column_name != ''
ORDER BY local_column_name
""")

dict_rows = dict_df.collect()
active_mappings = {row["local_column_name"]: row["global_column_name"] for row in dict_rows}

print(f"  Active mappings loaded: {len(active_mappings)}")
for local_col, global_col in sorted(active_mappings.items()):
    print(f"    {local_col:35s} -> {global_col}")

if not active_mappings:
    raise Exception(
        "No active column mappings found in dictionary. "
        "Run 07_build_column_mapping_dictionary first."
    )

# COMMAND ----------

# MAGIC %md ## STEP 4 — Load Raw Source Table

# COMMAND ----------

print(f"STEP 4 — Loading raw source table from {RAW_TABLE} ...")

raw_df = spark.table(RAW_TABLE)
raw_count = raw_df.count()
raw_columns = set(raw_df.columns)

print(f"  Raw rows    : {raw_count:,}")
print(f"  Raw columns : {len(raw_columns)}")
print(f"  Columns     : {sorted(raw_columns)}")

# COMMAND ----------

# MAGIC %md ## STEP 5 — Build Dynamic Select Expressions

# COMMAND ----------

print("STEP 5 — Building dynamic column rename expressions ...")

select_exprs = []
mapped_cols = []
unmapped_cols = []

for local_col, global_col in sorted(active_mappings.items()):
    if local_col in raw_columns:
        select_exprs.append(F.col(local_col).alias(global_col))
        mapped_cols.append((local_col, global_col))
    else:
        unmapped_cols.append(local_col)
        print(f"  WARNING: Dictionary column '{local_col}' not found in raw table — skipping.")

# Log any raw columns not in the dictionary (i.e., deliberately excluded like fecha_carga)
for raw_col in sorted(raw_columns):
    if raw_col not in active_mappings:
        print(f"  INFO: Raw column '{raw_col}' has no active dictionary mapping — excluded from harmonized output.")

print(f"\n  Mapped columns  : {len(mapped_cols)}")
print(f"  Unmapped (dict entry not in raw): {len(unmapped_cols)}")
print(f"  Excluded (no dict entry): {len(raw_columns) - len(mapped_cols)}")

# COMMAND ----------

# MAGIC %md ## STEP 6 — Build Harmonized DataFrame

# COMMAND ----------

print("STEP 6 — Building harmonized DataFrame ...")

# Apply column renames from dictionary
harmonized_df = raw_df.select(select_exprs)

# Add pipeline metadata columns
harmonized_df = (
    harmonized_df
    .withColumn("source_country",         F.lit(source_country))
    .withColumn("source_system",          F.lit(SOURCE_SYSTEM))
    .withColumn("harmonization_timestamp", F.current_timestamp())
    .withColumn("column_mapping_version",  F.lit(mapping_version))
    .withColumn("mapping_status",          F.lit("APPROVED_COLUMN_MAPPING"))
)

harm_count = harmonized_df.count()
harm_cols  = harmonized_df.columns

print(f"  Harmonized rows    : {harm_count:,}")
print(f"  Harmonized columns : {len(harm_cols)}")
print(f"  Columns: {harm_cols}")

# COMMAND ----------

# MAGIC %md ## STEP 7 — Write to property_insurance_monthly

# COMMAND ----------

print(f"STEP 7 — Writing to {HARM_TABLE} (overwrite + overwriteSchema) ...")

(
    harmonized_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(HARM_TABLE)
)

final_count = spark.table(HARM_TABLE).count()
print(f"  Rows written to harmonized table: {final_count:,}")

# COMMAND ----------

# MAGIC %md ## STEP 8 — Show Sample Output

# COMMAND ----------

print("STEP 8 — Sample harmonized rows:")
display(spark.table(HARM_TABLE).limit(5))

print("\nMapping summary:")
print(f"{'Source Column (Spanish)':40s}  ->  {'Global Column (English)':35s}  Match Type")
print("-" * 100)
for local_col, global_col in sorted(mapped_cols):
    match_row = next((r for r in dict_rows if r["local_column_name"] == local_col), None)
    mt = match_row["match_type"] if match_row else "UNKNOWN"
    print(f"  {local_col:40s}  ->  {global_col:35s}  {mt}")

# COMMAND ----------

# MAGIC %md ## STEP 9 — Log to workflow_run_metrics

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
    "apply_approved_column_mappings",
    "SUCCEEDED",
    _start,
    _end,
    final_count,
    f"Applied {len(mapped_cols)} column mappings (version={mapping_version}) from {SOURCE_SYSTEM}. "
    f"Wrote {final_count:,} rows to {HARM_TABLE}. "
    f"Source country: {source_country}.",
)], schema=log_schema)

log_df.write.format("delta").mode("append").saveAsTable(OPS_TABLE)

print(f"STEP 9 — Logged run record. RUN_ID={RUN_ID}")
print()
print("=" * 60)
print("  08_apply_approved_column_mappings COMPLETE")
print(f"  Rows written    : {final_count:,}")
print(f"  Columns mapped  : {len(mapped_cols)}")
print(f"  Mapping version : {mapping_version}")
print(f"  Source country  : {source_country}")
print(f"  Output table    : {HARM_TABLE}")
print("=" * 60)
