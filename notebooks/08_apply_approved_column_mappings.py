# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Apply Approved Column Mappings
# MAGIC
# MAGIC **CORE TRANSFORMATION NOTEBOOK**
# MAGIC
# MAGIC Reads the approved column mapping dictionary and uses it to dynamically rename
# MAGIC source columns to global column names. Then adds pipeline metadata columns and
# MAGIC writes the result to the harmonized target table.
# MAGIC
# MAGIC **No column renames are hardcoded in this notebook.**
# MAGIC All renames are driven entirely by `column_mapping_dictionary`.

# COMMAND ----------

# MAGIC %run ./_shared_utils

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "pt_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "harmonizing_agent", "Schema Name")
dbutils.widgets.text("source_country", "", "Source Country")
dbutils.widgets.text("mapping_version", "v1", "Mapping Version")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
source_country = dbutils.widgets.get("source_country").strip()
mapping_version = dbutils.widgets.get("mapping_version").strip()

DB = f"`{catalog_name}`.`{schema_name}`"
_cfg = load_harmonization_config()
_refs = get_table_refs(_cfg, DB)
RAW_TABLE = _refs["raw_table"]
DICT_TABLE = _refs["dict_table"]
HARM_TABLE = _refs["harm_table"]
OPS_TABLE = _refs["ops_table"]
SOURCE_SYSTEM = _refs["source_system"]

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

# MAGIC %md ## STEP 3 — Load Approved Column Mapping Dictionary

# COMMAND ----------

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

print(f"Active mappings loaded: {len(active_mappings)}")

if not active_mappings:
    raise Exception("No active column mappings found in dictionary. Run 07_build_column_mapping_dictionary first.")

# COMMAND ----------

# MAGIC %md ## STEP 4 — Load Raw Source Table

# COMMAND ----------

raw_df = spark.table(RAW_TABLE)
raw_count = raw_df.count()
raw_columns = set(raw_df.columns)

print(f"Raw rows: {raw_count:,}, columns: {len(raw_columns)}")

# COMMAND ----------

# MAGIC %md ## STEP 5 — Build Dynamic Select Expressions

# COMMAND ----------

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

print(f"Mapped: {len(mapped_cols)}, unmapped: {len(unmapped_cols)}, excluded: {len(raw_columns) - len(mapped_cols)}")

# COMMAND ----------

# MAGIC %md ## STEP 6 — Build Harmonized DataFrame

# COMMAND ----------

# Apply column renames from dictionary
harmonized_df = raw_df.select(select_exprs)

# Add pipeline metadata columns
harmonized_df = (
    harmonized_df.withColumn("source_country", F.lit(source_country))
    .withColumn("source_system", F.lit(SOURCE_SYSTEM))
    .withColumn("harmonization_timestamp", F.current_timestamp())
    .withColumn("column_mapping_version", F.lit(mapping_version))
    .withColumn("mapping_status", F.lit("APPROVED_COLUMN_MAPPING"))
)

harm_count = harmonized_df.count()
harm_cols = harmonized_df.columns

print(f"Harmonized rows: {harm_count:,}, columns: {len(harm_cols)}")

# COMMAND ----------

# MAGIC %md ## STEP 7 — Write to Harmonized Table

# COMMAND ----------

(harmonized_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(HARM_TABLE))

final_count = spark.table(HARM_TABLE).count()
print(f"Written {final_count:,} rows to {HARM_TABLE}")

# COMMAND ----------

# MAGIC %md ## STEP 8 — Show Sample Output

# COMMAND ----------

display(spark.table(HARM_TABLE).limit(5))

# COMMAND ----------

# MAGIC %md ## STEP 9 — Log to workflow_run_metrics

# COMMAND ----------

log_run_metric(
    spark,
    OPS_TABLE,
    RUN_ID,
    "apply_approved_column_mappings",
    "SUCCEEDED",
    _start,
    final_count,
    f"Applied {len(mapped_cols)} column mappings (version={mapping_version}) from {SOURCE_SYSTEM}. "
    f"Wrote {final_count:,} rows to {HARM_TABLE}. Source country: {source_country}.",
)
