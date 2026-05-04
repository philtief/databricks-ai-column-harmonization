# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Inventory Source Columns
# MAGIC
# MAGIC Reads the raw source table schema, collects up to 5 non-null distinct sample
# MAGIC values per column, and merges the result into `source_column_inventory`.
# MAGIC
# MAGIC This inventory is the input to the AI column-mapping notebook (04).

# COMMAND ----------

# MAGIC %run ./_shared_utils

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "your_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "harmonizing_agent", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()

DB = f"`{catalog_name}`.`{schema_name}`"
_cfg = load_harmonization_config()
_refs = get_table_refs(_cfg, DB)
RAW_TABLE = _refs["raw_table"]
INV_TABLE = _refs["inv_table"]
OPS_TABLE = _refs["ops_table"]
SOURCE_SYSTEM = _refs["source_system"]
SOURCE_TABLE = _refs["source_table_name"]

print(f"Config: {DB}")

# COMMAND ----------

# MAGIC %md ## Imports

# COMMAND ----------

import datetime as _dt
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, IntegerType, StringType, StructField, StructType, TimestampType

RUN_ID = str(uuid4())

# COMMAND ----------

# MAGIC %md ## Read Raw Table Schema

# COMMAND ----------

raw_df = spark.table(RAW_TABLE)
raw_schema = raw_df.schema

print(f"Found {len(raw_schema.fields)} fields in {RAW_TABLE}")

# COMMAND ----------

# MAGIC %md ## Collect Sample Values Per Column

# COMMAND ----------

_now = _dt.datetime.utcnow()
inventory_rows = []

for ordinal, field in enumerate(raw_schema.fields, 1):
    col_name = field.name
    dtype_str = field.dataType.simpleString()
    is_nullable = field.nullable

    # Collect up to 5 non-null distinct values as strings
    sample_vals_raw = raw_df.select(F.col(col_name)).where(F.col(col_name).isNotNull()).distinct().limit(5).collect()
    # Convert every value to string safely
    sample_values = [str(row[0]) for row in sample_vals_raw if row[0] is not None]

    inventory_rows.append(
        (
            SOURCE_SYSTEM,
            SOURCE_TABLE,
            col_name,
            dtype_str,
            sample_values,
            ordinal,
            is_nullable,
            _now,
        )
    )

print(f"Collected inventory for {len(inventory_rows)} columns")

# COMMAND ----------

# MAGIC %md ## Build DataFrame

# COMMAND ----------

inv_schema = StructType(
    [
        StructField("source_system", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("local_column_name", StringType(), False),
        StructField("local_data_type", StringType(), True),
        StructField("sample_values", ArrayType(StringType()), True),
        StructField("ordinal_position", IntegerType(), True),
        StructField("is_nullable", BooleanType(), True),
        StructField("detected_at", TimestampType(), True),
    ]
)

inv_df = spark.createDataFrame(inventory_rows, schema=inv_schema)
inv_df.createOrReplaceTempView("_inv_staged")

print(f"DataFrame rows: {inv_df.count()}")

# COMMAND ----------

# MAGIC %md ## MERGE into source_column_inventory

# COMMAND ----------

spark.sql(f"""
MERGE INTO {INV_TABLE} AS tgt
USING _inv_staged AS src
ON tgt.source_system = src.source_system
   AND tgt.local_column_name = src.local_column_name
WHEN MATCHED THEN UPDATE SET
  tgt.local_data_type  = src.local_data_type,
  tgt.sample_values    = src.sample_values,
  tgt.ordinal_position = src.ordinal_position,
  tgt.is_nullable      = src.is_nullable,
  tgt.detected_at      = src.detected_at
WHEN NOT MATCHED THEN INSERT (
  source_system, source_table, local_column_name, local_data_type,
  sample_values, ordinal_position, is_nullable, detected_at
) VALUES (
  src.source_system, src.source_table, src.local_column_name, src.local_data_type,
  src.sample_values, src.ordinal_position, src.is_nullable, src.detected_at
)
""")

final_count = spark.table(INV_TABLE).count()
print(f"Merge complete. Total rows in {INV_TABLE}: {final_count}")

# COMMAND ----------

# MAGIC %md ## Display Full Inventory

# COMMAND ----------

display(
    spark.table(INV_TABLE)
    .orderBy("source_system", "ordinal_position")
    .select(
        "ordinal_position",
        "source_system",
        "source_table",
        "local_column_name",
        "local_data_type",
        "sample_values",
        "is_nullable",
        "detected_at",
    )
)

# COMMAND ----------

# MAGIC %md ## Log to workflow_run_metrics

# COMMAND ----------

log_run_metric(
    spark,
    OPS_TABLE,
    RUN_ID,
    "inventory_source_columns",
    "SUCCEEDED",
    _now,
    final_count,
    f"Inventoried {len(inventory_rows)} source columns from {SOURCE_SYSTEM}.{SOURCE_TABLE}. Total inventory rows: {final_count}.",
)
