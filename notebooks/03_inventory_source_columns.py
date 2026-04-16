# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Inventory Source Columns
# MAGIC
# MAGIC Reads the raw Spain source table schema, collects up to 5 non-null distinct sample
# MAGIC values per column, and merges the result into `source_column_inventory_es`.
# MAGIC
# MAGIC This inventory is the input to the AI column-mapping notebook (04).

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "pt_catalog",        "Catalog Name")
dbutils.widgets.text("schema_name",  "harmonizing_agent", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name  = dbutils.widgets.get("schema_name").strip()

DB            = f"`{catalog_name}`.`{schema_name}`"
RAW_TABLE     = f"{DB}.`property_insurance_monthly_raw`"
INV_TABLE     = f"{DB}.`source_column_inventory_es`"
OPS_TABLE     = f"{DB}.`workflow_run_metrics`"
SOURCE_SYSTEM = "ES_PROPERTY_RAW"
SOURCE_TABLE  = "property_insurance_monthly_raw"

print("STEP 1 — Parameters loaded")
print(f"  catalog_name  : {catalog_name}")
print(f"  schema_name   : {schema_name}")
print(f"  raw_table     : {RAW_TABLE}")
print(f"  inv_table     : {INV_TABLE}")

# COMMAND ----------

# MAGIC %md ## STEP 2 — Imports

# COMMAND ----------

import datetime as _dt
from uuid import uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType, ArrayType, LongType
)

RUN_ID = str(uuid4())
print(f"STEP 2 — Imports done. RUN_ID = {RUN_ID}")

# COMMAND ----------

# MAGIC %md ## STEP 3 — Read Raw Table Schema

# COMMAND ----------

print(f"STEP 3 — Reading schema from {RAW_TABLE} ...")

raw_df = spark.table(RAW_TABLE)
raw_schema = raw_df.schema

print(f"  Found {len(raw_schema.fields)} fields:")
for i, field in enumerate(raw_schema.fields, 1):
    nullable_str = "nullable" if field.nullable else "not null"
    print(f"    [{i:02d}] {field.name}  ({field.dataType.simpleString()}, {nullable_str})")

# COMMAND ----------

# MAGIC %md ## STEP 4 — Collect Sample Values Per Column

# COMMAND ----------

print("STEP 4 — Collecting up to 5 non-null distinct sample values per column ...")

_now = _dt.datetime.utcnow()
inventory_rows = []

for ordinal, field in enumerate(raw_schema.fields, 1):
    col_name = field.name
    dtype_str = field.dataType.simpleString()
    is_nullable = field.nullable

    # Collect up to 5 non-null distinct values as strings
    sample_vals_raw = (
        raw_df
        .select(F.col(col_name))
        .where(F.col(col_name).isNotNull())
        .distinct()
        .limit(5)
        .collect()
    )
    # Convert every value to string safely
    sample_values = [str(row[0]) for row in sample_vals_raw if row[0] is not None]

    inventory_rows.append((
        SOURCE_SYSTEM,
        SOURCE_TABLE,
        col_name,
        dtype_str,
        sample_values,
        ordinal,
        is_nullable,
        _now,
    ))

    print(f"  [{ordinal:02d}] {col_name}: {len(sample_values)} sample(s) -> {sample_values[:3]}")

print(f"\n  Collected inventory for {len(inventory_rows)} columns.")

# COMMAND ----------

# MAGIC %md ## STEP 5 — Build DataFrame

# COMMAND ----------

print("STEP 5 — Building inventory DataFrame ...")

inv_schema = StructType([
    StructField("source_system",     StringType(),              False),
    StructField("source_table",      StringType(),              False),
    StructField("local_column_name", StringType(),              False),
    StructField("local_data_type",   StringType(),              True),
    StructField("sample_values",     ArrayType(StringType()),   True),
    StructField("ordinal_position",  IntegerType(),             True),
    StructField("is_nullable",       BooleanType(),             True),
    StructField("detected_at",       TimestampType(),           True),
])

inv_df = spark.createDataFrame(inventory_rows, schema=inv_schema)
inv_df.createOrReplaceTempView("_inv_staged")

print(f"  DataFrame rows: {inv_df.count()}")

# COMMAND ----------

# MAGIC %md ## STEP 6 — MERGE into source_column_inventory_es

# COMMAND ----------

print(f"STEP 6 — Merging into {INV_TABLE} ...")

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
print(f"  Merge complete. Total rows in {INV_TABLE}: {final_count}")

# COMMAND ----------

# MAGIC %md ## STEP 7 — Display Full Inventory

# COMMAND ----------

print("STEP 7 — Full source column inventory:")
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

# MAGIC %md ## STEP 8 — Log to workflow_run_metrics

# COMMAND ----------

_done = _dt.datetime.utcnow()

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
    "inventory_source_columns",
    "SUCCEEDED",
    _now,
    _done,
    final_count,
    f"Inventoried {len(inventory_rows)} source columns from {SOURCE_SYSTEM}.{SOURCE_TABLE}. Total inventory rows: {final_count}.",
)], schema=log_schema)

log_df.write.format("delta").mode("append").saveAsTable(OPS_TABLE)

print(f"STEP 8 — Logged run record to {OPS_TABLE}. RUN_ID={RUN_ID}")
print()
print("=" * 60)
print("  03_inventory_source_columns COMPLETE")
print(f"  Columns inventoried : {len(inventory_rows)}")
print(f"  Inventory table rows: {final_count}")
print("=" * 60)
