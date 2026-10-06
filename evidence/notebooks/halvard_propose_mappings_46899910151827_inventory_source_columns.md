# inventory_source_columns (job halvard_propose_mappings, run 46899910151827, task run 124944198738734, SUCCESS)

```python
%run ./_shared_utils
```

```python
dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "your_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "harmonizing_agent", "Schema Name")
dbutils.widgets.text("source_country", "ES", "Source Country")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
source_country = dbutils.widgets.get("source_country").strip()

DB = f"`{catalog_name}`.`{schema_name}`"
_cfg = load_harmonization_config()
_refs = get_table_refs(_cfg, DB, source_country)
RAW_TABLE = _refs["raw_table"]
INV_TABLE = _refs["inv_table"]
OPS_TABLE = _refs["ops_table"]
SOURCE_SYSTEM = _refs["source_system"]
SOURCE_TABLE = _refs["source_table_name"]

print(f"Config: {DB}")
```

Output:
```text
Config: `agent_marketplace_catalog`.`halvard_harmonization`
```

```python
import datetime as _dt
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, IntegerType, StringType, StructField, StructType, TimestampType

RUN_ID = str(uuid4())
_start = _dt.datetime.utcnow()
import json
```

Output:
```text
/home/spark-fe185385-6f4a-4b1d-bf42-40/.ipykernel/80/command-8770032077284480-3286475399:8: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
  _start = _dt.datetime.utcnow()
```

```python
raw_df = spark.table(RAW_TABLE)
raw_schema = raw_df.schema

print(f"Found {len(raw_schema.fields)} fields in {RAW_TABLE}")
```

Output:
```text
Found 27 fields in `agent_marketplace_catalog`.`halvard_harmonization`.`bronze_property_monthly_it`
```

```python
_now = _dt.datetime.utcnow()
inventory_rows = []

for ordinal, field in enumerate(raw_schema.fields, 1):
    col_name = field.name
    if col_name in {"_source_file", "_ingested_at", "_rescued_data"}:
        continue
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
```

Output:
```text
/home/spark-fe185385-6f4a-4b1d-bf42-40/.ipykernel/80/command-8770032077284484-3881342058:1: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
  _now = _dt.datetime.utcnow()

Collected inventory for 24 columns
```

```python
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
```

Output:
```text
DataFrame rows: 24
```

```python
spark.sql(f"DELETE FROM {INV_TABLE} WHERE source_system = '{SOURCE_SYSTEM}'")
(inv_df.write.format("delta").mode("append").saveAsTable(INV_TABLE))

final_count = spark.table(INV_TABLE).count()
print(f"Merge complete. Total rows in {INV_TABLE}: {final_count}")
```

Output:
```text
Merge complete. Total rows in `agent_marketplace_catalog`.`halvard_harmonization`.`source_column_inventory`: 48
```

```python
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
```

```python
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

summary = {
    "source_country": source_country,
    "source_system": SOURCE_SYSTEM,
    "source_table": SOURCE_TABLE,
    "columns_inventoried": len(inventory_rows),
    "inventory_rows": final_count,
}
dbutils.notebook.exit(json.dumps(summary))
```

Output:
```text
{"source_country": "IT", "source_system": "IT_PROPERTY_RAW", "source_table": "bronze_property_monthly_it", "columns_inventoried": 24, "inventory_rows": 48}
```
