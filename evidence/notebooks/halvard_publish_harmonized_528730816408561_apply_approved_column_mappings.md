# apply_approved_column_mappings (job halvard_publish_harmonized, run 528730816408561, task run 1069684751319793, SUCCESS)

```python
%run ./_shared_utils
```

```python
dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "your_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "harmonizing_agent", "Schema Name")
dbutils.widgets.text("source_country", "ES", "Source Country")
dbutils.widgets.text("mapping_version", "v1", "Mapping Version")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
source_country = dbutils.widgets.get("source_country").strip()
mapping_version = dbutils.widgets.get("mapping_version").strip()

DB = f"`{catalog_name}`.`{schema_name}`"
_cfg = load_harmonization_config()
_refs = get_table_refs(_cfg, DB, source_country)
RAW_TABLE = _refs["raw_table"]
DICT_TABLE = _refs["dict_table"]
HARM_TABLE = _refs["harm_table"]
OPS_TABLE = _refs["ops_table"]
SOURCE_SYSTEM = _refs["source_system"]

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

RUN_ID = str(uuid4())
_start = _dt.datetime.utcnow()
import json
```

Output:
```text
/home/spark-0edf19cb-8596-4e92-8571-4e/.ipykernel/69/command-8770032077284700-2846475485:7: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
  _start = _dt.datetime.utcnow()
```

```python
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
```

Output:
```text
Active mappings loaded: 23
```

```python
raw_df = spark.table(RAW_TABLE)
metadata_columns = {"_source_file", "_ingested_at", "_rescued_data"}
business_columns = [column for column in raw_df.columns if column not in metadata_columns]
raw_count = raw_df.count()
raw_columns = set(business_columns)

print(f"Raw rows: {raw_count:,}, columns: {len(raw_columns)}")
```

Output:
```text
Raw rows: 10,000, columns: 24
```

```python
select_exprs = []
mapped_cols = []
unmapped_cols = []
global_columns_mapped = set()

for local_col, global_col in sorted(active_mappings.items()):
    if local_col in raw_columns:
        if global_col in global_columns_mapped:
            print(f"  WARNING: Multiple local columns map to '{global_col}' — using the first mapping.")
            continue
        select_exprs.append(F.col(local_col).alias(global_col))
        mapped_cols.append((local_col, global_col))
        global_columns_mapped.add(global_col)
    else:
        unmapped_cols.append(local_col)
        print(f"  WARNING: Dictionary column '{local_col}' not found in raw table — skipping.")

# Log any raw columns not in the dictionary (i.e., deliberately excluded like fecha_carga)
for raw_col in sorted(raw_columns):
    if raw_col not in active_mappings:
        print(f"  INFO: Raw column '{raw_col}' has no active dictionary mapping — excluded from harmonized output.")

print(f"Mapped: {len(mapped_cols)}, unmapped: {len(unmapped_cols)}, excluded: {len(raw_columns) - len(mapped_cols)}")
```

Output:
```text
INFO: Raw column 'codice_agenzia_interno' has no active dictionary mapping — excluded from harmonized output.
Mapped: 23, unmapped: 0, excluded: 1
```

```python
# Apply renames and ensure every target column exists before replaceWhere.
target_columns = _cfg["target_model"]["columns"]
global_names = [column["name"] for column in target_columns]
target_types = {column["name"]: column["type"] for column in target_columns}
harmonized_df = raw_df.select(select_exprs)
existing_global_columns = set(harmonized_df.columns)
for global_name in global_names:
    if global_name not in existing_global_columns:
        harmonized_df = harmonized_df.withColumn(global_name, F.lit(None).cast(target_types[global_name]))

for global_name in global_names:
    harmonized_df = harmonized_df.withColumn(global_name, F.col(global_name).cast(target_types[global_name]))

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
```

Output:
```text
Harmonized rows: 10,000, columns: 28
```

```python
(
    harmonized_df.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"source_country = '{source_country}'")
    .saveAsTable(HARM_TABLE)
)

final_count = spark.table(HARM_TABLE).count()
print(f"Written {final_count:,} rows to {HARM_TABLE}")
```

Output:
```text
Written 20,000 rows to `agent_marketplace_catalog`.`halvard_harmonization`.`harmonized_property_monthly`
```

```python
display(spark.table(HARM_TABLE).limit(5))
```

```python
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

summary = {
    "source_country": source_country,
    "source_system": SOURCE_SYSTEM,
    "raw_rows": raw_count,
    "harmonized_rows": final_count,
    "mapped_columns": len(mapped_cols),
    "unmapped_dictionary_columns": len(unmapped_cols),
    "target_columns": len(global_names),
    "mapping_version": mapping_version,
}
dbutils.notebook.exit(json.dumps(summary))
```

Output:
```text
{"source_country": "IT", "source_system": "IT_PROPERTY_RAW", "raw_rows": 10000, "harmonized_rows": 20000, "mapped_columns": 23, "unmapped_dictionary_columns": 0, "target_columns": 23, "mapping_version": "v1"}
```
