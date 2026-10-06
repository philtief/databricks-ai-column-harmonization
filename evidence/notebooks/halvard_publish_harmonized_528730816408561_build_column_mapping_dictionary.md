# build_column_mapping_dictionary (job halvard_publish_harmonized, run 528730816408561, task run 284565256998009, SUCCESS)

```python
%run ./_shared_utils
```

```python
dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "your_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "harmonizing_agent", "Schema Name")
dbutils.widgets.text("mapping_version", "v1", "Mapping Version")
dbutils.widgets.text("source_country", "ES", "Source Country")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
mapping_version = dbutils.widgets.get("mapping_version").strip()
source_country = dbutils.widgets.get("source_country").strip()

DB = f"`{catalog_name}`.`{schema_name}`"
_cfg = load_harmonization_config()
_refs = get_table_refs(_cfg, DB, source_country)
CAND_TABLE = _refs["cand_table"]
DICT_TABLE = _refs["dict_table"]
OPS_TABLE = _refs["ops_table"]
SOURCE_SYSTEM = _refs["source_system"]
SOURCE_TABLE = _refs["source_table_name"]

print(f"Config: {DB}, version={mapping_version}")
```

Output:
```text
Config: `agent_marketplace_catalog`.`halvard_harmonization`, version=v1
```

```python
import datetime as _dt
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

RUN_ID = str(uuid4())
_start = _dt.datetime.utcnow()
import json
```

Output:
```text
/home/spark-9daee9c9-5ec3-40ce-9417-00/.ipykernel/68/command-8770032077284658-326138737:8: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
  _start = _dt.datetime.utcnow()
```

```python
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
```

Output:
```text
Found 23 APPROVED/CORRECTED candidates
```

```python
valid_mappings_df = approved_df.where(
    F.col("global_column_name").isNotNull()
    & (F.upper(F.col("global_column_name")) != "NO_MATCH")
    & (F.col("global_column_name") != "")
)

excluded_count = approved_count - valid_mappings_df.count()
valid_count = valid_mappings_df.count()

print(f"Valid mappings: {valid_count}, Excluded (NO_MATCH/null): {excluded_count}")
```

Output:
```text
Valid mappings: 23, Excluded (NO_MATCH/null): 0
```

```python
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
```

Output:
```text
1 column(s) rejected
```

```python
_now = _dt.datetime.utcnow()

dict_staged_df = (
    valid_mappings_df.withColumn("approved_by", F.coalesce(F.col("reviewed_by"), F.lit("system")))
    .withColumn("approved_at", F.coalesce(F.col("reviewed_at"), F.lit(_now).cast(TimestampType())))
    .withColumn("mapping_version", F.lit(mapping_version))
    .withColumn("active_flag", F.lit(True))
    .withColumn("mapping_comment", F.col("review_comment"))
    .withColumn("created_at", F.lit(_now).cast(TimestampType()))
    .withColumn("updated_at", F.lit(_now).cast(TimestampType()))
    .select(
        "source_system",
        "source_table",
        "local_column_name",
        "global_column_name",
        "match_type",
        "approved_by",
        "approved_at",
        "mapping_version",
        "active_flag",
        "mapping_comment",
        "created_at",
        "updated_at",
    )
)

dict_staged_df.createOrReplaceTempView("_dict_staged")
print(f"Dictionary rows ready: {dict_staged_df.count()}")
```

Output:
```text
/home/spark-9daee9c9-5ec3-40ce-9417-00/.ipykernel/68/command-8770032077284666-2794949090:1: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
  _now = _dt.datetime.utcnow()

Dictionary rows ready: 23
```

```python
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
```

```python
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
    spark.sql(f"""
        UPDATE {DICT_TABLE}
        SET active_flag = FALSE, updated_at = '{_now.isoformat()}'
        WHERE source_system = '{SOURCE_SYSTEM}' AND active_flag = TRUE
    """)
    print("No valid local columns; all current dictionary entries deactivated.")
```

Output:
```text
Stale entries deactivated (if any).
```

```python
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
```

Output:
```text
Active dictionary entries: 23
```

```python
log_run_metric(
    spark,
    OPS_TABLE,
    RUN_ID,
    "build_column_mapping_dictionary",
    "SUCCEEDED",
    _start,
    active_count,
    f"Dictionary built with {active_count} active entries. Approved: {approved_count}, Excluded (NO_MATCH): {excluded_count}, Rejected: {rejected_count}. Version: {mapping_version}.",
)

summary = {
    "source_country": source_country,
    "source_system": SOURCE_SYSTEM,
    "approved_candidates": approved_count,
    "valid_mappings": valid_count,
    "excluded_no_match": excluded_count,
    "rejected": rejected_count,
    "active_dictionary_entries": active_count,
    "mapping_version": mapping_version,
}
dbutils.notebook.exit(json.dumps(summary))
```

Output:
```text
{"source_country": "IT", "source_system": "IT_PROPERTY_RAW", "approved_candidates": 23, "valid_mappings": 23, "excluded_no_match": 0, "rejected": 1, "active_dictionary_entries": 23, "mapping_version": "v1"}
```
