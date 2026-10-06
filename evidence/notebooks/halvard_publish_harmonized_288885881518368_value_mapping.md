# value_mapping (job halvard_publish_harmonized, run 288885881518368, task run 279627349653975, SUCCESS)

```python
%run ./_shared_utils
```

```python
import datetime as _dt
import json
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from harmonization.governance import sql_str
from harmonization.value_mapping import build_value_prompt, categorical_targets, parse_value_response

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "your_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "harmonizing_agent", "Schema Name")
dbutils.widgets.text("source_country", "ES", "Source Country")
dbutils.widgets.text("ai_endpoint", "databricks-gpt-5-2", "AI Endpoint")
dbutils.widgets.text("mapping_version", "v1", "Mapping Version")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
source_country = dbutils.widgets.get("source_country").strip()
ai_endpoint = dbutils.widgets.get("ai_endpoint").strip()
mapping_version = dbutils.widgets.get("mapping_version").strip()

DB = f"`{catalog_name}`.`{schema_name}`"
_cfg = load_harmonization_config()
_refs = get_table_refs(_cfg, DB, source_country)
HARM_TABLE = _refs["harm_table"]
VDICT_TABLE = _refs["vdict_table"]
VCAND_TABLE = _refs["vcand_table"]
OPS_TABLE = _refs["ops_table"]
SOURCE_SYSTEM = _refs["source_system"]

print(f"Config: {DB}, country={source_country}, ai_endpoint={ai_endpoint}")

RUN_ID = str(uuid4())
_start = _dt.datetime.utcnow()
target_values = categorical_targets(_cfg)
now = _dt.datetime.utcnow()
```

Output:
```text
Config: `agent_marketplace_catalog`.`halvard_harmonization`, country=ES, ai_endpoint=databricks-claude-sonnet-4-6

/home/spark-55ac7719-ba97-4aaf-ab34-9c/.ipykernel/67/command-8770032077284730-2369161540:36: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
  _start = _dt.datetime.utcnow()
/home/spark-55ac7719-ba97-4aaf-ab34-9c/.ipykernel/67/command-8770032077284730-2369161540:38: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
  now = _dt.datetime.utcnow()
```

```python
translations = []
candidate_rows = []
ai_error_count = 0

for global_column, allowed_values in sorted(target_values.items()):
    if global_column not in spark.table(HARM_TABLE).columns:
        print(f"  [{global_column}] Not present in harmonized table; skipped.")
        continue

    local_df = spark.sql(f"""
        SELECT DISTINCT CAST(`{global_column}` AS STRING) AS local_value
        FROM {HARM_TABLE}
        WHERE source_country = '{source_country}'
          AND `{global_column}` IS NOT NULL
        ORDER BY local_value
    """)
    local_values = [row["local_value"] for row in local_df.collect()]
    if not local_values:
        print(f"  [{global_column}] No local values; skipped.")
        continue

    prompt_sql = sql_str(build_value_prompt(global_column, allowed_values, local_values))
    ai_result = spark.sql(f"SELECT ai_query({sql_str(ai_endpoint)}, {prompt_sql}) AS ai_result").collect()[0][
        "ai_result"
    ]
    try:
        proposed = parse_value_response(ai_result, allowed_values)
    except ValueError as error:
        ai_error_count += 1
        print(f"  [{global_column}] AI response invalid: {error}; values reported as unmapped.")
        proposed = {local_value: None for local_value in local_values}

    for local_value in local_values:
        proposed_value = proposed.get(local_value)
        approved = proposed_value is not None
        if approved:
            approved_by = "auto:allowed-list"
            print(f"  {global_column}: {local_value!r} -> {proposed_value!r} (approved yes)")
        else:
            approved_by = None
            print(f"  {global_column}: {local_value!r} -> null (approved no)")

        translations.append((global_column, local_value, proposed_value, approved))
        candidate_rows.append(
            (
                SOURCE_SYSTEM,
                global_column,
                local_value,
                proposed_value,
                None,
                "HIGH" if approved else "LOW",
                None,
                "APPROVED" if approved else "PENDING",
                proposed_value,
                approved_by,
                now,
                None,
                now,
                now,
            )
        )

print(f"Generated {len(candidate_rows)} value mapping candidates; AI errors: {ai_error_count}")
```

Output:
```text
customer_segment: 'Gran Empresa' -> 'Large Enterprise' (approved yes)
  customer_segment: 'Mediana Empresa' -> 'Medium Business' (approved yes)
  customer_segment: 'Particular' -> 'Individual' (approved yes)
  customer_segment: 'Pequeña Empresa' -> 'Small Business' (approved yes)
  distribution_channel: 'Agente' -> 'Agent' (approved yes)
  distribution_channel: 'Bancaseguros' -> 'Bancassurance' (approved yes)
  distribution_channel: 'Corredor' -> 'Broker' (approved yes)
  distribution_channel: 'Digital' -> 'Digital' (approved yes)
  distribution_channel: 'Directo' -> 'Direct' (approved yes)
  risk_type: 'Daños Eléctricos' -> 'Electrical Damage' (approved yes)
  risk_type: 'Daños por Agua' -> 'Water Damage' (approved yes)
  risk_type: 'Incendio' -> 'Fire' (approved yes)
  risk_type: 'Inundación' -> 'Flood' (approved yes)
  risk_type: 'Responsabilidad Civil' -> 'Liability' (approved yes)
  risk_type: 'Robo' -> 'Theft' (approved yes)
  risk_zone: 'Zona A' -> 'Zone A' (approved yes)
  risk_zone: 'Zona B' -> 'Zone B' (approved yes)
  risk_zone: 'Zona C' -> 'Zone C' (approved yes)
  risk_zone: 'Zona D' -> 'Zone D' (approved yes)
Generated 19 value mapping candidates; AI errors: 0
```

```python
candidate_schema = StructType(
    [
        StructField("source_system", StringType(), False),
        StructField("source_field", StringType(), False),
        StructField("raw_value", StringType(), False),
        StructField("proposed_harmonized_value", StringType(), True),
        StructField("proposed_description", StringType(), True),
        StructField("confidence", StringType(), True),
        StructField("ai_error_status", StringType(), True),
        StructField("review_status", StringType(), True),
        StructField("final_harmonized_value", StringType(), True),
        StructField("reviewed_by", StringType(), True),
        StructField("reviewed_at", TimestampType(), True),
        StructField("review_comment", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)

spark.sql(f"DELETE FROM {VCAND_TABLE} WHERE source_system = '{SOURCE_SYSTEM}'")
if candidate_rows:
    (
        spark.createDataFrame(candidate_rows, schema=candidate_schema)
        .write.format("delta")
        .mode("append")
        .saveAsTable(VCAND_TABLE)
    )

approved_translations = [
    (source_field, local_value, proposed_value)
    for source_field, local_value, proposed_value, approved in translations
    if approved
]

if approved_translations:
    dictionary_schema = StructType(
        [
            StructField("source_field", StringType(), False),
            StructField("raw_value", StringType(), False),
            StructField("harmonized_value", StringType(), False),
        ]
    )
    (
        spark.createDataFrame(approved_translations, schema=dictionary_schema).createOrReplaceTempView(
            "_value_dictionary_staged"
        )
    )
    spark.sql(f"""
MERGE INTO {VDICT_TABLE} AS tgt
USING _value_dictionary_staged AS src
ON tgt.source_system = '{SOURCE_SYSTEM}'
   AND tgt.source_field = src.source_field
   AND tgt.raw_value = src.raw_value
WHEN MATCHED AND (tgt.approved_by = 'auto:allowed-list' OR tgt.harmonized_value IS NULL) THEN UPDATE SET
  tgt.harmonized_value = src.harmonized_value,
  tgt.approval_status = 'APPROVED',
  tgt.approved_by = 'auto:allowed-list',
  tgt.approved_at = CAST('{now.isoformat()}' AS TIMESTAMP),
  tgt.mapping_version = '{mapping_version}',
  tgt.active_flag = TRUE,
  tgt.updated_at = CAST('{now.isoformat()}' AS TIMESTAMP)
WHEN NOT MATCHED THEN INSERT (
  source_system, source_field, raw_value, harmonized_value, approval_status,
  approved_by, approved_at, mapping_version, active_flag, created_at, updated_at
) VALUES (
  '{SOURCE_SYSTEM}', src.source_field, src.raw_value, src.harmonized_value, 'APPROVED',
  'auto:allowed-list', CAST('{now.isoformat()}' AS TIMESTAMP), '{mapping_version}', TRUE,
  CAST('{now.isoformat()}' AS TIMESTAMP), CAST('{now.isoformat()}' AS TIMESTAMP)
)
""")

print(f"Auto-approved value mappings: {len(approved_translations)}")
```

Output:
```text
Auto-approved value mappings: 19
```

```python
harmonized_df = spark.table(HARM_TABLE).where(F.col("source_country") == source_country)
translated_columns = []

for global_column in sorted(target_values):
    mappings = spark.sql(f"""
        SELECT raw_value, harmonized_value
        FROM {VDICT_TABLE}
        WHERE source_system = '{SOURCE_SYSTEM}'
          AND source_field = '{global_column}'
          AND active_flag = TRUE
    """).collect()
    if not mappings:
        continue

    # Lookup map local -> group value; values without a translation stay as delivered (DQ check 10 reports them).
    lookup = F.create_map(*[F.lit(v) for m in mappings for v in (m["raw_value"], m["harmonized_value"])])
    harmonized_df = harmonized_df.withColumn(
        global_column, F.coalesce(lookup[F.col(global_column)], F.col(global_column))
    )
    translated_columns.append(global_column)

harmonized_df = harmonized_df.withColumn("mapping_status", F.lit("APPROVED_COLUMN_AND_VALUE_MAPPING"))

final_count = harmonized_df.count()
if translated_columns:
    (
        harmonized_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"source_country = '{source_country}'")
        .saveAsTable(HARM_TABLE)
    )
else:
    print("No active value translations; harmonized table unchanged.")

final_table_count = spark.table(HARM_TABLE).count()
print(f"Translated columns: {translated_columns}; country rows: {final_count:,}; total rows: {final_table_count:,}")
```

Output:
```text
Translated columns: ['customer_segment', 'distribution_channel', 'risk_type', 'risk_zone']; country rows: 10,000; total rows: 10,000
```

```python
log_run_metric(
    spark,
    OPS_TABLE,
    RUN_ID,
    "value_mapping",
    "SUCCEEDED",
    _start,
    final_count,
    f"Generated {len(candidate_rows)} value candidates, auto-approved {len(approved_translations)}, "
    f"and translated {len(translated_columns)} columns.",
)

summary = {
    "source_country": source_country,
    "source_system": SOURCE_SYSTEM,
    "categorical_columns": len(target_values),
    "value_candidates": len(candidate_rows),
    "auto_approved": len(approved_translations),
    "unmapped_values": sum(1 for translation in translations if not translation[3]),
    "ai_errors": ai_error_count,
    "translated_columns": len(translated_columns),
}
dbutils.notebook.exit(json.dumps(summary))
```

Output:
```text
{"source_country": "ES", "source_system": "ES_PROPERTY_RAW", "categorical_columns": 4, "value_candidates": 19, "auto_approved": 19, "unmapped_values": 0, "ai_errors": 0, "translated_columns": 4}
```
