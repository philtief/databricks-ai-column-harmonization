# create_global_model_and_control_tables (job halvard_propose_mappings, run 46899910151827, task run 501822019941618, SUCCESS)

```python
%run ./_shared_utils
```

```python
dbutils.widgets.removeAll()
import json

dbutils.widgets.text("catalog_name", "your_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "harmonizing_agent", "Schema Name")
dbutils.widgets.text("source_country", "ES", "Source Country")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
source_country = dbutils.widgets.get("source_country").strip()

DB = f"`{catalog_name}`.`{schema_name}`"
_cfg = load_harmonization_config()
_refs = get_table_refs(_cfg, DB, source_country)
OPS_TABLE = _refs["ops_table"]

from uuid import uuid4

RUN_ID = str(uuid4())
_start = _dt.datetime.utcnow()

print(f"Config: {DB}, country={source_country}")
```

Output:
```text
Config: `agent_marketplace_catalog`.`halvard_harmonization`, country=IT

/home/spark-ae10c1ee-4c91-492e-b8b6-89/.ipykernel/67/command-8770032077284766-543059375:20: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
  _start = _dt.datetime.utcnow()
```

```python
results = []


def execute_ddl(label, sql):
    """Execute a DDL statement and record the result."""
    try:
        spark.sql(sql)
        results.append(("OK", label))
        print(f"  [OK]  {label}")
    except Exception as e:
        results.append(("ERROR", label, str(e)))
        print(f"  [ERR] {label}: {e}")
        raise
```

```python
# Raw table: NOT created here. The customer brings their own raw data.
# The Lakeflow pipeline writes the bronze tables (pipelines/ingest_country_feeds.py).

# Harmonized table: generated dynamically from config target_model
from harmonization.governance import sql_str

_target_table = _cfg["target_model"]["table_name"]

_col_defs = []
for _col in _cfg["target_model"]["columns"]:
    _col_name = _col["name"]
    _col_type = _col["type"]
    _col_defs.append(f"  `{_col_name}` {_col_type} COMMENT {sql_str(_col['description'])}")

# Pipeline metadata columns (always appended)
_col_defs.extend(
    [
        "  `source_country` STRING COMMENT 'Pipeline metadata: source country name'",
        "  `source_system` STRING COMMENT 'Pipeline metadata: source system identifier'",
        "  `harmonization_timestamp` TIMESTAMP COMMENT 'Pipeline metadata: when this row was harmonized'",
        "  `column_mapping_version` STRING COMMENT 'Pipeline metadata: column mapping dictionary version used'",
        "  `mapping_status` STRING COMMENT 'Pipeline metadata: mapping status flag'",
    ]
)

_col_defs_str = ",\n".join(_col_defs)
_n_cols = len(_cfg["target_model"]["columns"])

execute_ddl(
    _target_table,
    f"""
CREATE TABLE IF NOT EXISTS {DB}.`{_target_table}` (
{_col_defs_str}
)
USING DELTA
COMMENT 'Harmonized output table with global English column names. {_n_cols} business columns + 5 pipeline metadata columns.'
""",
)
```

Output:
```text
[OK]  harmonized_property_monthly
```

```python
execute_ddl(
    "source_column_inventory",
    f"""
CREATE TABLE IF NOT EXISTS {DB}.`source_column_inventory` (
  source_system      STRING           COMMENT 'Source system identifier from config',
  source_table       STRING           COMMENT 'Source table name',
  local_column_name  STRING           COMMENT 'Column name in the local source table',
  local_data_type    STRING           COMMENT 'Data type of the local column',
  sample_values      ARRAY<STRING>    COMMENT 'Up to 5 non-null distinct sample values as strings',
  ordinal_position   INT              COMMENT 'Column position in source table schema',
  is_nullable        BOOLEAN          COMMENT 'Whether the column allows null values',
  detected_at        TIMESTAMP        COMMENT 'When this column was first inventoried'
)
USING DELTA
COMMENT 'Inventory of all source columns detected in the raw table, with sample values for AI mapping.'
""",
)

execute_ddl(
    "global_target_columns",
    f"""
CREATE TABLE IF NOT EXISTS {DB}.`global_target_columns` (
  target_table        STRING        COMMENT 'Harmonized target table name',
  global_column_name  STRING        COMMENT 'Global English column name',
  global_data_type    STRING        COMMENT 'Data type in the global model',
  business_definition STRING        COMMENT 'Human-readable business definition',
  example_values      ARRAY<STRING> COMMENT 'Representative example values',
  required_flag       BOOLEAN       COMMENT 'Whether this column is required in the global model',
  semantic_group      STRING        COMMENT 'Semantic grouping for the column',
  created_at          TIMESTAMP     COMMENT 'When this column definition was created'
)
USING DELTA
COMMENT 'Authoritative global English column definitions. Shared across all source countries.'
""",
)

execute_ddl(
    "column_mapping_candidates",
    f"""
CREATE TABLE IF NOT EXISTS {DB}.`column_mapping_candidates` (
  candidate_id               BIGINT    COMMENT 'Surrogate row identifier',
  source_system              STRING    COMMENT 'Source system identifier',
  source_table               STRING    COMMENT 'Source table name',
  local_column_name          STRING    COMMENT 'Local source column name',
  local_data_type            STRING    COMMENT 'Data type of the local column',
  local_sample_values        ARRAY<STRING> COMMENT 'Sample values used as AI context',
  proposed_global_column_name STRING   COMMENT 'AI-proposed global column name',
  proposed_global_data_type  STRING    COMMENT 'AI-proposed global data type',
  proposed_match_type        STRING    COMMENT 'AI-proposed match type: DIRECT | SEMANTIC_TRANSLATION | DERIVED | NO_MATCH',
  mapping_rationale          STRING    COMMENT 'AI rationale for the proposed mapping',
  confidence                 STRING    COMMENT 'AI confidence: HIGH | MEDIUM | LOW',
  ai_error_status            STRING    COMMENT 'AI_ERROR if the AI call failed; NULL if successful',
  review_status              STRING    COMMENT 'Workflow review status: PENDING | APPROVED | CORRECTED | REJECTED',
  final_global_column_name   STRING    COMMENT 'Reviewer-set final global column name (overrides AI proposal if CORRECTED)',
  final_match_type           STRING    COMMENT 'Reviewer-set final match type',
  mandatory_flag             BOOLEAN   COMMENT 'True if this column is in the mandatory mapping gate list',
  reviewed_by                STRING    COMMENT 'Identity of the reviewer',
  reviewed_at                TIMESTAMP COMMENT 'When the review was performed',
  review_comment             STRING    COMMENT 'Free-text review comment',
  app_decision_source        STRING    COMMENT 'Source of review decision: DATABRICKS_APP | SQL_DIRECT | NULL for pending',
  created_at                 TIMESTAMP COMMENT 'When this candidate was first proposed',
  updated_at                 TIMESTAMP COMMENT 'When this candidate was last updated'
)
USING DELTA
COMMENT 'AI-proposed column mapping candidates. Human reviewers approve, correct, or reject each row.'
""",
)

execute_ddl(
    "column_mapping_dictionary",
    f"""
CREATE TABLE IF NOT EXISTS {DB}.`column_mapping_dictionary` (
  source_system      STRING    COMMENT 'Source system identifier',
  source_table       STRING    COMMENT 'Source table name',
  local_column_name  STRING    COMMENT 'Local source column name',
  global_column_name STRING    COMMENT 'Approved global column name',
  match_type         STRING    COMMENT 'Approved match type',
  approved_by        STRING    COMMENT 'Identity who approved this mapping',
  approved_at        TIMESTAMP COMMENT 'When the mapping was approved',
  mapping_version    STRING    COMMENT 'Version label for this dictionary entry',
  active_flag        BOOLEAN   COMMENT 'True if this mapping is currently active',
  mapping_comment    STRING    COMMENT 'Optional comment on the mapping',
  created_at         TIMESTAMP COMMENT 'When this dictionary entry was created',
  updated_at         TIMESTAMP COMMENT 'When this dictionary entry was last updated'
)
USING DELTA
COMMENT 'Approved column mapping dictionary. Maps each local source column name to its global equivalent.'
""",
)

execute_ddl(
    "column_mapping_audit",
    f"""
CREATE TABLE IF NOT EXISTS {DB}.`column_mapping_audit` (
  audit_id              BIGINT    COMMENT 'Audit event surrogate identifier',
  source_system         STRING    COMMENT 'Source system identifier',
  local_column_name     STRING    COMMENT 'The source column that was reviewed',
  old_review_status     STRING    COMMENT 'Review status before the action',
  new_review_status     STRING    COMMENT 'Review status after the action',
  old_global_column_name STRING   COMMENT 'Global column name before the action',
  new_global_column_name STRING   COMMENT 'Global column name after the action',
  old_match_type        STRING    COMMENT 'Match type before the action',
  new_match_type        STRING    COMMENT 'Match type after the action',
  action_by             STRING    COMMENT 'Identity who performed the action',
  action_at             TIMESTAMP COMMENT 'When the action was performed',
  action_comment        STRING    COMMENT 'Description of the action taken',
  action_source         STRING    COMMENT 'Source of the action: DATABRICKS_APP | SQL_DIRECT'
)
USING DELTA
COMMENT 'Immutable audit trail of every review action on column_mapping_candidates.'
""",
)
```

Output:
```text
[OK]  source_column_inventory
  [OK]  global_target_columns
  [OK]  column_mapping_candidates
  [OK]  column_mapping_dictionary
  [OK]  column_mapping_audit
```

```python
execute_ddl(
    "value_mapping_candidates",
    f"""
CREATE TABLE IF NOT EXISTS {DB}.`value_mapping_candidates` (
  candidate_id              BIGINT    COMMENT 'Surrogate row identifier',
  source_system             STRING    COMMENT 'Source system identifier',
  source_field              STRING    COMMENT 'Global field name that holds this value',
  raw_value                 STRING    COMMENT 'Raw categorical value from source',
  proposed_harmonized_value STRING    COMMENT 'AI-proposed harmonized equivalent',
  proposed_description      STRING    COMMENT 'AI-proposed description of the value',
  confidence                STRING    COMMENT 'AI confidence: HIGH | MEDIUM | LOW',
  ai_error_status           STRING    COMMENT 'AI_ERROR if the AI call failed; NULL if successful',
  review_status             STRING    COMMENT 'PENDING | APPROVED | CORRECTED | REJECTED',
  final_harmonized_value    STRING    COMMENT 'Reviewer-set final harmonized value',
  reviewed_by               STRING    COMMENT 'Identity of the reviewer',
  reviewed_at               TIMESTAMP COMMENT 'When the review was performed',
  review_comment            STRING    COMMENT 'Free-text review comment',
  created_at                TIMESTAMP COMMENT 'When this candidate was created',
  updated_at                TIMESTAMP COMMENT 'When this candidate was last updated'
)
USING DELTA
COMMENT 'Optional value translation candidates. Source categorical values proposed for harmonized equivalents.'
""",
)

execute_ddl(
    "value_mapping_dictionary",
    f"""
CREATE TABLE IF NOT EXISTS {DB}.`value_mapping_dictionary` (
  source_field       STRING    COMMENT 'Global field name',
  source_system      STRING    COMMENT 'Source system identifier',
  raw_value          STRING    COMMENT 'Original source categorical value',
  harmonized_value   STRING    COMMENT 'Approved harmonized value',
  approval_status    STRING    COMMENT 'APPROVED | CORRECTED',
  approved_by        STRING    COMMENT 'Identity who approved this translation',
  approved_at        TIMESTAMP COMMENT 'When the translation was approved',
  mapping_version    STRING    COMMENT 'Version label',
  active_flag        BOOLEAN   COMMENT 'True if this translation is currently active',
  created_at         TIMESTAMP COMMENT 'When this entry was created',
  updated_at         TIMESTAMP COMMENT 'When this entry was last updated'
)
USING DELTA
COMMENT 'Approved value translation dictionary. Maps source categorical values to harmonized equivalents.'
""",
)
```

Output:
```text
[OK]  value_mapping_candidates
  [OK]  value_mapping_dictionary
```

```python
execute_ddl(
    "workflow_run_metrics",
    f"""
CREATE TABLE IF NOT EXISTS {DB}.`workflow_run_metrics` (
  run_id        STRING    COMMENT 'UUID run identifier',
  workflow_name STRING    COMMENT 'Databricks workflow name',
  task_name     STRING    COMMENT 'Notebook task name',
  task_status   STRING    COMMENT 'SUCCEEDED | FAILED | INFO',
  started_at    TIMESTAMP COMMENT 'Task start time (UTC)',
  finished_at   TIMESTAMP COMMENT 'Task finish time (UTC)',
  row_count     BIGINT    COMMENT 'Number of rows processed or written',
  message       STRING    COMMENT 'Free-text summary message'
)
USING DELTA
COMMENT 'Per-task run telemetry for all workflow notebooks.'
""",
)

execute_ddl(
    "ai_mapping_usage_metrics",
    f"""
CREATE TABLE IF NOT EXISTS {DB}.`ai_mapping_usage_metrics` (
  run_id                   STRING    COMMENT 'UUID run identifier',
  source_system            STRING    COMMENT 'Source system identifier',
  mapping_type             STRING    COMMENT 'COLUMN or VALUE',
  source_field_or_column   STRING    COMMENT 'Source field or column that was processed',
  candidate_rows           BIGINT    COMMENT 'Number of candidates submitted to AI',
  success_rows             BIGINT    COMMENT 'Number of successful AI responses',
  failed_rows              BIGINT    COMMENT 'Number of failed AI responses',
  low_confidence_rows      BIGINT    COMMENT 'Number of LOW confidence responses',
  estimated_prompt_units   DOUBLE    COMMENT 'Estimated prompt token units',
  estimated_response_units DOUBLE    COMMENT 'Estimated response token units',
  estimated_cost_eur       DOUBLE    COMMENT 'Estimated cost in EUR (not billed cost)',
  recorded_at              TIMESTAMP COMMENT 'When this metric was recorded'
)
USING DELTA
COMMENT 'AI endpoint usage statistics and cost estimates per run.'
""",
)

execute_ddl(
    "data_quality_results",
    f"""
CREATE TABLE IF NOT EXISTS {DB}.`data_quality_results` (
  run_id       STRING    COMMENT 'UUID run identifier',
  source_system STRING   COMMENT 'Source system identifier',
  check_name   STRING    COMMENT 'Name of the data quality check',
  check_status STRING    COMMENT 'PASSED | FAILED | WARNING',
  metric_value DOUBLE    COMMENT 'Numeric metric value (count, ratio, etc.)',
  recorded_at  TIMESTAMP COMMENT 'When this result was recorded',
  details      STRING    COMMENT 'Free-text details or diagnostic message'
)
USING DELTA
COMMENT 'Data quality check results per pipeline run.'
""",
)
```

Output:
```text
[OK]  workflow_run_metrics
  [OK]  ai_mapping_usage_metrics
  [OK]  data_quality_results
```

```python
from pyspark.sql.types import ArrayType, BooleanType

_now = _dt.datetime.utcnow()

# Load target columns from config (reuses _cfg from the DDL section above)
global_cols = [
    (
        c["name"],
        c["type"],
        c["description"],
        c.get("examples", []),
        c.get("required", False),
        c.get("semantic_group", ""),
    )
    for c in _cfg["target_model"]["columns"]
]

print(f"Target model: {_target_table}, {len(global_cols)} columns")

gtc_schema = StructType(
    [
        StructField("target_table", StringType(), False),
        StructField("global_column_name", StringType(), False),
        StructField("global_data_type", StringType(), True),
        StructField("business_definition", StringType(), True),
        StructField("example_values", ArrayType(StringType()), True),
        StructField("required_flag", BooleanType(), True),
        StructField("semantic_group", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)

gtc_rows = [
    (_target_table, col_name, dtype, definition, examples, req, group, _now)
    for col_name, dtype, definition, examples, req, group in global_cols
]

gtc_df = spark.createDataFrame(gtc_rows, schema=gtc_schema)
gtc_df.createOrReplaceTempView("_gtc_staged")

spark.sql(f"""
MERGE INTO {DB}.`global_target_columns` AS tgt
USING _gtc_staged AS src
ON tgt.target_table = src.target_table
   AND tgt.global_column_name = src.global_column_name
WHEN MATCHED THEN UPDATE SET
  tgt.global_data_type    = src.global_data_type,
  tgt.business_definition = src.business_definition,
  tgt.example_values      = src.example_values,
  tgt.required_flag       = src.required_flag,
  tgt.semantic_group      = src.semantic_group
WHEN NOT MATCHED THEN INSERT (
  target_table, global_column_name, global_data_type, business_definition,
  example_values, required_flag, semantic_group, created_at
) VALUES (
  src.target_table, src.global_column_name, src.global_data_type, src.business_definition,
  src.example_values, src.required_flag, src.semantic_group, src.created_at
)
""")

gtc_count = spark.table(f"{DB}.`global_target_columns`").count()
print(f"global_target_columns: {gtc_count} rows")
display(spark.table(f"{DB}.`global_target_columns`").orderBy("semantic_group", "global_column_name"))
```

Output:
```text
Target model: harmonized_property_monthly, 23 columns

/home/spark-ae10c1ee-4c91-492e-b8b6-89/.ipykernel/67/command-8770032077284778-44074214:3: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
  _now = _dt.datetime.utcnow()

global_target_columns: 23 rows
```

```python
ok_count = sum(1 for r in results if r[0] == "OK")
err_count = sum(1 for r in results if r[0] == "ERROR")
print(f"DDL results: {ok_count} OK, {err_count} ERROR")

if err_count > 0:
    raise Exception(f"DDL creation had {err_count} errors. Review output above.")

summary = {
    "source_country": source_country,
    "source_system": _refs["source_system"],
    "ddl_ok": ok_count,
    "ddl_errors": err_count,
    "target_columns": len(global_cols),
    "global_target_columns": gtc_count,
}
```

Output:
```text
DDL results: 11 OK, 0 ERROR
```

```python
log_run_metric(
    spark,
    OPS_TABLE,
    RUN_ID,
    "create_global_model_and_control_tables",
    "SUCCEEDED",
    _start,
    gtc_count,
    f"Created all tables and views. global_target_columns prepopulated with {gtc_count} rows.",
)

dbutils.notebook.exit(json.dumps(summary))
```

Output:
```text
{"source_country": "IT", "source_system": "IT_PROPERTY_RAW", "ddl_ok": 11, "ddl_errors": 0, "target_columns": 23, "global_target_columns": 23}
```
