# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Create Global Model and Control Tables
# MAGIC
# MAGIC Creates all tables, views, and prepopulates the `global_target_columns` reference table.
# MAGIC
# MAGIC All objects live under `{catalog_name}.{schema_name}` (single schema).
# MAGIC
# MAGIC **Tables created:**
# MAGIC - Data: `property_insurance_monthly_raw`, `property_insurance_monthly`
# MAGIC - Column mapping: `source_column_inventory_es`, `global_target_columns`, `column_mapping_candidates_es`, `column_mapping_dictionary_es`, `column_mapping_audit_es`
# MAGIC - Value mapping: `value_mapping_candidates_es`, `value_mapping_dictionary_es`
# MAGIC - Ops: `workflow_run_metrics`, `ai_mapping_usage_metrics`, `data_quality_results`
# MAGIC
# MAGIC **Views created:** `vw_pending_column_mappings`, `vw_mapping_review_summary`, `vw_publish_readiness`, `vw_column_mapping_low_conf_es`, `vw_column_mapping_coverage_es`

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "pt_catalog",        "Catalog Name")
dbutils.widgets.text("schema_name",  "harmonizing_agent", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name  = dbutils.widgets.get("schema_name").strip()

DB = f"`{catalog_name}`.`{schema_name}`"

print("STEP 1 — Parameters loaded")
print(f"  catalog_name : {catalog_name}")
print(f"  schema_name  : {schema_name}")
print(f"  DB prefix    : {DB}")

# COMMAND ----------

# MAGIC %md ## STEP 2 — DDL Helper

# COMMAND ----------

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

print("STEP 2 — DDL helper ready.")

# COMMAND ----------

# MAGIC %md ## STEP 3 — Data Tables

# COMMAND ----------

print("STEP 3 — Creating data tables ...")

# ------------------------------------------------------------------
# property_insurance_monthly_raw
# ------------------------------------------------------------------
execute_ddl("property_insurance_monthly_raw", f"""
CREATE TABLE IF NOT EXISTS {DB}.`property_insurance_monthly_raw` (
  id_registro               BIGINT   COMMENT 'Unique record identifier',
  anio                      INT      COMMENT 'Reporting year',
  mes                       INT      COMMENT 'Reporting month 1-12',
  codigo_poliza             STRING   COMMENT 'Policy code',
  tipo_riesgo               STRING   COMMENT 'Risk type (Spanish)',
  provincia                 STRING   COMMENT 'Province / region',
  canal_distribucion        STRING   COMMENT 'Distribution channel (Spanish)',
  prima_neta                DOUBLE   COMMENT 'Net written premium EUR',
  prima_bruta               DOUBLE   COMMENT 'Gross written premium EUR',
  num_polizas_nuevas        INT      COMMENT 'New policies count',
  num_polizas_renovadas     INT      COMMENT 'Renewed policies count',
  num_polizas_canceladas    INT      COMMENT 'Cancelled policies count',
  num_siniestros_declarados INT      COMMENT 'Claims reported count',
  num_siniestros_pagados    INT      COMMENT 'Claims paid count',
  importe_siniestros_bruto  DOUBLE   COMMENT 'Gross claims incurred EUR',
  importe_reservas          DOUBLE   COMMENT 'Claims reserve EUR',
  gastos_gestion            DOUBLE   COMMENT 'Management expenses EUR',
  comisiones                DOUBLE   COMMENT 'Commissions EUR',
  ratio_siniestralidad      DOUBLE   COMMENT 'Loss ratio',
  segmento_cliente          STRING   COMMENT 'Customer segment (Spanish)',
  zona_riesgo               STRING   COMMENT 'Risk zone',
  cobertura_principal       STRING   COMMENT 'Primary coverage (Spanish)',
  moneda                    STRING   COMMENT 'ISO currency code',
  fecha_carga               TIMESTAMP COMMENT 'Technical load timestamp'
)
USING DELTA
COMMENT 'Spain property insurance monthly raw data — 24 local Spanish columns. Source system: ES_PROPERTY_RAW.'
""")

# ------------------------------------------------------------------
# property_insurance_monthly  (harmonized output — 23 business + 5 metadata)
# ------------------------------------------------------------------
execute_ddl("property_insurance_monthly", f"""
CREATE TABLE IF NOT EXISTS {DB}.`property_insurance_monthly` (
  record_id                   BIGINT    COMMENT 'Unique identifier for each record',
  reporting_year              INT       COMMENT 'Calendar year of the reporting period',
  reporting_month             INT       COMMENT 'Month number 1-12 of the reporting period',
  policy_number               STRING    COMMENT 'Unique policy identifier',
  risk_type                   STRING    COMMENT 'Category of insured risk',
  region                      STRING    COMMENT 'Geographic region or province',
  distribution_channel        STRING    COMMENT 'Channel through which policy was sold',
  net_written_premium_eur     DOUBLE    COMMENT 'Net written premium in EUR after reinsurance',
  gross_written_premium_eur   DOUBLE    COMMENT 'Gross written premium in EUR before reinsurance',
  new_policies_count          INT       COMMENT 'Number of new policies in period',
  renewed_policies_count      INT       COMMENT 'Number of policies renewed in period',
  cancelled_policies_count    INT       COMMENT 'Number of policies cancelled in period',
  claims_reported_count       INT       COMMENT 'Total claims notified in period',
  claims_paid_count           INT       COMMENT 'Number of claims settled and paid',
  gross_claims_incurred_eur   DOUBLE    COMMENT 'Total gross claims incurred in EUR',
  claims_reserve_eur          DOUBLE    COMMENT 'Claims reserve amount at reporting date in EUR',
  management_expenses_eur     DOUBLE    COMMENT 'Internal management expenses in EUR',
  commissions_eur             DOUBLE    COMMENT 'Commissions paid to distribution partners in EUR',
  loss_ratio                  DOUBLE    COMMENT 'Ratio of claims incurred to gross written premium',
  customer_segment            STRING    COMMENT 'Segment classification of policyholder',
  risk_zone                   STRING    COMMENT 'Risk zone classification',
  primary_coverage            STRING    COMMENT 'Primary coverage type in the policy',
  currency                    STRING    COMMENT 'ISO currency code for monetary amounts',
  source_country              STRING    COMMENT 'Pipeline metadata: source country name',
  source_system               STRING    COMMENT 'Pipeline metadata: source system identifier',
  harmonization_timestamp     TIMESTAMP COMMENT 'Pipeline metadata: when this row was harmonized',
  column_mapping_version      STRING    COMMENT 'Pipeline metadata: column mapping dictionary version used',
  mapping_status              STRING    COMMENT 'Pipeline metadata: mapping status flag'
)
USING DELTA
COMMENT 'Harmonized property insurance monthly data — global English column names. 23 business columns + 5 pipeline metadata columns.'
""")

# COMMAND ----------

# MAGIC %md ## STEP 4 — Column-Mapping Control Tables

# COMMAND ----------

print("STEP 4 — Creating column-mapping control tables ...")

# ------------------------------------------------------------------
# source_column_inventory_es
# ------------------------------------------------------------------
execute_ddl("source_column_inventory_es", f"""
CREATE TABLE IF NOT EXISTS {DB}.`source_column_inventory_es` (
  source_system      STRING           COMMENT 'Source system identifier e.g. ES_PROPERTY_RAW',
  source_table       STRING           COMMENT 'Source table name',
  local_column_name  STRING           COMMENT 'Column name in the local Spanish source table',
  local_data_type    STRING           COMMENT 'Data type of the local column',
  sample_values      ARRAY<STRING>    COMMENT 'Up to 5 non-null distinct sample values as strings',
  ordinal_position   INT              COMMENT 'Column position in source table schema',
  is_nullable        BOOLEAN          COMMENT 'Whether the column allows null values',
  detected_at        TIMESTAMP        COMMENT 'When this column was first inventoried'
)
USING DELTA
COMMENT 'Inventory of all source columns detected in the Spain raw table, with sample values for AI mapping.'
""")

# ------------------------------------------------------------------
# global_target_columns
# ------------------------------------------------------------------
execute_ddl("global_target_columns", f"""
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
COMMENT 'Authoritative global English column definitions. Shared across all source countries. Source countries map their local columns to these targets.'
""")

# ------------------------------------------------------------------
# column_mapping_candidates_es
# ------------------------------------------------------------------
execute_ddl("column_mapping_candidates_es", f"""
CREATE TABLE IF NOT EXISTS {DB}.`column_mapping_candidates_es` (
  candidate_id               BIGINT    COMMENT 'Surrogate row identifier',
  source_system              STRING    COMMENT 'Source system identifier',
  source_table               STRING    COMMENT 'Source table name',
  local_column_name          STRING    COMMENT 'Local Spanish column name',
  local_data_type            STRING    COMMENT 'Data type of the local column',
  local_sample_values        ARRAY<STRING> COMMENT 'Sample values used as AI context',
  proposed_global_column_name STRING   COMMENT 'AI-proposed global English column name',
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
COMMENT 'AI-proposed column mapping candidates for Spain source columns. Human reviewers approve, correct, or reject each row before mappings are promoted to the dictionary.'
""")

# ------------------------------------------------------------------
# column_mapping_dictionary_es
# ------------------------------------------------------------------
execute_ddl("column_mapping_dictionary_es", f"""
CREATE TABLE IF NOT EXISTS {DB}.`column_mapping_dictionary_es` (
  source_system      STRING    COMMENT 'Source system identifier',
  source_table       STRING    COMMENT 'Source table name',
  local_column_name  STRING    COMMENT 'Local Spanish column name',
  global_column_name STRING    COMMENT 'Approved global English column name',
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
COMMENT 'Approved column mapping dictionary. Maps each local Spanish column name to its global English equivalent. Drives the core transformation notebook.'
""")

# ------------------------------------------------------------------
# column_mapping_audit_es
# ------------------------------------------------------------------
execute_ddl("column_mapping_audit_es", f"""
CREATE TABLE IF NOT EXISTS {DB}.`column_mapping_audit_es` (
  audit_id              BIGINT    COMMENT 'Audit event surrogate identifier',
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
COMMENT 'Immutable audit trail of every review action performed on column_mapping_candidates_es.'
""")

# COMMAND ----------

# MAGIC %md ## STEP 4b — Schema Migration (Idempotent Column Additions)

# COMMAND ----------

print("STEP 4b — Applying schema migrations (add new columns to existing tables if missing) ...")

def _add_column_if_missing(table_ref, col_name, col_type, col_comment):
    """Add a column to an existing Delta table only if it does not already exist."""
    try:
        existing = [r["col_name"] for r in spark.sql(f"DESCRIBE TABLE {table_ref}").collect()]
        if col_name not in existing:
            spark.sql(f"ALTER TABLE {table_ref} ADD COLUMN `{col_name}` {col_type} COMMENT '{col_comment}'")
            print(f"  [ADDED]   {table_ref}.{col_name}")
        else:
            print(f"  [EXISTS]  {table_ref}.{col_name}")
    except Exception as e:
        print(f"  [ERR]     {table_ref}.{col_name}: {e}")
        raise

_add_column_if_missing(
    f"{DB}.`column_mapping_candidates_es`",
    "app_decision_source", "STRING",
    "Source of review decision: DATABRICKS_APP | SQL_DIRECT | NULL for pending"
)

_add_column_if_missing(
    f"{DB}.`column_mapping_audit_es`",
    "action_source", "STRING",
    "Source of the action: DATABRICKS_APP | SQL_DIRECT"
)

print("STEP 4b — Schema migration complete.")

# COMMAND ----------

# MAGIC %md ## STEP 5 — Value-Mapping Control Tables

# COMMAND ----------

print("STEP 5 — Creating value-mapping control tables (secondary / optional) ...")

# ------------------------------------------------------------------
# value_mapping_candidates_es
# ------------------------------------------------------------------
execute_ddl("value_mapping_candidates_es", f"""
CREATE TABLE IF NOT EXISTS {DB}.`value_mapping_candidates_es` (
  candidate_id              BIGINT    COMMENT 'Surrogate row identifier',
  source_field              STRING    COMMENT 'Global field name that holds this value',
  raw_value                 STRING    COMMENT 'Raw Spanish categorical value from source',
  proposed_harmonized_value STRING    COMMENT 'AI-proposed English equivalent',
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
COMMENT 'Optional value translation candidates. Spanish categorical values proposed for English equivalents. Secondary to column mapping.'
""")

# ------------------------------------------------------------------
# value_mapping_dictionary_es
# ------------------------------------------------------------------
execute_ddl("value_mapping_dictionary_es", f"""
CREATE TABLE IF NOT EXISTS {DB}.`value_mapping_dictionary_es` (
  source_field       STRING    COMMENT 'Global field name',
  raw_value          STRING    COMMENT 'Original Spanish categorical value',
  harmonized_value   STRING    COMMENT 'Approved English equivalent value',
  approval_status    STRING    COMMENT 'APPROVED | CORRECTED',
  approved_by        STRING    COMMENT 'Identity who approved this translation',
  approved_at        TIMESTAMP COMMENT 'When the translation was approved',
  mapping_version    STRING    COMMENT 'Version label',
  active_flag        BOOLEAN   COMMENT 'True if this translation is currently active',
  created_at         TIMESTAMP COMMENT 'When this entry was created',
  updated_at         TIMESTAMP COMMENT 'When this entry was last updated'
)
USING DELTA
COMMENT 'Approved value translation dictionary. Maps Spanish categorical values to English equivalents. Optional secondary step.'
""")

# COMMAND ----------

# MAGIC %md ## STEP 6 — Ops Tables

# COMMAND ----------

print("STEP 6 — Creating ops tables ...")

# ------------------------------------------------------------------
# workflow_run_metrics
# ------------------------------------------------------------------
execute_ddl("workflow_run_metrics", f"""
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
""")

# ------------------------------------------------------------------
# ai_mapping_usage_metrics
# ------------------------------------------------------------------
execute_ddl("ai_mapping_usage_metrics", f"""
CREATE TABLE IF NOT EXISTS {DB}.`ai_mapping_usage_metrics` (
  run_id                   STRING    COMMENT 'UUID run identifier',
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
""")

# ------------------------------------------------------------------
# data_quality_results
# ------------------------------------------------------------------
execute_ddl("data_quality_results", f"""
CREATE TABLE IF NOT EXISTS {DB}.`data_quality_results` (
  run_id       STRING    COMMENT 'UUID run identifier',
  check_name   STRING    COMMENT 'Name of the data quality check',
  check_status STRING    COMMENT 'PASSED | FAILED | WARNING',
  metric_value DOUBLE    COMMENT 'Numeric metric value (count, ratio, etc.)',
  recorded_at  TIMESTAMP COMMENT 'When this result was recorded',
  details      STRING    COMMENT 'Free-text details or diagnostic message'
)
USING DELTA
COMMENT 'Data quality check results per pipeline run.'
""")

# COMMAND ----------

# MAGIC %md ## STEP 7 — Views

# COMMAND ----------

print("STEP 7 — Creating views ...")

execute_ddl("vw_pending_column_mappings", f"""
CREATE OR REPLACE VIEW {DB}.`vw_pending_column_mappings` AS
SELECT
  candidate_id,
  source_system,
  local_column_name,
  local_data_type,
  local_sample_values,
  proposed_global_column_name,
  proposed_global_data_type,
  proposed_match_type,
  mapping_rationale,
  confidence,
  ai_error_status,
  review_status,
  mandatory_flag,
  created_at
FROM {DB}.`column_mapping_candidates_es`
WHERE review_status = 'PENDING'
ORDER BY mandatory_flag DESC, confidence DESC, local_column_name
""")

execute_ddl("vw_mapping_review_summary", f"""
CREATE OR REPLACE VIEW {DB}.`vw_mapping_review_summary` AS
SELECT
  review_status,
  mandatory_flag,
  confidence,
  COUNT(*) AS count,
  SUM(CASE WHEN ai_error_status IS NOT NULL THEN 1 ELSE 0 END) AS ai_error_count
FROM {DB}.`column_mapping_candidates_es`
GROUP BY review_status, mandatory_flag, confidence
""")

execute_ddl("vw_publish_readiness", f"""
CREATE OR REPLACE VIEW {DB}.`vw_publish_readiness` AS
WITH mandatory AS (
  SELECT local_column_name AS mandatory_column_name,
         review_status,
         final_global_column_name,
         final_match_type,
         reviewed_by,
         reviewed_at,
         app_decision_source,
         CASE WHEN review_status IN ('APPROVED','CORRECTED') THEN TRUE ELSE FALSE END AS is_ready
  FROM {DB}.`column_mapping_candidates_es`
  WHERE mandatory_flag = TRUE
)
SELECT * FROM mandatory
ORDER BY is_ready ASC, mandatory_column_name
""")

execute_ddl("vw_column_mapping_low_conf_es", f"""
CREATE OR REPLACE VIEW {DB}.`vw_column_mapping_low_conf_es` AS
SELECT
  candidate_id,
  source_system,
  local_column_name,
  local_data_type,
  proposed_global_column_name,
  proposed_match_type,
  confidence,
  ai_error_status,
  review_status,
  mandatory_flag,
  mapping_rationale
FROM {DB}.`column_mapping_candidates_es`
WHERE UPPER(confidence) = 'LOW' OR ai_error_status IS NOT NULL
ORDER BY mandatory_flag DESC, local_column_name
""")

execute_ddl("vw_column_mapping_coverage_es", f"""
CREATE OR REPLACE VIEW {DB}.`vw_column_mapping_coverage_es` AS
SELECT
  g.global_column_name,
  g.global_data_type,
  g.semantic_group,
  g.required_flag,
  g.business_definition,
  c.local_column_name,
  c.proposed_match_type,
  c.confidence,
  c.review_status,
  CASE
    WHEN c.review_status IN ('APPROVED','CORRECTED') THEN 'MAPPED'
    WHEN c.review_status = 'REJECTED'               THEN 'REJECTED'
    WHEN c.review_status = 'PENDING'                THEN 'PENDING'
    WHEN c.local_column_name IS NULL                THEN 'UNMAPPED'
    ELSE 'UNKNOWN'
  END AS coverage_status
FROM {DB}.`global_target_columns` g
LEFT JOIN {DB}.`column_mapping_candidates_es` c
  ON g.global_column_name = COALESCE(c.final_global_column_name, c.proposed_global_column_name)
ORDER BY g.semantic_group, g.global_column_name
""")

# COMMAND ----------

# MAGIC %md ## STEP 8 — Prepopulate global_target_columns

# COMMAND ----------

print("STEP 8 — Prepopulating global_target_columns (idempotent merge) ...")

import datetime as _dt
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, TimestampType

_now = _dt.datetime.utcnow()
_target_table = "property_insurance_monthly"

global_cols = [
    ("record_id",                 "BIGINT",  "Unique identifier for each record",                               ["1","2","3"],                                                    True,  "identity"),
    ("reporting_year",            "INT",     "Calendar year of the reporting period",                           ["2022","2023","2024","2025"],                                    True,  "time"),
    ("reporting_month",           "INT",     "Month number 1-12 of the reporting period",                       ["1","6","12"],                                                   True,  "time"),
    ("policy_number",             "STRING",  "Unique policy identifier",                                        ["ES-1001-2022","ES-5000-2024"],                                  True,  "policy"),
    ("risk_type",                 "STRING",  "Category of insured risk",                                        ["Fire","Flood","Theft","Water Damage"],                          True,  "risk"),
    ("region",                    "STRING",  "Geographic region or province",                                   ["Madrid","Barcelona","Valencia"],                                True,  "geography"),
    ("distribution_channel",      "STRING",  "Channel through which policy was sold",                           ["Agent","Broker","Direct","Bancassurance"],                      True,  "distribution"),
    ("net_written_premium_eur",   "DOUBLE",  "Net written premium in EUR after reinsurance",                    ["500.0","1200.5","3000.0"],                                      True,  "premium"),
    ("gross_written_premium_eur", "DOUBLE",  "Gross written premium in EUR before reinsurance",                 ["600.0","1500.0","4000.0"],                                      True,  "premium"),
    ("new_policies_count",        "INT",     "Number of new policies in period",                                ["1","5","12"],                                                   True,  "policy_count"),
    ("renewed_policies_count",    "INT",     "Number of policies renewed in period",                            ["10","25","50"],                                                 True,  "policy_count"),
    ("cancelled_policies_count",  "INT",     "Number of policies cancelled in period",                          ["0","1","3"],                                                    True,  "policy_count"),
    ("claims_reported_count",     "INT",     "Total claims notified in period",                                 ["0","1","5"],                                                    True,  "claims"),
    ("claims_paid_count",         "INT",     "Number of claims settled and paid",                               ["0","1","4"],                                                    True,  "claims"),
    ("gross_claims_incurred_eur", "DOUBLE",  "Total gross claims incurred in EUR",                              ["200.0","800.0","2500.0"],                                       True,  "claims"),
    ("claims_reserve_eur",        "DOUBLE",  "Claims reserve amount at reporting date in EUR",                  ["50.0","200.0","600.0"],                                         False, "claims"),
    ("management_expenses_eur",   "DOUBLE",  "Internal management expenses in EUR",                             ["30.0","100.0","350.0"],                                         True,  "expenses"),
    ("commissions_eur",           "DOUBLE",  "Commissions paid to distribution partners in EUR",                ["50.0","150.0","400.0"],                                         True,  "expenses"),
    ("loss_ratio",                "DOUBLE",  "Ratio of claims incurred to gross written premium",               ["0.35","0.65","1.10"],                                           False, "kpi"),
    ("customer_segment",          "STRING",  "Segment classification of policyholder",                          ["Individual","Small Business","Large Enterprise"],               True,  "customer"),
    ("risk_zone",                 "STRING",  "Risk zone classification",                                        ["Zone A","Zone B","Zone C"],                                     True,  "risk"),
    ("primary_coverage",          "STRING",  "Primary coverage type in the policy",                             ["Building","Contents","Both"],                                   True,  "coverage"),
    ("currency",                  "STRING",  "ISO currency code for monetary amounts",                          ["EUR"],                                                          True,  "monetary"),
]

gtc_schema = StructType([
    StructField("target_table",        StringType(),              False),
    StructField("global_column_name",  StringType(),              False),
    StructField("global_data_type",    StringType(),              True),
    StructField("business_definition", StringType(),              True),
    StructField("example_values",      ArrayType(StringType()),   True),
    StructField("required_flag",       BooleanType(),             True),
    StructField("semantic_group",      StringType(),              True),
    StructField("created_at",          TimestampType(),           True),
])

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
print(f"  global_target_columns rows: {gtc_count}")
display(spark.table(f"{DB}.`global_target_columns`").orderBy("semantic_group", "global_column_name"))

# COMMAND ----------

# MAGIC %md ## STEP 9 — Summary

# COMMAND ----------

print("STEP 9 — Summary of all created objects:")
print()
print(f"  Schema : {catalog_name}.{schema_name}")
print()
print("  DATA TABLES:")
print("    property_insurance_monthly_raw   — 24 Spanish source columns")
print("    property_insurance_monthly       — 28 cols (23 business + 5 metadata)")
print()
print("  COLUMN-MAPPING CONTROL TABLES:")
print("    source_column_inventory_es")
print("    global_target_columns            — prepopulated with 23 target columns")
print("    column_mapping_candidates_es")
print("    column_mapping_dictionary_es")
print("    column_mapping_audit_es")
print()
print("  VALUE-MAPPING CONTROL TABLES (optional):")
print("    value_mapping_candidates_es")
print("    value_mapping_dictionary_es")
print()
print("  OPS TABLES:")
print("    workflow_run_metrics")
print("    ai_mapping_usage_metrics")
print("    data_quality_results")
print()
print("  VIEWS:")
print("    vw_pending_column_mappings       — pending review candidates (for Databricks App)")
print("    vw_mapping_review_summary        — aggregated counts by status/mandatory/confidence")
print("    vw_publish_readiness             — mandatory columns with readiness flag")
print("    vw_column_mapping_low_conf_es    — low confidence / AI error candidates")
print("    vw_column_mapping_coverage_es    — global model coverage status")
print()

ok_count  = sum(1 for r in results if r[0] == "OK")
err_count = sum(1 for r in results if r[0] == "ERROR")
print(f"  DDL results: {ok_count} OK, {err_count} ERROR")

if err_count > 0:
    raise Exception(f"DDL creation had {err_count} errors. Review output above.")

# COMMAND ----------

# MAGIC %md ## STEP 10 — Log to workflow_run_metrics

# COMMAND ----------

import datetime as _dt2
from uuid import uuid4
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

RUN_ID = str(uuid4())
_now2 = _dt2.datetime.utcnow()

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
    "create_global_model_and_control_tables",
    "SUCCEEDED",
    _now2,
    _now2,
    gtc_count,
    f"Created all tables and views. global_target_columns prepopulated with {gtc_count} rows.",
)], schema=log_schema)

log_df.write.format("delta").mode("append").saveAsTable(f"{DB}.`workflow_run_metrics`")

print(f"STEP 10 — Logged run record. RUN_ID={RUN_ID}")
print()
print("=" * 60)
print("  02_create_global_model_and_control_tables COMPLETE")
print("=" * 60)
