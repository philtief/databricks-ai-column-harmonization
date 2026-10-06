# column_mapping_review_gate (job halvard_publish_harmonized, run 528730816408561, task run 624220932735854, SUCCESS)

```python
%pip install "psycopg[binary]>=3.2" "databricks-sdk>=0.81" -q
```

Output:
```text
[43mNote: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.[0m
```

```python
dbutils.library.restartPython()
```

```python
%run ./_shared_utils
```

```python
import json

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "your_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "harmonizing_agent", "Schema Name")
dbutils.widgets.text("source_country", "ES", "Source Country")
dbutils.widgets.text("lakebase_endpoint", "", "Lakebase Endpoint")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
source_country = dbutils.widgets.get("source_country").strip()
lakebase_endpoint = dbutils.widgets.get("lakebase_endpoint").strip()

if not lakebase_endpoint:
    raise ValueError("lakebase_endpoint must not be empty")

DB = f"`{catalog_name}`.`{schema_name}`"
from harmonization.config import get_mandatory_columns

_cfg = load_harmonization_config()
_refs = get_table_refs(_cfg, DB, source_country)
CAND_TABLE = _refs["cand_table"]
AUDIT_TABLE = _refs["audit_table"]
OPS_TABLE = _refs["ops_table"]
SOURCE_SYSTEM = _refs["source_system"]
MANDATORY_COLUMNS = get_mandatory_columns(_cfg, source_country)

print(f"Config: {DB}, country={source_country}, endpoint={lakebase_endpoint}")
```

Output:
```text
Config: `agent_marketplace_catalog`.`halvard_harmonization`, country=IT, endpoint=projects/halvard-harmonization/branches/production/endpoints/primary
```

```python
from datetime import datetime
from uuid import uuid4

from databricks.sdk import WorkspaceClient
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from harmonization.review_store import connect, fetch_queue

RUN_ID = str(uuid4())
_start = _dt.datetime.utcnow()
```

Output:
```text
/home/spark-6056dc18-fad9-4705-9e08-ed/.ipykernel/67/command-8770032077284642-4250730338:18: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
  _start = _dt.datetime.utcnow()
```

```python
workspace_client = WorkspaceClient()
conn = connect(workspace_client, lakebase_endpoint)
try:
    # All rows, not only decisions: a RESET in the app sets PENDING, and Delta must see that too.
    decisions = fetch_queue(conn, SOURCE_SYSTEM)
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT audit_id, source_system, local_column_name, old_status,
                   new_status, old_global_column_name, new_global_column_name,
                   action_by, action_at, action_comment, action_source
            FROM harmonization_review.review_audit
            WHERE source_system = %s
            ORDER BY audit_id
            """,
            (SOURCE_SYSTEM,),
        )
        audit_rows = cursor.fetchall()
finally:
    conn.close()

print(f"Decisions synced: {len(decisions)}; audit rows: {len(audit_rows)}")
```

Output:
```text
Decisions synced: 24; audit rows: 24
```

```python
now = _dt.datetime.utcnow()
decision_schema = StructType(
    [
        StructField("source_system", StringType(), False),
        StructField("local_column_name", StringType(), False),
        StructField("review_status", StringType(), True),
        StructField("final_global_column_name", StringType(), True),
        StructField("final_match_type", StringType(), True),
        StructField("reviewed_by", StringType(), True),
        StructField("reviewed_at", TimestampType(), True),
        StructField("review_comment", StringType(), True),
        StructField("app_decision_source", StringType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)

decision_rows = [
    (
        row["source_system"],
        row["local_column_name"],
        row["review_status"],
        row.get("final_global_column_name"),
        row.get("final_match_type"),
        row.get("reviewed_by"),
        row.get("reviewed_at"),
        row.get("review_comment"),
        "DATABRICKS_APP",
        now,
    )
    for row in decisions
]

if decision_rows:
    decision_df = spark.createDataFrame(decision_rows, schema=decision_schema)
    decision_df.createOrReplaceTempView("_review_decisions")
    spark.sql(f"""
MERGE INTO {CAND_TABLE} AS tgt
USING _review_decisions AS src
ON tgt.source_system = src.source_system
   AND tgt.local_column_name = src.local_column_name
WHEN MATCHED THEN UPDATE SET
  tgt.review_status = src.review_status,
  tgt.final_global_column_name = src.final_global_column_name,
  tgt.final_match_type = src.final_match_type,
  tgt.reviewed_by = src.reviewed_by,
  tgt.reviewed_at = src.reviewed_at,
  tgt.review_comment = src.review_comment,
  tgt.app_decision_source = src.app_decision_source,
  tgt.updated_at = src.updated_at
""")
```

Output:
```text
/home/spark-6056dc18-fad9-4705-9e08-ed/.ipykernel/67/command-8770032077284646-1996327584:1: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
  now = _dt.datetime.utcnow()
```

```python
audit_schema = StructType(
    [
        StructField("audit_id", LongType(), False),
        StructField("source_system", StringType(), False),
        StructField("local_column_name", StringType(), True),
        StructField("old_review_status", StringType(), True),
        StructField("new_review_status", StringType(), True),
        StructField("old_global_column_name", StringType(), True),
        StructField("new_global_column_name", StringType(), True),
        StructField("action_by", StringType(), True),
        StructField("action_at", TimestampType(), True),
        StructField("action_comment", StringType(), True),
        StructField("action_source", StringType(), True),
    ]
)

audit_delta_rows = [
    (
        row["audit_id"],
        row["source_system"],
        row["local_column_name"],
        row.get("old_status"),
        row.get("new_status"),
        row.get("old_global_column_name"),
        row.get("new_global_column_name"),
        row.get("action_by"),
        row.get("action_at"),
        row.get("action_comment"),
        row.get("action_source") or "DATABRICKS_APP",
    )
    for row in audit_rows
]

if audit_delta_rows:
    audit_df = spark.createDataFrame(audit_delta_rows, schema=audit_schema)
    audit_df.createOrReplaceTempView("_review_audit")
    spark.sql(f"""
MERGE INTO {AUDIT_TABLE} AS tgt
USING _review_audit AS src
ON tgt.source_system = src.source_system
   AND tgt.audit_id = src.audit_id
WHEN NOT MATCHED THEN INSERT (
  audit_id, source_system, local_column_name, old_review_status,
  new_review_status, old_global_column_name, new_global_column_name,
  old_match_type, new_match_type, action_by, action_at,
  action_comment, action_source
) VALUES (
  src.audit_id, src.source_system, src.local_column_name, src.old_review_status,
  src.new_review_status, src.old_global_column_name, src.new_global_column_name,
  NULL, NULL, src.action_by, src.action_at,
  src.action_comment, src.action_source
)
""")
```

```python
pending_blocking = spark.sql(f"""
SELECT local_column_name, review_status, proposed_global_column_name,
       confidence, ai_error_status, mandatory_flag
FROM {CAND_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
  AND mandatory_flag = TRUE
  AND review_status = 'PENDING'
ORDER BY local_column_name
""").collect()

no_target_blocking = spark.sql(f"""
SELECT local_column_name, review_status, proposed_global_column_name,
       final_global_column_name, confidence, ai_error_status, mandatory_flag
FROM {CAND_TABLE}
WHERE source_system = '{SOURCE_SYSTEM}'
  AND mandatory_flag = TRUE
  AND review_status <> 'PENDING'
  AND (final_global_column_name IS NULL OR final_global_column_name = '' OR upper(final_global_column_name) = 'NO_MATCH')
ORDER BY local_column_name
""").collect()

found_mandatory_cols = {
    row["local_column_name"]
    for row in spark.sql(f"""
        SELECT DISTINCT local_column_name
        FROM {CAND_TABLE}
        WHERE source_system = '{SOURCE_SYSTEM}' AND mandatory_flag = TRUE
    """).collect()
}
missing_mandatory = [column for column in MANDATORY_COLUMNS if column not in found_mandatory_cols]
blocking_count = len(pending_blocking) + len(no_target_blocking) + len(missing_mandatory)
gate_status = "PASSED" if blocking_count == 0 else "BLOCKED"

print(
    f"Gate decision: {gate_status}. Mandatory PENDING: {len(pending_blocking)}, "
    f"Reviewed but no target: {len(no_target_blocking)}, Missing: {len(missing_mandatory)}"
)
```

Output:
```text
Gate decision: PASSED. Mandatory PENDING: 0, Reviewed but no target: 0, Missing: 0
```

```python
log_run_metric(
    spark,
    OPS_TABLE,
    RUN_ID,
    "column_mapping_review_gate",
    gate_status,
    _start,
    len(decisions),
    f"Synced {len(decisions)} decisions and {len(audit_delta_rows)} audit rows. Gate {gate_status}.",
)

if gate_status == "BLOCKED":
    raise Exception(
        f"COLUMN MAPPING GATE BLOCKED: {blocking_count} mandatory column(s) are unresolved. "
        f"Pending: {[row['local_column_name'] for row in pending_blocking]}. "
        f"Reviewed-no-target: {[row['local_column_name'] for row in no_target_blocking]}. "
        f"Missing: {missing_mandatory}."
    )

print("Gate PASSED. Proceeding to 07_build_column_mapping_dictionary.")

summary = {
    "source_country": source_country,
    "source_system": SOURCE_SYSTEM,
    "decisions_synced": len(decisions),
    "audit_rows_synced": len(audit_delta_rows),
    "gate_status": gate_status,
    "blocking_issues": blocking_count,
    "synced_at": datetime.utcnow().isoformat(),
}
dbutils.notebook.exit(json.dumps(summary))
```

Output:
```text
{"source_country": "IT", "source_system": "IT_PROPERTY_RAW", "decisions_synced": 24, "audit_rows_synced": 24, "gate_status": "PASSED", "blocking_issues": 0, "synced_at": "2026-10-06T15:35:22.095004"}
```
