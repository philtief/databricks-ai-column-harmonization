# Databricks notebook source
# MAGIC %md
# MAGIC # Shared Utilities
# MAGIC
# MAGIC Called via `%run ./_shared_utils` from every workflow notebook.
# MAGIC Provides the shared `log_schema` and `log_run_metric()` helper.

# COMMAND ----------

import yaml
import datetime as _dt
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# Schema for workflow_run_metrics (used by every notebook)
LOG_SCHEMA = StructType([
    StructField("run_id",        StringType(),    False),
    StructField("workflow_name", StringType(),    True),
    StructField("task_name",     StringType(),    True),
    StructField("task_status",   StringType(),    True),
    StructField("started_at",    TimestampType(), True),
    StructField("finished_at",   TimestampType(), True),
    StructField("row_count",     LongType(),      True),
    StructField("message",       StringType(),    True),
])

WORKFLOW_NAME = "Column_Mapping_To_Global_Model"


def log_run_metric(spark_session, ops_table, run_id, task_name, status, started_at, row_count, message):
    """Append a single run metric row to the ops table."""
    finished = _dt.datetime.utcnow()
    row = [(run_id, WORKFLOW_NAME, task_name, status, started_at, finished, row_count, message)]
    df = spark_session.createDataFrame(row, schema=LOG_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(ops_table)
    print(f"Logged: {task_name} -> {status} (run_id={run_id[:8]}...)")


# COMMAND ----------

# MAGIC %md ## Config Loader

# COMMAND ----------

def load_harmonization_config():
    """Load config/harmonization_config.yaml from the bundle workspace path."""
    try:
        # When deployed via DAB, notebooks are at {root_path}/files/notebooks/
        # Config is at {root_path}/files/config/
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        parent_dir = "/".join(notebook_path.rsplit("/", 1)[:-1])
        config_ws_path = f"{parent_dir}/../config/harmonization_config.yaml"

        # Read from workspace filesystem
        # On Databricks, workspace files are accessible via /Workspace prefix
        local_path = f"/Workspace{config_ws_path}"
        with open(local_path) as f:
            config = yaml.safe_load(f)
        print(f"Config loaded from {config_ws_path}")
        return config
    except Exception as e:
        print(f"[WARN] Could not load config ({e}). Using inline defaults.")
        return None

# COMMAND ----------

# MAGIC %md ## Table Reference Builder

# COMMAND ----------

def get_table_refs(config, db_prefix):
    """Build fully-qualified table references from config.

    Returns a dict with keys for every table/view used by the workflow.
    Table names are generic (no country suffix). Country isolation is
    handled at the schema level via the db_prefix.
    """
    src_table = config["source_context"]["source_table"] if config else "property_insurance_monthly_raw"
    tgt_table = config["target_model"]["table_name"] if config else "property_insurance_monthly"
    src_system = config["source_context"]["source_system"] if config else "ES_PROPERTY_RAW"

    return {
        "raw_table": f"{db_prefix}.`{src_table}`",
        "harm_table": f"{db_prefix}.`{tgt_table}`",
        "source_system": src_system,
        "source_table_name": src_table,
        "target_table_name": tgt_table,
        "inv_table": f"{db_prefix}.`source_column_inventory`",
        "cand_table": f"{db_prefix}.`column_mapping_candidates`",
        "dict_table": f"{db_prefix}.`column_mapping_dictionary`",
        "audit_table": f"{db_prefix}.`column_mapping_audit`",
        "vcand_table": f"{db_prefix}.`value_mapping_candidates`",
        "vdict_table": f"{db_prefix}.`value_mapping_dictionary`",
        "gtc_table": f"{db_prefix}.`global_target_columns`",
        "ops_table": f"{db_prefix}.`workflow_run_metrics`",
        "usage_table": f"{db_prefix}.`ai_mapping_usage_metrics`",
        "dq_table": f"{db_prefix}.`data_quality_results`",
    }

# COMMAND ----------

print("_shared_utils loaded.")
