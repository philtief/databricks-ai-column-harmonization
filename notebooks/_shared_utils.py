# Databricks notebook source
# MAGIC %md
# MAGIC # Shared Utilities
# MAGIC
# MAGIC Called via `%run ./_shared_utils` from every workflow notebook.
# MAGIC Provides the shared `log_schema` and `log_run_metric()` helper.

# COMMAND ----------

import datetime as _dt

import yaml
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

# Schema for workflow_run_metrics (used by every notebook)
LOG_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("workflow_name", StringType(), True),
        StructField("task_name", StringType(), True),
        StructField("task_status", StringType(), True),
        StructField("started_at", TimestampType(), True),
        StructField("finished_at", TimestampType(), True),
        StructField("row_count", LongType(), True),
        StructField("message", StringType(), True),
    ]
)

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
        raise RuntimeError(
            f"FATAL: Could not load harmonization config ({e}). "
            "Ensure config/harmonization_config.yaml is deployed with the bundle. "
            "See config/harmonization_config.yaml.template for the expected format."
        ) from e


# COMMAND ----------

# MAGIC %md ## Table Reference Builder

# COMMAND ----------

# Delegated to harmonization.tables so the logic is unit-testable outside
# the notebook runtime. Importing into this %run-injected module exposes
# get_table_refs to every workflow notebook unchanged.
from harmonization.tables import get_table_refs

# COMMAND ----------

print("_shared_utils loaded.")
