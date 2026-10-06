# Databricks notebook source
# MAGIC %md
# MAGIC # Shared Utilities
# MAGIC
# MAGIC Called via `%run ./_shared_utils` from every workflow notebook.
# MAGIC Provides the shared `log_schema` and `log_run_metric()` helper.

# COMMAND ----------

import datetime as _dt

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
    """Load and validate config/harmonization_config.yaml from the deployed bundle (next to notebooks/)."""
    from harmonization.config import load_config  # src/ is on sys.path after the cell below runs

    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    return load_config(f"/Workspace{notebook_path.rsplit('/', 1)[0]}/../config/harmonization_config.yaml")


# COMMAND ----------

# MAGIC %md ## Table Reference Builder

# COMMAND ----------

# When deployed via DAB the bundle uploads src/harmonization/ under files/src.
# Walk up from the current notebook until we find a sibling `src/harmonization`
# directory. Works whether the notebook lives in notebooks/ (one level) or
# examples/<demo>/ (two levels).
import os as _os
import sys as _sys

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_current = "/Workspace" + _notebook_path
for _ in range(6):
    _current = _os.path.dirname(_current)
    _candidate = _os.path.join(_current, "src")
    if _os.path.isdir(_os.path.join(_candidate, "harmonization")):
        if _candidate not in _sys.path:
            _sys.path.insert(0, _candidate)
        break
else:
    raise RuntimeError(f"Could not locate src/harmonization above {_notebook_path}")

# Delegated to harmonization.tables so the logic is unit-testable outside
# the notebook runtime. Importing into this %run-injected module exposes
# get_table_refs to every workflow notebook unchanged.
from harmonization.tables import get_table_refs

# COMMAND ----------

print("_shared_utils loaded.")
