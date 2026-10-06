# Databricks notebook source
# MAGIC %md
# MAGIC # 11 — Evaluate AI Mapping Proposals
# MAGIC
# MAGIC Compares AI proposals with the country answer key before human review,
# MAGIC logs the metrics to MLflow, and persists metric rows to Unity Catalog.

# COMMAND ----------

# MAGIC %run ./_shared_utils

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

import json

import mlflow

from harmonization.config import get_source_context
from harmonization.evaluation import evaluate, load_answer_key, to_metric_rows

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "your_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "halvard_harmonization", "Schema Name")
dbutils.widgets.text("source_country", "ES", "Source Country")
dbutils.widgets.text("mapping_version", "v1", "Mapping Version")
dbutils.widgets.text("ai_endpoint", "", "AI Endpoint")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
source_country = dbutils.widgets.get("source_country").strip()
mapping_version = dbutils.widgets.get("mapping_version").strip()
ai_endpoint = dbutils.widgets.get("ai_endpoint").strip()

config = load_harmonization_config()
source_context = get_source_context(config, source_country)
source_system = source_context["source_system"]
cc = source_country.lower()
DB = f"`{catalog_name}`.`{schema_name}`"
CANDIDATES_TABLE = f"{DB}.`column_mapping_candidates`"
EVAL_RESULTS_TABLE = f"{DB}.`mapping_eval_results`"

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_notebook_dir = "/".join(_notebook_path.rsplit("/", 1)[:-1])
answer_key_path = f"/Workspace{_notebook_dir}/../examples/answer_keys/{cc}.json"
workspace_user = _notebook_path.split("/")[2]
experiment_path = f"/Users/{workspace_user}/halvard-column-harmonization"

print(f"Evaluating {source_system} using {answer_key_path}")

# COMMAND ----------

# MAGIC %md ## Load Proposals and Answer Key

# COMMAND ----------

answer_source_system, answer_key = load_answer_key(answer_key_path)
if answer_source_system != source_system:
    raise ValueError(f"Answer key source_system {answer_source_system!r} does not match {source_system!r}")

candidate_rows = spark.sql(
    f"""
    SELECT local_column_name, proposed_global_column_name, proposed_match_type,
           confidence, ai_error_status
    FROM {CANDIDATES_TABLE}
    WHERE source_system = '{source_system}'
    ORDER BY local_column_name
    """
).collect()
proposals = [row.asDict() for row in candidate_rows]
result = evaluate(proposals, answer_key)

print(f"Loaded {len(proposals)} proposals for {result.n_columns} answer-key columns.")

# COMMAND ----------

# MAGIC %md ## MLflow and Delta Metrics

# COMMAND ----------

mlflow.set_experiment(experiment_path)

with mlflow.start_run(run_name=f"{source_system}-{mapping_version}") as run:
    mlflow_run_id = run.info.run_id
    mlflow.log_params(
        {
            "source_system": source_system,
            "ai_endpoint": ai_endpoint or config["ai"]["endpoint"],
            "mapping_version": mapping_version,
            "n_columns": result.n_columns,
        }
    )
    confidence_metrics = {
        f"accuracy_{band_name.lower().replace('-', '_').replace(' ', '_')}": band["accuracy"]
        for band_name, band in result.by_confidence.items()
    }
    mlflow.log_metrics(
        {
            "accuracy": result.accuracy,
            "auto_accept_rate": result.auto_accept_rate,
            "review_load": result.review_load,
            **confidence_metrics,
        }
    )
    mlflow.log_dict(result.errors, "errors.json")

metric_rows = to_metric_rows(result, mlflow_run_id, source_system)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {EVAL_RESULTS_TABLE} (
        run_id STRING NOT NULL,
        source_system STRING NOT NULL,
        metric_name STRING NOT NULL,
        metric_value DOUBLE,
        slice STRING
    )
    """
)
spark.createDataFrame(metric_rows).write.format("delta").mode("append").saveAsTable(EVAL_RESULTS_TABLE)
print(f"MLflow run: {mlflow_run_id}; persisted {len(metric_rows)} metric rows.")

# COMMAND ----------

# MAGIC %md ## Column-Level Audit

# COMMAND ----------

proposals_by_column = {proposal["local_column_name"]: proposal for proposal in proposals}
error_by_column = {error["local_column"]: error for error in result.errors}
for local_column, expected in answer_key.items():
    error = error_by_column.get(local_column)
    if error:
        proposed = error["proposed"] or "NO_MATCH"
        confidence = error["confidence"] or "UNKNOWN"
    else:
        proposal = proposals_by_column.get(local_column, {})
        proposed = proposal.get("proposed_global_column_name") or "NO_MATCH"
        confidence = proposal.get("confidence") or "UNKNOWN"
    print(
        f"{local_column:<32} proposed={proposed!s:<28} expected={expected!s:<28} "
        f"confidence={confidence!s:<8} correct={error is None}"
    )

print(
    f"Accuracy: {result.accuracy:.1%}; auto-accept: {result.auto_accept_rate:.1%}; "
    f"review load: {result.review_load:.1%}"
)

# COMMAND ----------

summary = {
    "source_system": source_system,
    "mapping_version": mapping_version,
    "n_columns": result.n_columns,
    "n_correct": result.n_correct,
    "accuracy": result.accuracy,
    "auto_accept_rate": result.auto_accept_rate,
    "review_load": result.review_load,
    "by_confidence": result.by_confidence,
    "mlflow_run_id": mlflow_run_id,
}
dbutils.notebook.exit(json.dumps(summary))
