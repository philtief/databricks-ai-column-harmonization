# evaluate_ai_proposals (job halvard_propose_mappings, run 1121820803949496, task run 1066738395147766, SUCCESS)

```python
%run ./_shared_utils
```

```python
import json

import mlflow
from pyspark.sql import functions as F

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
# Not the bundle root name: that folder already exists and MLflow cannot create an experiment over it.
experiment_path = f"/Users/{workspace_user}/halvard-mapping-evaluation"

print(f"Evaluating {source_system} using {answer_key_path}")
```

Output:
```text
Evaluating ES_PROPERTY_RAW using /Workspace/Users/philipp.tiefenbacher@databricks.com/halvard-column-harmonization/files/notebooks/../examples/answer_keys/es.json
```

```python
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
```

Output:
```text
{"ts": "2026-10-06 14:00:39.507", "level": "WARNING", "logger": "pyspark.sql.connect.logging", "msg": "Effective usage policy for this session is cct.Cipqb2JzLzU2OTI5MDQwMDE1NjIzMS9ydW5zLzExMjE4MjA4MDM5NDk0OTYQASABKiRiZmFiM2E2Ny1iODQxLTRmNTgtYmEwYy00ODYwYWY0YTdlNDMyJDAxYTExMTgyLTQyYmItNzIyYS1iYmU1LTEyNjVlNzFiMDI2NjokNDNiNWQ0NTUtODU4Mi0zZmM0LWEyOWYtNThhMjZjNWNiMWFiQiRiZmFiM2E2Ny1iODQxLTRmNTgtYmEwYy00ODYwYWY0YTdlNDNKDAiH/ZPWBhDAvbWdA1ACWAFgAWiPhM2BhcWjDYgBAA==.", "context": {}}
{"ts": "2026-10-06 14:00:39.507", "level": "WARNING", "logger": "pyspark.sql.connect.logging", "msg": "Effective usage policy for this session is cct.Cipqb2JzLzU2OTI5MDQwMDE1NjIzMS9ydW5zLzExMjE4MjA4MDM5NDk0OTYQASABKiRiZmFiM2E2Ny1iODQxLTRmNTgtYmEwYy00ODYwYWY0YTdlNDMyJDAxYTExMTgyLTQyYmItNzIyYS1iYmU1LTEyNjVlNzFiMDI2NjokNDNiNWQ0NTUtODU4Mi0zZmM0LWEyOWYtNThhMjZjNWNiMWFiQiRiZmFiM2E2Ny1iODQxLTRmNTgtYmEwYy00ODYwYWY0YTdlNDNKDAiH/ZPWBhDAvbWdA1ACWAFgAWiPhM2BhcWjDYgBAA==.", "context": {}}
{"ts": "2026-10-06 14:00:39.509", "level": "WARNING", "logger": "pyspark.sql.connect.logging", "msg": "Effective usage policy for this session is cct.Cipqb2JzLzU2OTI5MDQwMDE1NjIzMS9ydW5zLzExMjE4MjA4MDM5NDk0OTYQASABKiRiZmFiM2E2Ny1iODQxLTRmNTgtYmEwYy00ODYwYWY0YTdlNDMyJDAxYTExMTgyLTQyYmItNzIyYS1iYmU1LTEyNjVlNzFiMDI2NjokNDNiNWQ0NTUtODU4Mi0zZmM0LWEyOWYtNThhMjZjNWNiMWFiQiRiZmFiM2E2Ny1iODQxLTRmNTgtYmEwYy00ODYwYWY0YTdlNDNKDAiH/ZPWBhDAvbWdA1ACWAFgAWiPhM2BhcWjDYgBAA==.", "context": {}}
{"ts": "2026-10-06 14:00:39.507", "level": "WARNING", "logger": "pyspark.sql.connect.logging", "msg": "Effective usage policy for this session is cct.Cipqb2JzLzU2OTI5MDQwMDE1NjIzMS9ydW5zLzExMjE4MjA4MDM5NDk0OTYQASABKiRiZmFiM2E2Ny1iODQxLTRmNTgtYmEwYy00ODYwYWY0YTdlNDMyJDAxYTExMTgyLTQyYmItNzIyYS1iYmU1LTEyNjVlNzFiMDI2NjokNDNiNWQ0NTUtODU4Mi0zZmM0LWEyOWYtNThhMjZjNWNiMWFiQiRiZmFiM2E2Ny1iODQxLTRmNTgtYmEwYy00ODYwYWY0YTdlNDNKDAiH/ZPWBhDAvbWdA1ACWAFgAWiPhM2BhcWjDYgBAA==.", "context": {}}
{"ts": "2026-10-06 14:00:39.509", "level": "WARNING", "logger": "pyspark.sql.connect.logging", "msg": "Effective usage policy for this session is cct.Cipqb2JzLzU2OTI5MDQwMDE1NjIzMS9ydW5zLzExMjE4MjA4MDM5NDk0OTYQASABKiRiZmFiM2E2Ny1iODQxLTRmNTgtYmEwYy00ODYwYWY0YTdlNDMyJDAxYTExMTgyLTQyYmItNzIyYS1iYmU1LTEyNjVlNzFiMDI2NjokNDNiNWQ0NTUtODU4Mi0zZmM0LWEyOWYtNThhMjZjNWNiMWFiQiRiZmFiM2E2Ny1iODQxLTRmNTgtYmEwYy00ODYwYWY0YTdlNDNKDAiH/ZPWBhDAvbWdA1ACWAFgAWiPhM2BhcWjDYgBAA==.", "context": {}}
{"ts": "2026-10-06 14:00:39.509", "level": "WARNING", "logger": "pyspark.sql.connect.logging", "msg": "Effective usage policy for this session is cct.Cipqb2JzLzU2OTI5MDQwMDE1NjIzMS9ydW5zLzExMjE4MjA4MDM5NDk0OTYQASABKiRiZmFiM2E2Ny1iODQxLTRmNTgtYmEwYy00ODYwYWY0YTdlNDMyJDAxYTExMTgyLTQyYmItNzIyYS1iYmU1LTEyNjVlNzFiMDI2NjokNDNiNWQ0NTUtODU4Mi0zZmM0LWEyOWYtNThhMjZjNWNiMWFiQiRiZmFiM2E2Ny1iODQxLTRmNTgtYmEwYy00ODYwYWY0YTdlNDNKDAiH/ZPWBhDAvbWdA1ACWAFgAWiPhM2BhcWjDYgBAA==.", "context": {}}

Loaded 24 proposals for 24 answer-key columns.
```

```python
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
        slice STRING,
        evaluated_at TIMESTAMP
    )
    """
)
(
    spark.createDataFrame(metric_rows)
    .withColumn("evaluated_at", F.current_timestamp())
    .write.format("delta")
    .mode("append")
    .saveAsTable(EVAL_RESULTS_TABLE)
)
print(f"MLflow run: {mlflow_run_id}; persisted {len(metric_rows)} metric rows.")
```

Output:
```text
2026/10/06 14:00:50 INFO mlflow.tracking.fluent: Experiment with name '/Users/philipp.tiefenbacher@databricks.com/halvard-mapping-evaluation' does not exist. Creating a new experiment.

MLflow run: 1fdc8e97bb644a19b1e3ddc70946f93f; persisted 6 metric rows.
```

```python
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
```

Output:
```text
id_registro                      proposed=record_id                    expected=record_id                    confidence=HIGH     correct=True
anio                             proposed=reporting_year               expected=reporting_year               confidence=HIGH     correct=True
mes                              proposed=reporting_month              expected=reporting_month              confidence=HIGH     correct=True
codigo_poliza                    proposed=policy_number                expected=policy_number                confidence=HIGH     correct=True
tipo_riesgo                      proposed=risk_type                    expected=risk_type                    confidence=HIGH     correct=True
provincia                        proposed=region                       expected=region                       confidence=HIGH     correct=True
canal_distribucion               proposed=distribution_channel         expected=distribution_channel         confidence=HIGH     correct=True
prima_neta                       proposed=net_written_premium_eur      expected=net_written_premium_eur      confidence=HIGH     correct=True
prima_bruta                      proposed=gross_written_premium_eur    expected=gross_written_premium_eur    confidence=HIGH     correct=True
num_polizas_nuevas               proposed=new_policies_count           expected=new_policies_count           confidence=HIGH     correct=True
num_polizas_renovadas            proposed=renewed_policies_count       expected=renewed_policies_count       confidence=HIGH     correct=True
num_polizas_canceladas           proposed=cancelled_policies_count     expected=cancelled_policies_count     confidence=HIGH     correct=True
num_siniestros_declarados        proposed=claims_reported_count        expected=claims_reported_count        confidence=HIGH     correct=True
num_siniestros_pagados           proposed=claims_paid_count            expected=claims_paid_count            confidence=HIGH     correct=True
importe_siniestros_bruto         proposed=gross_claims_incurred_eur    expected=gross_claims_incurred_eur    confidence=HIGH     correct=True
importe_reservas                 proposed=claims_reserve_eur           expected=claims_reserve_eur           confidence=HIGH     correct=True
gastos_gestion                   proposed=management_expenses_eur      expected=management_expenses_eur      confidence=HIGH     correct=True
comisiones                       proposed=commissions_eur              expected=commissions_eur              confidence=HIGH     correct=True
ratio_siniestralidad             proposed=loss_ratio                   expected=loss_ratio                   confidence=HIGH     correct=True
segmento_cliente                 proposed=customer_segment             expected=customer_segment             confidence=HIGH     correct=True
zona_riesgo                      proposed=risk_zone                    expected=risk_zone                    confidence=HIGH     correct=True
cobertura_principal              proposed=primary_coverage             expected=primary_coverage             confidence=HIGH     correct=True
moneda                           proposed=currency                     expected=currency                     confidence=HIGH     correct=True
fecha_carga                      proposed=NO_MATCH                     expected=None                         confidence=HIGH     correct=True
Accuracy: 100.0%; auto-accept: 100.0%; review load: 0.0%
```

```python
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
```

Output:
```text
{"source_system": "ES_PROPERTY_RAW", "mapping_version": "v1", "n_columns": 24, "n_correct": 24, "accuracy": 1.0, "auto_accept_rate": 1.0, "review_load": 0.0, "by_confidence": {"HIGH": {"n": 24, "correct": 24, "accuracy": 1.0, "share": 1.0}, "MEDIUM": {"n": 0, "correct": 0, "accuracy": 0.0, "share": 0.0}, "LOW": {"n": 0, "correct": 0, "accuracy": 0.0, "share": 0.0}}, "mlflow_run_id": "1fdc8e97bb644a19b1e3ddc70946f93f"}
```
