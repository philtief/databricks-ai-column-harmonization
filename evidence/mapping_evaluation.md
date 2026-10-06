# Mapping evaluation

_Collected 2026-10-06 15:06 UTC by scripts/collect_evidence.py from workspace https://fevm-agent-marketplace.cloud.databricks.com._

## Evaluation against the answer keys (latest run per country)

```sql
SELECT source_system, slice, metric_name, round(metric_value, 3) AS value, run_id AS mlflow_run_id, evaluated_at FROM agent_marketplace_catalog.halvard_harmonization.mapping_eval_results QUALIFY dense_rank() OVER (PARTITION BY source_system ORDER BY evaluated_at DESC) = 1 ORDER BY source_system, metric_name, slice
```

| source_system | slice | metric_name | value | mlflow_run_id | evaluated_at |
|---|---|---|---|---|---|
| ES_PROPERTY_RAW | ALL | accuracy | 1.0 | 1fdc8e97bb644a19b1e3ddc70946f93f | 2026-10-06T14:00:56.889Z |
| ES_PROPERTY_RAW | HIGH | accuracy | 1.0 | 1fdc8e97bb644a19b1e3ddc70946f93f | 2026-10-06T14:00:56.889Z |
| ES_PROPERTY_RAW | LOW | accuracy | 0.0 | 1fdc8e97bb644a19b1e3ddc70946f93f | 2026-10-06T14:00:56.889Z |
| ES_PROPERTY_RAW | MEDIUM | accuracy | 0.0 | 1fdc8e97bb644a19b1e3ddc70946f93f | 2026-10-06T14:00:56.889Z |
| ES_PROPERTY_RAW | ALL | auto_accept_rate | 1.0 | 1fdc8e97bb644a19b1e3ddc70946f93f | 2026-10-06T14:00:56.889Z |
| ES_PROPERTY_RAW | ALL | review_load | 0.0 | 1fdc8e97bb644a19b1e3ddc70946f93f | 2026-10-06T14:00:56.889Z |
| IT_PROPERTY_RAW | ALL | accuracy | 1.0 | 115a49bb133b4f6490851a24b854d78f | 2026-10-06T14:40:52.721Z |
| IT_PROPERTY_RAW | HIGH | accuracy | 1.0 | 115a49bb133b4f6490851a24b854d78f | 2026-10-06T14:40:52.721Z |
| IT_PROPERTY_RAW | LOW | accuracy | 0.0 | 115a49bb133b4f6490851a24b854d78f | 2026-10-06T14:40:52.721Z |
| IT_PROPERTY_RAW | MEDIUM | accuracy | 0.0 | 115a49bb133b4f6490851a24b854d78f | 2026-10-06T14:40:52.721Z |
| IT_PROPERTY_RAW | ALL | auto_accept_rate | 1.0 | 115a49bb133b4f6490851a24b854d78f | 2026-10-06T14:40:52.721Z |
| IT_PROPERTY_RAW | ALL | review_load | 0.0 | 115a49bb133b4f6490851a24b854d78f | 2026-10-06T14:40:52.721Z |
