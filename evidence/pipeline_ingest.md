# Pipeline ingest

_Collected 2026-10-06 15:53 UTC by scripts/collect_evidence.py from workspace https://fevm-agent-marketplace.cloud.databricks.com._

## Rows per bronze table

```sql
SELECT 'es' AS country, count(*) AS rows, count(DISTINCT _source_file) AS files FROM agent_marketplace_catalog.halvard_harmonization.bronze_property_monthly_es UNION ALL SELECT 'it', count(*), count(DISTINCT _source_file) FROM agent_marketplace_catalog.halvard_harmonization.bronze_property_monthly_it
```

| country | rows | files |
|---|---|---|
| es | 10000 | 12 |
| it | 10000 | 12 |

## Expectation results per update (event log)

```sql
SELECT origin.update_id, timestamp, details:flow_progress.data_quality.expectations AS expectations, details:flow_progress.metrics.num_output_rows AS output_rows, origin.flow_name FROM event_log(TABLE(agent_marketplace_catalog.halvard_harmonization.bronze_property_monthly_es)) WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality IS NOT NULL ORDER BY timestamp DESC LIMIT 10
```

| update_id | timestamp | expectations | output_rows | flow_name |
|---|---|---|---|---|
| af605821-e08d-4259-81ca-1cd5ad684022 | 2026-10-06T13:24:15.605Z | [{"name":"has_premium","dataset":"agent_marketplace_catalog.halvard_harmonization.bronze_property_monthly_es","passed_records":10000,"failed_records":0},{"name":"valid_period","dataset":"agent_marketplace_catalog.halvard_harmonization.bronze_property_monthly_es","passed_records":10000,"failed_records":0}] | 10000 | agent_marketplace_catalog.halvard_harmonization.bronze_property_monthly_es |
| af605821-e08d-4259-81ca-1cd5ad684022 | 2026-10-06T13:24:15.505Z | [{"name":"has_premium","dataset":"agent_marketplace_catalog.halvard_harmonization.bronze_property_monthly_it","passed_records":10000,"failed_records":0},{"name":"valid_period","dataset":"agent_marketplace_catalog.halvard_harmonization.bronze_property_monthly_it","passed_records":10000,"failed_records":0}] | 10000 | agent_marketplace_catalog.halvard_harmonization.bronze_property_monthly_it |
