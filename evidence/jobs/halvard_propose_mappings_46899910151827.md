# halvard_propose_mappings run 46899910151827

- State: SUCCESS
- Started: 2026-10-06 14:36:42 UTC
- Duration: 4.5 min
- Parameters: `{"catalog_name": "agent_marketplace_catalog", "schema_name": "halvard_harmonization", "source_country": "IT", "ai_endpoint": "databricks-claude-sonnet-4-6", "mapping_version": "v1", "lakebase_endpoint": "projects/halvard-harmonization/branches/production/endpoints/primary", "app_name": "halvard-harmonization-review"}`
- URL: https://fevm-agent-marketplace.cloud.databricks.com/?o=7474653189849615#job/569290400156231/run/46899910151827

| Task | State | Minutes | Exit summary |
|---|---|---|---|
| bootstrap_catalog | SUCCESS | 0.6 | `{"catalog": "agent_marketplace_catalog", "schema": "halvard_harmonization", "status": "ready"}` |
| create_global_model_and_control_tables | SUCCESS | 0.6 | `{"source_country": "IT", "source_system": "IT_PROPERTY_RAW", "ddl_ok": 11, "ddl_errors": 0, "target_columns": 23, "global_target_columns": 23}` |
| generate_country_files | SUCCESS | 0.3 | `{"files_written": 0, "files_skipped": 24, "rows_per_country": {"ES": 10000, "IT": 10000}}` |
| ingest_country_feeds | SUCCESS | 1.0 | `` |
| inventory_source_columns | SUCCESS | 1.0 | `{"source_country": "IT", "source_system": "IT_PROPERTY_RAW", "source_table": "bronze_property_monthly_it", "columns_inventoried": 24, "inventory_rows": 48}` |
| ai_propose_column_mappings | SUCCESS | 0.8 | `{"source_country": "IT", "source_system": "IT_PROPERTY_RAW", "source_table": "bronze_property_monthly_it", "total_source_columns": 24, "approved_columns": 0, "pending_columns": 24, "approved_examples_used": 23, "success_rows": 24, "error_rows": 0, "low_confidence_rows": 0, "total_candidates": 48}` |
| evaluate_ai_proposals | SUCCESS | 0.7 | `{"source_system": "IT_PROPERTY_RAW", "mapping_version": "v1", "n_columns": 24, "n_correct": 24, "accuracy": 1.0, "auto_accept_rate": 1.0, "review_load": 0.0, "by_confidence": {"HIGH": {"n": 24, "correct": 24, "accuracy": 1.0, "share": 1.0}, "MEDIUM": {"n": 0, "correct": 0, "accuracy": 0.0, "share": 0.0}, "LOW": {"n": 0, "correct": 0, "accuracy": 0.0, "share": 0.0}}, "mlflow_run_id": "115a49bb133b4` |
| publish_review_queue | SUCCESS | 0.8 | `{"source_country": "IT", "source_system": "IT_PROPERTY_RAW", "source_table": "bronze_property_monthly_it", "candidates": 24, "pending": 24, "approved_or_corrected": 0, "mandatory_pending": 14, "rows_pushed": 24}` |
