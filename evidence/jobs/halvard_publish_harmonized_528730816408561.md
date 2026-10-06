# halvard_publish_harmonized run 528730816408561

- State: SUCCESS
- Started: 2026-10-06 15:34:11 UTC
- Duration: 4.6 min
- Parameters: `{"catalog_name": "agent_marketplace_catalog", "schema_name": "halvard_harmonization", "source_country": "IT", "ai_endpoint": "databricks-claude-sonnet-4-6", "mapping_version": "v1", "lakebase_endpoint": "projects/halvard-harmonization/branches/production/endpoints/primary", "privileged_principals": "", "app_name": "halvard-harmonization-review"}`
- URL: https://fevm-agent-marketplace.cloud.databricks.com/?o=7474653189849615#job/732511532591017/run/528730816408561

| Task | State | Minutes | Exit summary |
|---|---|---|---|
| column_mapping_review_gate | SUCCESS | 1.2 | `{"source_country": "IT", "source_system": "IT_PROPERTY_RAW", "decisions_synced": 24, "audit_rows_synced": 24, "gate_status": "PASSED", "blocking_issues": 0, "synced_at": "2026-10-06T15:35:22.095004"}` |
| build_column_mapping_dictionary | SUCCESS | 0.4 | `{"source_country": "IT", "source_system": "IT_PROPERTY_RAW", "approved_candidates": 23, "valid_mappings": 23, "excluded_no_match": 0, "rejected": 1, "active_dictionary_entries": 23, "mapping_version": "v1"}` |
| apply_approved_column_mappings | SUCCESS | 0.3 | `{"source_country": "IT", "source_system": "IT_PROPERTY_RAW", "raw_rows": 10000, "harmonized_rows": 20000, "mapped_columns": 23, "unmapped_dictionary_columns": 0, "target_columns": 23, "mapping_version": "v1"}` |
| value_mapping | SUCCESS | 1.2 | `{"source_country": "IT", "source_system": "IT_PROPERTY_RAW", "categorical_columns": 4, "value_candidates": 19, "auto_approved": 19, "unmapped_values": 0, "ai_errors": 0, "translated_columns": 4}` |
| validate_and_monitor | SUCCESS | 0.6 | `{"source_country": "IT", "source_system": "IT_PROPERTY_RAW", "raw_rows": 10000, "harmonized_rows": 10000, "checks": 31, "passed": 31, "warnings": 0, "failed": 0, "overall_status": "SUCCEEDED"}` |
| apply_governance | SUCCESS | 0.8 | `{"catalog_name": "agent_marketplace_catalog", "schema_name": "halvard_harmonization", "current_user": "philipp.tiefenbacher@databricks.com", "app_service_principal": "83b3161c-5563-4469-8548-7e4b979eeb1d", "privileged_principals": ["philipp.tiefenbacher@databricks.com", "83b3161c-5563-4469-8548-7e4b979eeb1d"], "ok": ["Comment bronze table", "Tag bronze table", "Comment bronze table", "Tag bronze t` |
