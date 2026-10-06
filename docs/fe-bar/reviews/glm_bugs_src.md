### 1. notebooks/02_create_global_model_and_control_tables.py:267 — fresh-run schema migration executes before the tables it migrates exist
Severity: BLOCKER

Failure scenario: On a new catalog/schema, task `create_global_model_and_control_tables` reaches `_add_column_if_missing` for `ai_mapping_usage_metrics` before notebook 02 creates that table. `DESCRIBE TABLE` raises “Table ... does not exist”, the task raises, and the propose job cannot reach inventory, AI proposal, review-queue, or evaluation tasks.

Evidence: Lines 267–293 call `_add_column_if_missing` on `ai_mapping_usage_metrics`, `data_quality_results`, `value_mapping_candidates`, and `value_mapping_dictionary`. Those CREATE TABLE statements occur later at lines 301–408. The same issue can occur for control tables deployed before the new migration columns, but the four tables are absent on a clean deployment.

Fix: Move the `_add_column_if_missing` calls after every table-creation block, or omit migration calls for tables created in the same notebook run.

### 2. src/harmonization/review_store.py:84 — job-created Lakebase schema is not usable by the app service principal
Severity: BLOCKER

Failure scenario: The propose job runs before the app has started. Notebook 05 calls `ensure_schema(conn)` as the job owner, so the Postgres role running the app cannot read or write `harmonization_review.review_queue` or insert into `review_audit` (whose sequence is also required). App startup and every review action fail with permission denied.

Evidence: `ensure_schema` at lines 84–89 creates the schema/tables and commits but takes no grant target. `apps/column_mapping_review_app/app.py:266–298` connects and calls the same function using the app's `PGUSER`; `apps/column_mapping_review_app/app.yaml` injects the Lakebase platform variables, while `notebooks/05_prepare_app_review_views.py:239–247` creates the schema from the job. The contract says the app's service principal creates the schema on first start and owns it, but it does not guarantee the app starts before the propose job.

Fix: Pass the app service principal to `ensure_schema` from notebook 05 and have it grant USAGE on the schema and DML on the tables plus USAGE on sequences to that role, as an idempotent operation before returning from the propose job.
