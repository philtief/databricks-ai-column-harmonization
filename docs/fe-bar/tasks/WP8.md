# WP8: review app on Lakebase + Overview, Publish and Ask Genie pages

## Goal
The Streamlit app in `apps/column_mapping_review_app/` is the business surface. Today it does DML on Delta
through the SQL warehouse. Move all review reads and writes to Lakebase through
`src/harmonization/review_store.py`, add a country selector, and add three pages. Keep the existing look
(CSS, pills, layout) and the existing pages' behaviour.

## Files
`apps/column_mapping_review_app/app.py` (you may split it into `app.py` + `lakebase.py` + `genie.py` in the same
folder), `apps/column_mapping_review_app/app.yaml`, `apps/column_mapping_review_app/requirements.txt`,
`tests/test_app.py`, and new `tests/test_app_*.py` files if you split.

## Packaging note
The app folder is deployed on its own, so `src/` is NOT available at runtime. Copy
`src/harmonization/review_store.py` into `apps/column_mapping_review_app/review_store.py` as a build step: add
`scripts/sync_app_modules.sh` (cp + a header comment "generated, edit src/harmonization/review_store.py"), run it,
and add a test that fails if the two files differ (ignoring the header).

## Behaviour
- Connection: `review_store.connect(WorkspaceClient(), os.environ["LAKEBASE_ENDPOINT"], host=os.environ["PGHOST"],
  dbname=os.environ["PGDATABASE"], user=os.environ["PGUSER"])`. Cache the connection with `st.cache_resource` and
  reconnect on `psycopg.OperationalError` (tokens expire after 1 hour; scale-to-zero means the first connect may
  need a retry, so retry 3 times with backoff). Call `ensure_schema` once on startup.
- Reviewer identity: keep the existing `get_current_user()` (Databricks Apps forwarded user headers).
- Sidebar: country selector (ES / IT) mapped to source_system using a small dict in the app (ES_PROPERTY_RAW,
  IT_PROPERTY_RAW); all review pages are filtered by it.
- Existing pages Pending Review, Approved, Rejected and Dashboard: read from `fetch_queue`; actions call
  `record_decision` (APPROVE, CORRECT, REJECT, RESET) with `source="DATABRICKS_APP"`. Remove the old Delta DML
  helpers that become unused. Global target column options still come from Delta `global_target_columns` via
  `run_sql` (keep the warehouse path for analytics reads).
- NEW page "Group Overview" (make it the default first page): per country, review progress from `status_summary`
  (approved / corrected / rejected / pending, mandatory pending); from Delta via `run_sql`: harmonized row count
  per country, and from the metric view
  `SELECT \`Country\`, MEASURE(\`Gross Written Premium\`) AS gwp, MEASURE(\`Loss Ratio\`) AS loss_ratio,
  MEASURE(\`Combined Ratio\`) AS combined_ratio FROM <db>.mv_group_property_kpis GROUP BY ALL`; and the latest
  mapping evaluation (`mapping_eval_results`: accuracy and auto_accept_rate per source_system, latest run).
  Show the KPIs as `st.metric` tiles and one bar chart. Tolerate missing tables (show an info box, not a stack trace).
- "Publish Readiness" page: keep it, and add a "Publish harmonized data for <country>" button enabled only when no
  mandatory column is PENDING. It calls `WorkspaceClient().jobs.run_now(job_id=int(os.environ["PUBLISH_JOB_ID"]),
  job_parameters={"source_country": <ES|IT>})` and shows the run link and state (poll with a refresh button, not a loop).
- NEW page "Ask the group data": text input plus the sample questions as buttons. Uses
  `w.genie.start_conversation_and_wait(space_id=os.environ["GENIE_SPACE_ID"], content=q)`, then follow-ups in the
  same conversation with `w.genie.create_message_and_wait`. Show the text answer, the generated SQL in an
  expander, and the result rows as a dataframe (`w.genie.get_message_attachment_query_result`). Keep that logic in
  pure, tested helpers (`extract_genie_answer(message) -> dict(text, sql, attachment_id)`).
- `app.yaml` env: `CATALOG_NAME`, `SCHEMA_NAME`, and `valueFrom` for `DATABRICKS_WAREHOUSE_ID` (resource `sql-warehouse`),
  `PUBLISH_JOB_ID` (resource `publish-job`) and `GENIE_SPACE_ID` (resource `genie-space`). The Lakebase PG* vars are
  injected by the `postgres` resource. Defaults: catalog `agent_marketplace_catalog`, schema `halvard_harmonization`.
- `requirements.txt`: add `psycopg[binary]>=3.2`; bump `databricks-sdk>=0.81.0`.

## Tests
Mock-based like the existing `tests/test_app.py`: the country mapping; the publish-button enablement rule;
`extract_genie_answer` with text-only, SQL and empty messages; decision helpers calling `record_decision` with the
right arguments (mock the store); the connect retry logic (mock OperationalError twice then success); and the module-sync test.
Lint + tests green, coverage ≥ 80% (the app is outside `src/`, so coverage only counts src; still test the app helpers).

## Updates after Stage A review (binding)
- `review_store.connect` returns a connection with `dict_row`; rows are dicts. Read `src/harmonization/review_store.py`
  for the exact signatures before using it.
- The Genie API lives at `WorkspaceClient().genie` (`databricks.sdk.service.dashboards.GenieAPI`): `start_conversation_and_wait`,
  `create_message_and_wait`, `get_message_attachment_query_result`.
- Metric view dimensions: `Country`, `Reporting Period`, `Distribution Channel`, `Customer Segment`, `Risk Zone`, `Region`
  (see `sql/mv_group_property_kpis.yaml`).
