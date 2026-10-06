# WP7: multi-country keys + Lakebase wiring in the workflow notebooks

## Goal
Make notebooks 02–10 run once per country (job parameter `source_country`, `ES` or `IT`), key every mapping
table by `(source_system, local_column_name)`, read raw data from the Lakeflow bronze tables, and move the
human review state through Lakebase: notebook 05 pushes the review queue, notebook 06 pulls decisions back.
Stage A is merged: `get_source_context` (config.py), `review_store.py`, `ingest.py`, `evaluation.py`,
`governance.py` and `genie_space.py` exist. Read them and use them; do not re-implement them.

## Files
`notebooks/02_*` … `notebooks/10_*`, `notebooks/_shared_utils.py`, `src/harmonization/tables.py`,
`src/harmonization/llm.py` (only the few-shot addition below), `src/harmonization/value_mapping.py` (NEW), and their tests.

## Changes per notebook
- All: widget `source_country` (default `ES`), plus `lakebase_endpoint` where Lakebase is used. Resolve
  `src = get_source_context(cfg, source_country)` and use `src["source_system"]` / `src["source_table"]`.
  Every notebook ends with `dbutils.notebook.exit(json.dumps(summary))`, a dict of the task's key numbers
  (rows, counts per status, etc.). Keep the existing `log_run_metric` calls.
- `_shared_utils.py` / `tables.py`: `get_table_refs` must use the new config shape (the raw table now
  depends on the country). Change the signature to `get_table_refs(cfg, DB, source_country)` and update all callers + tests.
- 02: `column_mapping_audit` gains `source_system STRING` right after `audit_id`. Views
  (`vw_pending_column_mappings`, `vw_mapping_review_summary`, `vw_publish_readiness`, `vw_column_mapping_low_conf`,
  `vw_column_mapping_coverage`) include `source_system` in their select/group-by. The target table is
  `harmonized_property_monthly` (from config) and keeps `source_country` and pipeline metadata columns.
- 03: inventory from the bronze table; exclude metadata columns `_source_file`, `_ingested_at`, `_rescued_data`.
  Every row carries `source_system`. Replace only this source_system's rows (idempotent rerun).
- 04: MERGE `ON tgt.source_system = src.source_system AND tgt.local_column_name = src.local_column_name`.
  Few-shot reuse: add an optional `approved_examples: list[tuple[str, str]] | None` argument to
  `build_ai_query_sql` / `render_static_prompt` that appends the block "Previously approved mappings from other
  subsidiaries (local -> global):" plus the lines. Notebook 04 loads active dictionary rows whose
  `source_system <> current` (max 60) and passes them. Unit-test the prompt change in `tests/test_llm.py`.
  Usage metrics rows carry source_system.
- 05: after building the review views, push the queue to Lakebase. At the top of the notebook:
  `%pip install "psycopg[binary]>=3.2" "databricks-sdk>=0.81" -q` then `dbutils.library.restartPython()`
  (put widget reads AFTER the restart). Rows: every candidate for this source_system with
  local_data_type, local_sample_values, proposed_*, mapping_rationale, confidence, mandatory_flag, mapping_version.
  `conn = review_store.connect(WorkspaceClient(), lakebase_endpoint)`; `ensure_schema`; `upsert_queue`; commit.
  Print `status_summary`. Exit summary includes rows_pushed.
- 06: same %pip header. Pull `fetch_decisions(conn, source_system)` and MERGE them into Delta
  `column_mapping_candidates` (review_status, final_global_column_name, final_match_type, reviewed_by,
  reviewed_at, review_comment, app_decision_source = 'LAKEBASE_APP', updated_at). Then copy the
  Lakebase `review_audit` rows for this source_system into Delta `column_mapping_audit` by MERGE on
  (source_system, audit_id): insert only. Then run the existing gate logic, filtered to source_system.
  Print how many decisions were synced.
- 07: build the dictionary for this source_system only (MERGE keyed by source_system + local_column_name).
- 08: read the bronze table, drop metadata columns, apply this source_system's active dictionary, cast each
  global column to its target type from the config, add `source_country` and the existing pipeline columns,
  and write with `.mode("overwrite").option("replaceWhere", f"source_country = '{country_name}'")` (no
  overwriteSchema). If the table is still empty or missing, create it first via the 02 DDL (it already exists after 02).
- 09: value translation becomes real. NEW `src/harmonization/value_mapping.py` (pure, tested):
  `categorical_targets(cfg) -> dict[str, list[str]]` (target columns of type STRING whose `semantic_group` is in
  {distribution, customer, risk} → their `examples` list as allowed values);
  `build_value_prompt(column, allowed, local_values) -> str`;
  `parse_value_response(text, allowed) -> dict[str, str | None]` (JSON object local→global; any value not in
  `allowed` becomes None). The notebook collects the distinct local values per categorical column from the
  harmonized rows of this country, calls `ai_query(ai_endpoint, prompt)` once per column (SQL `SELECT ai_query(...)`),
  writes `value_mapping_candidates`, auto-approves only values that map into the allowed list (approved_by =
  'auto:allowed-list') into `value_mapping_dictionary`, and applies the dictionary to the harmonized rows of this
  country (replaceWhere again). Print every translation (column, local, global, approved yes/no). Never stop
  the run because of a single unmapped value; report it.
- 10: DQ checks filtered to this country's rows (`source_country`); add the check `values_in_allowed_list` per
  categorical column (WARNING severity). DQ results rows carry source_system.

## Gate
Lint + tests green, coverage ≥ 80%. Add/adjust unit tests for `tables.py`, `llm.py`, `value_mapping.py`.
Notebooks cannot run locally; check them by reading. In `WP_NOTES.md`, list for every notebook the widget
names it reads (the integrator wires job parameters from that list).
