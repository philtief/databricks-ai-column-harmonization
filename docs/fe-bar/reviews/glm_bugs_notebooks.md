### 1. notebooks/02_create_global_model_and_control_tables.py:267 — Clean-run schema migration references tables before they exist
Severity: BLOCKER
Failure scenario: On a new catalog/schema, notebook 02 reaches `_add_column_if_missing(f"{DB}.`ai_mapping_usage_metrics`", ...)` after creating only the harmonized, inventory, candidates, dictionary, and audit tables. `DESCRIBE TABLE` raises `TABLE_OR_VIEW_NOT_FOUND`, so the first propose job fails before the value-mapping and ops tables are created.
Evidence: Lines 267–293 call the migration helper for `ai_mapping_usage_metrics`, `data_quality_results`, `value_mapping_candidates`, and `value_mapping_dictionary`. Those tables are only created later at lines 301–345 and 371–407. The helper raises on any error (lines 235–246).
Fix: Move the `source_system` migrations after all referenced tables are created, or create every table before running the migrations.

### 2. notebooks/06_column_mapping_review_gate.py:88 — Reset decisions are never synced from Lakebase to Delta
Severity: MAJOR
Failure scenario: A reviewer approves a mandatory column, resets it to PENDING in the app, then starts the publish job. Lakebase contains the reset, but Delta still contains `APPROVED`; the notebook reports zero blocking issues and publishes an already-revoked decision.
Evidence: Notebook 06 fetches only `review_status <> 'PENDING'` via `fetch_decisions` (lines 87–88; `src/harmonization/review_store.py:155–157`). `record_decision` implements `RESET` by setting `review_status = "PENDING"` (`src/harmonization/review_store.py:192–205`), so that row is excluded. The Delta merge runs only for fetched rows (lines 145–162).
Fix: Fetch every queue row for the source system and merge all of them, including `PENDING`, so a reset restores Delta's prior pending state.

### 3. notebooks/06_column_mapping_review_gate.py:230 — Review gate can pass mandatory approvals with no mapping target
Severity: MAJOR
Failure scenario: The AI fails or proposes `NULL` for a mandatory column. A steward clicks APPROVE in the app. Notebook 06 accepts the resulting `APPROVED` status, but notebook 07 excludes the null target, and notebook 08 can then fail with no active mappings or omit a required target column.
Evidence: The gate blocks only mandatory `PENDING` rows and rejected rows with no target (lines 230–249). It does not inspect the effective target for `APPROVED`/`CORRECTED`. `record_decision` sets an approved target to the proposal, which may be null (`src/harmonization/review_store.py:192–244`). Notebook 07 removes null and `NO_MATCH` targets at lines 101–105.
Fix: Treat a mandatory approved/corrected row with no effective non-`NO_MATCH` target as blocking, or require a target before the app can approve it.

### 4. notebooks/00_bootstrap_catalog.py:80 — Bootstrap notebook does not reach the required JSON exit
Severity: MINOR
Failure scenario: The propose job's first task succeeds, but the evidence collector receives no notebook exit summary because the contract requires every notebook to end with `dbutils.notebook.exit(json.dumps(summary))`.
Evidence: Notebook 00 ends at line 81 with a print statement. `docs/fe-bar/PLAN.md`, "Notebook outputs," requires each notebook to end with a JSON summary via `dbutils.notebook.exit`.
Fix: Build a small summary (catalog, schema, and verification counts) and call `dbutils.notebook.exit(json.dumps(summary))` after verification.

### 5. notebooks/06_column_mapping_review_gate.py:139 — Synced decision lineage is relabelled to an undefined source
Severity: MINOR
Failure scenario: A decision made with the default app source `DATABRICKS_APP` is copied into Delta as `LAKEBASE_APP`; the contract's valid candidate values are `DATABRICKS_APP` and `SQL_DIRECT`, so downstream audit/source reporting is wrong.
Evidence: Notebook 06 hardcodes `app_decision_source = "LAKEBASE_APP"` (lines 129–141) and uses that fallback for audit rows at line 198. The contract and table comment define `DATABRICKS_APP` (`docs/fe-bar/PLAN.md`, Lakebase section; `notebooks/02_create_global_model_and_control_tables.py:174`, `221`), and `review_store.connect/record_decision` defaults to `DATABRICKS_APP` (`src/harmonization/review_store.py:173–182`).
Fix: Preserve `review_queue`'s decision source where available, or map the Lakebase sync result to the contract value `DATABRICKS_APP`.

### 6. notebooks/04_ai_propose_column_mappings.py:363 — AI usage metrics swap `mapping_type` and `source_field_or_column`
Severity: MINOR
Failure scenario: Every column-mapping usage row stores the source system in `mapping_type` and the literal `"COLUMN"` in `source_field_or_column`, reversing the schema-defined meanings and making usage aggregation by mapping type/source column wrong.
Evidence: The table defines `mapping_type` as `COLUMN or VALUE` and `source_field_or_column` as the processed source field/column (`notebooks/02_create_global_model_and_control_tables.py:371–387`). The notebook's row is `(RUN_ID, SOURCE_SYSTEM, "COLUMN", SOURCE_SYSTEM, ...)` (lines 360–377).
Fix: Change the second and third values to `"COLUMN"` and `SOURCE_SYSTEM`, respectively.
