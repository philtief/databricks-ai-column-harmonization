# Over-Engineering Review

## `apps/column_mapping_review_app/app.py`

- `apps/column_mapping_review_app/app.py:L225`: delete: `confidence_pill()` is unused. Delete it.
- `apps/column_mapping_review_app/app.py:L316`: delete: `_esc()` is unused. Delete it.
- `apps/column_mapping_review_app/app.py:L396`: shrink: the `_record_review_action()` wrapper only forwards arguments to `record_decision()`. Pass the store function to `call_review_store()` directly.

## `apps/column_mapping_review_app/review_store.py`

- `apps/column_mapping_review_app/review_store.py:L1`: delete: this is a byte-for-byte generated copy of the 297-line Lakebase store. Deploy the shared module once and import it; keep `sync_app_modules.sh` only if the app deployment cannot include `src/`.

## `notebooks/02_create_global_model_and_control_tables.py`

- `notebooks/02_create_global_model_and_control_tables.py:L416`: delete: notebook 05 recreates the same five review views with the same DDL. Keep one creation point, preferably notebook 05.

## `notebooks/_shared_utils.py`

- `notebooks/_shared_utils.py:L48`: shrink: this reimplements YAML loading already provided by `harmonization.config.load_config()`. Resolve the deployed path, then delegate to the existing loader.

## `scripts/genie_benchmark.py`

- `scripts/genie_benchmark.py:L26`: yagni: the raw HTTP fallback and response-shape parsers duplicate the pinned Databricks SDK's Genie methods. Use `genie.start_conversation_and_wait()` only.

## `scripts/genie_space.py`

- `scripts/genie_space.py:L27`: yagni: the generic string/dict extractor and raw HTTP fallback cover SDK versions excluded by the installed dependency. Use the SDK's `create_space()` result directly.

## `src/harmonization/config.py`

- `src/harmonization/config.py:L68`: delete: `get_target_columns()`, `get_target_column_names()`, and `get_semantic_fields()` are unused by the workflow. Callers can read the already-validated config directly.

## `src/harmonization/constants.py`

- `src/harmonization/constants.py:L1`: delete: no production code imports these constants. Match types already exist in `llm.DEFAULT_MATCH_TYPES`; the review statuses and metadata names are duplicated as literals where used.

## `src/harmonization/governance.py`

- `src/harmonization/governance.py:L26`: shrink: `sql_str()` duplicates `llm._sql_quote()` for Spark/SQL string literals. Keep one shared SQL literal helper.

## `src/harmonization/llm.py`

- `src/harmonization/llm.py:L187`: delete: `build_mapping_prompt()` is used only by tests, not the vectorized production path. Delete the ad-hoc single-column entry point.

net: -563 lines possible.
