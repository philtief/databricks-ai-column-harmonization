# WP4: evaluate AI mapping proposals against the answer key (MLflow)

## Goal
Measure how good the LLM's column-mapping proposals are, before any human touches them, for each
country. These numbers back the value claims in the deck, so they must be correct.

## Files
- NEW `src/harmonization/evaluation.py`: pure Python.
  ```python
  @dataclass
  class EvalResult: n_columns, n_correct, accuracy, by_confidence: dict[str, dict]  # {"HIGH": {"n":..,"correct":..,"accuracy":..,"share":..}}
                    auto_accept_rate, review_load, errors: list[dict]  # errors: local_column, proposed, expected, confidence
  def evaluate(proposals: list[dict], answer_key: dict[str, str | None]) -> EvalResult
  def to_metric_rows(result, run_id, source_system) -> list[dict]    # rows for mapping_eval_results
  def load_answer_key(path) -> tuple[str, dict]                       # reads examples/answer_keys/<cc>.json
  ```
  A proposal dict has `local_column_name`, `proposed_global_column_name`, `proposed_match_type`, `confidence`,
  `ai_error_status`. Correct means: proposed == expected (case-insensitive), or expected is None and the proposal
  is NO_MATCH or empty. AI errors count as wrong. `auto_accept_rate` = share of all columns that are HIGH and
  correct. `review_load` = share of columns where a reviewer must change something (wrong, or AI error).
  Columns in the answer key but missing from the proposals count as wrong (proposed None).
- NEW `notebooks/11_evaluate_ai_proposals.py`: widgets catalog_name, schema_name, source_country,
  mapping_version, ai_endpoint. `%run ./_shared_utils`. Reads `column_mapping_candidates` for the country's
  source_system (from `get_source_context(config, country)["source_system"]`; config via
  `load_harmonization_config()`), loads `../examples/answer_keys/<cc>.json` relative to the notebook
  folder in the workspace (`/Workspace` + notebook dir, like `_shared_utils.load_harmonization_config`).
  Logs an MLflow run in experiment `/Users/<current user>/halvard-column-harmonization`
  (params: source_system, ai_endpoint, mapping_version, n_columns; metrics: accuracy, auto_accept_rate,
  review_load, accuracy_<band>; artifact `errors.json`). Appends rows to Delta `mapping_eval_results`
  (create if missing). Prints a readable table of every column: local column, proposed, expected, confidence,
  correct. Exits with `dbutils.notebook.exit(json.dumps({... metrics ..., "mlflow_run_id": ...}))`.
- NEW `tests/test_evaluation.py`

## Tests
All-correct, all-wrong, mixed confidence bands, None/NO_MATCH handling, case-insensitivity, AI error rows,
missing proposals, an empty band, metric rows shape, and answer-key loading from a tmp file.
