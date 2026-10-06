# Correctness Review

### 1. `scripts/genie_space.py:46` — Genie space creation crashes before returning the space ID
Severity: BLOCKER
Failure scenario: Run the script without `--space-id`; `create_space` returns a `GenieSpace` dataclass, then the script calls `extract_space_id` with that object and raises `AttributeError: 'GenieSpace' object has no attribute 'get'`.
Evidence: `space = workspace.genie.create_space(**payload)` is followed by `space_id = extract_space_id(space.model_dump() if hasattr(space, "model_dump") else space)`. In the installed SDK, `GenieSpace` has neither `model_dump` nor `get`; it exposes `space_id` and `as_shallow_dict()`.
Fix: Use `space_id = extract_space_id(space.as_shallow_dict(), fallback_id=None)` or `space_id = space.space_id`.

### 2. `apps/column_mapping_review_app/app.yaml:25` — `GENIE_SPACE_ID` references a nonexistent app resource
Severity: BLOCKER
Failure scenario: Deploy or validate the app; `GENIE_SPACE_ID` asks for `valueFrom: genie-space`, but `resources/app.yml` defines only `sql-warehouse`, `publish-job`, and `postgres`.
Evidence: `apps/column_mapping_review_app/app.yaml:23-26` references `publish-job` and `genie-space`; `resources/app.yml:8-20` contains no `genie-space` resource.
Fix: Add a Genie space resource named `genie-space` to `resources/app.yml`, or set `GENIE_SPACE_ID` after resource attachment using the supported finalization mechanism.

### 3. `resources/app.yml:20` — Lakebase database path uses a hyphenated database name
Severity: BLOCKER
Failure scenario: Attach the Postgres resource to the app; the path targets `databricks-postgres` instead of the contracted `databricks_postgres`, so resource attachment fails or connects to the wrong database.
Evidence: `resources/app.yml` defines `databases/databricks-postgres`; `docs/fe-bar/PLAN.md:100` and `review_store.connect` use `databricks_postgres`.
Fix: Change the path to `projects/${var.lakebase_project}/branches/production/databases/databricks_postgres`.

### 4. `apps/column_mapping_review_app/app.py:793` — Ask Genie misreads the installed SDK message and result objects
Severity: BLOCKER
Failure scenario: A successful Genie response returns the user prompt as “answer,” fails to extract SQL, and never displays result rows.
Evidence: The code treats `message.content` as the answer and `attachment.query` as SQL. The installed SDK defines `GenieMessage.content` as user message content; `GenieAttachment.query` is a `GenieQueryAttachment`, while generated SQL is in `.query.query`; the text answer is in `.text.content`. Also, `get_message_attachment_query_result` returns a response with `statement_response.result` and `statement_response.manifest`, not top-level `result` and `manifest` (`app.py:823-831`).
Fix: Normalize SDK objects through `as_shallow_dict()`/`as_dict()` before extraction, read answer text and SQL from attachment fields, and read rows from `response.statement_response`.

### 5. `scripts/genie_benchmark.py:63` — SDK Genie benchmark always records zero SQL answers
Severity: MAJOR
Failure scenario: With the installed SDK, each benchmark question reports `SUCCESS` but has no conversation ID, message ID, attachments, SQL, or rows.
Evidence: The code tests `hasattr(message, "model_dump")`; `GenieMessage` is a dataclass and has no `model_dump`. The fallback stores the raw message under `{"message": message}`, so the subsequent `response.get(...)` calls cannot extract identifiers or attachments.
Fix: Convert the returned message with `message.as_shallow_dict()` or `dataclasses.asdict(message)` before parsing.

### 6. `apps/column_mapping_review_app/app.py:463` — Publish readiness treats rejected mandatory mappings as ready
Severity: MAJOR
Failure scenario: A reviewer rejects a mandatory mapping without a target; the app enables Publish, but the publish job always fails its review gate.
Evidence: `publish_is_ready` returns true when mandatory pending count is zero. The gate separately blocks mandatory rows whose status is `REJECTED` and final target is empty (`notebooks/06_column_mapping_review_gate.py:240-249,285-291`), and `record_decision` sets that target to `NULL` on rejection (`src/harmonization/review_store.py:199-202`).
Fix: Require every mandatory row to be `APPROVED` or `CORRECTED` before enabling Publish, or additionally require rejected rows to have a target.

### 7. `apps/column_mapping_review_app/app.py:37` — “Latest” evaluation metrics are selected by a non-chronological run ID
Severity: MAJOR
Failure scenario: Rerun AI proposals for the same country. `ORDER BY run_id DESC` orders MLflow UUID-like run IDs lexicographically, so the Overview can display metrics from the earlier run.
Evidence: `mapping_eval_results.run_id` is written from `mlflow` run IDs (`notebooks/11_evaluate_ai_proposals.py:86-109`), not a monotonically increasing run timestamp or sequence.
Fix: Persist a run timestamp or monotonically increasing sequence in the evaluation results and order by that; otherwise fetch the selected run timestamp from MLflow.
