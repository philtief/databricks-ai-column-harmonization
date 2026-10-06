# How AI built this, and what the review caught

The build used two AI models in fixed roles. A planner and reviewer (Claude Opus 5.5 in Claude Code)
wrote the plan, the interface contracts, and the task briefs. It also reviewed every diff, ran the tests, deployed, and
wrote the narrative. Implementers (GLM 5.3 Flash through Codex, on the Databricks AI Gateway) wrote the code
for one work package each, in parallel git worktrees.

## The pattern

1. **Contracts first.** `docs/fe-bar/PLAN.md` section 3 fixes every name before any code exists: tables,
   files, job parameters, the Lakebase DDL, function signatures, and environment variables. Parallel work
   packages then integrate without negotiation.
2. **One brief per work package.** Each brief in `docs/fe-bar/tasks/` lists the files the package may touch,
   the exact API, and the tests. `_COMMON.md` holds the shared rules, for example "no commits, no CLI calls, and
   lint and tests must pass".
3. **Sandboxed implementers.** Each implementer ran with `codex exec -s workspace-write -C <worktree>`. It can
   write only its own worktree, and it cannot commit or call the workspace.
4. **A test gate on every commit.** A pre-commit hook runs `ruff` and `pytest` with a coverage floor of 80%.
   Merge commits do not run hooks, so the reviewer ran the full suite after each merge.
5. **The reviewer reads, then runs.** The reviewer read each diff against the brief, ran the code where
   possible, and fixed or rejected the result before the merge.

Run command for one package:

```bash
codex exec --skip-git-repo-check -m system.ai.glm-5-3-flash -s workspace-write -C wt/wp3 \
  -c 'approval_policy="never"' -c 'model_reasoning_effort="medium"' \
  "Read TASK.md and complete it fully. Follow every rule in it. Do not stop until the gate passes."
```

## Work packages

| WP | Scope | Implementer |
|---|---|---|
| 1 | Country file drops (ES and IT), answer keys, multi-source config | GLM 5.3 Flash |
| 2 | Lakeflow pipeline with Auto Loader and expectations | GLM 5.3 Flash |
| 3 | Lakebase review store (psycopg 3) | GLM 5.3 Flash |
| 4 | Evaluation of model proposals against answer keys, MLflow | GLM 5.3 Flash |
| 5 | Unity Catalog tags, row filter, grants, metric view | GLM 5.3 Flash |
| 6 | Genie space as code, benchmark | GLM 5.3 Flash |
| 7 | Multi-country notebooks, Lakebase wiring, value translation | GLM 5.3 Flash |
| 8 | App on Lakebase, Overview, Publish, and Ask pages | GLM 5.3 Flash |
| 9 to 11 | Bundle, deployment, evidence, documents | Claude |

## Defects the review caught

Green tests did not mean correct code. These defects passed the implementer's own gate:

| WP | Defect | How the review found it | Fix |
|---|---|---|---|
| 3 | The Postgres test fixture never started, so 8 of 18 tests were skipped every time | `pytest -rs` showed the skip reason; a manual `pg_ctl` start worked | Moved the socket to a short `/tmp` path (macOS limits Unix socket paths to about 104 bytes) |
| 3 | `record_decision` unpacked a `dict_row` into its keys, so APPROVE stored the text `"proposed_global_column_name"` as the approved target | Exposed once the Postgres tests ran | Read the row with `tuple_row`; the audit now records the previous final target |
| 3 | A test table mixed up its own columns (it compared a column name with a match type) | Same | Rewrote the cases, and added a test for RESET after CORRECT |
| 1 | `mandatory_source_columns` stayed a single list of Spanish names, so the gate would block Italy | The implementer flagged it in `WP_NOTES.md` | Moved to `sources.<country>.mandatory_columns`, derived for Italy through the answer key |
| 2 | The brief named a Spanish period column (`periodo`) that the generator never writes | The implementer flagged the conflict | Expectation now checks `anio`/`mes` and `anno_riferimento`/`mese_riferimento` |
| 2 | The pipeline found `src/` through `__file__`, which pipeline source files do not guarantee | Code review | The bundle passes `src_path` in the pipeline configuration |
| 5 | Claim counts were tagged `kpi_type=financial` | Read the generated SQL | Tag only monetary (DOUBLE) columns |
| 5 | The metric view month dimension had no year | Read the YAML | `Reporting Period = make_date(reporting_year, reporting_month, 1)` |
| 6 | The implementer reported that the SDK has no Genie API | Checked the installed SDK: it is `databricks.sdk.service.dashboards.GenieAPI` | The code prefers the SDK methods; the brief for WP8 names them |
| all | The worktrees shared one virtual environment whose editable install pointed at the main checkout, so tests imported the wrong code | Collection errors in WP1 | One virtual environment per worktree |

Two of these defects (the Spanish period column and the Spanish-only mandatory list) came from the
planner's own brief. The implementers flagged both instead of hiding them. That is the main reason for the
`WP_NOTES.md` rule.

## Code review by GLM 5.3 Flash

After Stage B, four read-only reviews ran in parallel on GLM 5.3 Flash: three bug slices (`src/`, notebooks,
app and scripts) and one ponytail pass for over-engineering. The briefs are `docs/fe-bar/tasks/REVIEW_*.md`.
The raw reports and the verdict on each finding are in `docs/fe-bar/reviews/`. Of 15 bug findings, 12 were
confirmed and fixed. The ponytail pass removed dead helpers, a duplicate set of views, and the REST fallbacks in
the Genie scripts. Net code change: −706 / +372 lines.

## Defects that only the live runs caught

Unit tests cannot catch platform behaviour. Each run below failed, and its run ID is in `evidence/`.

| Run | Defect | Fix |
|---|---|---|
| ES propose 1 | Notebook 02 altered tables before it created them (the GLM review predicted this) | Deleted the migration block |
| ES propose 1 | Bundle cycle: the publish job referenced the app SP, and the app referenced the publish job | Jobs get the app name; notebooks resolve the SP with `w.apps.get` |
| ES propose 2 | `databricks-claude-sonnet-5` is not enabled for `ai_query` batch inference | `databricks-claude-sonnet-4-6` |
| ES propose 3 | Usage-metrics `StructType` lacked `source_system` (`AXIS_LENGTH_MISMATCH`). The GLM review had flagged this row, and the first verdict rejected the finding by comparing the wrong schema | Added the field; a static check compared every other `createDataFrame` site |
| ES propose 3 | Claude wrapped 22 of 24 JSON answers in a ```` ```json ```` fence, so `from_json` returned null and the proposals counted as AI errors | `regexp_extract` of the outermost `{...}` before `from_json` |
| ES propose 4 | The MLflow experiment path equalled the bundle root folder (the planner's brief prescribed it) | Renamed the experiment |
| ES propose 4 | Notebook 05 aborted once (SIGABRT) right after `restartPython`; the automatic task retry succeeded | None; recorded |
| ES publish 1 | Notebook 06 called `get_mandatory_columns` without importing it | Import; the lint gate now fails on undefined names in notebooks |
| ES publish 2 | Notebook 08 read `.name` on `DataFrame.columns`, which are strings | `set(df.columns)` |
| ES publish 3 | Notebook 09 used a Spark `Column` in an `if` | One `create_map` lookup instead of a `when` chain |
| ES publish 3 | The group code lists missed values the feeds use (`Digital`, `Medium Business`, `Zone D`, two risk types), so KPIs by channel would mix languages | Completed the code lists in the config |
| ES publish 3 | Workspace tag policies rejected `domain=property_insurance`, `layer=control`, `data_owner=…`, `business_owner=group_actuarial` (four repairs) | Tags that conform to the policies; a check of every tag against the policies |
| Genie create | The API accepts one text instruction and requires id-sorted lists | Builder merges instructions and sorts by id |
| Genie benchmark | Two of six answers were wrong: a filter on `'italy'` (data holds `IT`) and a guessed review status | Instructions for country codes and review status, the candidates table, one example SQL; 6/6 correct |
| App start | `LAKEBASE_ENDPOINT` is not injected; the app SP could not run `CREATE ... IF NOT EXISTS` on a job-created schema | Env value in `app.yaml`; grant CREATE; DDL only on first run (tested under `SET ROLE`) |
| App, first look | Overview counted summary rows (2 instead of 23); a DataFrame iterated as rows; numeric reviewer id; raw floats in KPI tiles; white-on-white dropdown | Fixed; tests for the count and format helpers |
