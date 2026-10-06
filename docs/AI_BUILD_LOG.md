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
| 9–11 | Bundle, deployment, evidence, documents | Claude |

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
