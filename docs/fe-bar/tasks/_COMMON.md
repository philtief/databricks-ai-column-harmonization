# Common rules for every work package

You are working in a git worktree of `databricks-ai-column-harmonization`, a Databricks project.
Read `docs/fe-bar/PLAN.md` first, especially section 3 "Fixed interface contracts". Never rename a
table, file, parameter, function or env var defined there.

Rules:
1. Edit or create ONLY the files listed under "Files" in your task. If you think another file must
   change, write that down in `WP_NOTES.md` instead of changing it.
2. Match the existing code style: notebooks in Databricks source format (`# Databricks notebook source`,
   `# COMMAND ----------`, `# MAGIC %md`), widgets for parameters, `%run ./_shared_utils` (or the right
   relative path), and helpers in `src/harmonization/` kept pure and unit-tested.
3. Put pure logic in `src/harmonization/*.py` and test it in `tests/`. Notebooks stay thin.
4. Use the Databricks Python SDK, not raw REST calls. Use SQL for Unity Catalog operations.
5. No customer names, no real data. Fictional company: Halvard Insurance Group.
6. Gate, which must pass before you finish:
   `bash scripts/lint.sh && bash scripts/test.sh`
   (the venv is at `.venv`; run `.venv/bin/ruff format <files>` to fix formatting). Coverage must stay ≥ 80%.
7. Do NOT run `git commit`, `git push`, or any `databricks` CLI command. Do not touch `databricks.yml`.
8. Finish by writing `WP_NOTES.md`: what you built, files changed, bundle/job wiring the integrator must
   add, open questions, and the final gate output (last 5 lines).
