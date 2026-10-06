# Review: correctness bugs (read-only)

You review code in a Databricks project (`databricks-ai-column-harmonization`). Read `docs/fe-bar/PLAN.md`
section 3 for the contracts. Do NOT change any file except your output file. Do not run `git` writes or any
`databricks` command. You may run `bash scripts/test.sh` and small local Python checks.

## Scope
SLICE

## What to look for
Real defects that make the build fail or produce wrong numbers when it runs on Databricks:
- wrong table, column, widget, parameter or env-var names versus the contracts and versus the other files
  (check every name you see against where it is defined; cross-file mismatches are the most likely bug)
- Spark/SQL errors: wrong function names, invalid SQL, MERGE keys that do not match, `replaceWhere` on a
  column that is not in the data, type mismatches on write, quoting bugs
- logic errors: wrong filters (country/source_system), off-by-one, None handling, idempotency on rerun
  (running a job twice must not duplicate rows), a gate that cannot pass or cannot fail
- Lakebase: connection, transaction and commit handling, SQL injection, token expiry
- notebooks: code that runs before `dbutils.library.restartPython()` and is lost, widgets read before they exist,
  `%run` paths, `dbutils.notebook.exit` not reached
- tests that pass but do not test what their name says

Do not report style, naming taste, missing docs, or over-engineering.

## Output
Write `REVIEW_OUT.md`. One entry per finding, most severe first:
```
### <n>. <file>:<line> — <one-line defect>
Severity: BLOCKER | MAJOR | MINOR
Failure scenario: <concrete input/state -> wrong result or crash>
Evidence: <the exact code, and the other file/line it conflicts with>
Fix: <smallest fix>
```
Only report findings you are confident about after reading both sides of a cross-file reference.
Quality over quantity: 5 real bugs beat 30 guesses. If you find nothing, say so.
