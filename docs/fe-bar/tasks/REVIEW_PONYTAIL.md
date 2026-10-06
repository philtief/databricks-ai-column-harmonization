# Review: over-engineering (ponytail, read-only)

You review a Databricks project (`databricks-ai-column-harmonization`) for unnecessary complexity only.
Do NOT change any file except your output file. Correctness bugs, security and performance are out of scope.

## Scope
`src/harmonization/`, `notebooks/`, `pipelines/`, `examples/generate_country_files.py`, `apps/column_mapping_review_app/`,
`scripts/*.py`. Ignore `tests/` (one smoke test or assert-based check is the minimum, never flag it).

## Method (the ladder; stop at the first rung that holds)
1. Does this need to exist at all? (speculative feature, config nobody sets, dead code)
2. Already in this codebase? (a helper re-implemented in another file, e.g. two SQL quoting helpers, two config loaders)
3. Does the Python stdlib do it?
4. Does the platform do it natively? (Spark/SQL function, Databricks SDK method, Unity Catalog feature, Streamlit widget)
5. Does an already-installed dependency solve it?
6. Can it be one line?

## Output
Write `REVIEW_OUT.md`. One line per finding:
`<file>:L<line>: <tag> <what>. <replacement>.`
Tags: `delete:` `stdlib:` `native:` `yagni:` `shrink:`.
Group by file. End with `net: -<N> lines possible.` If nothing to cut: `Lean already. Ship.`
Do not flag input validation at trust boundaries, error handling that prevents data loss, or security measures.
