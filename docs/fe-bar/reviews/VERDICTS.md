# Review verdicts

Four read-only reviews ran on GLM 5.3 Flash at commit `fe-bar/insurance-group-close` after Stage B (three
bug slices, one ponytail over-engineering pass). Claude checked every finding against the code, the
installed SDK, or a live query before acting.

## Bug reviews (15 findings)

| Report | # | Finding | Verdict | Action |
|---|---|---|---|---|
| notebooks | 1 | Notebook 02 alters tables before it creates them | Confirmed by a live run: the first Spain propose run failed in task `create_global_model_and_control_tables` | Deleted the migration block (all tables already have the columns in their `CREATE`) |
| notebooks | 2 | A RESET in the app never reaches Delta | Confirmed | Notebook 06 syncs every queue row, not only decided rows |
| notebooks | 3 | Gate passes a mandatory column that is approved without a target | Confirmed | Gate blocks any reviewed mandatory row with no target or `NO_MATCH` |
| notebooks | 4 | Notebook 00 has no JSON exit | Confirmed | Added |
| notebooks | 5 | Decision source relabelled `LAKEBASE_APP` | Confirmed | Uses the contract value `DATABRICKS_APP` |
| notebooks | 6 | Usage metrics swap two columns | Rejected: the row order matches the table columns | none |
| src | 1 | Same as notebooks #1 | Duplicate | see above |
| src | 2 | Job-created Lakebase schema not usable by the app SP | Confirmed; already fixed in the commit after the review base | `ensure_schema(grant_to=...)` |
| app/scripts | 1 | `genie_space.py` calls `model_dump()` on an SDK dataclass | Confirmed against the installed SDK | Rewrote SDK-only |
| app/scripts | 2 | `GENIE_SPACE_ID` references a missing app resource | Confirmed; planned (the space exists only after the first publish) | Resource added after space creation |
| app/scripts | 3 | Lakebase database path uses `databricks-postgres` | Rejected: the database resource ID is hyphenated (`list-databases`), the Postgres name is `databricks_postgres`; deploy attached it | none |
| app/scripts | 4 | Ask Genie shows the user's question as the answer and misreads SQL and rows | Confirmed against the SDK dataclasses | Rewrote extraction with `as_dict()`; tests now use real SDK objects |
| app/scripts | 5 | Benchmark records zero SQL answers | Confirmed | Rewrote SDK-only |
| app/scripts | 6 | Publish enabled while a mandatory column has no target | Confirmed | Same rule as the gate |
| app/scripts | 7 | "Latest" evaluation ordered by MLflow run ID | Confirmed | `evaluated_at` column, ordered by it |

Found by Claude while fixing, missed by all reviews:
- **SQL literal escaping.** Both quoting helpers escaped `'` as `''`. Spark joins adjacent literals, so `'it''s'`
  returns `its` (checked on the SQL warehouse). Every prompt or comment with an apostrophe changed without an error.
  One helper (`governance.sql_str`) now uses backslash escapes.
- **Evaluation slice.** The Overview ranked `accuracy` rows across slices, so it could show HIGH-band accuracy
  as overall accuracy. It now reads `slice = 'ALL'`.

## Ponytail review (claimed net -563 lines)

| Finding | Verdict | Action |
|---|---|---|
| `confidence_pill`, `_esc` unused in the app | Confirmed (grep) | Deleted |
| `_record_review_action` only forwards | Valid, trivial | Kept (two lines) |
| App copy of `review_store.py` | Rejected: the app folder deploys alone; the sync script plus a drift test keep it honest | none |
| Notebook 02 duplicates the five review views of notebook 05 | Confirmed: no reader before 05 | Deleted from 02 |
| `_shared_utils` re-implements config loading | Confirmed | Delegates to `load_config` (also validates) |
| Genie scripts: REST fallbacks and shape parsers | Confirmed | SDK-only: 210 to 94 lines |
| Unused config getters | Confirmed | Deleted |
| `constants.py` unused | Confirmed | Deleted |
| `sql_str` duplicates `_sql_quote` | Confirmed (and both were wrong, see above) | One helper |
| `build_mapping_prompt` used only by tests | Confirmed | Deleted |
