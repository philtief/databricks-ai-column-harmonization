# FE Bar build plan: Halvard Insurance Group, multi-country close

Branch: `fe-bar/insurance-group-close`. Base: `main` @ `6e5f381` (154 tests, 100% coverage, lint clean).

## 1. Scenario

**Company (fictional):** Halvard Insurance Group, a European property and casualty group with 14 country
subsidiaries. Each subsidiary submits monthly property-insurance reporting files in its own local schema
and language (`prima_bruta`, `premi_lordi_contabilizzati`, ...).

**Problem:** Group Finance and Group Actuarial consolidate these feeds into one English group data model
for the monthly close, loss-ratio steering, and regulatory reporting. Mapping a new or changed local
schema is manual spreadsheet work by data stewards and actuaries. A new country feed takes weeks to
onboard, and schema drift in an existing feed delays the close.

**Personas**
- Executive sponsor: Group CFO. KPIs: close duration, cost of the reporting process, audit findings.
- Domain owner: Head of Group Actuarial / Group Data Office. KPIs: weeks to onboard a country feed,
  steward hours per feed, mapping error rate, lineage for every reported number.

**Outcome we demonstrate:** a second country (Italy) is onboarded from raw files to governed,
queryable group KPIs in one working session instead of weeks, with an AI that proposes the mappings, a
human who approves them, and an audit trail for every decision.

The value numbers (section 7) combine measured results from our runs with labelled assumptions.

## 2. Requirement coverage (the integrated journey)

```
 country files (CSV, ES + IT, synthetic)
   │  UC Volume  /Volumes/<cat>/<sch>/landing/<cc>/
   ▼
 Lakeflow Spark Declarative Pipeline (Auto Loader)          ── Lakeflow
   bronze_property_monthly_es / _it  (+ expectations)
   ▼
 inventory → ai_query mapping proposals (+ confidence)       ── Gen AI
   evaluation vs answer key, logged to MLflow                ── ML eval
   ▼
 Lakebase Postgres: review_queue / review_audit              ── Lakebase (operational)
   ▲  reviewers approve / correct / reject in the App        ── Databricks App
   ▼
 publish job: gate → dictionary → harmonized_property_monthly
   ▼
 Unity Catalog: tags, comments, row filter per country,      ── Unity Catalog
   grants, lineage, metric view mv_group_property_kpis
   ▼
 Genie space "Halvard Group Property KPIs"                   ── Genie
   embedded in the App ("Ask the group data")
```

| Requirement | Today | Gap closed by |
|---|---|---|
| Lakeflow ingest of raw data | Generator writes a Delta table directly | WP1 file drops + WP2 SDP pipeline (Auto Loader, expectations) |
| Unity Catalog governance | Control tables and comments | WP5 tags, row filter, grants, metric view, lineage evidence |
| Lakebase operational serving | None (app does DML on Delta via warehouse) | WP3 review store; WP7/WP8 wiring |
| ML or Gen AI | `ai_query` proposals | WP4 evaluation vs answer key + MLflow; IT run reuses approved ES dictionary as few-shot context (stretch) |
| Genie | None | WP6 space as code + benchmark |
| Databricks App | Streamlit review app | WP8 Lakebase backend, Overview page, Genie page, Publish button |
| Execution evidence as text | None | Phase 4 `evidence/` |
| Specific industry problem | Generic framework | README, value model, deck |

## 3. Fixed interface contracts

All work packages code against these names. Do not rename them.

**Workspace:** profile `pt` (fevm-agent-marketplace). Catalog `agent_marketplace_catalog`, schema
`halvard_harmonization`. Warehouse: `Serverless Starter Warehouse` (`41754a8563a43a49`).
LLM endpoint for mappings: `databricks-claude-sonnet-4-6` (job parameter `ai_endpoint`).

**Countries:** `ES` (Spain, source_system `ES_PROPERTY_RAW`), `IT` (Italy, source_system `IT_PROPERTY_RAW`).
Lowercase country code `cc` in paths and table names.

**Files**
- Landing: `/Volumes/<cat>/<sch>/landing/<cc>/property_monthly_<YYYYMM>.csv` (header row, comma, UTF-8).
  12 months per country, about 10,000 rows per country in total. Deterministic seed per country.
  Idempotent: an existing file is not rewritten.
- Answer keys (committed, static): `examples/answer_keys/<cc>.json`:
  `{"source_system": "...", "mappings": {"<local_column>": "<global_column or null>"}}`.

**Config** (`config/harmonization_config.yaml`): `target_model` unchanged; `source_context` replaced by
```yaml
sources:
  ES: {domain, source_system: ES_PROPERTY_RAW, source_table: bronze_property_monthly_es, description}
  IT: {domain, source_system: IT_PROPERTY_RAW, source_table: bronze_property_monthly_it, description}
```
`harmonization.config.get_source_context(config, country) -> dict` returns one entry (KeyError message
lists the valid countries). Existing callers move to it.

**Delta tables** (in `<cat>.<sch>`)
- `bronze_property_monthly_<cc>`: all source columns + `_source_file STRING`, `_ingested_at TIMESTAMP`
  (`_rescued_data` kept by Auto Loader).
- Existing control tables. `column_mapping_candidates`, `column_mapping_dictionary` and
  `column_mapping_audit` are keyed by `(source_system, local_column_name)`. The audit table gains
  `source_system`.
- `harmonized_property_monthly` (= `target_model.table_name` renamed): union of countries, written per
  country with `replaceWhere source_country = '<Country>'`.
- `mapping_eval_results`: one row per (run_id, source_system, metric_name, metric_value, slice).
- Metric view `mv_group_property_kpis` over `harmonized_property_monthly`.

**Lakebase** — project `halvard-harmonization`, branch `production`, database `databricks_postgres`,
Postgres schema `harmonization_review`. Endpoint path passed as job parameter `lakebase_endpoint`
(`projects/halvard-harmonization/branches/production/endpoints/primary`). Scale-to-zero on.

```sql
review_queue(
  source_system text, local_column_name text, local_data_type text, local_sample_values jsonb,
  proposed_global_column_name text, proposed_match_type text, mapping_rationale text, confidence text,
  mandatory_flag boolean, review_status text NOT NULL DEFAULT 'PENDING',
  final_global_column_name text, final_match_type text, reviewed_by text, reviewed_at timestamptz,
  review_comment text, mapping_version text, published_at timestamptz, updated_at timestamptz,
  PRIMARY KEY (source_system, local_column_name))
review_audit(
  audit_id bigserial PRIMARY KEY, source_system text, local_column_name text,
  old_status text, new_status text, old_global_column_name text, new_global_column_name text,
  action_by text, action_at timestamptz DEFAULT now(), action_comment text, action_source text)
```
The app's service principal creates the schema on first start and owns it, as the Lakebase skill requires.
Jobs run as the project owner, who has DML access.

`src/harmonization/review_store.py` (psycopg 3, every function takes an open connection):
`ensure_schema`, `upsert_queue(conn, rows) -> int` (AI fields refresh only while `PENDING`, so human
decisions are never overwritten), `fetch_queue(conn, source_system=None, status=None)`,
`record_decision(conn, source_system, local_column_name, action, user, final_global=None,
final_match_type=None, comment="", source="DATABRICKS_APP") -> bool` (actions APPROVE | CORRECT |
REJECT | RESET; queue update and audit insert in one transaction; REJECT requires a comment, CORRECT
requires a target), `fetch_decisions(conn, source_system)`, `status_summary(conn)`,
`connect(workspace_client, endpoint_path, host=None, dbname="databricks_postgres", user=None)`.

**Jobs** (the existing single job is split in two)
- `halvard_propose_mappings` (params: catalog_name, schema_name, source_country, ai_endpoint,
  mapping_version, lakebase_endpoint): bootstrap → generate_country_files → ingest_country_feeds
  (pipeline task) → create_global_model_and_control_tables → inventory → ai_propose → publish_review_queue
  (05, pushes to Lakebase) → evaluate_ai_proposals (11).
- `halvard_publish_harmonized` (same params): review_gate (06, pulls decisions from Lakebase into Delta,
  then gates) → build_dictionary (07) → apply (08) → value_mapping (09) → validate (10) →
  apply_governance (12).
- The app's Publish button triggers `halvard_publish_harmonized` for the selected country.

**App env vars:** `CATALOG_NAME`, `SCHEMA_NAME`, `DATABRICKS_WAREHOUSE_ID`, `PUBLISH_JOB_ID`,
`GENIE_SPACE_ID`, plus the Lakebase vars the platform injects (`PGHOST`, `PGDATABASE`, `PGUSER`,
`PGPORT`, `LAKEBASE_ENDPOINT`).

**Notebook outputs:** each notebook ends with `dbutils.notebook.exit(json.dumps(summary))`. The summary
holds the task's key numbers, which the evidence collector reads.

## 4. Work packages

Every GLM package runs in its own git worktree with `TASK.md` (the brief is in `docs/fe-bar/tasks/`),
creates or edits only the files it is assigned, and must leave `bash scripts/lint.sh && bash
scripts/test.sh` green. Claude reviews every diff, reruns the gate, and merges. Bundle (`databricks.yml`)
edits are integration work done by Claude, so packages do not conflict on it.

### Stage A (parallel; new files, or files no other package touches)

| WP | Owner | Files | Done when |
|---|---|---|---|
| WP1 data + config | GLM | `examples/spain_demo/01_generate_spain_raw_data.py` → `examples/generate_country_files.py` (ES+IT), `examples/answer_keys/{es,it}.json`, `config/harmonization_config.yaml`, `config/harmonization_config.yaml.template`, `src/harmonization/config.py`, `src/harmonization/generator.py` (pure row/column specs), tests | Italian schema: 24 Italian columns, at least 3 semantic traps and 1 NO_MATCH column; answer keys cover every column; unit tests for generator specs + `get_source_context` |
| WP2 Lakeflow | GLM | `pipelines/ingest_country_feeds.py`, `src/harmonization/ingest.py` (pure helpers: table names, paths, expectation dict), tests | SDP Python (`from pyspark import pipelines as dp`), one streaming table per country from pipeline config `countries`, `landing_root`; expectations `valid_period` (warn) and `has_premium` (drop) |
| WP3 Lakebase store | GLM | `src/harmonization/review_store.py`, `tests/test_review_store.py` | Tests run against a throwaway local Postgres 17 (`pg_ctl`; skipped if absent); covers every action, idempotent upsert, no clobbering of decisions, audit rows |
| WP4 Evaluation | GLM | `src/harmonization/evaluation.py`, `notebooks/11_evaluate_ai_proposals.py`, tests | accuracy, accuracy and coverage per confidence band, auto-accept rate (HIGH and correct), review load; MLflow run with params, metrics and a confusion artifact; writes `mapping_eval_results` |
| WP5 Governance | GLM | `src/harmonization/governance.py` (SQL builders), `notebooks/12_apply_governance.py`, `sql/mv_group_property_kpis.yaml`, tests | tags (table and column), row filter function `country_row_filter`, grants to app SP and groups (`try/except` if a group is missing), metric view create; prints SHOW GRANTS, tags and DESCRIBE output |
| WP6 Genie | GLM | `genie/space_config.yaml`, `src/harmonization/genie_space.py` (serialized_space v2 builder), `scripts/genie_space.py` (create or update), `scripts/genie_benchmark.py`, tests | builder unit-tested; benchmark writes `evidence/genie_benchmark.md` with question, SQL, top rows and status |

### Stage B (sequential, after Stage A merges)

| WP | Owner | Files | Done when |
|---|---|---|---|
| WP7 multi-country + Lakebase wiring | GLM | `notebooks/02–10`, `src/harmonization/tables.py` | keys `(source_system, local_column_name)`; country from `source_country`; 05 pushes queue, 06 pulls decisions; 08 uses `replaceWhere`; every notebook exits with a JSON summary |
| WP8 app | GLM | `apps/column_mapping_review_app/*`, `tests/test_app.py` | review actions call `review_store`; country selector; pages Overview (status per country, harmonized rows, loss ratio by country from the metric view), Review queue, Publish (runs publish job), Ask Genie (Conversation API: answer, SQL, table) |

### Stage C (Claude)

| WP | Files | Done when |
|---|---|---|
| WP9 bundle + deploy | `databricks.yml`, `resources/*.yml`, `scripts/setup_lakebase.sh`, `scripts/finalize_app.sh` | `bundle validate` passes, deploy works, app has postgres, job, warehouse and genie resources |
| WP10 evidence | `scripts/collect_evidence.py`, `evidence/**` | see section 5 |
| WP11 docs + deck | `README.md`, `docs/DECISIONS.md`, `docs/AI_BUILD_LOG.md`, `docs/VALUE_MODEL.md`, `docs/DEMO_SCRIPT.md`, `deck/DECK.md` (+ PDF) | writing guide and humanizer pass done; numbers match `evidence/` |

## 5. Phases and test gates

| Phase | Steps | Gate (must pass before moving on) |
|---|---|---|
| 0 Setup | branch, venv, pre-commit hook, plan, briefs | baseline lint + 154 tests green ✅ |
| 1 Stage A | WP1–WP6 in parallel worktrees | per WP: lint + tests green, Claude diff review, merged one at a time with the full suite rerun after each merge |
| 2 Stage B | WP7, then WP8 | same; `databricks bundle validate -p pt` |
| 3 Deploy + run | Lakebase project, bundle deploy, app deploy (app first, so its SP owns the Lakebase schema), Genie space | propose job ES SUCCEEDED → review in app (some decisions clicked in the app, the rest scripted from the answer key through `review_store`, disclosed in the evidence) → publish ES SUCCEEDED → propose IT → review → publish IT SUCCEEDED; DQ: 0 FAILED; Genie benchmark ≥ 5/6 answered with SQL |
| 4 Evidence | `collect_evidence.py` | every file in section 6 exists, is non-empty, and contains real run IDs |
| 5 Narrative | docs, deck, demo script | numbers trace to evidence; humanizer pass; no customer names (`grep` check) |
| 6 Final check | requirement checklist (section 8) | all rows ✅; then ask Philipp before pushing |

## 6. Evidence the evaluator can read (all text)

```
evidence/
  README.md                      index: what ran, when, run IDs, how to reproduce
  jobs/<job>_<run_id>.md         task list, state, duration, notebook exit summary per task
  notebooks/<task>.md            cell source + text outputs, converted from the exported run
  pipeline_ingest.md             pipeline update ID, rows per table, expectation pass/fail counts (event log)
  ai_mapping_proposals_<cc>.md   every proposal: local column, proposed target, confidence, rationale
  mapping_evaluation.md          accuracy per country/confidence, MLflow run IDs
  lakebase_review_state.md       review_queue status counts, last 20 audit rows (psql output)
  uc_governance.md               tags, row filter definition, grants, lineage rows from system tables
  harmonized_kpis.md             metric view query results: GWP, claims, loss ratio by country/month
  dq_results.md                  data_quality_results for each publish run
  genie_benchmark.md             questions, generated SQL, results
  app_smoke.md                   app URL, deployment ID, /health check, page text snapshot (Playwright accessibility tree)
```

## 7. Value model (filled in Phase 5)

Measured: proposal accuracy, share of columns needing human edits, minutes from file drop to harmonized
table, reviewer minutes for the Italy feed. Assumed (labelled as such, with ranges): number of
subsidiaries and feeds, columns per feed, steward minutes per column with spreadsheets, schema change
frequency, loaded cost per steward hour, close days lost to late feeds. The value model reports a low,
base and high case. No figure is presented as a customer fact.

## 8. Final requirement checklist

- [ ] Specific industry + problem stated in the first paragraph of README
- [ ] Lakeflow pipeline ingests raw files; run evidence in `evidence/pipeline_ingest.md`
- [ ] UC governance applied; evidence in `evidence/uc_governance.md`
- [ ] Lakebase holds operational review state; evidence in `evidence/lakebase_review_state.md`
- [ ] Gen AI proposals + evaluation; evidence in `evidence/ai_mapping_proposals_*.md`, `mapping_evaluation.md`
- [ ] Genie space answers questions; evidence in `evidence/genie_benchmark.md`
- [ ] App deployed and running; evidence in `evidence/app_smoke.md`
- [ ] Journey integrated: one country flows end to end through every stage, with the run IDs connected
- [ ] DECISIONS.md, AI_BUILD_LOG.md, VALUE_MODEL.md, DEMO_SCRIPT.md, deck
- [ ] No real customer data or identifiers; repo public
- [ ] Resources scale to zero (serverless jobs, warehouse auto-stop, Lakebase scale-to-zero, app can be stopped)

## 9. Risks

| Risk | Mitigation |
|---|---|
| App SP cannot create the Lakebase schema | deploy the app before any job writes to Lakebase; `ensure_schema` on app start |
| Row filter hides rows from the app or Genie | owner + app SP in the filter's privileged list; evidence shows the definition |
| Metric view YAML rejected | test with `CREATE` on the warehouse during WP5 review; fall back to a plain view |
| GLM output drifts from the contracts | contracts are in every brief; Claude reviews each diff before merge |
| Lineage system table lags | lineage evidence is non-blocking; rerun collection later |
| Shared FEVM workspace holds other customers' schemas | everything lives in `halvard_harmonization`; evidence queries filter to that schema |
