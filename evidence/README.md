# Execution evidence

Everything in this folder is plain text, collected from the live workspace (`fevm-agent-marketplace`, catalog
`agent_marketplace_catalog`, schema `halvard_harmonization`) on 2026-10-06. Most of it comes from
`scripts/collect_evidence.py`; the Genie and ablation files come from the scripts named below.

## One country, end to end (Italy)

| Step | Platform | Run / object | Result | File |
|---|---|---|---|---|
| Raw CSV files land in a volume, Auto Loader ingests them | Lakeflow pipeline `halvard_ingest_country_feeds` | propose run `46899910151827`, task `ingest_country_feeds` | 10,000 rows in `bronze_property_monthly_it` | `jobs/halvard_propose_mappings_46899910151827.md`, `pipeline_ingest.md` |
| The model proposes a target for every column | `ai_query` on `databricks-claude-sonnet-4-6` | same run, task `ai_propose_column_mappings` | 24 proposals, all HIGH | `ai_mapping_proposals.md` |
| Proposals are evaluated against the answer key | MLflow experiment `halvard-mapping-evaluation` | MLflow run `115a49bb133b4f6490851a24b854d78f` | 24/24 correct | `mapping_evaluation.md` |
| Review queue in operational store | Lakebase `halvard-harmonization` | same run, task `publish_review_queue` | 24 rows PENDING | `lakebase_review_state.md` |
| Stewards decide in the app; the app publishes | Databricks App `halvard-harmonization-review` | 3 app decisions + 21 scripted; Publish button | job run `528730816408561` started from the app | `app_pages.md` |
| Gate, dictionary, mapping, value translation, DQ | Lakeflow job `halvard_publish_harmonized` | run `528730816408561` | 23 mappings, 19/19 values, 31/31 checks | `jobs/halvard_publish_harmonized_528730816408561.md`, `dq_results.md` |
| Governance | Unity Catalog | task `apply_governance` | tags, row filter, grants, metric view, lineage | `uc_governance.md` |
| Group KPIs | UC metric view `mv_group_property_kpis` | | ES and IT side by side | `harmonized_kpis.md` |
| Questions in natural language | Genie space `01f1c192d2f31c5bb935f924df7b1bd3` | `scripts/genie_benchmark.py` | 6/6 answered with SQL | `genie_benchmark.md` |

Spain ran the same path first: propose `1121820803949496`, publish `288885881518368`. Its review was scripted.
Its governance task failed four times on workspace tag policies before the fix; the evidence keeps those attempts.

## Other files

| File | Content |
|---|---|
| `notebooks/*.md` | Cell source and text output of every notebook task of the four runs |
| `ablation_generic_context.md` | Model alone with a generic description (24/24), and with legacy abbreviated column names (18/24, HIGH 12/12) |
| `app_status.md` | App deployment, state, and resources |

## Disclosures

- All data is synthetic (`examples/generate_country_files.py`). Halvard Insurance Group is fictional.
- Spain's 24 review decisions and 21 of Italy's were recorded by `scripts/review_from_answer_key.py` through the same
  `review_store.record_decision` the app uses; the audit trail marks them `ANSWER_KEY_SCRIPT`.
- Notebook 05 and 06 aborted once each (SIGABRT after `%pip install psycopg`); the automatic retry succeeded.
