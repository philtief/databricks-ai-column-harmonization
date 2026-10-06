# Halvard Insurance Group: a new country feed in the group close in one working week, not six

Halvard (fictional) is a European property and casualty group with 14 country subsidiaries. Every month each
subsidiary sends its reporting file in its own schema and language: `prima_bruta` in Spain,
`premi_lordi_contabilizzati` in Italy. Group Finance and Group Actuarial map these files by hand into one English group
model before they can report loss ratio, expense ratio, and combined ratio. A new or changed feed takes weeks of
steward time, and a wrong mapping changes the reported numbers without any error.

This build makes that mapping a governed, AI-assisted workflow on Databricks. The model proposes every mapping. A
data steward approves it in an app. The pipeline then publishes governed group KPIs that Finance can query in plain
language. It ran end to end for Spain and Italy on synthetic data, and `evidence/` holds the run output as text.

**Industry anchor:** Databricks Insurance Outcome Map, *CFO & FP&A: Financial Projections & Reporting* (KPIs: loss
ratio, expense ratio) and *Regulatory Compliance: Risk Management and Reporting*.

## Results

| What | Result | Evidence |
|---|---|---|
| Italy, raw files to governed group KPIs | about 9 minutes of compute (propose 4.5 min, publish 4.6 min) plus the review | `evidence/jobs/` |
| Model proposals, descriptive column names | 24/24 correct for Spain and for Italy, including traps (net vs. gross premium, reported vs. paid claims) | `evidence/mapping_evaluation.md` |
| Model proposals, legacy abbreviated names (stress test) | 18/24 correct; every HIGH-confidence proposal (12/12) correct; all 6 errors MEDIUM or LOW | `evidence/ablation_generic_context.md` |
| Data quality on the harmonized table | 31/31 checks passed per country; 19/19 category values translated into the group code lists | `evidence/dq_results.md` |
| Questions in natural language (Genie) | 6/6 answered with SQL | `evidence/genie_benchmark.md` |
| Estimated value, base case (assumptions labelled) | EUR 160k per year less consolidation effort; 70% fewer steward hours per new feed | `docs/VALUE_MODEL.md` |

The stress test is the reason for the human gate. Unreviewed, 6 of 24 mappings would have been wrong, among them
commissions mapped to gross written premium. The confidence score separates the safe half from the half that
needs a person.

## The journey

```
Country CSV files ──► Lakeflow pipeline (Auto Loader, expectations) ──► bronze tables, one per country
                                                                              │
             ai_query on Claude Sonnet 4.6 proposes a target per column ◄─────┘
             MLflow logs accuracy against the answer key
                                    │
                                    ▼
             Lakebase Postgres: review queue, decisions, audit trail
                                    ▲
             Databricks App: stewards approve / correct / reject, then press Publish
                                    │
                                    ▼
             publish job: review gate ► dictionary ► harmonized_property_monthly ► value translation ► 31 DQ checks
                                    │
                                    ▼
             Unity Catalog: tags, per-country row filter, grants, lineage, metric view mv_group_property_kpis
                                    │
                                    ▼
             Genie space "Halvard Group Property KPIs", also embedded in the app
```

| Requirement | Where |
|---|---|
| Lakeflow: ingest raw data | `pipelines/ingest_country_feeds.py`, `resources/pipeline.yml`, `resources/jobs.yml` |
| Unity Catalog: govern it | `notebooks/12_apply_governance.py`, `src/harmonization/governance.py`, `sql/mv_group_property_kpis.yaml` |
| Lakebase: operational serving | `src/harmonization/review_store.py`, notebooks 05 and 06 |
| Gen AI and ML | `notebooks/04_ai_propose_column_mappings.py`, `09_optional_value_mapping.py`, `11_evaluate_ai_proposals.py` |
| Genie | `genie/space_config.yaml`, `scripts/genie_space.py`, `scripts/genie_benchmark.py` |
| Databricks App | `apps/column_mapping_review_app/` |

## Documents

- `docs/VALUE_MODEL.md`: measured inputs, labelled assumptions, low, base, and high cases.
- `docs/DECISIONS.md`: ten decisions, each with the rejected alternative and its cost.
- `docs/AI_BUILD_LOG.md`: how two AI models built and reviewed this, and every defect that review and live runs caught.
- `docs/DEMO_SCRIPT.md`: tell-show-tell for the business and the technical persona, with objection handling.
- `deck/DECK.md` and `deck/DECK.pdf`: the business presentation.
- `docs/fe-bar/`: the build plan, the work-package briefs, and the code-review reports.
- `docs/FRAMEWORK.md` and `docs/SETUP.md`: the underlying framework and how to adapt it to another domain.

## Run it

Prerequisites: Databricks CLI ≥ 0.294 with a profile, a catalog you can write to, a serverless SQL warehouse, and a
Lakebase Autoscaling project (`databricks postgres create-project halvard-harmonization`).

```bash
databricks bundle deploy                                                   # pipeline, two jobs, app
databricks bundle run halvard_propose_mappings --params source_country=ES  # files -> proposals -> Lakebase queue
python scripts/review_from_answer_key.py --country ES                     # or review in the app
databricks bundle run halvard_publish_harmonized --params source_country=ES
python scripts/genie_space.py --catalog <cat> --schema <schema> --warehouse-id <id>   # then set var.genie_space_id
databricks bundle deploy && databricks bundle run review_app
python scripts/collect_evidence.py --runs <run ids>
```

Then repeat propose, review (in the app), and Publish (the app's button) for `IT`.

Tests: `bash scripts/lint.sh && bash scripts/test.sh` (231 tests; the Lakebase store tests run against a local
Postgres 17).

All data is synthetic. Halvard Insurance Group is fictional.
