# Value model

The model has two kinds of input. **Measured** inputs come from the runs in `evidence/`. **Assumed** inputs
are estimates for a European P&C group like Halvard, and discovery with the customer must confirm them. No assumed
figure is a customer fact.

## Measured in this build

| Input | Value | Evidence |
|---|---|---|
| Model proposals, descriptive column names (ES, IT) | 24/24 and 24/24 correct, all HIGH | `evidence/mapping_evaluation.md` |
| Model proposals, legacy abbreviated names (IT stress test) | 18/24 correct | `evidence/ablation_generic_context.md` |
| Of these, HIGH confidence | 12 of 24 columns, 12/12 correct | same |
| Of these, MEDIUM or LOW | 12 of 24 columns, 6/12 correct | same |
| Raw files to review queue, one country (propose job) | 4.5 min | `evidence/jobs/halvard_propose_mappings_*.md` |
| Review decisions to governed group table (publish job tasks) | about 4 min of task time | `evidence/jobs/halvard_publish_harmonized_*.md` |
| Data quality checks on the harmonized table, ES | 31/31 passed | `evidence/dq_results.md` |
| Category values translated into group code lists, ES | 19/19 | `evidence/dq_results.md` |

The stress test is the realistic case. Every error had MEDIUM or LOW confidence, so a steward can accept HIGH
proposals after a short check and spend the review time on the other half.

## Assumptions

| # | Assumption | Low | Base | High |
|---|---|---|---|---|
| A1 | Country subsidiaries that report to the group | 8 | 14 | 20 |
| A2 | Manual effort per feed and month to reshape, map, and check the local file (person-days) | 1.0 | 1.5 | 2.5 |
| A3 | Remaining effort per feed and month with the pipeline (exception handling, person-days) | 0.5 | 0.25 | 0.15 |
| A4 | Loaded cost of a reporting analyst or actuary (EUR per day) | 640 | 760 | 880 |
| A5 | Columns per real subsidiary feed (the demo feeds have 24) | 120 | 180 | 300 |
| A6 | Manual mapping time per column: find the meaning, confirm with the local team, document (minutes) | 10 | 20 | 30 |
| A7 | Review time per column with the app: HIGH 2 min, other 10 min, with the measured 50/50 split | 6 | 6 | 6 |
| A8 | Elapsed time to onboard a new entity manually (weeks) | 4 | 6 | 8 |

## Results

**1. Monthly consolidation effort (executive sponsor: cost of the close).**

`annual saving = A1 × 12 × (A2 − A3) × A4`

| Case | Calculation | EUR per year |
|---|---|---|
| Low | 8 × 12 × 0.5 × 640 | 30,700 |
| Base | 14 × 12 × 1.25 × 760 | 159,600 |
| High | 20 × 12 × 2.35 × 880 | 496,300 |

**2. Onboarding a new entity or a changed feed (domain owner: weeks to onboard, steward hours).**

`steward hours per feed = A5 × A7 / 60`, against `A5 × A6 / 60` today.

| Case | Today (hours) | With the app (hours) | Reduction |
|---|---|---|---|
| Low | 120 × 10 / 60 = 20 | 120 × 6 / 60 = 12 | 40% |
| Base | 180 × 20 / 60 = 60 | 180 × 6 / 60 = 18 | 70% |
| High | 300 × 30 / 60 = 150 | 300 × 6 / 60 = 30 | 80% |

Elapsed time follows from this. The pipeline runs in minutes, and 18 steward hours fit into one working week.
Today, the base case takes about 6 weeks (A8).

**3. Error exposure (both personas: audit findings, restatements).**

In the stress test, 6 of 24 unreviewed mappings were wrong, among them commissions mapped to gross written premium.
Such an error does not fail any technical check. It changes the reported premium and every ratio built on it. The
review gate, the audit trail in Lakebase, and the lineage in Unity Catalog address this risk. The model does not
put a euro value on it, because the cost of a restatement depends on the customer.

## What the model leaves out

- The close-duration effect (days earlier). It depends on the customer's critical path.
- Licence and platform cost. Serverless jobs, the warehouse, and Lakebase scale to zero, so cost follows use.
- Feeds that change format, not only column names (for example a pivot from monthly rows to monthly columns).
