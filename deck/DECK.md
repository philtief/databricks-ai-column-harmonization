---
marp: true
paginate: true
size: 16:9
style: |
  section { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 24px; color: #1B3139; padding: 56px 64px; }
  h1 { color: #FF3621; font-size: 40px; margin-bottom: 12px; }
  h2 { color: #1B3139; font-size: 32px; }
  table { font-size: 20px; }
  th { background: #1B3139; color: #fff; }
  .big { font-size: 56px; font-weight: 700; color: #FF3621; line-height: 1.1; }
  .small { font-size: 16px; color: #5A6F77; }
  section.lead { background: #1B3139; color: #fff; }
  section.lead h1 { color: #fff; font-size: 48px; }
---

<!-- _class: lead -->

# A new country feed in the group close in one working week, not six

Halvard Insurance Group · Group Finance and Group Actuarial

<span class="small">Prototype on Databricks with synthetic data. Halvard is fictional.</span>

---

# Fourteen subsidiaries, fourteen schemas, one group number

- Every month 14 country subsidiaries send reporting files in their own schema and language.
- Group Finance and Actuarial map them by hand before anyone sees a group loss ratio.
- Onboarding a new entity or a changed feed takes about **6 weeks** of steward time.
- A wrong mapping does not fail. It changes the reported premium and every ratio built on it.

<span class="small">Databricks Insurance Outcome Map: CFO & FP&A, Financial Projections & Reporting (loss ratio, expense ratio).</span>

---

# What changes

| | Today | With the prototype |
|---|---|---|
| New feed onboarded | about 6 weeks | **one working week** (9 min of compute plus about 18 steward hours) |
| Steward hours per 180-column feed | about 60 | **about 18 (−70%)** |
| Monthly consolidation effort, 14 feeds | 1.5 person-days per feed | 0.25 per feed: **about EUR 160k a year** |
| Mapping errors that reach the group numbers | not measured | stopped at a gate with a named approver |

<span class="small">Base case. Measured inputs from the runs; other inputs are assumptions to confirm in discovery (docs/VALUE_MODEL.md).</span>

---

# For the Group CFO: a faster, cheaper, defensible close

- **Cost of the close.** About EUR 160k a year less manual consolidation in the base case (range EUR 31k to 496k).
- **New entities report sooner.** An acquired or new subsidiary joins the group numbers in a week.
- **Defensible numbers.** Every group figure traces to a local column, the person who approved it, and the run that published it.
- **Pay for use.** Serverless jobs, an auto-stopping warehouse, and a database that scales to zero.

---

# For the Head of Group Actuarial: stewards decide, the model does the first pass

- The model proposes every mapping with a rationale and a confidence level.
- Stewards review in one app: approve, correct, or reject with a reason.
- A gate blocks publication while a mandatory column has no decision.
- Category values (`Agente`, `Diretto`) translate only into the group code lists.
- Controllers ask "Which channel has the highest combined ratio in Italy?" and get the answer with its SQL.

---

# What we showed: Italy, from raw files to group KPIs

| Step | Time | Result |
|---|---|---|
| Ingest 12 monthly files | in the 4.5-min propose run | 10,000 rows, quality rules applied |
| Model proposes 24 mappings | same run | 24/24 correct, reused Spain's approved mappings |
| Steward review | not timed in the prototype | 23 approved, 1 rejected with a reason (3 decisions clicked in the app, 21 scripted) |
| Publish from the app | 4.6 min | 31/31 data quality checks, 19/19 values translated |
| Ask in plain language | seconds | 6/6 benchmark questions answered with SQL |

Italy and Spain now sit side by side: loss ratio 64.6% and 64.5%, combined ratio 83.5% for both.

---

# The model knows when it is unsure

Stress test: Italian feed with legacy abbreviated column names (`PR_NT`, `SIN_PAG`, `PRVG`).

| Confidence | Columns | Correct |
|---|---|---|
| HIGH | 12 | **12** |
| MEDIUM | 11 | 6 |
| LOW | 1 | 0 |

- Unreviewed, 6 of 24 mappings would be wrong. One example: commissions mapped to gross written premium.
- Every error had MEDIUM or LOW confidence. Stewards accept the HIGH half after a short check and spend their time on the other half.

---

# How it works

```
Country files ─► Lakeflow pipeline ─► bronze tables
                                        │
           Claude Sonnet 4.6 (ai_query) proposes mappings, MLflow scores them
                                        │
           Lakebase: review queue and audit ◄──► Databricks App (review, Publish)
                                        │
           publish job: gate ─► dictionary ─► group table ─► value translation ─► 31 checks
                                        │
           Unity Catalog: tags, row filter per country, lineage, metric view ─► Genie
```

The app, Genie, and group reporting read the same governed KPIs on one platform.

---

# Trust by design

- **Human gate.** No mapping reaches group reporting without a named approver.
- **Audit trail.** Each decision and its audit record commit in one transaction.
- **Need to know.** A row filter shows each steward only the steward's own country. Group Actuarial sees all.
- **Lineage.** Unity Catalog records bronze table → group table → metric view.
- **Measured AI.** Each run scores the model against a labelled answer key, so the team sees the effect of a model change before it goes live.

---

# Assumptions behind the value

| Input | Low | Base | High |
|---|---|---|---|
| Subsidiaries | 8 | 14 | 20 |
| Manual effort per feed and month (person-days) | 1.0 | 1.5 | 2.5 |
| Remaining effort with the pipeline (person-days) | 0.5 | 0.25 | 0.15 |
| Loaded cost per day (EUR) | 640 | 760 | 880 |
| **Annual saving (EUR)** | **31k** | **160k** | **496k** |

Measured in the prototype: model accuracy and calibration, run times, data quality. To confirm in a pilot: everything in this table.

---

<!-- _class: lead -->

# Next step: a two-week pilot on two real feeds

1. You share two subsidiary feeds and the group model (or synthetic copies).
2. We run them through the same workflow in your workspace.
3. We measure steward hours, error rate, and elapsed time against your current process.

Decision at the end of week two: extend to all 14 subsidiaries or stop.
