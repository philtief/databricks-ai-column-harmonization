# Demo script: tell, show, tell (12 minutes)

Audience: the executive sponsor (Group CFO) and the domain owner (Head of Group Actuarial). A technical
stakeholder (Head of Data Platform) joins for the questions.

## Tell (2 minutes)

"Each month 14 subsidiaries send you reporting files in 14 local schemas. Your team maps them by hand before you see
one group loss ratio. A new entity takes about six weeks to onboard, and a wrong mapping changes your numbers without
an error.

Today I show you Italy going from raw files to governed group KPIs in one session. The model proposes every
mapping, your steward approves it, and every number has an audit trail. Our estimate for a group of your size is
around EUR 160k a year in consolidation effort and 70% fewer steward hours per new feed. We confirm those inputs
with you."

## Show (8 minutes)

| Min | Screen | Say |
|---|---|---|
| 0:00 | App, Group Overview, Spain | "Spain is live: 10,000 rows, loss ratio 64.5%, combined ratio 83.5%. All 24 mappings reviewed." |
| 1:00 | Switch the country to Italy, Pending Review Queue | "Italy arrived this morning. The pipeline ingested the files and the model proposed 24 mappings. Nothing is published yet." |
| 2:00 | Select `premi_netti` | "Net premium, not gross. The model explains why, and it cites the mapping we approved for Spain." |
| 3:00 | Select `codice_agenzia_interno`, reject with a reason | "An internal agency code. No group target, so it stays out of group reporting. The reason goes into the audit trail." |
| 4:00 | Publish Readiness | "The gate checks that every mandatory column has a decision and a target. Only then does Publish turn on." |
| 4:30 | Press Publish (or show the finished run) | "This starts the publish job: dictionary, mapping, translation of category values, 31 data quality checks, governance." |
| 5:30 | Group Overview, Italy | "Italy is in the group model. Loss ratio 64.6%, side by side with Spain." |
| 6:30 | Ask the group data: "Which distribution channel has the highest combined ratio in Italy?" | "Your controllers ask in plain language. Genie answers from the same governed metric view, and shows the SQL." |
| 7:30 | Ask: "Which local column feeds net_written_premium_eur in Spain?" | "Every group number traces back to a local column and the person who approved it." |

## Tell (2 minutes)

"Italy went into the group model in one session; today that takes about six weeks. The model did the first pass
and said when it was unsure: with cryptic legacy column names it got 18 of 24 right, and every error had medium or
low confidence. Your steward made each decision, and the audit trail records it. Finance and Actuarial then query
the group KPIs in plain language under the same governance.

Next step: give us two real feeds and your group model for a two-week pilot. We measure steward hours and errors
against your current process."

## Objections: business persona

| Objection | Answer |
|---|---|
| "AI mapping my regulatory numbers? I can't explain that to the auditor." | The model never publishes. A named steward approves each mapping, the audit trail in Lakebase records who did what and why, and Unity Catalog lineage shows every hop from local column to KPI. |
| "Your 100% accuracy looks too good." | It is, for clean column names. That is why we also tested cryptic legacy names: 18 of 24. The point is calibration: every HIGH proposal was right, and every error had MEDIUM or LOW confidence, so the steward knows where to look. |
| "Where does EUR 160k come from?" | 14 feeds × 12 months × 1.25 person-days saved × EUR 760 per day. Every input is labelled as an assumption in the value model; the pilot measures them. |
| "Our feeds have 300 columns, not 24." | The cost per column is what scales. At 300 columns the review takes about 30 hours with the app against about 150 by hand. |
| "What if a subsidiary changes its file?" | Auto Loader keeps unexpected columns in a rescue column and the run continues; the new column goes to the review queue; the data quality checks flag missing values. |

## Objections: technical persona

| Objection | Answer |
|---|---|
| "Why Lakebase and not Delta tables for the review?" | Reviewers write one row per click, concurrently, and each decision and its audit record must commit together. That is OLTP. The first version did DML on Delta through the SQL warehouse; every click started a statement. Delta keeps the copy for lineage and Genie. |
| "Why not apply the mapping in the pipeline?" | A mapping is a human decision with its own lifecycle. The pipeline stays the same for every country; the publish job applies the approved dictionary. |
| "Who sees which country?" | A Unity Catalog row filter: stewards see their own country, Group Actuarial sees all. Genie and every SQL query respect it. |
| "What does it cost when nobody uses it?" | Serverless jobs, an auto-stopping warehouse, Lakebase scale-to-zero after 5 minutes. The app can be stopped. |
| "Which model, and can we change it?" | Claude Sonnet 4.6 through `ai_query`. The endpoint is a job parameter, and the evaluation notebook scores any endpoint against the answer key, so the team sees the effect of a change before it goes live. |
| "How was this built so fast?" | Two AI models with fixed roles, contracts first, a test gate on every commit, and a reviewer that reads every diff. `docs/AI_BUILD_LOG.md` lists the 30+ defects that review and live runs caught. |
