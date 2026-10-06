# Decisions and trade-offs

Each entry states the decision, the alternative we rejected, and the cost of the choice.

## 1. Ingest files with Auto Loader in a Lakeflow pipeline

The subsidiaries deliver monthly CSV files, so a Lakeflow Spark Declarative Pipeline reads them with Auto
Loader from a Unity Catalog volume. Auto Loader processes only new files, infers the column types, and keeps
unexpected columns in `_rescued_data`. A schema change in a local feed therefore does not stop the close.

Rejected: Lakeflow Connect. It targets SaaS applications and databases, not file drops from subsidiaries.

Cost: the pipeline infers types from the data. A local feed that changes a column from a number to text
triggers a rescue, not an error, so the data quality checks in notebook 10 must catch it.

## 2. Keep local column names in bronze, harmonize after the review

The bronze tables store the local columns as delivered. The mapping to the group model happens in the publish
job, after a person approves the mappings. The pipeline stays the same for every country, and a mapping
change never requires a pipeline change.

Rejected: apply the mappings inside the pipeline. A mapping is a human decision with an audit trail. It
changes on a different schedule from the data.

Cost: two orchestration units (the pipeline and the publish job) instead of one.

## 3. The model proposes, a person decides

The model (`ai_query` on a Foundation Model API endpoint) proposes a target column, a match type, a
confidence, and a rationale for each local column. A data steward approves, corrects, or rejects each
proposal. The review gate (notebook 06) stops the publish job while a mandatory column is still pending.

Rejected: apply HIGH-confidence proposals automatically. The harmonized numbers feed group reporting, so
each mapping needs a named approver. The evaluation (notebook 11) measures how often HIGH proposals are
correct. That measurement is the evidence a team needs before it relaxes the rule for some columns.

Cost: a person must look at every column once per feed version.

## 4. Lakebase holds the review workflow state

The review queue, the decisions, and the audit trail live in Lakebase Postgres. The app writes one row per
click in a single transaction (the decision and its audit record together). Lakebase scales to zero after five
minutes without traffic.

Rejected: DML on Delta tables through the SQL warehouse, which the first version of the app used. Each
click then started a warehouse statement, concurrent reviewers wrote to the same Delta table, and the
decision and its audit row were two statements, not one transaction.

Rejected: Lakebase synced tables for the queue. A synced table is read-only in Postgres, and the queue is
the table that reviewers change. The queue holds tens of rows per country, so the job writes it with a
plain upsert, and the publish job reads the decisions back into Delta.

Cost: two copies of the review state (Lakebase for operations, Delta for lineage and Genie). Notebook 06
synchronizes them before every publish run.

## 5. One run per country

Both jobs take `source_country` as a parameter. The publish job replaces only that country's rows in
`harmonized_property_monthly` (`replaceWhere`). One country's review does not block another country's close,
and a failed run affects one country only.

Rejected: one job that processes all countries. A single pending column in one country would hold every
country.

## 6. Reuse approved mappings as examples for the next country

When the Italian feed arrives, notebook 04 adds the approved Spanish mappings to the prompt as examples.
The second country gets the decisions that stewards already made.

Cost: a wrong approval in one country can bias the proposals for the next one. The evaluation per country
shows this effect if it occurs.

## 7. Translate category values only into the allowed values

Local feeds use local category values (`Agente`, `Diretto`). Notebook 09 asks the model to translate them,
and it accepts a translation only when the result is in the list of allowed values of the group model. A value
that does not map stays as delivered, and the data quality check `values_in_allowed_list` reports it.

Rejected: free translation by the model. The model can then invent a category, and the group KPIs by
channel or segment become wrong without any error.

## 8. Define the KPIs once in a metric view

`mv_group_property_kpis` defines gross written premium, loss ratio, expense ratio, and combined ratio once.
The app and the Genie space both query this metric view, so they cannot disagree on a ratio.

Rejected: a SQL formula in each consumer.

## 9. Govern by country with a row filter

A subsidiary steward sees only the rows of the steward's own country. Group Actuarial sees all countries. The row
filter `country_row_filter` on `harmonized_property_monthly` enforces this in Unity Catalog, so it also applies
to Genie and to every SQL query.

Cost: the filter checks account group membership. The demo workspace has no such groups, so the deployer and the
app service principal are in the privileged list of the filter. Production uses the groups.

## 10. Keep the Streamlit app

The existing review app has tests and a reviewed interface, so this build extends it. It keeps Streamlit
instead of a new React application.

Cost: Streamlit reruns the script on each interaction. The app caches the Lakebase connection and keeps
the analytics reads on the SQL warehouse.
