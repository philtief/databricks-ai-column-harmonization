# Setup Guide

End-to-end guide for bringing your own raw data and target schema to the column harmonization workflow.

If you only want to try the shipped Spain property-insurance demo first, jump to [Path A](#path-a-try-the-spain-demo). Otherwise follow [Path B](#path-b-bring-your-own-data) for production setup.

---

## Prerequisites

- Databricks workspace with Unity Catalog enabled (Runtime 13.0+ or Serverless)
- A SQL Warehouse you can query (Serverless recommended)
- `databricks` CLI installed and authenticated (`databricks auth login --host https://<workspace>`)
- A catalog where you have `CREATE SCHEMA` (or an existing schema you can write to)
- Python 3.10+ locally (only for tests/lint)

Check authentication:

```bash
databricks current-user me
```

---

## Path A: Try the Spain Demo

The Spain demo creates 10k synthetic Spanish-language insurance rows and walks the entire workflow end-to-end. Use it to verify your setup before bringing real data.

```bash
git clone https://github.com/philtief/databricks-column-harmonization.git
cd databricks-column-harmonization

# Use the demo config
cp examples/spain_demo/harmonization_config.yaml config/harmonization_config.yaml

# Set your catalog in databricks.yml
#   parameters:
#     - name: catalog_name
#       default: <your_catalog>

databricks bundle deploy
databricks bundle run Column_Mapping_To_Global_Model
```

The workflow will pause at the review gate (task 06). Open the Streamlit app, approve the proposed mappings, then re-run. See `examples/spain_demo/README.md` for a deeper tour.

---

## Path B: Bring Your Own Data

This is the path for real work. You define **two things**:

1. Your **local source schema** — the raw table that already exists in your workspace.
2. Your **global target model** — the harmonized schema you want to produce.

The framework matches them with an LLM and a human reviewer in the loop.

### Step 1: Clone and Authenticate

```bash
git clone https://github.com/philtief/databricks-column-harmonization.git
cd databricks-column-harmonization
databricks auth login --host https://<workspace>.cloud.databricks.com
databricks current-user me
```

### Step 2: Identify Your Local Source Table

Your raw data must already live in Unity Catalog. If you have it as files, load it once into a Delta table:

```python
spark.read.parquet("/Volumes/your_catalog/raw/landing/")\
     .write.mode("overwrite").saveAsTable("your_catalog.your_schema.your_raw_table")
```

You will need to know:

- The **catalog**, **schema**, and **table name** of your raw data.
- Roughly what columns exist and what they mean. The framework introspects column names, types, and a sample of values automatically — but the more you know, the better you can write the LLM prompt context.

Quickly inventory your raw table:

```sql
DESCRIBE TABLE your_catalog.your_schema.your_raw_table;
SELECT * FROM your_catalog.your_schema.your_raw_table LIMIT 20;
```

### Step 3: Define Your Global Target Model

Open `config/harmonization_config.yaml`. The shipped file is a generic CRM starter — replace its sections to match your domain. Or copy the placeholder template:

```bash
cp config/harmonization_config.yaml.template config/harmonization_config.yaml
```

Fill in **five sections**:

#### `source_context` — describes the raw side to the LLM

```yaml
source_context:
  domain: "EU motor insurance quarterly reporting"
  source_system: "DE_MOTOR_RAW"             # any short identifier
  source_table: "motor_insurance_quarterly_raw"  # must match your raw table name
  description: >
    Quarterly insurance reporting data. Source columns are in German, mixed with
    business shorthand. Target uses standard English names.
```

Be specific in `description`. The LLM reads it directly. Mention source language, business domain, naming conventions — anything that helps disambiguate similar columns.

#### `target_model.columns` — your global schema

This is the heart of the configuration. Each entry tells the LLM: "this is a valid mapping target." The `description` is the disambiguator — write it for an LLM reader.

```yaml
target_model:
  table_name: "motor_insurance_quarterly"
  columns:
    - name: policy_id
      type: STRING
      description: "Unique policy identifier assigned at underwriting"
      examples: ["DE-001-2024", "DE-002-2024"]
      required: true
      semantic_group: policy

    - name: gross_written_premium_eur
      type: DOUBLE
      description: "Gross written premium in EUR, before reinsurance"
      examples: [1250.50, 890.00]
      required: true
      semantic_group: premium

    - name: country_of_risk
      type: STRING
      description: "ISO 3166 country name where the risk is located"
      examples: ["Germany", "Austria"]
      required: false
      semantic_group: geography
```

Field guide:

| Field | Required | Purpose |
|-------|----------|---------|
| `name` | yes | Target column name (lowercase, snake_case, must start with a letter) |
| `type` | yes | SQL type: `STRING`, `INT`, `BIGINT`, `DOUBLE`, `TIMESTAMP`, `DATE`, etc. |
| `description` | yes | Business definition — read by the LLM. Be precise. |
| `examples` | no | Sample values shown to the LLM. Helps disambiguate similar columns. |
| `required` | no | If `true`, the harmonized table must have this column populated; not-null is enforced in notebook 10. |
| `semantic_group` | no | Logical grouping shown in the review app. |

#### `mandatory_source_columns` — gating the human review

List the **local** column names (from your raw table) that absolutely must be reviewed before the workflow proceeds. The review gate (notebook 06) blocks on these.

```yaml
mandatory_source_columns:
  - policy_nr
  - bruttopraemie
  - schadenkosten
```

#### `semantic_fields` — optional value-level translation

Target columns whose **values** may need translation (e.g., German categories → English). Notebook 09 applies these only if approved entries exist in the value-mapping dictionary; otherwise this is a no-op.

```yaml
semantic_fields:
  - lifecycle_stage
  - country_of_risk
```

#### `ai` — LLM endpoint and (optional) prompt customization

Sensible defaults are used when omitted. Most users only set `endpoint`:

```yaml
ai:
  endpoint: "databricks-gpt-5-2"   # any Foundation Model endpoint
```

Advanced customization (full prompt template, custom vocabularies, extra context) is documented in [README.md → Customizing the LLM](../README.md#customizing-the-llm).

#### `data_quality_rules` — domain business rules (optional)

Add SQL-predicate constraints evaluated against the harmonized table. Always-on checks (row parity, mapping coverage, not-null, mapping version) run regardless.

```yaml
data_quality_rules:
  - name: gross_gte_net
    description: "gross_written_premium_eur >= net_written_premium_eur"
    predicate: >
      gross_written_premium_eur IS NOT NULL
      AND net_written_premium_eur IS NOT NULL
      AND gross_written_premium_eur < net_written_premium_eur
    severity: FAILED   # or WARNING

  - name: ratio_in_range
    description: "loss_ratio in [0, 2]"
    predicate: "loss_ratio IS NOT NULL AND (loss_ratio < 0 OR loss_ratio > 2)"
    severity: WARNING
```

Rows where the predicate is `TRUE` are counted as violations. `FAILED` raises a workflow exception; `WARNING` only logs.

### Step 4: Edit `databricks.yml`

Set the workspace and catalog:

```yaml
parameters:
  - name: catalog_name
    default: your_catalog        # ← change this
  - name: schema_name
    default: harmonizing_agent   # ← keep or change
```

If your CLI uses a profile (instead of env vars), uncomment:

```yaml
workspace:
  profile: your-profile-name
```

### Step 5: Remove the Demo Data Task

For production with your own data, the synthetic generator is not needed. Edit `databricks.yml`:

1. Delete the `generate_demo_data` task entry.
2. In the `inventory_source_columns` task, remove `- task_key: generate_demo_data` from `depends_on` so it depends only on `create_global_model_and_control_tables`.

### Step 6: Deploy

```bash
databricks bundle deploy --target dev
```

This creates the workflow job (10 tasks for production / 11 with the demo) and the Streamlit review app.

### Step 7: Configure the App

The app needs three IDs as environment variables. Get them:

```bash
databricks jobs list --name Column_Mapping_To_Global_Model --output json | jq -r '.[0].job_id'
databricks warehouses list --output json | jq -r '.[0].id'
```

Edit `apps/column_mapping_review_app/app.yaml`:

```yaml
env:
  - name: CATALOG_NAME
    value: "your_catalog"
  - name: SCHEMA_NAME
    value: "harmonizing_agent"
  - name: DATABRICKS_WAREHOUSE_ID
    value: "abc123def456"
  - name: WORKFLOW_JOB_ID
    value: "123456789"
```

Redeploy:

```bash
databricks bundle deploy --target dev
```

### Step 8: Grant Permissions to the App Service Principal

The app runs as an auto-created service principal that needs catalog, schema, and warehouse access:

```sql
GRANT USE CATALOG ON CATALOG your_catalog TO `<app-sp-id>`;
GRANT USE SCHEMA ON SCHEMA your_catalog.harmonizing_agent TO `<app-sp-id>`;
GRANT SELECT, MODIFY ON SCHEMA your_catalog.harmonizing_agent TO `<app-sp-id>`;
```

Plus `CAN_USE` on the SQL Warehouse (Warehouse permissions UI). Service principals are not in the default `users` group.

Find the app SP ID on the Databricks Apps page or via `databricks apps list`.

### Step 9: Run the Workflow

```bash
databricks bundle run Column_Mapping_To_Global_Model
```

The workflow runs through to the review gate (task 06) and pauses. Tasks 00–05 finish in seconds for small tables, minutes for larger ones.

### Step 10: Review Mappings in the App

Open the app URL (visible in the Databricks Apps page). For each source column:

- **Approve** the AI proposal as-is, or
- **Correct** it to a different target column, or
- **Reject** if the column should be excluded from the harmonized output.

All `mandatory_source_columns` must reach a non-PENDING state before the gate releases. The app shows progress toward that gate.

### Step 11: Resume the Workflow

```bash
databricks bundle run Column_Mapping_To_Global_Model
```

Tasks 07–10 run automatically:

- 07 builds the column-mapping dictionary from approved candidates
- 08 transforms the raw table into the harmonized table
- 09 applies optional value-level translations (no-op if empty)
- 10 runs always-on + config-driven data quality checks

If quality checks fail, the workflow raises an exception. Inspect `data_quality_results` in your schema for the run details.

---

## Worked Example: German motor insurance → English global model

Suppose your raw table `de_motor_raw.motor_quarterly_raw` looks like this:

| police_nr | bruttopraemie | nettoschadenaufwand | quartal     |
|-----------|---------------|---------------------|-------------|
| DE-001    | 1250.50       | 800.00              | 2024-Q3     |

You want a clean English schema. Here is the minimal config:

```yaml
source_context:
  domain: "Germany motor insurance quarterly"
  source_system: "DE_MOTOR_RAW"
  source_table: "motor_quarterly_raw"
  description: >
    Quarterly motor insurance figures. Source columns are German.
    Target uses English snake_case.

target_model:
  table_name: "motor_insurance_quarterly"
  columns:
    - name: policy_id
      type: STRING
      description: "Unique policy identifier"
      examples: ["DE-001"]
      required: true
      semantic_group: policy

    - name: gross_written_premium_eur
      type: DOUBLE
      description: "Gross written premium in EUR, before reinsurance"
      required: true
      semantic_group: premium

    - name: net_claims_incurred_eur
      type: DOUBLE
      description: "Net claims incurred in EUR (claims paid + reserves change)"
      required: true
      semantic_group: claims

    - name: reporting_period
      type: STRING
      description: "Reporting period in YYYY-Qn format"
      examples: ["2024-Q3"]
      required: true
      semantic_group: time

mandatory_source_columns:
  - police_nr
  - bruttopraemie
  - nettoschadenaufwand

semantic_fields: []

ai:
  endpoint: "databricks-gpt-5-2"
```

The LLM proposes:

| Local            | Proposed target              | Match type | Confidence |
|------------------|------------------------------|------------|------------|
| police_nr        | policy_id                    | DIRECT     | HIGH       |
| bruttopraemie    | gross_written_premium_eur    | SEMANTIC_TRANSLATION | HIGH |
| nettoschadenaufwand | net_claims_incurred_eur   | SEMANTIC_TRANSLATION | HIGH |
| quartal          | reporting_period             | SEMANTIC_TRANSLATION | MEDIUM |

Reviewer approves in the app, gate releases, harmonized output is produced.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `FATAL: Could not load harmonization config` | `config/harmonization_config.yaml` is missing or malformed. Validate: `python -c "import yaml; yaml.safe_load(open('config/harmonization_config.yaml'))"` |
| App shows `DATABRICKS_WAREHOUSE_ID environment variable is not set` | Set the env vars in `apps/column_mapping_review_app/app.yaml` and redeploy. |
| App shows `No data available yet` | Workflow has not produced candidates yet. Run the workflow first. |
| Review gate fails with `mandatory columns still PENDING` | Open the app, approve/correct all mandatory columns, re-run the workflow. |
| App SP gets `access denied` | Run the GRANT statements in Step 8 plus `CAN_USE` on the warehouse. |
| `ai_query` returns `AI_ERROR` for some columns | LLM call failed for that row. Inspect `mapping_rationale` and `ai_error_status` in `column_mapping_candidates`. Improve the source-column `description` in `target_model`, or supply a richer `prompt_template`. |
| Workflow fails with `Catalog '<x>' does not exist` | Notebook 00 tries to create it; if you lack `CREATE CATALOG`, ask an admin to create the catalog upfront. |

---

## What Gets Created in Your Schema

After a successful run, your schema contains:

| Object | Type | Purpose |
|--------|------|---------|
| `<source_table>` | Table | Your raw data (existing or demo) |
| `<target_table>` | Table | Harmonized output |
| `column_mapping_candidates` | Table | AI proposals + review state |
| `column_mapping_dictionary` | Table | Approved mappings (versioned) |
| `column_mapping_audit` | Table | Append-only audit log |
| `value_mapping_dictionary` | Table | Approved value-level translations |
| `data_quality_results` | Table | DQ check results per run |
| `workflow_run_metrics` | Table | Per-task run metrics |
| `ai_usage_metrics` | Table | LLM call counts and cost estimates |
| `vw_latest_column_mapping_summary` | View | Reviewer-facing summary |

All under `<catalog>.<schema>` — generic naming, no domain suffixes.

---

## Next Steps

- Customize the LLM prompt → [README.md → Customizing the LLM](../README.md#customizing-the-llm)
- Add domain-specific quality rules → [README.md → Data Quality Rules](../README.md#data-quality-rules)
- Run the test suite locally → `pip install -e ".[dev]" && pytest tests/`
- Read project conventions for contributors → [CLAUDE.md](../CLAUDE.md)
