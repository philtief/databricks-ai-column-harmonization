# Column Mapping to Global Model

AI-powered column harmonization from local source schemas to a standardized global English data model, with human-in-the-loop review via a Databricks App.

## Setup Guide: Deploying to a Customer's Databricks Workspace

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- SQL Warehouse (Serverless recommended)
- A catalog you have `CREATE SCHEMA` permissions on (or an existing catalog)
- Databricks CLI installed and authenticated (`databricks auth login`)
- Git and Python 3.10+ installed locally

### Step 1: Clone and Configure CLI

```bash
git clone https://github.com/philtief/databricks-column-harmonization.git
cd databricks-column-harmonization

# Authenticate with the target workspace
databricks auth login --host https://<workspace>.cloud.databricks.com
```

Verify your CLI profile works: `databricks current-user me`

### Step 2: Create `config/harmonization_config.yaml`

Copy the template and fill in your values:
```bash
cp config/harmonization_config.yaml.template config/harmonization_config.yaml
```

Edit each section:

**source_context** — Describe the source data for the LLM:
```yaml
source_context:
  domain: "Germany motor insurance quarterly reporting"
  source_system: "DE_MOTOR_RAW"
  source_table: "motor_insurance_quarterly_raw"
  description: >
    Quarterly reporting data from Germany motor insurance.
    Source columns are in German. Target model uses English names.
```

**target_model** — Define the ground truth global data model. Each column's `description` is read by the LLM to find the best match:
```yaml
target_model:
  table_name: "motor_insurance_quarterly"
  columns:
    - name: policy_id
      type: STRING
      description: "Unique policy identifier"
      examples: ["DE-001-2024"]
      required: true
      semantic_group: policy
    # ... define all target columns
```

**mandatory_source_columns** — Source columns that must be reviewed before the workflow proceeds:
```yaml
mandatory_source_columns:
  - versicherungsnummer
  - quartal
  # ... list your critical source columns
```

**semantic_fields** — Target columns eligible for optional value translation (e.g., German to English categories):
```yaml
semantic_fields:
  - vehicle_type
  - coverage_type
```

### Step 3: Edit `databricks.yml`

Set the `catalog_name` parameter default to your catalog:

```yaml
parameters:
  - name: catalog_name
    default: your_catalog_name    # <-- change this
  - name: schema_name
    default: harmonizing_agent
  - name: source_country
    default: Germany              # <-- change this
```

If your workspace uses a CLI profile, uncomment and set:
```yaml
workspace:
  profile: your-profile-name
```

### Step 4: Remove the Demo Data Task

The workflow includes a demo task (`generate_demo_data`) that creates synthetic Spain data. For production use with your own data:

1. In `databricks.yml`, delete the `generate_demo_data` task
2. Update `inventory_source_columns` to depend only on `create_global_model_and_control_tables`
3. Ensure your raw source table already exists in `{catalog_name}.{schema_name}`

To test with the Spain demo first, skip this step — the demo task will create sample data.

### Step 5: Deploy

```bash
databricks bundle deploy --target dev
```

This creates:
- The Databricks workflow job (11 tasks)
- The Streamlit review app

### Step 6: Configure the Databricks App

After deployment, get the job ID:
```bash
databricks jobs list --name Column_Mapping_To_Global_Model --output json | jq '.[0].job_id'
```

Get the SQL Warehouse ID:
```bash
databricks warehouses list --output json | jq '.[0].id'
```

Edit `apps/column_mapping_review_app/app.yaml` with these values:
```yaml
env:
  - name: CATALOG_NAME
    value: "your_catalog_name"
  - name: DATABRICKS_WAREHOUSE_ID
    value: "abc123def456"
  - name: WORKFLOW_JOB_ID
    value: "123456789"
```

Then redeploy:
```bash
databricks bundle deploy --target dev
```

### Step 7: Grant Permissions to the App Service Principal

The Databricks App runs as an auto-created service principal. Grant it access:

```sql
-- Run in SQL Editor or a notebook
GRANT USE CATALOG ON CATALOG your_catalog_name TO `<app-service-principal-id>`;
GRANT USE SCHEMA ON SCHEMA your_catalog_name.harmonizing_agent TO `<app-service-principal-id>`;
GRANT SELECT, MODIFY ON SCHEMA your_catalog_name.harmonizing_agent TO `<app-service-principal-id>`;
```

Also grant `CAN_USE` on the SQL Warehouse to the service principal (via Warehouse permissions UI or API).

Find the service principal ID in the Databricks App settings page.

### Step 8: Run the Workflow

```bash
databricks bundle run Column_Mapping_To_Global_Model
```

The workflow will:
1. Bootstrap catalog and schema (task 00)
2. Generate or load source data (task 01)
3. Create all control tables and the global target model (task 02)
4. Inventory source columns with sample values (task 03)
5. Use AI to propose column mappings (task 04)
6. Prepare review views (task 05)
7. **PAUSE at the review gate** (task 06) — waiting for human review

### Step 9: Review Mappings in the Databricks App

Open the app URL (shown in the Databricks Apps page). For each source column:
- **Approve** if the AI proposal is correct
- **Correct** if you want a different target column
- **Reject** if the column should be excluded

All mandatory columns must be approved/corrected before the gate passes.

### Step 10: Resume the Workflow

After reviewing, re-run the workflow. It will pick up from the review gate:
```bash
databricks bundle run Column_Mapping_To_Global_Model
```

Tasks 07-10 run automatically:
- Build the approved mapping dictionary
- Apply mappings to create the harmonized table
- Optionally translate categorical values
- Run data quality checks

### Verification

Check the harmonized output:
```sql
SELECT * FROM your_catalog_name.harmonizing_agent.motor_insurance_quarterly LIMIT 10;
```

Check data quality results:
```sql
SELECT * FROM your_catalog_name.harmonizing_agent.data_quality_results ORDER BY recorded_at DESC;
```

## Project Conventions

- **Config-driven**: All domain customization is in `config/harmonization_config.yaml`. Do not hardcode domain-specific values in notebooks.
- **Table names are generic**: No country suffixes. Country isolation is handled at the schema level.
- **Config is mandatory**: Notebooks fail loudly if config YAML is missing. No silent fallbacks.
- **All notebooks are domain-agnostic**: Demo data generation lives in `examples/spain_demo/`.
- **Pre-commit hooks**: Black (line-length 120), flake8, mypy, pytest with 80% minimum coverage.
- **Tests**: `pytest tests/ --cov=src --cov-report=term-missing` (73 tests, 100% coverage on src/).
- **Linting**: `bash scripts/lint.sh` runs black, flake8, mypy.

## Key Files

| File | Purpose |
|------|---------|
| `config/harmonization_config.yaml` | Single source of truth for domain configuration |
| `config/harmonization_config.yaml.template` | Config template with placeholders for new deployments |
| `databricks.yml` | DAB definition: workflow job + Streamlit app |
| `notebooks/_shared_utils.py` | Shared helpers: logging, config loading, table refs |
| `notebooks/00, 02-10` | Workflow notebooks (all domain-agnostic) |
| `examples/spain_demo/` | Demo data generator (Spain property insurance) |
| `apps/column_mapping_review_app/app.py` | Streamlit review app |
| `src/harmonization/` | Extracted Python modules (config loader, validation, constants) |
| `tests/` | pytest test suite |
