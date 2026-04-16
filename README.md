# Column Mapping to Global Model

An 11-step Databricks workflow that uses AI to map source-specific column names (Spain property insurance) to a standardized global English data model. Includes a Streamlit review app for human-in-the-loop approval before mappings are applied.

## Architecture

```
Raw Source Data (Spanish columns)
        │
        ▼
┌─────────────────────────────┐
│  AI Column Mapping Engine   │  ← Uses databricks-gpt-5-2 via ai_query()
│  (proposes mappings with    │
│   confidence scores)        │
└────────────┬────────────────┘
             │
             ▼
┌─────────────────────────────┐
│  Streamlit Review App       │  ← Human reviews, approves, or overrides
│  (deployed as Databricks    │     each column mapping
│   App)                      │
└────────────┬────────────────┘
             │
             ▼
┌─────────────────────────────┐
│  Apply & Validate           │  ← Approved mappings applied to create
│  (harmonized table +        │     harmonized output table
│   quality checks)           │
└─────────────────────────────┘
```

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- SQL Warehouse (Serverless recommended)
- Databricks CLI configured (`databricks auth login`)
- A catalog you have `CREATE SCHEMA` permissions on

## Quick Start

```bash
# 1. Clone
git clone https://github.com/philtief/databricks-column-harmonization.git
cd databricks-column-harmonization

# 2. Configure databricks.yml
#    - Set your catalog name in the default parameter
#    - Ensure your Databricks CLI profile is configured

# 3. Deploy
databricks bundle deploy
databricks bundle run Column_Mapping_To_Global_Model
```

## Workflow Steps

| Step | Notebook | Purpose |
|------|----------|---------|
| 00 | `bootstrap_catalog` | Create catalog and schemas if they don't exist |
| 01 | `generate_spain_raw_data` | Generate synthetic Spain property insurance data |
| 02 | `create_global_model_and_control_tables` | Define 23-column global English target model and control tables |
| 03 | `inventory_source_columns` | Catalog all source columns with metadata and sample values |
| 04 | `ai_propose_column_mappings` | AI proposes column mappings with confidence scores and match types |
| 05 | `prepare_app_review_views` | Create views for the Streamlit review app |
| 06 | `column_mapping_review_gate` | **Human gate** — pauses workflow until all mandatory columns are approved |
| 07 | `build_column_mapping_dictionary` | Build final mapping dictionary from approved mappings |
| 08 | `apply_approved_column_mappings` | Apply mappings to transform raw data into harmonized table |
| 09 | `optional_value_mapping` | Apply optional value-level transformations (e.g., translate category values) |
| 10 | `validate_and_monitor` | Run quality checks: row parity, null counts, business rule validation |

## Review App

The Streamlit app (`apps/column_mapping_review_app/`) lets reviewers:

- See AI-proposed mappings with confidence scores (HIGH / MEDIUM / LOW)
- Approve, reject, or override each mapping
- Track progress toward mandatory column coverage
- Signal completion to unblock the workflow gate

Deploy it as a Databricks App after the workflow creates the review views (step 05).

## Configuration

### `databricks.yml`

Key parameters to customize:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `catalog_name` | `my_catalog` | Unity Catalog catalog to use |
| `schema_name` | `harmonizing_agent` | Schema for all tables and views |
| `raw_table_name` | `spain_property_insurance_raw` | Source table name |
| `harmonized_table_name` | `property_insurance_monthly` | Output harmonized table |
| `num_rows` | `5000` | Number of synthetic rows to generate |
| `model_name` | `databricks-gpt-5-2` | Foundation model for AI mapping |

### Review App (`app.yaml`)

Set these environment variables before deploying the app:

- `CATALOG_NAME` — your catalog name
- `SCHEMA_NAME` — schema name (default: `harmonizing_agent`)
- `DATABRICKS_WAREHOUSE_ID` — SQL Warehouse ID
- `WORKFLOW_JOB_ID` — Job ID (set after deploying the workflow)

## Adapting for Another Country

To map a different source dataset to the same global model:

1. Replace notebook `01` with your own data source (or point to an existing table)
2. The 23-column global model in notebook `02` stays the same
3. Run the workflow — AI will propose new mappings for your columns
4. Review and approve via the Streamlit app
5. Notebooks `07`–`10` apply the approved mappings automatically

## Project Structure

```
├── databricks.yml                  # Databricks Asset Bundle definition
├── notebooks/                      # 11 workflow notebooks (00–10)
├── apps/column_mapping_review_app/ # Streamlit review app
├── workflow/deploy_workflow.py     # Alternative: deploy via SDK
├── sql/review_queries.sql          # SQL queries for review views
├── src/harmonization/              # Extracted Python modules (testable)
│   ├── constants.py                # Shared constants (mandatory columns, etc.)
│   ├── data_generation.py          # Synthetic data generation logic
│   └── validation.py               # Quality check functions
├── tests/                          # pytest test suite (64 tests, 100% coverage on src/)
├── scripts/                        # Lint and test wrapper scripts
└── pyproject.toml                  # Python project configuration
```

## Development

```bash
# Create venv and install dev dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Run tests
pytest tests/ --cov=src --cov-report=term-missing

# Run linting
black --check --line-length 120 src/ tests/ apps/
flake8 src/ tests/ apps/ notebooks/
mypy src/
```

Pre-commit hooks enforce linting and tests before every commit.
