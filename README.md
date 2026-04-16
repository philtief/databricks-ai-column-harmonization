# Column Mapping to Global Model

An 11-step Databricks workflow that uses AI to map source-specific column names to a standardized global English data model. Includes a Streamlit review app for human-in-the-loop approval before mappings are applied.

Ships with a Spain property insurance example. Adapt it to your own domain by editing `config/harmonization_config.yaml`.

## Architecture

```
Raw Source Data (local columns)
        |
        v
+-----------------------------+
|  AI Column Mapping Engine   |  <- Uses ai_query() with configurable LLM
|  (proposes mappings with    |
|   confidence scores)        |
+-------------+---------------+
              |
              v
+-----------------------------+
|  Streamlit Review App       |  <- Human reviews, approves, or overrides
|  (deployed as Databricks    |     each column mapping
|   App)                      |
+-------------+---------------+
              |
              v
+-----------------------------+
|  Apply & Validate           |  <- Approved mappings build the harmonized
|  (harmonized table +        |     output table with quality checks
|   quality checks)           |
+-----------------------------+
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

# 2. Edit config/harmonization_config.yaml
#    - Set your source context (domain, language, source system)
#    - Define your target data model (column names, types, descriptions)
#    - List mandatory source columns and semantic fields

# 3. Configure databricks.yml
#    - Set the catalog_name default parameter
#    - Ensure your Databricks CLI profile is configured

# 4. Deploy and run
databricks bundle deploy
databricks bundle run Column_Mapping_To_Global_Model
```

## Configuration: `config/harmonization_config.yaml`

This is the main file you edit to adapt the solution to your domain. It controls:

| Section | What it does |
|---------|-------------|
| `source_context` | Domain description fed to the LLM when proposing mappings. Be specific about the source language, business domain, and naming conventions. |
| `target_model.columns` | The global English column definitions (name, type, description, examples). Each column's `description` is read by the LLM to find the best match. |
| `mandatory_source_columns` | Source columns that must be reviewed and approved before the workflow proceeds past the review gate. |
| `semantic_fields` | Target fields eligible for optional value mapping (e.g., translating category values from Spanish to English). |
| `ai` | LLM endpoint and token cost estimates for monitoring. |

## Workflow Steps

| Step | Notebook | Purpose |
|------|----------|---------|
| 00 | `bootstrap_catalog` | Create catalog and schemas if they don't exist |
| 01 | `generate_spain_raw_data` | Generate synthetic Spain property insurance data |
| 02 | `create_global_model_and_control_tables` | Define global target model (from config) and all control tables |
| 03 | `inventory_source_columns` | Catalog all source columns with metadata and sample values |
| 04 | `ai_propose_column_mappings` | AI proposes column mappings with confidence scores and match types |
| 05 | `prepare_app_review_views` | Create views for the Streamlit review app |
| 06 | `column_mapping_review_gate` | **Human gate** -- pauses workflow until all mandatory columns are approved |
| 07 | `build_column_mapping_dictionary` | Build final mapping dictionary from approved mappings |
| 08 | `apply_approved_column_mappings` | Apply mappings to transform raw data into harmonized table |
| 09 | `optional_value_mapping` | Apply optional value-level translations (e.g., Spanish to English categories) |
| 10 | `validate_and_monitor` | Run quality checks: row parity, null counts, business rule validation |

## Review App

The Streamlit app (`apps/column_mapping_review_app/`) is deployed as a Databricks App and lets reviewers:

- See AI-proposed mappings with confidence scores (HIGH / MEDIUM / LOW)
- Approve, reject, or override each mapping
- Track progress toward mandatory column coverage
- Signal completion to unblock the workflow gate

Set these environment variables in `app.yaml` after deployment:

- `CATALOG_NAME` -- your catalog name
- `DATABRICKS_WAREHOUSE_ID` -- SQL Warehouse ID
- `WORKFLOW_JOB_ID` -- Job ID (set after deploying the workflow)

## Adapting for Another Country / Domain

1. Edit `config/harmonization_config.yaml`:
   - Update `source_context` with your domain description
   - Define your target columns in `target_model.columns` with LLM-friendly descriptions
   - Set your `mandatory_source_columns`
2. Replace notebook `01` with your own data source (or point to an existing table)
3. Run `databricks bundle deploy && databricks bundle run Column_Mapping_To_Global_Model`
4. Review and approve mappings via the Streamlit app
5. Notebooks `07`-`10` apply the approved mappings automatically

## Project Structure

```
├── databricks.yml                  # Databricks Asset Bundle definition (jobs + app)
├── config/
│   └── harmonization_config.yaml   # Domain config: target model, AI context, mandatory columns
├── notebooks/                      # 11 workflow notebooks (00-10) + shared utils
├── apps/column_mapping_review_app/ # Streamlit review app (deployed as Databricks App)
├── sql/review_queries.sql          # SQL queries for review views
├── src/harmonization/              # Extracted Python modules (testable)
│   ├── config.py                   # Config loader
│   ├── constants.py                # Shared constants (defaults)
│   ├── data_generation.py          # Synthetic data generation logic
│   └── validation.py               # Quality check functions
├── tests/                          # pytest test suite (79 tests, 100% coverage on src/)
├── scripts/                        # Lint and test wrapper scripts
└── pyproject.toml                  # Python project configuration
```

## Development

```bash
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
