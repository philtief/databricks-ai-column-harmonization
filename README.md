# Column Mapping to Global Model

AI-powered column harmonization from local source schemas to a standardized global English data model. A Databricks workflow uses an LLM to propose column mappings, then a Streamlit review app lets humans approve, correct, or reject each mapping before the harmonized output is created.

Bring your own local dataset and target data model. The framework handles the mapping, review, and transformation.

## Architecture

```
Raw Source Data (local columns, any language)
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

- Databricks workspace with Unity Catalog enabled (Runtime 13.0+)
- SQL Warehouse (Serverless recommended)
- Databricks CLI configured (`databricks auth login`)
- A catalog you have `CREATE SCHEMA` permissions on
- Your raw source table already loaded into Databricks
- Python 3.10+ (only needed for local linting and tests, not for deployment)

## Quick Start

```bash
# 1. Clone
git clone https://github.com/philtief/databricks-column-harmonization.git
cd databricks-column-harmonization

# 2. Create your config from the template
cp config/harmonization_config.yaml.template config/harmonization_config.yaml

# 3. Edit config/harmonization_config.yaml
#    - Set source_context: domain, source_system, source_table
#    - Define target_model: your global column names, types, descriptions
#    - List mandatory_source_columns and semantic_fields

# 4. Edit databricks.yml
#    - Set the catalog_name default parameter
#    - Remove the generate_demo_data task (it's for the demo only)
#    - Update inventory_source_columns to depend only on create_global_model_and_control_tables

# 5. Deploy and run
databricks bundle deploy
databricks bundle run Column_Mapping_To_Global_Model
```

## Configuration: `config/harmonization_config.yaml`

This is the main file you edit to adapt the solution to your domain. See `config/harmonization_config.yaml.template` for the format.

| Section | What it does |
|---------|-------------|
| `source_context` | Domain description fed to the LLM when proposing mappings. Be specific about the source language, business domain, and naming conventions. |
| `target_model.columns` | The global English column definitions (name, type, description, examples). Each column's `description` is read by the LLM to find the best match. |
| `mandatory_source_columns` | Source columns that must be reviewed and approved before the workflow proceeds past the review gate. |
| `semantic_fields` | Target fields eligible for optional value mapping (e.g., translating category values from German to English). |
| `ai` | LLM endpoint and token cost estimates for monitoring. |

## Workflow Steps

| Step | Notebook | Purpose |
|------|----------|---------|
| 00 | `bootstrap_catalog` | Create catalog and schemas if they don't exist |
| 01 | `generate_demo_data` | *(Demo only)* Generate synthetic Spain data. Remove this task for production. |
| 02 | `create_global_model_and_control_tables` | Create harmonized table (from config), control tables, and views |
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
- Move approved mappings back to pending if needed
- Track progress toward mandatory column coverage
- Signal completion to unblock the workflow gate

Set these environment variables in `app.yaml` after deployment:

- `CATALOG_NAME` -- your catalog name
- `DATABRICKS_WAREHOUSE_ID` -- SQL Warehouse ID
- `WORKFLOW_JOB_ID` -- Job ID (set after deploying the workflow)

## Project Structure

```
├── databricks.yml                  # Databricks Asset Bundle definition (jobs + app)
├── config/
│   ├── harmonization_config.yaml   # Your domain config (copy from template)
│   └── harmonization_config.yaml.template  # Config template with placeholders
├── notebooks/                      # Workflow notebooks (00, 02-10) + shared utils
├── examples/
│   └── spain_demo/                 # Demo: Spain property insurance data generator
├── apps/column_mapping_review_app/ # Streamlit review app (deployed as Databricks App)
├── src/harmonization/              # Extracted Python modules (testable)
│   ├── config.py                   # Config loader
│   ├── constants.py                # Framework constants
│   ├── data_generation.py          # Demo data generation logic
│   └── validation.py               # Quality check functions
├── tests/                          # pytest test suite (73 tests, 100% coverage on src/)
├── scripts/                        # Lint and test wrapper scripts
└── pyproject.toml                  # Python project configuration
```

## Running the Demo

A Spain property insurance demo is included in `examples/spain_demo/`. To try it:

1. Use the shipped `config/harmonization_config.yaml` as-is (it contains the Spain config)
2. Set the `catalog_name` in `databricks.yml` to your catalog
3. Deploy and run — the `generate_demo_data` task creates 10k synthetic rows
4. Review mappings in the Streamlit app, then re-run the workflow to complete

See `examples/spain_demo/README.md` for details.

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
