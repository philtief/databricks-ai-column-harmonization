# Column Harmonization on Databricks

AI-powered column harmonization from local source schemas to a configurable global data model. A Databricks workflow uses an LLM to propose column mappings, then a Streamlit review app lets humans approve, correct, or reject each mapping before the harmonized output is created.

Bring your own raw dataset, your own target data model, and (optionally) your own prompt template. The framework handles the mapping, review, and transformation.

## Architecture

```
Raw Source Data (local columns, any language)
        |
        v
+-----------------------------+
|  AI Column Mapping Engine   |  <- ai_query() with a configurable LLM endpoint
|  (proposes mappings with    |     and prompt template (config/harmonization_config.yaml)
|   confidence scores)        |
+-------------+---------------+
              |
              v
+-----------------------------+
|  Streamlit Review App       |  <- Reviewers approve, correct, or reject
|  (deployed as a Databricks  |     each proposed mapping
|   App)                      |
+-------------+---------------+
              |
              v
+-----------------------------+
|  Apply & Validate           |  <- Approved mappings produce the harmonized
|  (harmonized table +        |     table; quality checks (configurable)
|   data quality results)     |     gate the workflow
+-----------------------------+
```

## Prerequisites

- Databricks workspace with Unity Catalog enabled (Runtime 13.0+)
- SQL Warehouse (Serverless recommended)
- Databricks CLI configured (`databricks auth login`)
- A catalog you have `CREATE SCHEMA` permissions on (or an existing catalog)
- Your raw source table already loaded into Databricks
- Python 3.10+ (only needed for local lint/tests, not for deployment)

## Quick Start

```bash
# 1. Clone
git clone https://github.com/philtief/databricks-column-harmonization-dev.git
cd databricks-column-harmonization-dev

# 2. Edit your domain config
# The shipped config/harmonization_config.yaml is a generic CRM starter.
# Replace it with your own values, or copy from an example or template:
#   cp config/harmonization_config.yaml.template config/harmonization_config.yaml

# 3. Edit databricks.yml — set the catalog_name parameter

# 4. Deploy and run
databricks bundle deploy
databricks bundle run Column_Mapping_To_Global_Model
```

For a step-by-step walkthrough — including how to bring your own local source schema and global target model — see [docs/SETUP.md](docs/SETUP.md).

## Configuration: `config/harmonization_config.yaml`

This is the single file you edit to adapt the solution to your domain.

| Section | Purpose |
|---------|---------|
| `source_context` | Domain description fed to the LLM. Be specific about source language, business domain, and naming conventions. |
| `target_model.columns` | Your global column definitions (name, type, description, examples). Each `description` is read by the LLM to find the best match. |
| `mandatory_source_columns` | Source columns that must be reviewed before the workflow proceeds past the review gate. |
| `semantic_fields` | Target fields eligible for optional value mapping (e.g., translating category values across languages). |
| `ai` | LLM endpoint, optional custom prompt template, vocabularies, cost estimates. See *Customizing the LLM* below. |
| `data_quality_rules` | Optional list of SQL-predicate constraints evaluated by notebook 10 against the harmonized table. |

## Customizing the LLM

All LLM knobs live in the `ai:` block. Sensible defaults are used when omitted.

```yaml
ai:
  endpoint: "databricks-gpt-5-2"           # any Databricks Foundation Model endpoint

  # Optional: full prompt template using ${name} placeholders.
  # Static placeholders (substituted before the SQL is built):
  #   ${context}            ${target_columns}   ${match_types}
  #   ${confidence_levels}  ${response_schema}
  # Dynamic placeholders (turned into SQL column references):
  #   ${local_column_name}  ${local_data_type}  ${sample_values}
  prompt_template: |
    You are a data harmonization expert.
    Source column "${local_column_name}" has type ${local_data_type}.
    Sample values: ${sample_values}.
    Domain context: ${context}.
    Valid targets: ${target_columns}.
    Match types: ${match_types}.
    Return JSON: ${response_schema}

  # Optional vocabulary overrides
  match_types: ["DIRECT", "SEMANTIC_TRANSLATION", "DERIVED", "NO_MATCH"]
  confidence_levels: ["HIGH", "MEDIUM", "LOW"]
  response_keys: ["global_column_name", "match_type", "rationale", "confidence"]

  # Optional extra placeholders accessible in prompt_template via ${key}
  extra_context:
    tone: "concise"
    target_language: "English"

  # Cost monitoring
  estimated_prompt_tokens_per_column: 200
  estimated_response_tokens_per_column: 80
  estimated_cost_per_1k_tokens: 0.002
```

The runtime endpoint can also be overridden per job-run via the workflow's `ai_endpoint` widget.

## Data Quality Rules

Always-on checks run regardless of configuration:
- `row_count_parity` — raw row count must equal harmonized row count
- `column_mapping_coverage` — all `mandatory_source_columns` must have active dictionary entries
- `not_null_<col>` — for every `required: true` target column
- `column_mapping_version_present` — every harmonized row carries a recognized `mapping_status`

Add domain-specific business rules in YAML:

```yaml
data_quality_rules:
  - name: gross_gte_net
    description: "gross_premium >= net_premium"
    predicate: "gross_premium IS NOT NULL AND net_premium IS NOT NULL AND gross_premium < net_premium"
    severity: FAILED   # or WARNING

  - name: ratio_in_range
    description: "ratio in [0, 1]"
    predicate: "ratio IS NOT NULL AND (ratio < 0 OR ratio > 1)"
    severity: WARNING
```

Predicates are SQL expressions: rows for which the predicate is `TRUE` are counted as violations.

## Workflow Steps

| Step | Notebook | Purpose |
|------|----------|---------|
| 00 | `bootstrap_catalog` | Create catalog and schema if missing |
| 01 | `generate_demo_data` | *(Demo only)* Generate synthetic data. Remove for production. |
| 02 | `create_global_model_and_control_tables` | Create harmonized table (from config), control tables, views |
| 03 | `inventory_source_columns` | Catalog all source columns with metadata and sample values |
| 04 | `ai_propose_column_mappings` | LLM proposes mappings via vectorized `ai_query()` |
| 05 | `prepare_app_review_views` | Create review views for the Streamlit app |
| 06 | `column_mapping_review_gate` | **Human gate** — pauses workflow until mandatory columns are approved |
| 07 | `build_column_mapping_dictionary` | Build the final mapping dictionary from approved mappings |
| 08 | `apply_approved_column_mappings` | Transform raw data into the harmonized table |
| 09 | `optional_value_mapping` | Apply optional value-level translations |
| 10 | `validate_and_monitor` | Run always-on + config-driven quality checks |

## Review App

The Streamlit app (`apps/column_mapping_review_app/`) runs as a Databricks App and lets reviewers:

- See AI-proposed mappings with confidence scores (HIGH / MEDIUM / LOW)
- Approve, reject, or override each mapping
- Move approved mappings back to pending if needed
- Track progress toward mandatory column coverage
- Signal completion to unblock the workflow gate

After deploying, set these env vars in `app.yaml`:

- `CATALOG_NAME` — your catalog
- `SCHEMA_NAME` — your schema (must match the workflow's `schema_name` parameter)
- `DATABRICKS_WAREHOUSE_ID` — SQL Warehouse ID
- `WORKFLOW_JOB_ID` — Job ID (set after deploying the workflow)

## Project Structure

```
.
├── databricks.yml                  # DAB definition (workflow + app)
├── config/
│   ├── harmonization_config.yaml   # Active config (generic starter by default)
│   └── harmonization_config.yaml.template  # Placeholder template
├── notebooks/                      # Workflow notebooks (00, 02-10) + _shared_utils
├── examples/
│   └── spain_demo/
│       ├── 01_generate_spain_raw_data.py    # Synthetic data generator
│       ├── harmonization_config.yaml         # Spain demo config
│       └── README.md
├── apps/column_mapping_review_app/ # Streamlit review app
├── src/harmonization/              # Pure-Python testable modules
│   ├── config.py                   # Config loader and getters
│   ├── constants.py                # Framework constants
│   ├── llm.py                      # Configurable prompt + ai_query SQL builder
│   ├── tables.py                   # Table reference builder
│   └── validation.py               # Generic data quality helpers
├── tests/                          # pytest test suite
├── scripts/                        # Lint and test wrappers
└── pyproject.toml                  # Python project + ruff configuration
```

## Running the Spain Demo

A Spain property-insurance demo is shipped under `examples/spain_demo/`. To try it:

```bash
# Use the demo's config (overwrites the generic starter)
cp examples/spain_demo/harmonization_config.yaml config/harmonization_config.yaml

# Set the catalog_name in databricks.yml, then deploy
databricks bundle deploy
databricks bundle run Column_Mapping_To_Global_Model
```

The `generate_demo_data` task creates 10k synthetic Spanish-language rows. Review mappings in the Streamlit app, then re-run the workflow to complete.

See `examples/spain_demo/README.md` for details.

## Development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Run tests
pytest tests/ --cov=src --cov-report=term-missing

# Run linting (ruff)
bash scripts/lint.sh
# or directly:
ruff check src/ tests/ apps/ notebooks/ examples/
ruff format --check src/ tests/ apps/
```

CI runs ruff + pytest with a minimum 80% coverage gate (see `.github/workflows/ci.yml`).

## License

Apache 2.0. See `LICENSE`.
