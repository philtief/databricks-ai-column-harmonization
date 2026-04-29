# Column Harmonization on Databricks

AI-powered column harmonization from local source schemas to a configurable global data model, with human-in-the-loop review via a Databricks App.

## Project Conventions

- **Config-driven**: All domain customization lives in `config/harmonization_config.yaml`. Notebooks must not hard-code domain values, column names, or business rules.
- **Generic table names**: No country/region suffix on table names. Multi-source isolation is handled at the catalog/schema level.
- **Config is mandatory**: Notebooks fail loudly if config YAML is missing. No silent fallbacks.
- **Demo data lives in `examples/`**: The Spain property-insurance demo (`examples/spain_demo/`) is one example among many — never special-case the Spain config in the core workflow.
- **Public LLM parameterization**: All LLM knobs (endpoint, prompt template, vocabularies, cost) are read by `harmonization.llm.load_llm_config(yaml)`; the notebook only calls `build_ai_query_sql(...)`. Never inline a prompt string in a notebook.
- **Lint with ruff**: `bash scripts/lint.sh` runs `ruff check` + `ruff format --check`. Pre-commit hooks enforce both.
- **Tests**: `pytest tests/` runs the full suite; CI gates on 80% line coverage of `src/`.

## Architecture

```
config/harmonization_config.yaml
        |
        +-- source_context, target_model, mandatory_source_columns,
        |   semantic_fields, ai (LLM config), data_quality_rules
        v
[ Workflow (notebooks/00,02-10) ]   [ Streamlit Review App ]
        |                                       ^
        v                                       |
  Delta tables in <catalog>.<schema>  <---------+
```

## Repository Layout

```
config/                            Domain config (YAML)
notebooks/                         Workflow notebooks (00, 02-10) + _shared_utils
examples/spain_demo/               Spain property-insurance demo
apps/column_mapping_review_app/    Streamlit review app
src/harmonization/                 Pure-Python testable modules
  config.py                        YAML loader + getters
  constants.py                     Framework invariants
  llm.py                           Prompt template + ai_query SQL builder
  tables.py                        Delta table reference builder
  validation.py                    Generic data quality helpers
tests/                             pytest suite
scripts/lint.sh                    ruff lint + format check
```

## Customer Setup

End-to-end customer setup is documented in [docs/SETUP.md](docs/SETUP.md). It walks through bringing your own local source schema and global target model. Do not duplicate setup steps in this file — keep this file focused on conventions and contributor context.

## Key Files

| File | Purpose |
|------|---------|
| `config/harmonization_config.yaml` | Active domain configuration |
| `config/harmonization_config.yaml.template` | Placeholder template for new deployments |
| `databricks.yml` | DAB definition: workflow job + Streamlit app |
| `notebooks/_shared_utils.py` | Shared helpers: logging, config loader, table refs |
| `notebooks/00, 02-10` | Workflow notebooks (all domain-agnostic) |
| `examples/spain_demo/` | Spain property-insurance demo (data + config) |
| `apps/column_mapping_review_app/app.py` | Streamlit review app |
| `src/harmonization/llm.py` | LLM prompt template + `ai_query()` SQL builder |
| `src/harmonization/validation.py` | Data quality check helpers |
| `tests/` | pytest test suite |
