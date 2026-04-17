# Spain Property Insurance Demo

This directory contains the demo data generator for Spain property insurance — 10,000 synthetic rows with 24 Spanish-language columns.

## What it does

`01_generate_spain_raw_data.py` creates the raw source table (`property_insurance_monthly_raw`) with synthetic data. It runs as a Databricks notebook task in the workflow.

## How to use the demo

The demo is pre-wired in `databricks.yml` as the `generate_demo_data` task. Deploy and run with no changes:

```bash
databricks bundle deploy
databricks bundle run Column_Mapping_To_Global_Model
```

The shipped `config/harmonization_config.yaml` contains the Spain property insurance configuration.

## Switching to your own data

1. Copy `config/harmonization_config.yaml.template` to `config/harmonization_config.yaml`
2. Fill in your source context, target model, and mandatory columns
3. In `databricks.yml`, remove the `generate_demo_data` task and update `inventory_source_columns` to depend only on `create_global_model_and_control_tables`
4. Ensure your raw source table already exists in the target catalog/schema
5. Deploy and run
