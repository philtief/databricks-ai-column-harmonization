# WP1: country file drops (ES + IT), answer keys, multi-source config

## Goal
Turn the single Spain generator, which writes a Delta table, into a generator that writes monthly CSV files
per country into a UC Volume landing zone, so that a Lakeflow pipeline ingests raw files. Add Italy as a
second subsidiary with its own Italian-language schema. Make the config multi-source.

## Files
- NEW `src/harmonization/generator.py`: pure Python, no Spark. Column specs and deterministic row
  generation per country: `COUNTRIES = {"ES": ..., "IT": ...}`, `column_specs(country) -> list[ColumnSpec]`,
  `generate_rows(country, n_rows, seed) -> list[dict]`, `month_partitions(rows) -> dict[str, list[dict]]`
  (key `YYYYMM`), `to_csv(rows, columns) -> str`, `file_name(yyyymm) -> "property_monthly_<YYYYMM>.csv"`.
  Port the Spanish columns, weights and value logic from `examples/spain_demo/01_generate_spain_raw_data.py`
  so the Spanish data stays equivalent.
- NEW `examples/generate_country_files.py`: Databricks notebook. Widgets `catalog_name`, `schema_name`,
  `countries` (default `ES,IT`), `rows_per_country` (default 10000). Creates volume `landing` if missing
  (`CREATE VOLUME IF NOT EXISTS`). Writes `/Volumes/<cat>/<sch>/landing/<cc>/property_monthly_<YYYYMM>.csv`
  for 12 months and skips files that already exist (idempotent). Ends with
  `dbutils.notebook.exit(json.dumps({...files_written, files_skipped, rows per country...}))`.
- DELETE `examples/spain_demo/01_generate_spain_raw_data.py`, and update `examples/spain_demo/README.md`
  into `examples/README.md` describing both countries.
- NEW `examples/answer_keys/es.json`, `examples/answer_keys/it.json`: format
  `{"source_system": "ES_PROPERTY_RAW", "mappings": {"<local_column>": "<global_column or null>"}}`.
  Every generated column appears exactly once. Targets must be names from `target_model.columns` in the config.
- EDIT `config/harmonization_config.yaml` and `.template`: replace `source_context` with `sources:` (ES, IT)
  exactly as in PLAN.md section 3. `source_table` is `bronze_property_monthly_es` / `_it`. Rename
  `target_model.table_name` to `harmonized_property_monthly`. Write a precise Italian `description` for the
  LLM, like the Spanish one.
- EDIT `src/harmonization/config.py`: add `get_source_context(config, country)`. Keep the existing
  validation and adapt it to `sources`. Update `tests/test_config.py`.
- NEW `tests/test_generator.py`.

## Italian schema requirements
- 24 Italian column names (e.g. `periodo_riferimento`, `codice_polizza`, `premi_lordi_contabilizzati`,
  `premi_netti`, `sinistri_denunciati`, `sinistri_pagati`, `costo_sinistri_lordo`, `riserva_sinistri`,
  `provvigioni`, `spese_gestione`, `canale_distributivo`, `segmento_cliente`, `zona_rischio`, `regione`, ...).
- Italian categorical values (`Agente`, `Broker`, `Diretto`, `Bancassicurazione`; regions such as Lombardia, Lazio).
- At least 3 semantic traps: names that look like another target, e.g. `premi_netti` (net) vs
  `premi_lordi_contabilizzati` (gross), `sinistri_denunciati` (reported) vs `sinistri_pagati` (paid),
  and a ratio column `rapporto_sinistri_premi`.
- At least 1 column with no global target (answer key `null`), e.g. `codice_agenzia_interno`.
- Same value ranges as Spain so that KPIs are comparable. Seeds: ES=42, IT=43.

## Tests (minimum)
Deterministic output for a seed; 12 distinct months; every answer-key column exists in the specs and the
reverse; answer-key targets are valid config targets; CSV header matches specs; `get_source_context`
for ES, IT, and an unknown country.
