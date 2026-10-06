# WP2: Lakeflow Spark Declarative Pipeline for country feeds

## Goal
Ingest the CSV files that WP1 lands in `/Volumes/<cat>/<sch>/landing/<cc>/` into one bronze streaming
table per country with Auto Loader, data-quality expectations and ingestion metadata.

## Files
- NEW `src/harmonization/ingest.py`: pure helpers.
  `bronze_table_name(cc) -> "bronze_property_monthly_<cc>"`, `landing_path(catalog, schema, cc)`,
  `parse_countries("ES,IT") -> ["es","it"]` (strip, lowercase, dedupe, reject empty or non-alpha codes
  with a ValueError), `expectations_for(cc) -> {"warn": {...}, "drop": {...}}`.
  The ES and IT period and premium columns differ: ES `periodo` / `prima_bruta` (check the real
  names in `examples/spain_demo/01_generate_spain_raw_data.py`), IT `periodo_riferimento` /
  `premi_lordi_contabilizzati`. Warn rule `valid_period`: period column not null. Drop rule
  `has_premium`: gross premium not null and >= 0.
- NEW `pipelines/ingest_country_feeds.py`: SDP Python source file (not a notebook).
  `from pyspark import pipelines as dp`. Reads pipeline configuration keys `countries`, `catalog`,
  `schema` with `spark.conf.get(...)`. In a loop, defines one `@dp.table(name=bronze_table_name(cc),
  comment=..., table_properties={"quality": "bronze"})` per country. Each table is a
  `spark.readStream.format("cloudFiles")` with `cloudFiles.format=csv`, `header=true`,
  `cloudFiles.inferColumnTypes=true`, `cloudFiles.schemaEvolutionMode=rescue`, and adds
  `_source_file` (`F.col("_metadata.file_path")`) and `_ingested_at` (`F.current_timestamp()`).
  Applies `dp.expect_all(warn)` and `dp.expect_all_or_drop(drop)`. Use a factory function to avoid the
  loop-closure bug. Import the helpers by adding the bundle `src/` folder to `sys.path`, resolved
  relative to `__file__` with a fallback search like `notebooks/_shared_utils.py`.
- NEW `tests/test_ingest.py`.
- `WP_NOTES.md` must contain the exact YAML resource snippet for a `pipelines:` resource (serverless,
  `catalog`, `schema`, `configuration` with countries/catalog/schema, library `file: ./pipelines/ingest_country_feeds.py`)
  and a job `pipeline_task` snippet that the integrator adds.

## Tests (minimum)
Table names, path format, country parsing including bad input, and expectation SQL strings for ES and IT.
If you can, guard pipeline-file import behaviour with a test that mocks `pyspark.pipelines` and checks that
two tables are registered for `ES,IT`.
