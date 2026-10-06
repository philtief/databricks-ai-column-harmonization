"""Lakeflow Spark Declarative Pipeline source for country bronze feeds."""

# ruff: noqa: E402, F821

from __future__ import annotations

import sys
from pathlib import Path

from pyspark import pipelines as dp
from pyspark.sql import functions as F


def _add_src_to_path() -> None:
    # The bundle passes the deployed src folder explicitly; __file__ is only a fallback
    # because pipeline source files are not guaranteed to define it.
    configured = spark.conf.get("src_path", "")
    if configured:
        if configured not in sys.path:
            sys.path.insert(0, configured)
        return
    current = Path(__file__).resolve().parent
    for _ in range(6):
        candidate = current / "src"
        if (candidate / "harmonization").is_dir():
            if str(candidate) not in sys.path:
                sys.path.insert(0, str(candidate))
            return
        current = current.parent
    raise RuntimeError(f"Could not locate harmonization src/ above {__file__}")


_add_src_to_path()

from harmonization.ingest import bronze_table_name, expectations_for, landing_path, parse_countries


def _define_bronze_table(catalog: str, schema: str, cc: str) -> None:
    expectations = expectations_for(cc)

    @dp.table(
        name=bronze_table_name(cc),
        comment="Monthly property feed landed by Auto Loader.",
        table_properties={"quality": "bronze"},
    )
    @dp.expect_all(expectations["warn"])
    @dp.expect_all_or_drop(expectations["drop"])
    def read_bronze_feed():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .load(landing_path(catalog, schema, cc))
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
        )


catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")
countries = parse_countries(spark.conf.get("countries"))

for country in countries:
    _define_bronze_table(catalog, schema, country)
