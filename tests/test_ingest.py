"""Tests for Lakeflow ingest helpers and pipeline registration."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = ROOT / "src"

sys.path.insert(0, str(SOURCE_PATH))
for module_name in [name for name in sys.modules if name == "harmonization" or name.startswith("harmonization.")]:
    del sys.modules[module_name]

from harmonization.config import load_config  # noqa: E402
from harmonization.ingest import (  # noqa: E402
    bronze_table_name,
    expectations_for,
    landing_path,
    parse_countries,
)


def test_worktree_local_config_loads():
    assert load_config()["target_model"]["columns"]


class TestIngestHelpers:
    def test_bronze_table_name(self):
        assert bronze_table_name("es") == "bronze_property_monthly_es"

    def test_landing_path_format(self):
        assert landing_path("cat", "sch", "es") == "/Volumes/cat/sch/landing/es"

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("ES,IT", ["es", "it"]),
            (" ES , IT ", ["es", "it"]),
            ("IT,ES,IT", ["it", "es"]),
        ],
    )
    def test_parse_countries(self, raw, expected):
        assert parse_countries(raw) == expected

    @pytest.mark.parametrize("raw", ["E,", "E1,ES", ",ES", ""])
    def test_parse_countries_rejects_invalid_codes(self, raw):
        with pytest.raises(ValueError, match="Invalid country code"):
            parse_countries(raw)

    def test_expectations_for_es(self):
        assert expectations_for("es") == {
            "warn": {"valid_period": "anio IS NOT NULL AND mes BETWEEN 1 AND 12"},
            "drop": {"has_premium": "prima_bruta IS NOT NULL AND prima_bruta >= 0"},
        }

    def test_expectations_for_it(self):
        assert expectations_for("it") == {
            "warn": {"valid_period": "anno_riferimento IS NOT NULL AND mese_riferimento BETWEEN 1 AND 12"},
            "drop": {"has_premium": ("premi_lordi_contabilizzati IS NOT NULL AND premi_lordi_contabilizzati >= 0")},
        }

    def test_expectations_for_unknown_country(self):
        with pytest.raises(ValueError, match="Unsupported country code: de"):
            expectations_for("de")


def _load_pipeline(monkeypatch):
    registered = []

    def table(**kwargs):
        def decorator(function):
            registered.append((kwargs["name"], kwargs, function))
            return function

        return decorator

    def expectation(expectations):
        def decorator(function):
            function.expectations = expectations
            return function

        return decorator

    pipeline_module = SimpleNamespace(
        table=table,
        tables=registered,
        expect_all=expectation,
        expect_all_or_drop=expectation,
    )
    pyspark_module = ModuleType("pyspark")
    pyspark_module.pipelines = pipeline_module

    functions_module = ModuleType("pyspark.sql.functions")
    functions_module.col = MagicMock(return_value="metadata_column")
    functions_module.current_timestamp = MagicMock(return_value="current_timestamp")
    sql_module = ModuleType("pyspark.sql")
    sql_module.functions = functions_module

    monkeypatch.setitem(sys.modules, "pyspark", pyspark_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions_module)

    class FakeDataFrame:
        def __init__(self):
            self.columns = []

        def withColumn(self, name, _column):
            self.columns.append(name)
            return self

    class FakeReadStream:
        def __init__(self):
            self.options = []

        def format(self, _format):
            return self

        def option(self, name, value):
            self.options.append((name, value))
            return self

        def load(self, path):
            self.path = path
            self.data_frame = FakeDataFrame()
            return self.data_frame

    class FakeSpark:
        def __init__(self):
            self.conf = SimpleNamespace(
                get={
                    "catalog": "cat",
                    "schema": "sch",
                    "countries": "ES,IT",
                }.get
            )
            self.readStream = FakeReadStream()

    fake_spark = FakeSpark()
    pipeline_globals = {"spark": fake_spark}
    spec = importlib.util.spec_from_file_location(
        "test_country_feed_pipeline", ROOT / "pipelines" / "ingest_country_feeds.py"
    )
    pipeline = importlib.util.module_from_spec(spec)
    pipeline.__dict__.update(pipeline_globals)
    spec.loader.exec_module(pipeline)
    return pipeline, pipeline_module, fake_spark


def test_pipeline_registers_one_table_per_country(monkeypatch):
    pipeline, pipeline_module, fake_spark = _load_pipeline(monkeypatch)

    assert [name for name, _, _ in pipeline_module.tables] == [
        "bronze_property_monthly_es",
        "bronze_property_monthly_it",
    ]
    for _, _, table_function in pipeline_module.tables:
        table_function()
    assert fake_spark.readStream.path == "/Volumes/cat/sch/landing/it"
    assert fake_spark.readStream.options[-4:] == [
        ("cloudFiles.format", "csv"),
        ("header", "true"),
        ("cloudFiles.inferColumnTypes", "true"),
        ("cloudFiles.schemaEvolutionMode", "rescue"),
    ]
    assert fake_spark.readStream.data_frame.columns == [
        "_source_file",
        "_ingested_at",
    ]
    assert pipeline.catalog == "cat"
    assert pipeline.schema == "sch"
    assert pipeline.countries == ["es", "it"]
