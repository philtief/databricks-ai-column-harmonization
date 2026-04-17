"""Tests for the harmonization config loader."""

import pytest
import yaml

from harmonization.config import (
    get_ai_context,
    get_mandatory_columns,
    get_semantic_fields,
    get_source_system,
    get_source_table,
    get_target_column_names,
    get_target_columns,
    get_target_table,
    load_config,
)


@pytest.fixture()
def config():
    """Load the real config file."""
    return load_config()


class TestLoadConfig:
    def test_loads_default_config(self, config):
        assert "source_context" in config
        assert "target_model" in config

    def test_raises_on_missing_file(self):
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path.yaml")

    def test_raises_on_missing_keys(self, tmp_path):
        bad_config = tmp_path / "bad.yaml"
        bad_config.write_text(yaml.dump({"source_context": {}}))
        with pytest.raises(ValueError, match="missing required keys"):
            load_config(str(bad_config))

    def test_raises_on_empty_columns(self, tmp_path):
        bad_config = tmp_path / "bad.yaml"
        data = {
            "source_context": {},
            "target_model": {"columns": []},
            "mandatory_source_columns": [],
            "semantic_fields": [],
            "ai": {},
        }
        bad_config.write_text(yaml.dump(data))
        with pytest.raises(ValueError, match="must not be empty"):
            load_config(str(bad_config))

    def test_raises_on_column_missing_keys(self, tmp_path):
        bad_config = tmp_path / "bad.yaml"
        data = {
            "source_context": {},
            "target_model": {"columns": [{"name": "x"}]},
            "mandatory_source_columns": [],
            "semantic_fields": [],
            "ai": {},
        }
        bad_config.write_text(yaml.dump(data))
        with pytest.raises(ValueError, match="missing keys"):
            load_config(str(bad_config))

    def test_raises_on_invalid_column_name(self, tmp_path):
        bad_config = tmp_path / "bad.yaml"
        data = {
            "source_context": {},
            "target_model": {"columns": [{"name": "bad-name", "type": "STRING", "description": "test"}]},
            "mandatory_source_columns": [],
            "semantic_fields": [],
            "ai": {},
        }
        bad_config.write_text(yaml.dump(data))
        with pytest.raises(ValueError, match="invalid name"):
            load_config(str(bad_config))


class TestGetTargetColumns:
    def test_returns_23_columns(self, config):
        cols = get_target_columns(config)
        assert len(cols) == 23

    def test_tuple_structure(self, config):
        cols = get_target_columns(config)
        name, dtype, desc, examples, required, group = cols[0]
        assert name == "record_id"
        assert dtype == "BIGINT"
        assert isinstance(desc, str) and len(desc) > 0
        assert isinstance(examples, list)
        assert isinstance(required, bool)
        assert isinstance(group, str)

    def test_column_names_are_unique(self, config):
        names = get_target_column_names(config)
        assert len(names) == len(set(names))

    def test_column_names_are_non_empty_strings(self, config):
        for name in get_target_column_names(config):
            assert isinstance(name, str) and len(name) > 0


class TestGetMandatoryColumns:
    def test_returns_14_columns(self, config):
        cols = get_mandatory_columns(config)
        assert len(cols) == 14

    def test_all_non_empty_strings(self, config):
        for col in get_mandatory_columns(config):
            assert isinstance(col, str) and len(col) > 0


class TestGetSemanticFields:
    def test_returns_5_fields(self, config):
        fields = get_semantic_fields(config)
        assert len(fields) == 5

    def test_all_non_empty_strings(self, config):
        for field in get_semantic_fields(config):
            assert isinstance(field, str) and len(field) > 0


class TestGetAiContext:
    def test_returns_non_empty_string(self, config):
        ctx = get_ai_context(config)
        assert isinstance(ctx, str)
        assert len(ctx) > 20

    def test_contains_domain_keywords(self, config):
        ctx = get_ai_context(config)
        assert "Spain" in ctx or "insurance" in ctx


class TestGetSourceSystem:
    def test_returns_source_system(self, config):
        assert get_source_system(config) == "ES_PROPERTY_RAW"


class TestGetSourceTable:
    def test_returns_source_table(self, config):
        assert get_source_table(config) == "property_insurance_monthly_raw"


class TestGetTargetTable:
    def test_returns_target_table(self, config):
        assert get_target_table(config) == "property_insurance_monthly"
