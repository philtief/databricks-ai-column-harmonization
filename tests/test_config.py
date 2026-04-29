"""Tests for the harmonization config loader.

These tests are intentionally domain-agnostic: they exercise the loader's
contract (required keys, structural validation, getter shapes) rather than
asserting specific column counts or domain values from the shipped YAML.
The shipped ``config/harmonization_config.yaml`` is treated as one valid
example among many — the loader should work for any well-formed YAML.
"""

from __future__ import annotations

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

# A self-contained, valid config used to test the loader independently of
# whatever YAML happens to ship in the repo's config/ folder.
VALID_CONFIG: dict = {
    "source_context": {
        "domain": "Demo CRM",
        "source_system": "DEMO_RAW",
        "source_table": "demo_raw",
        "description": "Demo customer contact records for testing.",
    },
    "target_model": {
        "table_name": "demo_clean",
        "columns": [
            {
                "name": "id",
                "type": "STRING",
                "description": "Primary identifier",
                "examples": ["A1", "A2"],
                "required": True,
                "semantic_group": "identity",
            },
            {
                "name": "email",
                "type": "STRING",
                "description": "Email address",
                "examples": ["a@b.com"],
                "required": True,
                "semantic_group": "contact",
            },
            {
                "name": "country",
                "type": "STRING",
                "description": "Country name",
                "examples": ["Germany"],
                "required": False,
                "semantic_group": "geo",
            },
        ],
    },
    "mandatory_source_columns": ["id", "email"],
    "semantic_fields": ["country"],
    "ai": {"endpoint": "test-endpoint"},
}


@pytest.fixture()
def config(tmp_path):
    """Write the canonical VALID_CONFIG to a temp file and load it."""
    p = tmp_path / "h.yaml"
    p.write_text(yaml.dump(VALID_CONFIG))
    return load_config(str(p))


@pytest.fixture()
def shipped_config():
    """Load the YAML that actually ships in config/. Used only for tests
    asserting structural validity of the shipped file, not its content."""
    return load_config()


class TestLoadConfig:
    def test_loads_minimal_valid_config(self, config):
        assert "source_context" in config
        assert "target_model" in config
        assert "mandatory_source_columns" in config
        assert "semantic_fields" in config
        assert "ai" in config

    def test_shipped_config_loads(self, shipped_config):
        assert "target_model" in shipped_config
        assert len(shipped_config["target_model"]["columns"]) > 0

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

    def test_raises_on_column_name_starting_with_digit(self, tmp_path):
        bad_config = tmp_path / "bad.yaml"
        data = {
            "source_context": {},
            "target_model": {"columns": [{"name": "1col", "type": "STRING", "description": "x"}]},
            "mandatory_source_columns": [],
            "semantic_fields": [],
            "ai": {},
        }
        bad_config.write_text(yaml.dump(data))
        with pytest.raises(ValueError, match="invalid name"):
            load_config(str(bad_config))


class TestGetTargetColumns:
    def test_returns_all_configured_columns(self, config):
        cols = get_target_columns(config)
        assert len(cols) == 3

    def test_tuple_structure(self, config):
        cols = get_target_columns(config)
        name, dtype, desc, examples, required, group = cols[0]
        assert name == "id"
        assert dtype == "STRING"
        assert isinstance(desc, str) and len(desc) > 0
        assert isinstance(examples, list)
        assert isinstance(required, bool)
        assert isinstance(group, str)

    def test_optional_fields_default_to_safe_values(self, tmp_path):
        cfg = {
            "source_context": {},
            "target_model": {
                "columns": [{"name": "x", "type": "STRING", "description": "y"}],
            },
            "mandatory_source_columns": [],
            "semantic_fields": [],
            "ai": {},
        }
        p = tmp_path / "h.yaml"
        p.write_text(yaml.dump(cfg))
        cols = get_target_columns(load_config(str(p)))
        _, _, _, examples, required, group = cols[0]
        assert examples == []
        assert required is False
        assert group == ""

    def test_column_names_are_unique(self, config):
        names = get_target_column_names(config)
        assert len(names) == len(set(names))

    def test_column_names_are_non_empty_strings(self, config):
        for name in get_target_column_names(config):
            assert isinstance(name, str) and len(name) > 0

    def test_shipped_config_columns_are_unique(self, shipped_config):
        names = get_target_column_names(shipped_config)
        assert len(names) == len(set(names)), "shipped config has duplicate column names"


class TestGetMandatoryColumns:
    def test_returns_configured_mandatory_columns(self, config):
        assert get_mandatory_columns(config) == ["id", "email"]

    def test_all_non_empty_strings(self, config):
        for col in get_mandatory_columns(config):
            assert isinstance(col, str) and len(col) > 0

    def test_shipped_config_mandatory_columns_are_strings(self, shipped_config):
        for col in get_mandatory_columns(shipped_config):
            assert isinstance(col, str) and len(col) > 0


class TestGetSemanticFields:
    def test_returns_configured_semantic_fields(self, config):
        assert get_semantic_fields(config) == ["country"]

    def test_all_non_empty_strings(self, config):
        for field in get_semantic_fields(config):
            assert isinstance(field, str) and len(field) > 0


class TestGetAiContext:
    def test_returns_non_empty_string(self, config):
        ctx = get_ai_context(config)
        assert isinstance(ctx, str)
        assert len(ctx) > 5

    def test_strips_whitespace(self, tmp_path):
        cfg = dict(VALID_CONFIG)
        cfg["source_context"] = dict(cfg["source_context"])
        cfg["source_context"]["description"] = "  hello world  \n"
        p = tmp_path / "h.yaml"
        p.write_text(yaml.dump(cfg))
        loaded = load_config(str(p))
        assert get_ai_context(loaded) == "hello world"


class TestGetSourceSystem:
    def test_returns_source_system(self, config):
        assert get_source_system(config) == "DEMO_RAW"


class TestGetSourceTable:
    def test_returns_source_table(self, config):
        assert get_source_table(config) == "demo_raw"


class TestGetTargetTable:
    def test_returns_target_table(self, config):
        assert get_target_table(config) == "demo_clean"
