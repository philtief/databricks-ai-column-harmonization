"""Tests for the harmonization config loader.

These tests are intentionally domain-agnostic: they exercise the loader's
contract (required keys, structural validation, getter shapes) rather than
asserting specific column counts or domain values from the shipped YAML.
The shipped ``config/harmonization_config.yaml`` is treated as one valid
example among many — the loader should work for any well-formed YAML.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from harmonization.config import (
    get_ai_context,
    get_mandatory_columns,
    get_source_context,
    get_source_system,
    get_source_table,
    get_target_table,
    load_config,
)

# A self-contained, valid config used to test the loader independently of
# whatever YAML happens to ship in the repo's config/ folder.
VALID_CONFIG: dict = {
    "sources": {
        "DE": {
            "domain": "Demo CRM Germany",
            "source_system": "DEMO_RAW",
            "source_table": "demo_raw",
            "description": "Demo customer contact records for testing.",
            "mandatory_columns": ["id", "email"],
        },
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
        assert "sources" in config
        assert "target_model" in config
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
        bad_config.write_text(yaml.dump({"sources": {}}))
        with pytest.raises(ValueError, match="missing required keys"):
            load_config(str(bad_config))

    def test_raises_on_empty_sources(self, tmp_path):
        data = dict(VALID_CONFIG)
        data["sources"] = {}
        bad_config = tmp_path / "bad.yaml"
        bad_config.write_text(yaml.dump(data))
        with pytest.raises(ValueError, match="sources must be a non-empty object"):
            load_config(str(bad_config))

    def test_raises_on_lowercase_country_key(self, tmp_path):
        data = dict(VALID_CONFIG)
        data["sources"] = {"de": dict(data["sources"]["DE"])}
        bad_config = tmp_path / "bad.yaml"
        bad_config.write_text(yaml.dump(data))
        with pytest.raises(ValueError, match="uppercase country code"):
            load_config(str(bad_config))

    def test_raises_on_missing_source_key(self, tmp_path):
        data = dict(VALID_CONFIG)
        incomplete = dict(data["sources"]["DE"])
        del incomplete["source_table"]
        data["sources"] = {"DE": incomplete}
        bad_config = tmp_path / "bad.yaml"
        bad_config.write_text(yaml.dump(data))
        with pytest.raises(ValueError, match="Source DE missing keys"):
            load_config(str(bad_config))

    def test_raises_on_empty_columns(self, tmp_path):
        bad_config = tmp_path / "bad.yaml"
        data = {
            "sources": dict(VALID_CONFIG["sources"]),
            "target_model": {"columns": []},
            "semantic_fields": [],
            "ai": {},
        }
        bad_config.write_text(yaml.dump(data))
        with pytest.raises(ValueError, match="must not be empty"):
            load_config(str(bad_config))

    def test_raises_on_column_missing_keys(self, tmp_path):
        bad_config = tmp_path / "bad.yaml"
        data = {
            "sources": dict(VALID_CONFIG["sources"]),
            "target_model": {"columns": [{"name": "x"}]},
            "semantic_fields": [],
            "ai": {},
        }
        bad_config.write_text(yaml.dump(data))
        with pytest.raises(ValueError, match="missing keys"):
            load_config(str(bad_config))

    def test_raises_on_invalid_column_name(self, tmp_path):
        bad_config = tmp_path / "bad.yaml"
        data = {
            "sources": dict(VALID_CONFIG["sources"]),
            "target_model": {"columns": [{"name": "bad-name", "type": "STRING", "description": "test"}]},
            "semantic_fields": [],
            "ai": {},
        }
        bad_config.write_text(yaml.dump(data))
        with pytest.raises(ValueError, match="invalid name"):
            load_config(str(bad_config))

    def test_raises_on_column_name_starting_with_digit(self, tmp_path):
        bad_config = tmp_path / "bad.yaml"
        data = {
            "sources": dict(VALID_CONFIG["sources"]),
            "target_model": {"columns": [{"name": "1col", "type": "STRING", "description": "x"}]},
            "semantic_fields": [],
            "ai": {},
        }
        bad_config.write_text(yaml.dump(data))
        with pytest.raises(ValueError, match="invalid name"):
            load_config(str(bad_config))


class TestShippedTargetModel:
    def test_shipped_config_columns_are_unique(self, shipped_config):
        names = [column["name"] for column in shipped_config["target_model"]["columns"]]
        assert len(names) == len(set(names)), "shipped config has duplicate column names"


class TestGetMandatoryColumns:
    def test_returns_configured_mandatory_columns(self, config):
        assert get_mandatory_columns(config, "DE") == ["id", "email"]

    def test_unknown_country_raises(self, config):
        with pytest.raises(KeyError, match="Valid countries"):
            get_mandatory_columns(config, "FR")

    def test_shipped_mandatory_columns_exist_in_answer_keys(self, shipped_config):
        # Every mandatory column must be a real generated column (answer keys list all of them).
        root = Path(__file__).resolve().parents[1]
        for country in ("ES", "IT"):
            key = json.loads((root / "examples" / "answer_keys" / f"{country.lower()}.json").read_text())
            cols = get_mandatory_columns(shipped_config, country)
            assert cols, country
            assert set(cols) <= set(key["mappings"]), country

    def test_source_without_mandatory_columns_raises(self, tmp_path):
        data = {
            **VALID_CONFIG,
            "sources": {"DE": {k: v for k, v in VALID_CONFIG["sources"]["DE"].items() if k != "mandatory_columns"}},
        }
        bad = tmp_path / "bad.yaml"
        bad.write_text(yaml.dump(data))
        with pytest.raises(ValueError, match="mandatory_columns"):
            load_config(str(bad))


class TestGetAiContext:
    def test_returns_non_empty_string(self, config):
        ctx = get_ai_context(config, "DE")
        assert isinstance(ctx, str)
        assert len(ctx) > 5

    def test_strips_whitespace(self, tmp_path):
        cfg = dict(VALID_CONFIG)
        cfg["sources"] = {"DE": dict(cfg["sources"]["DE"])}
        cfg["sources"]["DE"]["description"] = "  hello world  \n"
        p = tmp_path / "h.yaml"
        p.write_text(yaml.dump(cfg))
        loaded = load_config(str(p))
        assert get_ai_context(loaded, "DE") == "hello world"


class TestGetSourceContext:
    def test_returns_configured_country_context(self, config):
        assert get_source_context(config, "DE") == {
            "domain": "Demo CRM Germany",
            "source_system": "DEMO_RAW",
            "source_table": "demo_raw",
            "description": "Demo customer contact records for testing.",
            "mandatory_columns": ["id", "email"],
        }

    def test_returns_non_empty_context_for_each_country(self, tmp_path):
        sources = {
            "DE": {
                "domain": "D",
                "source_system": "DE_RAW",
                "source_table": "de_raw",
                "description": "German",
                "mandatory_columns": ["id"],
            },
            "IT": {
                "domain": "D",
                "source_system": "IT_RAW",
                "source_table": "it_raw",
                "description": "Italian",
                "mandatory_columns": ["id"],
            },
        }
        cfg = dict(VALID_CONFIG)
        cfg["sources"] = sources
        p = tmp_path / "h.yaml"
        p.write_text(yaml.dump(cfg))
        loaded = load_config(str(p))

        assert get_source_system(loaded, "DE") == "DE_RAW"
        assert get_source_table(loaded, "IT") == "it_raw"
        assert get_ai_context(loaded, "IT") == "Italian"

    def test_unknown_country_lists_valid_countries(self, config):
        with pytest.raises(KeyError, match="Valid countries: \\['DE'\\]"):
            get_source_context(config, "ES")

    def test_shipped_context_has_both_countries(self, shipped_config):
        expected = {"domain", "source_system", "source_table", "description", "mandatory_columns"}
        assert set(get_source_context(shipped_config, "ES")) == expected
        assert set(get_source_context(shipped_config, "IT")) == expected


class TestGetTargetTable:
    def test_returns_target_table(self, config):
        assert get_target_table(config) == "demo_clean"
