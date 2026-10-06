"""Load and validate the harmonization configuration YAML."""

import os
import re
from pathlib import Path
from typing import Any

import yaml

_REQUIRED_KEYS = {"sources", "target_model", "semantic_fields", "ai"}
_REQUIRED_SOURCE_KEYS = {"domain", "source_system", "source_table", "description", "mandatory_columns"}
_REQUIRED_COLUMN_KEYS = {"name", "type", "description"}
_VALID_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Default config path: config/harmonization_config.yaml relative to repo root
_DEFAULT_CONFIG_PATH = os.path.join(Path(__file__).resolve().parents[2], "config", "harmonization_config.yaml")


def load_config(path: str | None = None) -> dict[str, Any]:
    """Load the harmonization config from a YAML file.

    Args:
        path: Path to the YAML file. If None, uses the default location.

    Returns:
        Parsed config dict.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError: If required keys are missing.
    """
    config_path = path or _DEFAULT_CONFIG_PATH
    with open(config_path) as f:
        config = yaml.safe_load(f)

    missing = _REQUIRED_KEYS - set(config.keys())
    if missing:
        raise ValueError(f"Config missing required keys: {sorted(missing)}")

    columns = config.get("target_model", {}).get("columns", [])
    if not columns:
        raise ValueError("target_model.columns must not be empty")

    for i, col in enumerate(columns):
        col_missing = _REQUIRED_COLUMN_KEYS - set(col.keys())
        if col_missing:
            raise ValueError(f"Column {i} ({col.get('name', '?')}) missing keys: {sorted(col_missing)}")
        col_name = col["name"]
        if not _VALID_IDENTIFIER.match(col_name):
            raise ValueError(f"Column {i} has invalid name '{col_name}'. Use only letters, digits, and underscores.")

    sources = config["sources"]
    if not isinstance(sources, dict) or not sources:
        raise ValueError("sources must be a non-empty object")
    for country, context in sources.items():
        if country != country.upper():
            raise ValueError(f"Source '{country}' must use an uppercase country code")
        if not isinstance(context, dict):
            raise ValueError(f"Source '{country}' must be an object")
        source_missing = _REQUIRED_SOURCE_KEYS - set(context.keys())
        if source_missing:
            raise ValueError(f"Source {country} missing keys: {sorted(source_missing)}")

    result: dict[str, Any] = config
    return result


def get_mandatory_columns(config: dict, country: str) -> list[str]:
    """Return the local columns of one country that must be reviewed before the gate passes."""
    return list(get_source_context(config, country)["mandatory_columns"])


def get_source_context(config: dict, country: str) -> dict:
    """Return one country source context.

    Raises:
        KeyError: If the country is not configured.
    """
    sources = config["sources"]
    if country not in sources:
        raise KeyError(f"Unknown country '{country}'. Valid countries: {sorted(sources)}")
    return sources[country]


def get_ai_context(config: dict, country: str) -> str:
    """Return the source domain description for the AI prompt."""
    result: str = get_source_context(config, country)["description"].strip()
    return result


def get_source_system(config: dict, country: str) -> str:
    """Return the source system identifier."""
    result: str = get_source_context(config, country)["source_system"]
    return result


def get_source_table(config: dict, country: str) -> str:
    """Return the raw source table name from config."""
    result: str = get_source_context(config, country)["source_table"]
    return result


def get_target_table(config: dict) -> str:
    """Return the harmonized target table name from config."""
    result: str = config["target_model"]["table_name"]
    return result
