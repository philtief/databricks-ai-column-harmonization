"""Load and validate the harmonization configuration YAML."""

import os
from pathlib import Path
from typing import Any

import yaml

_REQUIRED_KEYS = {"source_context", "target_model", "mandatory_source_columns", "semantic_fields", "ai"}
_REQUIRED_COLUMN_KEYS = {"name", "type", "description"}

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

    result: dict[str, Any] = config
    return result


def get_target_columns(config: dict) -> list[tuple]:
    """Convert config column definitions to the tuple format needed by notebook 02.

    Returns list of (name, type, description, examples, required, semantic_group) tuples.
    """
    return [
        (
            col["name"],
            col["type"],
            col["description"],
            col.get("examples", []),
            col.get("required", False),
            col.get("semantic_group", ""),
        )
        for col in config["target_model"]["columns"]
    ]


def get_target_column_names(config: dict) -> list[str]:
    """Return the list of global target column names from config."""
    return [col["name"] for col in config["target_model"]["columns"]]


def get_mandatory_columns(config: dict) -> list[str]:
    """Return the mandatory source columns list from config."""
    return list(config["mandatory_source_columns"])


def get_semantic_fields(config: dict) -> list[str]:
    """Return the semantic fields eligible for value mapping."""
    return list(config["semantic_fields"])


def get_ai_context(config: dict) -> str:
    """Return the source domain description for the AI prompt."""
    result: str = config["source_context"]["description"].strip()
    return result


def get_source_system(config: dict) -> str:
    """Return the source system identifier."""
    result: str = config["source_context"]["source_system"]
    return result


def get_source_table(config: dict) -> str:
    """Return the raw source table name from config."""
    result: str = config["source_context"]["source_table"]
    return result


def get_target_table(config: dict) -> str:
    """Return the harmonized target table name from config."""
    result: str = config["target_model"]["table_name"]
    return result
