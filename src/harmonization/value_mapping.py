"""Pure helpers for AI-assisted categorical value mapping."""

from __future__ import annotations

import json
import re
from typing import Any

_SEMANTIC_GROUPS = {"distribution", "customer", "risk"}
_JSON_OBJECT_RE = re.compile(r"\{.*\}", re.DOTALL)


def categorical_targets(config: dict[str, Any]) -> dict[str, list[str]]:
    """Return allowed global values for categorical target columns."""
    return {
        column["name"]: list(column["examples"])
        for column in config["target_model"]["columns"]
        if column["type"].upper() == "STRING"
        and column.get("semantic_group") in _SEMANTIC_GROUPS
        and column.get("examples")
    }


def build_value_prompt(column: str, allowed: list[str], local_values: list[str]) -> str:
    """Build an ``ai_query`` prompt for one categorical column."""
    allowed_values = ", ".join(allowed)
    local_values_sql = ", ".join(local_values)
    return (
        f"Translate every local value in the column '{column}' to one global value. "
        f"Allowed global values: {allowed_values}. "
        f"Local values: {local_values_sql}. "
        "Return ONLY a JSON object mapping each local value to its global value, "
        "or null when no safe translation exists: "
        '{"local_value": "global_value"}'
    )


def parse_value_response(text: str | None, allowed: list[str]) -> dict[str, str | None]:
    """Parse a local-to-global JSON response and reject values outside ``allowed``."""
    if text is None:
        raise ValueError("AI response is not a JSON object")
    match = _JSON_OBJECT_RE.search(text)
    if not match:
        raise ValueError("AI response is not a JSON object")

    try:
        decoded = json.loads(match.group(0))
    except json.JSONDecodeError as error:
        raise ValueError("AI response is not a JSON object") from error
    if not isinstance(decoded, dict):
        raise ValueError("AI response is not a JSON object")

    allowed_set = set(allowed)
    return {
        str(local): str(global_value) if global_value is not None and str(global_value) in allowed_set else None
        for local, global_value in decoded.items()
    }
