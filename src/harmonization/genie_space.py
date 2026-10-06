"""Pure helpers for Genie space-as-code and benchmark rendering."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

SPACE_VERSION = 2
REQUIRED_SPACE_KEYS = {"title", "description", "tables", "text_instructions", "sample_questions", "example_sqls"}


def load_space_config(path: str | Path) -> dict[str, Any]:
    """Load and validate the Genie space definition."""
    config_path = Path(path)
    with config_path.open(encoding="utf-8") as stream:
        cfg = yaml.safe_load(stream)
    if not isinstance(cfg, dict):
        raise ValueError("Genie space config must be a YAML mapping")
    missing = REQUIRED_SPACE_KEYS - set(cfg)
    if missing:
        raise ValueError(f"Genie space config missing required keys: {sorted(missing)}")
    for key in ("tables", "text_instructions", "sample_questions", "example_sqls"):
        if not cfg[key] or not isinstance(cfg[key], list):
            raise ValueError(f"Genie space config {key} must be a non-empty list")
    return cfg


def _item_id(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:32]


def _format_sql(sql: str, catalog: str, schema: str) -> list[str]:
    rendered = sql.replace("{{ catalog }}", catalog).replace("{{ schema }}", schema).strip()
    return [f"{line}\n" for line in rendered.splitlines()[:-1]] + [rendered.splitlines()[-1]]


def build_serialized_space(cfg: dict[str, Any], catalog: str, schema: str) -> dict[str, Any]:
    """Build deterministic serialized-space version 2."""
    tables = [
        {
            "identifier": f"{catalog}.{schema}.{table}",
            "column_configs": [],
        }
        for table in sorted(cfg["tables"])
    ]
    sample_questions = [
        {"id": _item_id({"question": [question]}), "question": [question]}
        for question in sorted(cfg["sample_questions"])
    ]
    text_instructions = [
        {"id": _item_id({"content": [content]}), "content": [content]} for content in cfg["text_instructions"]
    ]
    example_sqls = [
        {
            "id": _item_id({"question": [item["question"]], "sql": _format_sql(item["sql"], catalog, schema)}),
            "question": [item["question"]],
            "sql": _format_sql(item["sql"], catalog, schema),
        }
        for item in cfg["example_sqls"]
    ]
    return {
        "version": SPACE_VERSION,
        "config": {"sample_questions": sample_questions},
        "data_sources": {"tables": tables},
        "instructions": {
            "text_instructions": text_instructions,
            "example_question_sqls": example_sqls,
        },
    }


def to_api_payload(
    cfg: dict[str, Any], catalog: str, schema: str, warehouse_id: str, parent_path: str
) -> dict[str, Any]:
    """Build the Databricks create-or-update API payload."""
    serialized = json.dumps(
        build_serialized_space(cfg, catalog, schema),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        "title": cfg["title"],
        "description": cfg["description"],
        "warehouse_id": warehouse_id,
        "parent_path": parent_path,
        "serialized_space": serialized,
    }


def render_benchmark(results: list[dict[str, Any]]) -> str:
    """Render successful, no-SQL, and failed Genie questions as markdown."""
    space_id = next(
        (result.get("space_id") for result in results if result.get("space_id")),
        "(provided by CLI)",
    )
    lines = [
        "# Genie benchmark",
        "",
        f"- timestamp: {datetime.now(UTC).isoformat()}",
        "",
        f"- space id: {space_id}",
        "",
    ]
    for result in results:
        lines.extend([f"## {result['question']}", "", f"Status: {result['status']}", ""])
        if result.get("answer"):
            lines.extend(["**Answer**", "", result["answer"], ""])
        if result.get("description"):
            lines.extend(["**Query description**", "", result["description"], ""])
        if result.get("sql"):
            lines.extend(["**Generated SQL**", "", "```sql", result["sql"], "```", ""])
        elif result["status"] == "SUCCESS":
            lines.append("No SQL was returned.")
            lines.append("")
        if result.get("error"):
            lines.extend([f"Error: {result['error']}", ""])
        if result.get("columns"):
            columns = result["columns"]
            lines.append("| " + " | ".join(str(column) for column in columns) + " |")
            lines.append("|" + "---|" * len(columns))
            for row in result["rows"]:
                lines.append("| " + " | ".join(str(value) for value in row) + " |")
            lines.append("")
    sql_count = sum(1 for result in results if result.get("sql"))
    lines.append(f"answered with SQL: {sql_count}/{len(results)}")
    lines.append("")
    return "\n".join(lines)
