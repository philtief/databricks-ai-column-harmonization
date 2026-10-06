import json
import re

import pytest

from harmonization.genie_space import (
    build_serialized_space,
    load_space_config,
    render_benchmark,
    to_api_payload,
)

CONFIG_PATH = "genie/space_config.yaml"
HEX_32 = re.compile(r"^[0-9a-f]{32}$")
CATALOG = "agent_marketplace_catalog"
SCHEMA = "halvard_harmonization"
TABLE_NAMES = {
    f"{CATALOG}.{SCHEMA}.{name}"
    for name in (
        "harmonized_property_monthly",
        "mv_group_property_kpis",
        "column_mapping_candidates",
        "column_mapping_dictionary",
        "data_quality_results",
        "mapping_eval_results",
    )
}


def test_load_space_config_contains_required_content():
    cfg = load_space_config(CONFIG_PATH)

    assert cfg["title"] == "Halvard Group Property KPIs"
    assert len(cfg["tables"]) == 6
    assert len(cfg["sample_questions"]) == 6
    assert len(cfg["example_sqls"]) >= 3
    assert any("loss ratio" in instruction.lower() for instruction in cfg["text_instructions"])
    assert any("EUR" in instruction for instruction in cfg["text_instructions"])
    assert any("metric view" in instruction.lower() for instruction in cfg["text_instructions"])


def test_build_serialized_space_matches_reference_shape():
    cfg = load_space_config(CONFIG_PATH)
    space = build_serialized_space(cfg, CATALOG, SCHEMA)

    assert space["version"] == 2
    assert set(space) == {"version", "config", "data_sources", "instructions"}
    assert {item["identifier"] for item in space["data_sources"]["tables"]} == TABLE_NAMES
    assert [item["identifier"] for item in space["data_sources"]["tables"]] == sorted(
        item["identifier"] for item in space["data_sources"]["tables"]
    )
    assert len(space["config"]["sample_questions"]) == 6
    assert all(HEX_32.match(item["id"]) for item in space["config"]["sample_questions"])
    assert all(isinstance(item["question"], list) for item in space["config"]["sample_questions"])
    # The API allows exactly one text-instruction item; every configured paragraph is in its content.
    assert len(space["instructions"]["text_instructions"]) == 1
    assert len(space["instructions"]["text_instructions"][0]["content"]) == len(cfg["text_instructions"])
    assert all(HEX_32.match(item["id"]) for item in space["instructions"]["text_instructions"])
    assert all(isinstance(item["content"], list) for item in space["instructions"]["text_instructions"])
    assert len(space["instructions"]["example_question_sqls"]) >= 3
    assert all(HEX_32.match(item["id"]) for item in space["instructions"]["example_question_sqls"])
    sql_blocks = ["\n".join(item["sql"]) for item in space["instructions"]["example_question_sqls"]]
    assert any("MEASURE(`Loss Ratio`)" in sql for sql in sql_blocks)


def test_build_serialized_space_is_deterministic():
    cfg = load_space_config(CONFIG_PATH)

    assert build_serialized_space(cfg, CATALOG, SCHEMA) == build_serialized_space(cfg, CATALOG, SCHEMA)


def test_to_api_payload_serializes_space():
    cfg = load_space_config(CONFIG_PATH)
    payload = to_api_payload(cfg, CATALOG, SCHEMA, "warehouse-id", "/Users/test")

    assert payload["title"] == "Halvard Group Property KPIs"
    assert payload["description"]
    assert payload["warehouse_id"] == "warehouse-id"
    assert payload["parent_path"] == "/Users/test"
    assert isinstance(payload["serialized_space"], str)
    assert json.loads(payload["serialized_space"]) == build_serialized_space(cfg, CATALOG, SCHEMA)


def test_render_benchmark_with_success_no_sql_and_error():
    results = [
        {
            "question": "Show GWP",
            "status": "SUCCESS",
            "answer": "GWP is 100.",
            "sql": "SELECT 100 AS gwp",
            "description": "Direct query",
            "columns": ["gwp"],
            "rows": [[100]],
            "error": None,
        },
        {
            "question": "Explain loss ratio",
            "status": "SUCCESS",
            "answer": "Claims divided by premium.",
            "sql": None,
            "description": None,
            "columns": [],
            "rows": [],
            "error": None,
        },
        {
            "question": "Broken question",
            "status": "ERROR",
            "answer": None,
            "sql": None,
            "description": None,
            "columns": [],
            "rows": [],
            "error": "request failed",
        },
    ]

    rendered = render_benchmark(results)

    assert "# Genie benchmark" in rendered
    assert "answered with SQL: 1/3" in rendered
    assert "```sql\nSELECT 100 AS gwp\n```" in rendered
    assert "| gwp |\n|---|\n| 100 |" in rendered
    assert "No SQL was returned." in rendered
    assert "Error: request failed" in rendered


def test_load_space_config_rejects_missing_file():
    with pytest.raises(FileNotFoundError):
        load_space_config("/tmp/does-not-exist.yaml")
