"""Tests for harmonization.value_mapping."""

from __future__ import annotations

import pytest

from harmonization.value_mapping import build_value_prompt, categorical_targets, parse_value_response


@pytest.fixture
def cfg():
    return {
        "target_model": {
            "columns": [
                {"name": "risk_type", "type": "STRING", "semantic_group": "risk", "examples": ["Fire", "Flood"]},
                {
                    "name": "customer_segment",
                    "type": "STRING",
                    "semantic_group": "customer",
                    "examples": ["Individual"],
                },
                {"name": "policy_number", "type": "STRING", "semantic_group": "policy", "examples": ["P-1"]},
                {"name": "reporting_year", "type": "INT", "semantic_group": "time", "examples": ["2025"]},
            ]
        }
    }


def test_categorical_targets_filters_string_semantic_groups(cfg):
    assert categorical_targets(cfg) == {
        "risk_type": ["Fire", "Flood"],
        "customer_segment": ["Individual"],
    }


def test_build_value_prompt_contains_column_and_allowed_values():
    prompt = build_value_prompt("risk_type", ["Fire", "Flood"], ["Incendio", "Alluvione"])
    assert "risk_type" in prompt
    assert "Fire, Flood" in prompt
    assert "Incendio, Alluvione" in prompt
    assert "JSON" in prompt


def test_parse_value_response_filters_invalid_values():
    parsed = parse_value_response('{"Incendio": "Fire", "Unknown": "NotAllowed"}', ["Fire", "Flood"])
    assert parsed == {"Incendio": "Fire", "Unknown": None}


def test_parse_value_response_handles_text_and_missing():
    parsed = parse_value_response('Here it is: {"Incendio": "Fire", "Alluvione": null}', ["Fire", "Flood"])
    assert parsed == {"Incendio": "Fire", "Alluvione": None}


def test_parse_value_response_requires_json_object():
    with pytest.raises(ValueError, match="JSON object"):
        parse_value_response("not json", ["Fire"])
