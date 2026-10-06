from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
del sys.modules["harmonization"]

from harmonization.evaluation import evaluate, load_answer_key, to_metric_rows  # noqa: E402


def proposal(
    column: str,
    target: str | None = None,
    confidence: str = "HIGH",
    error: str | None = None,
) -> dict:
    return {
        "local_column_name": column,
        "proposed_global_column_name": target,
        "proposed_match_type": "NO_MATCH" if target in (None, "NO_MATCH") else "EXACT",
        "confidence": confidence,
        "ai_error_status": error,
    }


def test_all_correct_columns_are_accurate_and_automatically_accepted():
    key = {"premium": "net_written_premium_eur", "missing_column": None}
    proposals = [proposal("premium", "net_written_premium_eur"), proposal("missing_column", "NO_MATCH")]

    result = evaluate(proposals, key)

    assert (result.n_columns, result.n_correct, result.accuracy) == (2, 2, 1.0)
    assert result.auto_accept_rate == 1.0
    assert result.review_load == 0.0
    assert result.errors == []
    assert result.by_confidence["HIGH"] == {
        "n": 2,
        "correct": 2,
        "accuracy": 1.0,
        "share": 1.0,
    }


def test_all_wrong_and_ai_errors_require_review():
    key = {"premium": "net_written_premium_eur", "region": "region"}
    proposals = [
        proposal("premium", "policy_number"),
        proposal("region", "region", error="AI_ERROR"),
    ]

    result = evaluate(proposals, key)

    assert result.n_correct == 0
    assert result.accuracy == 0.0
    assert result.review_load == 1.0
    assert result.auto_accept_rate == 0.0
    assert [error["local_column"] for error in result.errors] == ["premium", "region"]
    assert result.errors[1]["proposed"] == "region"


def test_case_insensitive_values_and_mixed_bands():
    key = {"premium": "Net_Written_Premium_EUR", "region": "region", "country": "country"}
    proposals = [
        proposal("premium", "net_written_premium_eur", "HIGH"),
        proposal("region", "REGION", "MEDIUM"),
        proposal("country", "policy_number", "LOW"),
    ]

    result = evaluate(proposals, key)

    assert result.accuracy == 2 / 3
    assert result.auto_accept_rate == 1 / 3
    assert result.review_load == 1 / 3
    assert result.by_confidence["HIGH"]["accuracy"] == 1.0
    assert result.by_confidence["MEDIUM"]["accuracy"] == 1.0
    assert result.by_confidence["LOW"]["accuracy"] == 0.0


def test_empty_bands_and_unknown_confidence_are_reported():
    key = {"value": "value"}

    result = evaluate([proposal("value", "value", "VERY_HIGH")], key)

    assert result.by_confidence["VERY_HIGH"]["n"] == 1
    assert result.by_confidence["HIGH"] == {"n": 0, "correct": 0, "accuracy": 0.0, "share": 0.0}
    assert result.by_confidence["MEDIUM"]["n"] == 0
    assert result.by_confidence["LOW"]["n"] == 0


def test_null_and_no_match_expectations_are_correct():
    key = {"ignored": None, "optional": None}
    proposals = [proposal("ignored", None), proposal("optional", "NO_MATCH", "LOW")]

    result = evaluate(proposals, key)

    assert result.n_correct == 2
    assert result.accuracy == 1.0
    assert result.auto_accept_rate == 0.5
    assert result.review_load == 0.0


def test_missing_proposal_counts_as_wrong_with_none():
    result = evaluate([], {"missing": "target"})

    assert result.n_columns == 1
    assert result.n_correct == 0
    assert result.review_load == 1.0
    assert result.errors == [{"local_column": "missing", "proposed": None, "expected": "target", "confidence": None}]


def test_metric_rows_match_delta_contract():
    result = evaluate([proposal("premium", "target", "HIGH")], {"premium": "target"})

    rows = to_metric_rows(result, "run-1", "ES_PROPERTY_RAW")

    assert rows[:3] == [
        {
            "run_id": "run-1",
            "source_system": "ES_PROPERTY_RAW",
            "metric_name": "accuracy",
            "metric_value": 1.0,
            "slice": "ALL",
        },
        {
            "run_id": "run-1",
            "source_system": "ES_PROPERTY_RAW",
            "metric_name": "auto_accept_rate",
            "metric_value": 1.0,
            "slice": "ALL",
        },
        {
            "run_id": "run-1",
            "source_system": "ES_PROPERTY_RAW",
            "metric_name": "review_load",
            "metric_value": 0.0,
            "slice": "ALL",
        },
    ]
    assert rows[3:] == [
        {
            "run_id": "run-1",
            "source_system": "ES_PROPERTY_RAW",
            "metric_name": "accuracy",
            "metric_value": 1.0,
            "slice": "HIGH",
        },
        {
            "run_id": "run-1",
            "source_system": "ES_PROPERTY_RAW",
            "metric_name": "accuracy",
            "metric_value": 0.0,
            "slice": "MEDIUM",
        },
        {
            "run_id": "run-1",
            "source_system": "ES_PROPERTY_RAW",
            "metric_name": "accuracy",
            "metric_value": 0.0,
            "slice": "LOW",
        },
    ]


def test_load_answer_key(tmp_path):
    path = tmp_path / "es.json"
    path.write_text(
        json.dumps(
            {
                "source_system": "ES_PROPERTY_RAW",
                "mappings": {"premium": "net_written_premium_eur", "ignored": None},
            }
        )
    )

    assert load_answer_key(str(path)) == (
        "ES_PROPERTY_RAW",
        {"premium": "net_written_premium_eur", "ignored": None},
    )
