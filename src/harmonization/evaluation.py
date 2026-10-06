"""Pure evaluation of AI mapping proposals against an answer key."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

CONFIDENCE_BANDS = ("HIGH", "MEDIUM", "LOW")


@dataclass(frozen=True)
class EvalResult:
    """Metrics for the answer-key column universe."""

    n_columns: int
    n_correct: int
    accuracy: float
    by_confidence: dict[str, dict[str, float | int]]
    auto_accept_rate: float
    review_load: float
    errors: list[dict] = field(default_factory=list)


def evaluate(proposals: list[dict], answer_key: dict[str, str | None]) -> EvalResult:
    """Evaluate one proposal for each column in ``answer_key``.

    Columns missing from ``proposals`` use ``None`` as their proposal. Extra
    proposals are ignored because the answer key defines the labelled universe.
    """
    proposal_by_column = {
        proposal["local_column_name"]: proposal
        for proposal in proposals
        if proposal.get("local_column_name") in answer_key
    }
    by_confidence = {band: {"n": 0, "correct": 0, "accuracy": 0.0, "share": 0.0} for band in CONFIDENCE_BANDS}
    errors: list[dict] = []
    n_correct = 0
    auto_accept_count = 0
    review_count = 0

    for local_column, expected_value in answer_key.items():
        expected = None if expected_value in (None, "") else str(expected_value)
        proposal = proposal_by_column.get(local_column, {})
        proposed_value = proposal.get("proposed_global_column_name")
        proposed = None if proposed_value in (None, "") else str(proposed_value)
        confidence_value = proposal.get("confidence")
        confidence = None if confidence_value in (None, "") else str(confidence_value).upper()
        ai_error_status = proposal.get("ai_error_status")
        ai_error = ai_error_status not in (None, "")

        proposed_normalized = proposed.casefold() if proposed is not None else "no_match"
        expected_normalized = expected.casefold() if expected is not None else "no_match"
        correct = not ai_error and proposed_normalized == expected_normalized

        by_confidence.setdefault(
            confidence or "UNKNOWN",
            {"n": 0, "correct": 0, "accuracy": 0.0, "share": 0.0},
        )
        band = by_confidence[confidence or "UNKNOWN"]
        band["n"] += 1
        if correct:
            band["correct"] += 1
            n_correct += 1
        if confidence == "HIGH" and correct:
            auto_accept_count += 1
        if not correct or ai_error:
            review_count += 1
            errors.append(
                {
                    "local_column": local_column,
                    "proposed": proposed_value,
                    "expected": expected_value,
                    "confidence": confidence_value,
                }
            )

    for band in by_confidence.values():
        n = band["n"]
        band["accuracy"] = band["correct"] / n if n else 0.0
        band["share"] = n / len(answer_key) if answer_key else 0.0

    n_columns = len(answer_key)
    return EvalResult(
        n_columns=n_columns,
        n_correct=n_correct,
        accuracy=n_correct / n_columns if n_columns else 0.0,
        by_confidence=by_confidence,
        auto_accept_rate=auto_accept_count / n_columns if n_columns else 0.0,
        review_load=review_count / n_columns if n_columns else 0.0,
        errors=errors,
    )


def to_metric_rows(result: EvalResult, run_id: str, source_system: str) -> list[dict]:
    """Convert evaluation metrics into ``mapping_eval_results`` rows."""
    rows = [
        {
            "run_id": run_id,
            "source_system": source_system,
            "metric_name": "accuracy",
            "metric_value": result.accuracy,
            "slice": "ALL",
        },
        {
            "run_id": run_id,
            "source_system": source_system,
            "metric_name": "auto_accept_rate",
            "metric_value": result.auto_accept_rate,
            "slice": "ALL",
        },
        {
            "run_id": run_id,
            "source_system": source_system,
            "metric_name": "review_load",
            "metric_value": result.review_load,
            "slice": "ALL",
        },
    ]
    rows.extend(
        {
            "run_id": run_id,
            "source_system": source_system,
            "metric_name": "accuracy",
            "metric_value": band["accuracy"],
            "slice": band_name,
        }
        for band_name, band in result.by_confidence.items()
    )
    return rows


def load_answer_key(path: str) -> tuple[str, dict[str, str | None]]:
    """Read an answer-key JSON file and return its source system and mappings."""
    with open(path) as file_handle:
        payload = json.load(file_handle)
    return payload["source_system"], payload["mappings"]
