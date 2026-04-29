"""Pure-Python data quality check helpers used by notebook 10.

The checks are intentionally generic so the notebook can be driven entirely
from ``config/harmonization_config.yaml``: row parity, null checks for any
required column, arbitrary SQL-predicate constraints, and mapping coverage.

Domain-specific business rules (premium ordering, claim ordering, ratio
ranges, etc.) are expressed as user-defined ``data_quality_rules`` entries
in the YAML config — not hard-coded here.

Each check returns a 4-tuple:
    (check_name, status, metric_value, details)
where ``status`` is one of ``PASSED``, ``WARNING``, ``FAILED``.
"""

from __future__ import annotations

CheckResult = tuple[str, str, float | int, str]


def check_row_parity(raw_count: int, harm_count: int) -> CheckResult:
    """Check that raw and harmonized row counts match exactly."""
    if raw_count == harm_count:
        return ("row_count_parity", "PASSED", harm_count, f"raw={raw_count:,}, harmonized={harm_count:,}")
    return (
        "row_count_parity",
        "FAILED",
        abs(raw_count - harm_count),
        f"MISMATCH: raw={raw_count:,}, harmonized={harm_count:,}",
    )


def check_null_count(column_name: str, null_count: int) -> CheckResult:
    """Check that a required column has no null values."""
    status = "PASSED" if null_count == 0 else "FAILED"
    return (f"not_null_{column_name}", status, null_count, f"{null_count} null {column_name} values")


def check_constraint(
    rule_name: str,
    violation_count: int,
    description: str,
    *,
    severity: str = "FAILED",
) -> CheckResult:
    """Generic constraint check.

    Args:
        rule_name: Stable identifier for the check (used as the row key in
            the data_quality_results table).
        violation_count: Number of rows that violate the rule.
        description: Human-readable description of the rule (e.g.,
            ``"gross_premium >= net_premium"``).
        severity: Status to report when violation_count > 0. Use
            ``"WARNING"`` for soft constraints and ``"FAILED"`` for hard ones.

    Returns:
        A 4-tuple ``(rule_name, status, violation_count, details)``.
    """
    if severity not in ("WARNING", "FAILED"):
        raise ValueError(f"severity must be WARNING or FAILED, got {severity!r}")
    if violation_count == 0:
        return (rule_name, "PASSED", 0, f"{description}: 0 violations")
    return (rule_name, severity, violation_count, f"{description}: {violation_count} violations")


def check_mapping_coverage(mandatory_columns: list[str], active_dict_columns: set[str]) -> CheckResult:
    """Check that all mandatory source columns have active dictionary entries."""
    covered = [c for c in mandatory_columns if c in active_dict_columns]
    coverage_pct = len(covered) / len(mandatory_columns) * 100 if mandatory_columns else 100.0
    status = "PASSED" if coverage_pct >= 100.0 else "WARNING"
    return (
        "column_mapping_coverage",
        status,
        round(coverage_pct, 1),
        f"{len(covered)}/{len(mandatory_columns)} mandatory columns have active dict entries",
    )


def check_mapping_version(bad_status_count: int) -> CheckResult:
    """Check that all harmonized rows have an expected mapping_status value."""
    status = "WARNING" if bad_status_count > 0 else "PASSED"
    return (
        "column_mapping_version_present",
        status,
        bad_status_count,
        f"{bad_status_count} rows without expected mapping_status flag",
    )
