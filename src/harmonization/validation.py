"""Pure-Python data quality check logic extracted from notebook 10.

Each check function takes simple scalar inputs (counts, booleans)
and returns a (check_name, status, metric_value, details) tuple.
This allows unit testing without PySpark.
"""


def check_row_parity(raw_count: int, harm_count: int) -> tuple:
    """Check that raw and harmonized row counts match."""
    if raw_count == harm_count:
        return ("row_count_parity", "PASSED", harm_count, f"raw={raw_count:,}, harmonized={harm_count:,}")
    return (
        "row_count_parity",
        "FAILED",
        abs(raw_count - harm_count),
        f"MISMATCH: raw={raw_count:,}, harmonized={harm_count:,}",
    )


def check_null_count(column_name: str, null_count: int) -> tuple:
    """Check that a required column has no null values."""
    status = "PASSED" if null_count == 0 else "FAILED"
    return (f"not_null_{column_name}", status, null_count, f"{null_count} null {column_name} values")


def check_premium_ordering(violation_count: int) -> tuple:
    """Check that gross_written_premium_eur >= net_written_premium_eur."""
    status = "PASSED" if violation_count == 0 else "FAILED"
    return (
        "numeric_premium_gross_gte_net",
        status,
        violation_count,
        f"{violation_count} rows where gross_written_premium_eur < net_written_premium_eur",
    )


def check_claims_ordering(violation_count: int) -> tuple:
    """Check that claims_paid_count <= claims_reported_count."""
    status = "PASSED" if violation_count == 0 else "FAILED"
    return (
        "numeric_claims_paid_lte_reported",
        status,
        violation_count,
        f"{violation_count} rows where claims_paid_count > claims_reported_count",
    )


def check_loss_ratio(violation_count: int, column_present: bool) -> tuple:
    """Check that loss_ratio is in [0, 2] range."""
    if not column_present:
        return (
            "loss_ratio_in_range",
            "WARNING",
            0,
            "loss_ratio column not present in harmonized table (computed column not yet applied)",
        )
    status = "WARNING" if violation_count > 0 else "PASSED"
    return (
        "loss_ratio_in_range",
        status,
        violation_count,
        f"{violation_count} rows where loss_ratio is outside [0, 2]",
    )


def check_mapping_coverage(mandatory_columns: list[str], active_dict_columns: set[str]) -> tuple:
    """Check that all mandatory columns have active dictionary entries."""
    covered = [c for c in mandatory_columns if c in active_dict_columns]
    coverage_pct = len(covered) / len(mandatory_columns) * 100 if mandatory_columns else 100.0
    status = "PASSED" if coverage_pct >= 100.0 else "WARNING"
    return (
        "column_mapping_coverage",
        status,
        round(coverage_pct, 1),
        f"{len(covered)}/{len(mandatory_columns)} mandatory columns have active dict entries",
    )


def check_mapping_version(bad_status_count: int) -> tuple:
    """Check that all rows have an expected mapping_status value."""
    status = "WARNING" if bad_status_count > 0 else "PASSED"
    return (
        "column_mapping_version_present",
        status,
        bad_status_count,
        f"{bad_status_count} rows without expected mapping_status flag",
    )
