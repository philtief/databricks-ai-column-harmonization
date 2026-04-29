"""Tests for the generic data quality check helpers."""

from __future__ import annotations

import pytest

from harmonization.validation import (
    check_constraint,
    check_mapping_coverage,
    check_mapping_version,
    check_null_count,
    check_row_parity,
)


class TestCheckRowParity:
    def test_equal_counts_pass(self):
        _, status, metric, _ = check_row_parity(1000, 1000)
        assert status == "PASSED"
        assert metric == 1000

    def test_unequal_counts_fail(self):
        _, status, metric, _ = check_row_parity(1000, 999)
        assert status == "FAILED"
        assert metric == 1

    def test_harmonized_greater_also_fails(self):
        _, status, metric, _ = check_row_parity(900, 1000)
        assert status == "FAILED"
        assert metric == 100

    def test_zero_counts_pass(self):
        _, status, _, _ = check_row_parity(0, 0)
        assert status == "PASSED"

    def test_check_name(self):
        name, _, _, _ = check_row_parity(10, 10)
        assert name == "row_count_parity"

    def test_details_format(self):
        _, _, _, details = check_row_parity(1000, 950)
        assert "raw=1,000" in details
        assert "harmonized=950" in details


class TestCheckNullCount:
    def test_zero_nulls_pass(self):
        _, status, metric, _ = check_null_count("record_id", 0)
        assert status == "PASSED"
        assert metric == 0

    def test_nonzero_nulls_fail(self):
        _, status, metric, _ = check_null_count("record_id", 5)
        assert status == "FAILED"
        assert metric == 5

    def test_check_name_includes_column(self):
        name, _, _, _ = check_null_count("reporting_year", 0)
        assert name == "not_null_reporting_year"

    def test_arbitrary_column_name(self):
        name, _, _, _ = check_null_count("anything_goes", 0)
        assert name == "not_null_anything_goes"


class TestCheckConstraint:
    def test_zero_violations_pass(self):
        name, status, metric, details = check_constraint(
            "premium_gross_gte_net",
            0,
            "gross_premium >= net_premium",
        )
        assert name == "premium_gross_gte_net"
        assert status == "PASSED"
        assert metric == 0
        assert "0 violations" in details

    def test_violations_default_to_failed(self):
        _, status, metric, details = check_constraint(
            "claims_paid_lte_reported",
            7,
            "claims_paid <= claims_reported",
        )
        assert status == "FAILED"
        assert metric == 7
        assert "7 violations" in details

    def test_severity_warning(self):
        _, status, _, _ = check_constraint(
            "loss_ratio_in_range",
            3,
            "loss_ratio in [0,2]",
            severity="WARNING",
        )
        assert status == "WARNING"

    def test_severity_warning_with_zero_violations_still_passes(self):
        _, status, _, _ = check_constraint(
            "soft_check",
            0,
            "soft check",
            severity="WARNING",
        )
        assert status == "PASSED"

    def test_invalid_severity_raises(self):
        with pytest.raises(ValueError, match="severity must be"):
            check_constraint("rule", 1, "desc", severity="CRITICAL")

    def test_description_in_passing_details(self):
        _, _, _, details = check_constraint("r", 0, "my rule")
        assert "my rule" in details


class TestCheckMappingCoverage:
    def test_full_coverage_pass(self):
        mandatory = ["a", "b", "c"]
        active = {"a", "b", "c", "d"}
        _, status, metric, _ = check_mapping_coverage(mandatory, active)
        assert status == "PASSED"
        assert metric == 100.0

    def test_partial_coverage_warning(self):
        mandatory = ["a", "b", "c"]
        active = {"a", "b"}
        _, status, metric, _ = check_mapping_coverage(mandatory, active)
        assert status == "WARNING"
        assert metric < 100.0
        assert metric == pytest.approx(66.7, abs=0.1)

    def test_no_coverage_warning(self):
        mandatory = ["a", "b"]
        active: set[str] = set()
        _, status, metric, _ = check_mapping_coverage(mandatory, active)
        assert status == "WARNING"
        assert metric == 0.0

    def test_empty_mandatory_pass(self):
        _, status, metric, _ = check_mapping_coverage([], set())
        assert status == "PASSED"
        assert metric == 100.0

    def test_active_columns_not_in_mandatory_dont_count(self):
        # Extra active columns shouldn't push coverage above 100
        _, _, metric, _ = check_mapping_coverage(["a"], {"a", "b", "c"})
        assert metric == 100.0


class TestCheckMappingVersion:
    def test_no_bad_status_pass(self):
        _, status, metric, _ = check_mapping_version(0)
        assert status == "PASSED"
        assert metric == 0

    def test_bad_status_warning(self):
        _, status, metric, _ = check_mapping_version(10)
        assert status == "WARNING"
        assert metric == 10

    def test_check_name(self):
        name, _, _, _ = check_mapping_version(0)
        assert name == "column_mapping_version_present"
