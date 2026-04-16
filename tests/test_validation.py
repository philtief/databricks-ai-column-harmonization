"""Tests for validation check functions."""

from harmonization.validation import (
    check_row_parity,
    check_null_count,
    check_premium_ordering,
    check_claims_ordering,
    check_loss_ratio,
    check_mapping_coverage,
    check_mapping_version,
)


class TestCheckRowParity:
    def test_equal_counts_pass(self):
        name, status, metric, details = check_row_parity(1000, 1000)
        assert status == "PASSED"
        assert metric == 1000

    def test_unequal_counts_fail(self):
        name, status, metric, details = check_row_parity(1000, 999)
        assert status == "FAILED"
        assert metric == 1

    def test_zero_counts_pass(self):
        name, status, metric, details = check_row_parity(0, 0)
        assert status == "PASSED"

    def test_check_name(self):
        name, _, _, _ = check_row_parity(10, 10)
        assert name == "row_count_parity"


class TestCheckNullCount:
    def test_zero_nulls_pass(self):
        name, status, metric, details = check_null_count("record_id", 0)
        assert status == "PASSED"
        assert metric == 0

    def test_nonzero_nulls_fail(self):
        name, status, metric, details = check_null_count("record_id", 5)
        assert status == "FAILED"
        assert metric == 5

    def test_check_name_includes_column(self):
        name, _, _, _ = check_null_count("reporting_year", 0)
        assert name == "not_null_reporting_year"


class TestCheckPremiumOrdering:
    def test_no_violations_pass(self):
        name, status, metric, details = check_premium_ordering(0)
        assert status == "PASSED"

    def test_violations_fail(self):
        name, status, metric, details = check_premium_ordering(3)
        assert status == "FAILED"
        assert metric == 3


class TestCheckClaimsOrdering:
    def test_no_violations_pass(self):
        name, status, metric, details = check_claims_ordering(0)
        assert status == "PASSED"

    def test_violations_fail(self):
        name, status, metric, details = check_claims_ordering(7)
        assert status == "FAILED"
        assert metric == 7


class TestCheckLossRatio:
    def test_no_violations_pass(self):
        name, status, metric, details = check_loss_ratio(0, column_present=True)
        assert status == "PASSED"

    def test_violations_warning(self):
        name, status, metric, details = check_loss_ratio(2, column_present=True)
        assert status == "WARNING"
        assert metric == 2

    def test_column_missing_warning(self):
        name, status, metric, details = check_loss_ratio(0, column_present=False)
        assert status == "WARNING"
        assert "not present" in details


class TestCheckMappingCoverage:
    def test_full_coverage_pass(self):
        mandatory = ["a", "b", "c"]
        active = {"a", "b", "c", "d"}
        name, status, metric, details = check_mapping_coverage(mandatory, active)
        assert status == "PASSED"
        assert metric == 100.0

    def test_partial_coverage_warning(self):
        mandatory = ["a", "b", "c"]
        active = {"a", "b"}
        name, status, metric, details = check_mapping_coverage(mandatory, active)
        assert status == "WARNING"
        assert metric < 100.0

    def test_no_coverage_warning(self):
        mandatory = ["a", "b"]
        active = set()
        name, status, metric, details = check_mapping_coverage(mandatory, active)
        assert status == "WARNING"
        assert metric == 0.0

    def test_empty_mandatory_pass(self):
        name, status, metric, details = check_mapping_coverage([], set())
        assert status == "PASSED"


class TestCheckMappingVersion:
    def test_no_bad_status_pass(self):
        name, status, metric, details = check_mapping_version(0)
        assert status == "PASSED"

    def test_bad_status_warning(self):
        name, status, metric, details = check_mapping_version(10)
        assert status == "WARNING"
        assert metric == 10
