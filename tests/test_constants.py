"""Tests for framework-level constants."""

from harmonization.constants import MATCH_TYPE_OPTIONS, METADATA_COLUMNS, REVIEW_STATUSES


class TestMatchTypeOptions:
    def test_contains_all_types(self):
        assert "DIRECT" in MATCH_TYPE_OPTIONS
        assert "SEMANTIC_TRANSLATION" in MATCH_TYPE_OPTIONS
        assert "DERIVED" in MATCH_TYPE_OPTIONS
        assert "NO_MATCH" in MATCH_TYPE_OPTIONS

    def test_count(self):
        assert len(MATCH_TYPE_OPTIONS) == 4


class TestReviewStatuses:
    def test_contains_all_statuses(self):
        for s in ["PENDING", "APPROVED", "CORRECTED", "REJECTED"]:
            assert s in REVIEW_STATUSES

    def test_count(self):
        assert len(REVIEW_STATUSES) == 4


class TestMetadataColumns:
    def test_contains_expected_columns(self):
        assert "source_country" in METADATA_COLUMNS
        assert "harmonization_timestamp" in METADATA_COLUMNS

    def test_count(self):
        assert len(METADATA_COLUMNS) == 5
