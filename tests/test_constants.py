"""Tests for harmonization constants."""

from harmonization.constants import (
    MANDATORY_COLUMNS,
    GLOBAL_TARGET_COLUMNS,
    SEMANTIC_FIELDS,
    MATCH_TYPE_OPTIONS,
    RAW_COLUMN_NAMES,
)


def test_mandatory_columns_count():
    assert len(MANDATORY_COLUMNS) == 14


def test_mandatory_columns_unique():
    assert len(MANDATORY_COLUMNS) == len(set(MANDATORY_COLUMNS))


def test_global_target_columns_count():
    assert len(GLOBAL_TARGET_COLUMNS) == 23


def test_global_target_columns_unique():
    assert len(GLOBAL_TARGET_COLUMNS) == len(set(GLOBAL_TARGET_COLUMNS))


def test_semantic_fields_subset_of_global():
    for field in SEMANTIC_FIELDS:
        assert field in GLOBAL_TARGET_COLUMNS, f"{field} not in GLOBAL_TARGET_COLUMNS"


def test_mandatory_columns_subset_of_raw():
    for col in MANDATORY_COLUMNS:
        assert col in RAW_COLUMN_NAMES, f"{col} not in RAW_COLUMN_NAMES"


def test_raw_column_names_count():
    assert len(RAW_COLUMN_NAMES) == 24


def test_match_type_options():
    assert "DIRECT" in MATCH_TYPE_OPTIONS
    assert "SEMANTIC_TRANSLATION" in MATCH_TYPE_OPTIONS
    assert "DERIVED" in MATCH_TYPE_OPTIONS
    assert "NO_MATCH" in MATCH_TYPE_OPTIONS
    assert len(MATCH_TYPE_OPTIONS) == 4
