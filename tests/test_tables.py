"""Tests for harmonization.tables.get_table_refs."""

from __future__ import annotations

import pytest

from harmonization.tables import get_table_refs


@pytest.fixture
def cfg():
    return {
        "sources": {
            "ES": {"source_system": "ES_PROPERTY_RAW", "source_table": "bronze_property_monthly_es"},
            "IT": {"source_system": "IT_PROPERTY_RAW", "source_table": "bronze_property_monthly_it"},
        },
        "target_model": {"table_name": "demo_clean_table"},
    }


@pytest.fixture
def db_prefix():
    return "`my_cat`.`my_schema`"


class TestGetTableRefs:
    def test_returns_dict(self, cfg, db_prefix):
        refs = get_table_refs(cfg, db_prefix, "ES")
        assert isinstance(refs, dict)

    def test_raw_table_uses_country_source_table(self, cfg, db_prefix):
        refs = get_table_refs(cfg, db_prefix, "IT")
        assert refs["raw_table"] == "`my_cat`.`my_schema`.`bronze_property_monthly_it`"

    def test_harm_table_uses_target_table(self, cfg, db_prefix):
        refs = get_table_refs(cfg, db_prefix, "ES")
        assert refs["harm_table"] == "`my_cat`.`my_schema`.`demo_clean_table`"

    def test_source_system_passes_through(self, cfg, db_prefix):
        refs = get_table_refs(cfg, db_prefix, "ES")
        assert refs["source_system"] == "ES_PROPERTY_RAW"

    def test_unqualified_table_names_present(self, cfg, db_prefix):
        refs = get_table_refs(cfg, db_prefix, "ES")
        assert refs["source_table_name"] == "bronze_property_monthly_es"
        assert refs["target_table_name"] == "demo_clean_table"

    def test_all_required_keys_present(self, cfg, db_prefix):
        refs = get_table_refs(cfg, db_prefix, "ES")
        expected_keys = {
            "raw_table",
            "harm_table",
            "source_system",
            "source_table_name",
            "target_table_name",
            "inv_table",
            "cand_table",
            "dict_table",
            "audit_table",
            "vcand_table",
            "vdict_table",
            "gtc_table",
            "ops_table",
            "usage_table",
            "dq_table",
        }
        assert expected_keys == set(refs.keys())

    @pytest.mark.parametrize(
        "key,expected_table_name",
        [
            ("inv_table", "source_column_inventory"),
            ("cand_table", "column_mapping_candidates"),
            ("dict_table", "column_mapping_dictionary"),
            ("audit_table", "column_mapping_audit"),
            ("vcand_table", "value_mapping_candidates"),
            ("vdict_table", "value_mapping_dictionary"),
            ("gtc_table", "global_target_columns"),
            ("ops_table", "workflow_run_metrics"),
            ("usage_table", "ai_mapping_usage_metrics"),
            ("dq_table", "data_quality_results"),
        ],
    )
    def test_control_table_names(self, cfg, db_prefix, key, expected_table_name):
        refs = get_table_refs(cfg, db_prefix, "ES")
        assert refs[key] == f"{db_prefix}.`{expected_table_name}`"

    def test_all_qualified_refs_use_backticks(self, cfg, db_prefix):
        refs = get_table_refs(cfg, db_prefix, "ES")
        qualified_keys = (
            "raw_table",
            "harm_table",
            "inv_table",
            "cand_table",
            "dict_table",
            "audit_table",
            "vcand_table",
            "vdict_table",
            "gtc_table",
            "ops_table",
            "usage_table",
            "dq_table",
        )
        for k in qualified_keys:
            assert refs[k].startswith("`")
            assert refs[k].endswith("`")
            assert refs[k].count("`") == 6  # cat, schema, table — 6 backticks

    def test_works_with_different_db_prefix(self, cfg):
        refs = get_table_refs(cfg, "`other`.`other_schema`", "ES")
        assert refs["raw_table"] == "`other`.`other_schema`.`bronze_property_monthly_es`"

    def test_missing_source_context_key_raises(self, db_prefix):
        with pytest.raises(KeyError):
            get_table_refs({}, db_prefix, "ES")

    def test_missing_target_model_key_raises(self, db_prefix):
        bad = {"sources": {}, "target_model": {"table_name": "tgt"}}
        with pytest.raises(KeyError):
            get_table_refs(bad, db_prefix, "ES")

    def test_special_chars_in_table_names_pass_through(self, db_prefix):
        cfg = {
            "sources": {"ES": {"source_system": "X", "source_table": "table with spaces"}},
            "target_model": {"table_name": "tgt"},
        }
        refs = get_table_refs(cfg, db_prefix, "ES")
        # Backticks already wrap names, so spaces are valid
        assert refs["raw_table"] == "`my_cat`.`my_schema`.`table with spaces`"
