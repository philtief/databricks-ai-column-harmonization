"""Fully-qualified Delta table references derived from the harmonization config.

This is the single source of truth for the workflow's table layout — every
notebook reads its table names from :func:`get_table_refs`. Table names are
generic (no country/region suffix); customer isolation is handled at the
catalog/schema level via ``db_prefix``.
"""

from __future__ import annotations

from typing import Any


def get_table_refs(config: dict[str, Any], db_prefix: str, source_country: str) -> dict[str, str]:
    """Return the dict of qualified Delta table references used by the workflow.

    Args:
        config: Parsed ``harmonization_config.yaml`` dict.
        db_prefix: A backtick-quoted catalog.schema string that prefixes
            every table reference. Example: ``"`my_cat`.`my_schema`"``.

    Returns:
        A dict whose values are fully qualified, backtick-quoted table or
        view references safe to interpolate directly into Spark SQL strings.
    """
    source_context = config["sources"][source_country]
    src_table = source_context["source_table"]
    tgt_table = config["target_model"]["table_name"]

    return {
        "raw_table": f"{db_prefix}.`{src_table}`",
        "harm_table": f"{db_prefix}.`{tgt_table}`",
        "source_system": source_context["source_system"],
        "source_table_name": src_table,
        "target_table_name": tgt_table,
        "inv_table": f"{db_prefix}.`source_column_inventory`",
        "cand_table": f"{db_prefix}.`column_mapping_candidates`",
        "dict_table": f"{db_prefix}.`column_mapping_dictionary`",
        "audit_table": f"{db_prefix}.`column_mapping_audit`",
        "vcand_table": f"{db_prefix}.`value_mapping_candidates`",
        "vdict_table": f"{db_prefix}.`value_mapping_dictionary`",
        "gtc_table": f"{db_prefix}.`global_target_columns`",
        "ops_table": f"{db_prefix}.`workflow_run_metrics`",
        "usage_table": f"{db_prefix}.`ai_mapping_usage_metrics`",
        "dq_table": f"{db_prefix}.`data_quality_results`",
    }
