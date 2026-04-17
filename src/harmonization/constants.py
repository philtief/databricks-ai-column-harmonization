"""Framework-level constants for the column harmonization solution.

Domain-specific values (mandatory columns, target columns, semantic fields)
are loaded at runtime from config/harmonization_config.yaml.
This module only contains constants that are framework invariants.
"""

MATCH_TYPE_OPTIONS = ["DIRECT", "SEMANTIC_TRANSLATION", "DERIVED", "NO_MATCH"]

REVIEW_STATUSES = ["PENDING", "APPROVED", "CORRECTED", "REJECTED"]

METADATA_COLUMNS = [
    "source_country",
    "source_system",
    "harmonization_timestamp",
    "column_mapping_version",
    "mapping_status",
]
