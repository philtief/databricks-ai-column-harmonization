-- ============================================================
-- Column Mapping Review Queries
-- ============================================================
--
-- BEFORE USING: Replace all occurrences of
--   YOUR_CATALOG.harmonizing_agent
-- with your actual catalog and schema name, e.g.
--   my_catalog.harmonizing_agent
--
-- Use these queries in the Databricks SQL Editor to inspect,
-- approve, correct, and reject AI-proposed column mappings.
--
-- After reviewing all mandatory columns, re-run task
--   column_mapping_review_gate
-- in the workflow Column_Mapping_To_Global_Model.
--
-- Mandatory columns are defined in config/harmonization_config.yaml
-- under the mandatory_source_columns key.
-- ============================================================


-- ============================================================
-- SECTION 1: INSPECTION
-- ============================================================

-- 1.1  View all PENDING column mappings
SELECT
    candidate_id,
    local_column_name,
    local_data_type,
    local_sample_values,
    proposed_global_column_name,
    proposed_match_type,
    mapping_rationale,
    confidence,
    ai_error_status,
    mandatory_flag,
    created_at
FROM YOUR_CATALOG.harmonizing_agent.vw_pending_column_mappings
ORDER BY mandatory_flag DESC, local_column_name;


-- 1.2  View low-confidence and AI-error mappings
SELECT
    candidate_id,
    local_column_name,
    local_data_type,
    proposed_global_column_name,
    proposed_match_type,
    confidence,
    ai_error_status,
    review_status,
    mandatory_flag,
    mapping_rationale
FROM YOUR_CATALOG.harmonizing_agent.vw_column_mapping_low_conf
ORDER BY mandatory_flag DESC, local_column_name;


-- 1.3  View mandatory columns and their current review status
SELECT
    local_column_name,
    local_data_type,
    proposed_global_column_name,
    proposed_match_type,
    confidence,
    ai_error_status,
    review_status,
    final_global_column_name,
    reviewed_by,
    reviewed_at
FROM YOUR_CATALOG.harmonizing_agent.column_mapping_candidates
WHERE mandatory_flag = TRUE
ORDER BY review_status, local_column_name;


-- 1.4  All candidates grouped by review_status
SELECT
    review_status,
    mandatory_flag,
    COUNT(*) AS count,
    COLLECT_LIST(local_column_name) AS columns
FROM YOUR_CATALOG.harmonizing_agent.column_mapping_candidates
GROUP BY review_status, mandatory_flag
ORDER BY mandatory_flag DESC, review_status;


-- ============================================================
-- SECTION 2: APPROVE
-- ============================================================

-- 2.1  Approve a single column mapping (replace 'your_column' with the local column name)
UPDATE YOUR_CATALOG.harmonizing_agent.column_mapping_candidates
SET
    review_status          = 'APPROVED',
    final_global_column_name = proposed_global_column_name,
    final_match_type         = proposed_match_type,
    reviewed_by              = current_user(),
    reviewed_at              = current_timestamp(),
    review_comment           = 'Approved after review',
    updated_at               = current_timestamp()
WHERE local_column_name = 'your_column'
  AND review_status = 'PENDING';


-- 2.2  Bulk approve all non-error PENDING mandatory columns
UPDATE YOUR_CATALOG.harmonizing_agent.column_mapping_candidates
SET
    review_status          = 'APPROVED',
    final_global_column_name = proposed_global_column_name,
    final_match_type         = proposed_match_type,
    reviewed_by              = current_user(),
    reviewed_at              = current_timestamp(),
    review_comment           = 'Bulk approved after review',
    updated_at               = current_timestamp()
WHERE mandatory_flag = TRUE
  AND review_status = 'PENDING'
  AND ai_error_status IS NULL
  AND proposed_global_column_name IS NOT NULL
  AND UPPER(proposed_global_column_name) != 'NO_MATCH';


-- 2.3  Bulk approve ALL pending candidates with valid proposals
UPDATE YOUR_CATALOG.harmonizing_agent.column_mapping_candidates
SET
    review_status          = 'APPROVED',
    final_global_column_name = proposed_global_column_name,
    final_match_type         = proposed_match_type,
    reviewed_by              = current_user(),
    reviewed_at              = current_timestamp(),
    review_comment           = 'Bulk approved after review',
    updated_at               = current_timestamp()
WHERE review_status = 'PENDING'
  AND ai_error_status IS NULL
  AND proposed_global_column_name IS NOT NULL;


-- ============================================================
-- SECTION 3: CORRECT
-- ============================================================

-- 3.1  Override AI proposal (replace placeholders with your column names)
UPDATE YOUR_CATALOG.harmonizing_agent.column_mapping_candidates
SET
    review_status          = 'CORRECTED',
    final_global_column_name = 'correct_global_column_name',
    final_match_type         = 'SEMANTIC_TRANSLATION',
    reviewed_by              = current_user(),
    reviewed_at              = current_timestamp(),
    review_comment           = 'Corrected: mapped to the correct global column',
    updated_at               = current_timestamp()
WHERE local_column_name = 'your_local_column'
  AND review_status = 'PENDING';


-- ============================================================
-- SECTION 4: REJECT
-- ============================================================

-- 4.1  Reject a column (replace 'your_column' and comment with your reason)
UPDATE YOUR_CATALOG.harmonizing_agent.column_mapping_candidates
SET
    review_status  = 'REJECTED',
    reviewed_by    = current_user(),
    reviewed_at    = current_timestamp(),
    review_comment = 'Not a business field. Exclude from harmonized output.',
    updated_at     = current_timestamp()
WHERE local_column_name = 'your_column'
  AND review_status = 'PENDING';


-- ============================================================
-- SECTION 5: COVERAGE QUERIES
-- ============================================================

-- 5.1  Active approved column mapping dictionary
SELECT
    local_column_name,
    global_column_name,
    match_type,
    mapping_version,
    active_flag,
    approved_by,
    approved_at,
    mapping_comment
FROM YOUR_CATALOG.harmonizing_agent.column_mapping_dictionary
WHERE active_flag = TRUE
ORDER BY local_column_name;


-- 5.2  Check if the review gate will pass (returns rows only if blocking)
SELECT
    local_column_name,
    review_status,
    mandatory_flag,
    proposed_global_column_name,
    confidence,
    ai_error_status,
    'BLOCKING' AS issue_type
FROM YOUR_CATALOG.harmonizing_agent.column_mapping_candidates
WHERE mandatory_flag = TRUE
  AND (
      review_status = 'PENDING'
      OR (review_status = 'REJECTED' AND (final_global_column_name IS NULL OR final_global_column_name = ''))
  )
ORDER BY local_column_name;
-- If this query returns 0 rows, the gate will PASS.
