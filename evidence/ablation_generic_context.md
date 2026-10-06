# Ablation: model alone vs. model with context

Same Italian columns, same model (`databricks-claude-sonnet-4-6`), same prompt template.
Generic description: "Monthly property insurance reporting data from a European subsidiary. Column names are in the local language.". No approved mappings from other countries.

## IT: 24/24 correct with the generic description (with context: 24/24)

Auto-accept rate (HIGH and correct): 1.00. Review load: 0.00.

| Local column | Proposed | Expected | Confidence |
|---|---|---|---|
| (none wrong) | | | |

## ES: 24/24 correct with the generic description (with context: 24/24)

Auto-accept rate (HIGH and correct): 1.00. Review load: 0.00.

| Local column | Proposed | Expected | Confidence |
|---|---|---|---|
| (none wrong) | | | |


## IT with legacy abbreviated column names (stress test)

Column names replaced by abbreviations (e.g. `premi_netti` -> `PR_NT`, `sinistri_pagati` -> `SIN_PAG`); same values, same answer key. Description: "Monthly property insurance reporting data from a European subsidiary. Column names are legacy abbreviations."

Result: 18/24 correct. Auto-accept rate 0.50, review load 0.25.

| Confidence | Columns | Correct |
|---|---|---|
| HIGH | 12 | 12 |
| MEDIUM | 11 | 6 |
| LOW | 1 | 0 |

| Local column | Proposed | Expected | Confidence |
|---|---|---|---|
| NR_PL_CN | new_policies_count | cancelled_policies_count | MEDIUM |
| CST_SIN_LRD | claims_reserve_eur | gross_claims_incurred_eur | MEDIUM |
| RIS_SIN | net_written_premium_eur | claims_reserve_eur | LOW |
| SP_GST | gross_written_premium_eur | management_expenses_eur | MEDIUM |
| PRVG | gross_written_premium_eur | commissions_eur | MEDIUM |
| COD_AG_INT | policy_number | None | MEDIUM |
