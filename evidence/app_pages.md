# App: rendered page text and live actions

App `halvard-harmonization-review`, URL https://halvard-harmonization-review-7474653189849615.aws.databricksapps.com.
Captured 2026-10-06 from the deployed app (Chrome, signed in as philipp.tiefenbacher@databricks.com) with the
page's rendered text (`innerText`). Status of the deployment: `evidence/app_status.md`.

## Live actions in the app (Italy, 2026-10-06 ~15:25–15:35 UTC)

| Action in the app | Result |
|---|---|
| Pending Review Queue, IT: **Approve** `anno_riferimento` → `reporting_year` | Lakebase `review_queue` APPROVED, audit row `action_source = DATABRICKS_APP` |
| **Approve** `canale_distributivo` → `distribution_channel` | same |
| **Reject** `codice_agenzia_interno` (proposal NO_MATCH), reason "Internal agency code; no group target. Keep out of group reporting." | REJECTED with comment |
| The remaining 21 IT decisions | scripted with `scripts/review_from_answer_key.py`, `action_source = ANSWER_KEY_SCRIPT` |
| Publish Readiness, IT: **Publish harmonized data for IT** | App started job run `528730816408561` (`evidence/jobs/halvard_publish_harmonized_528730816408561.md`), SUCCESS |
| Ask the group data: "Show combined ratio by distribution channel." | Genie answered with SQL and a table: Agent 0.832, Bancassurance 0.838, Broker 0.830, Digital 0.875, Direct 0.834 (Spain only; Italy was not yet published) |

Audit split for IT in Lakebase (`evidence/lakebase_review_state.md`): 2 APPROVED + 1 REJECTED from `DATABRICKS_APP`
by philipp.tiefenbacher@databricks.com; 21 APPROVED from `ANSWER_KEY_SCRIPT`.

The model's rationale for `anno_riferimento` shows the cross-country reuse: "This aligns with the previously approved
mapping of anio -> reporting_year from the Spanish subsidiary."

## Rendered pages (country ES, after both publishes)

### Group Overview
```text
HARMONIZED ROWS 10000 | APPROVED / CORRECTED 23 | REJECTED 1 | PENDING 0 | MANDATORY PENDING 0
GROSS WRITTEN PREMIUM €41.3M | LOSS RATIO 64.5% | COMBINED RATIO 83.5%
MAPPING ACCURACY 100.0% | AUTO-ACCEPT RATE 100.0%
Review Progress (bar chart)
```

### Review Dashboard
```text
TOTAL COLUMNS 24 | APPROVED / CORRECTED 23 | REJECTED 1 | PENDING 0 | COVERAGE 95.8%
MANDATORY RESOLVED 14 / 14 | MANDATORY UNRESOLVED 0
Count by Review Status | Confidence Distribution by Review Status
```

### Publish Readiness
```text
READY TO PUBLISH — All mandatory mappings for ES are resolved.
Mandatory Columns: APPROVED anio, canal_distribucion, cobertura_principal, codigo_poliza, id_registro, mes, moneda,
  num_siniestros_declarados, num_siniestros_pagados, prima_bruta, prima_neta, provincia, segmento_cliente, tipo_riesgo
Non-Mandatory Columns: RESOLVED 9 | PENDING 0 | REJECTED 1
  APPROVED comisiones → commissions_eur
  REJECTED fecha_carga → NO_MATCH
  APPROVED gastos_gestion → management_expenses_eur
  APPROVED importe_reservas → claims_reserve_eur
  APPROVED importe_siniestros_bruto → gross_claims_incurred_eur
  APPROVED num_polizas_canceladas → cancelled_policies_count
  APPROVED num_polizas_nuevas → new_policies_count
  APPROVED num_polizas_renovadas → renewed_policies_count
  APPROVED ratio_siniestralidad → loss_ratio
  APPROVED zona_riesgo → risk_zone
[button] Publish harmonized data for ES
```

### Approved Mappings / Rejected Mappings
```text
23 mapping(s) approved or corrected — Select column to reset [anio] — Reset to Pending
Rejected: Select column to reconsider [fecha_carga] — Optional comment — Reset to Pending
```

## Defects found by looking at the deployed app (all fixed, see git log)

1. `LAKEBASE_ENDPOINT` is not injected by the postgres resource → set in `app.yaml`.
2. App SP could not run `CREATE TABLE IF NOT EXISTS` on a job-created schema → grant CREATE, skip DDL when tables exist.
3. Overview counted summary rows instead of their counts (showed 2 instead of 23).
4. Evaluation metrics: a DataFrame was iterated as rows → `AttributeError`.
5. Reviewer identity was a numeric id → `X-Forwarded-Email`.
6. KPI tiles were unformatted floats → €M and %.
7. Sidebar dropdown text white on white.
