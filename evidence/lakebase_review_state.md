# Lakebase review state

Endpoint `projects/halvard-harmonization/branches/production/endpoints/primary`, schema `harmonization_review`.

## Queue status

| source_system | review_status | mandatory_flag | count |
|---|---|---|---|
| ES_PROPERTY_RAW | APPROVED | False | 9 |
| ES_PROPERTY_RAW | APPROVED | True | 14 |
| ES_PROPERTY_RAW | REJECTED | False | 1 |
| IT_PROPERTY_RAW | APPROVED | False | 9 |
| IT_PROPERTY_RAW | APPROVED | True | 14 |
| IT_PROPERTY_RAW | REJECTED | False | 1 |

## Last 40 audit rows

| audit_id | source_system | local_column_name | old_status | new_status | new_global_column_name | action_by | action_source | action_at |
|---|---|---|---|---|---|---|---|---|
| 48 | IT_PROPERTY_RAW | zona_rischio | PENDING | APPROVED | risk_zone | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:54.207767+00:00 |
| 47 | IT_PROPERTY_RAW | valuta | PENDING | APPROVED | currency | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:53.660381+00:00 |
| 46 | IT_PROPERTY_RAW | tipo_rischio | PENDING | APPROVED | risk_type | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:53.117670+00:00 |
| 45 | IT_PROPERTY_RAW | spese_gestione | PENDING | APPROVED | management_expenses_eur | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:52.568231+00:00 |
| 44 | IT_PROPERTY_RAW | sinistri_pagati | PENDING | APPROVED | claims_paid_count | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:52.023351+00:00 |
| 43 | IT_PROPERTY_RAW | sinistri_denunciati | PENDING | APPROVED | claims_reported_count | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:51.478952+00:00 |
| 42 | IT_PROPERTY_RAW | segmento_cliente | PENDING | APPROVED | customer_segment | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:50.928542+00:00 |
| 41 | IT_PROPERTY_RAW | riserva_sinistri | PENDING | APPROVED | claims_reserve_eur | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:50.385110+00:00 |
| 40 | IT_PROPERTY_RAW | regione | PENDING | APPROVED | region | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:49.832270+00:00 |
| 39 | IT_PROPERTY_RAW | rapporto_sinistri_premi | PENDING | APPROVED | loss_ratio | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:49.281618+00:00 |
| 38 | IT_PROPERTY_RAW | provvigioni | PENDING | APPROVED | commissions_eur | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:48.737357+00:00 |
| 37 | IT_PROPERTY_RAW | premi_netti | PENDING | APPROVED | net_written_premium_eur | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:48.191009+00:00 |
| 36 | IT_PROPERTY_RAW | premi_lordi_contabilizzati | PENDING | APPROVED | gross_written_premium_eur | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:47.643754+00:00 |
| 35 | IT_PROPERTY_RAW | num_polizze_rinnovate | PENDING | APPROVED | renewed_policies_count | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:47.101005+00:00 |
| 34 | IT_PROPERTY_RAW | num_polizze_nuove | PENDING | APPROVED | new_policies_count | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:46.550421+00:00 |
| 33 | IT_PROPERTY_RAW | num_polizze_cancellate | PENDING | APPROVED | cancelled_policies_count | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:45.657104+00:00 |
| 32 | IT_PROPERTY_RAW | mese_riferimento | PENDING | APPROVED | reporting_month | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:45.106718+00:00 |
| 31 | IT_PROPERTY_RAW | id_riga | PENDING | APPROVED | record_id | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:44.560888+00:00 |
| 30 | IT_PROPERTY_RAW | costo_sinistri_lordo | PENDING | APPROVED | gross_claims_incurred_eur | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:44.015848+00:00 |
| 29 | IT_PROPERTY_RAW | copertura_principale | PENDING | APPROVED | primary_coverage | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:43.463704+00:00 |
| 28 | IT_PROPERTY_RAW | codice_polizza | PENDING | APPROVED | policy_number | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 15:32:42.687467+00:00 |
| 27 | IT_PROPERTY_RAW | codice_agenzia_interno | PENDING | REJECTED |  | philipp.tiefenbacher@databricks.com | DATABRICKS_APP | 2026-10-06 15:30:05.848836+00:00 |
| 26 | IT_PROPERTY_RAW | canale_distributivo | PENDING | APPROVED | distribution_channel | philipp.tiefenbacher@databricks.com | DATABRICKS_APP | 2026-10-06 15:29:21.293407+00:00 |
| 25 | IT_PROPERTY_RAW | anno_riferimento | PENDING | APPROVED | reporting_year | philipp.tiefenbacher@databricks.com | DATABRICKS_APP | 2026-10-06 15:22:47.620942+00:00 |
| 24 | ES_PROPERTY_RAW | zona_riesgo | PENDING | APPROVED | risk_zone | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:02:05.935069+00:00 |
| 23 | ES_PROPERTY_RAW | tipo_riesgo | PENDING | APPROVED | risk_type | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:02:05.369970+00:00 |
| 22 | ES_PROPERTY_RAW | segmento_cliente | PENDING | APPROVED | customer_segment | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:02:04.817683+00:00 |
| 21 | ES_PROPERTY_RAW | ratio_siniestralidad | PENDING | APPROVED | loss_ratio | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:02:04.160163+00:00 |
| 20 | ES_PROPERTY_RAW | provincia | PENDING | APPROVED | region | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:02:03.596615+00:00 |
| 19 | ES_PROPERTY_RAW | prima_neta | PENDING | APPROVED | net_written_premium_eur | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:02:03.037693+00:00 |
| 18 | ES_PROPERTY_RAW | prima_bruta | PENDING | APPROVED | gross_written_premium_eur | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:02:02.476254+00:00 |
| 17 | ES_PROPERTY_RAW | num_siniestros_pagados | PENDING | APPROVED | claims_paid_count | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:02:01.899891+00:00 |
| 16 | ES_PROPERTY_RAW | num_siniestros_declarados | PENDING | APPROVED | claims_reported_count | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:02:01.337822+00:00 |
| 15 | ES_PROPERTY_RAW | num_polizas_renovadas | PENDING | APPROVED | renewed_policies_count | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:02:00.767632+00:00 |
| 14 | ES_PROPERTY_RAW | num_polizas_nuevas | PENDING | APPROVED | new_policies_count | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:02:00.207952+00:00 |
| 13 | ES_PROPERTY_RAW | num_polizas_canceladas | PENDING | APPROVED | cancelled_policies_count | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:01:59.655088+00:00 |
| 12 | ES_PROPERTY_RAW | moneda | PENDING | APPROVED | currency | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:01:59.106651+00:00 |
| 11 | ES_PROPERTY_RAW | mes | PENDING | APPROVED | reporting_month | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:01:58.536317+00:00 |
| 10 | ES_PROPERTY_RAW | importe_siniestros_bruto | PENDING | APPROVED | gross_claims_incurred_eur | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:01:57.944215+00:00 |
| 9 | ES_PROPERTY_RAW | importe_reservas | PENDING | APPROVED | claims_reserve_eur | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:01:57.380321+00:00 |
