# Lakebase review state

Endpoint `projects/halvard-harmonization/branches/production/endpoints/primary`, schema `harmonization_review`.

## Queue status

| source_system | review_status | mandatory_flag | count |
|---|---|---|---|
| ES_PROPERTY_RAW | APPROVED | False | 9 |
| ES_PROPERTY_RAW | APPROVED | True | 14 |
| ES_PROPERTY_RAW | REJECTED | False | 1 |
| IT_PROPERTY_RAW | PENDING | False | 10 |
| IT_PROPERTY_RAW | PENDING | True | 14 |

## Last 40 audit rows

| audit_id | source_system | local_column_name | old_status | new_status | new_global_column_name | action_by | action_source | action_at |
|---|---|---|---|---|---|---|---|---|
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
| 8 | ES_PROPERTY_RAW | id_registro | PENDING | APPROVED | record_id | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:01:56.819456+00:00 |
| 7 | ES_PROPERTY_RAW | gastos_gestion | PENDING | APPROVED | management_expenses_eur | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:01:56.260703+00:00 |
| 6 | ES_PROPERTY_RAW | fecha_carga | PENDING | REJECTED |  | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:01:55.366981+00:00 |
| 5 | ES_PROPERTY_RAW | comisiones | PENDING | APPROVED | commissions_eur | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:01:54.807144+00:00 |
| 4 | ES_PROPERTY_RAW | codigo_poliza | PENDING | APPROVED | policy_number | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:01:54.251479+00:00 |
| 3 | ES_PROPERTY_RAW | cobertura_principal | PENDING | APPROVED | primary_coverage | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:01:53.686177+00:00 |
| 2 | ES_PROPERTY_RAW | canal_distribucion | PENDING | APPROVED | distribution_channel | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:01:53.133577+00:00 |
| 1 | ES_PROPERTY_RAW | anio | PENDING | APPROVED | reporting_year | answer-key-script (philipp.tiefenbacher@databricks.com) | ANSWER_KEY_SCRIPT | 2026-10-06 14:01:52.331939+00:00 |
