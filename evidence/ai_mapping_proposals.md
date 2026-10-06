# Ai mapping proposals

_Collected 2026-10-06 15:06 UTC by scripts/collect_evidence.py from workspace https://fevm-agent-marketplace.cloud.databricks.com._

## Model proposals per country (before review)

```sql
SELECT source_system, local_column_name, proposed_global_column_name, proposed_match_type, confidence, review_status, final_global_column_name, left(mapping_rationale, 120) AS rationale FROM agent_marketplace_catalog.halvard_harmonization.column_mapping_candidates ORDER BY source_system, local_column_name
```

| source_system | local_column_name | proposed_global_column_name | proposed_match_type | confidence | review_status | final_global_column_name | rationale |
|---|---|---|---|---|---|---|---|
| ES_PROPERTY_RAW | anio | reporting_year | SEMANTIC_TRANSLATION | HIGH | APPROVED | reporting_year | 'anio' is Spanish for 'year' (año), and the sample value 2025 confirms it represents a calendar year. In the context of  |
| ES_PROPERTY_RAW | canal_distribucion | distribution_channel | SEMANTIC_TRANSLATION | HIGH | APPROVED | distribution_channel | 'canal_distribucion' directly translates from Spanish as 'distribution channel'. The sample values (Directo, Bancaseguro |
| ES_PROPERTY_RAW | cobertura_principal | primary_coverage | SEMANTIC_TRANSLATION | HIGH | APPROVED | primary_coverage | 'cobertura_principal' translates directly from Spanish as 'primary coverage'. The sample values (Ambos/Both, Contenido/C |
| ES_PROPERTY_RAW | codigo_poliza | policy_number | SEMANTIC_TRANSLATION | HIGH | APPROVED | policy_number | 'codigo_poliza' translates directly from Spanish as 'policy code', which is the unique identifier for an insurance polic |
| ES_PROPERTY_RAW | comisiones | commissions_eur | SEMANTIC_TRANSLATION | HIGH | APPROVED | commissions_eur | 'comisiones' is the Spanish word for 'commissions'. The sample values are numeric doubles consistent with monetary amoun |
| ES_PROPERTY_RAW | fecha_carga | NO_MATCH | NO_MATCH | HIGH | REJECTED |  | 'fecha_carga' translates to 'load date' or 'upload date' in English, representing the technical ETL/ingestion timestamp  |
| ES_PROPERTY_RAW | gastos_gestion | management_expenses_eur | SEMANTIC_TRANSLATION | HIGH | APPROVED | management_expenses_eur | 'gastos_gestion' translates directly from Spanish as 'management expenses' or 'administrative/operating expenses'. The n |
| ES_PROPERTY_RAW | id_registro | record_id | SEMANTIC_TRANSLATION | HIGH | APPROVED | record_id | 'id_registro' translates directly from Spanish as 'record identifier/ID', which is a conceptual equivalent to 'record_id |
| ES_PROPERTY_RAW | importe_reservas | claims_reserve_eur | SEMANTIC_TRANSLATION | HIGH | APPROVED | claims_reserve_eur | 'importe_reservas' translates directly from Spanish as 'reserve amount', referring to the monetary value of claims reser |
| ES_PROPERTY_RAW | importe_siniestros_bruto | gross_claims_incurred_eur | SEMANTIC_TRANSLATION | HIGH | APPROVED | gross_claims_incurred_eur | 'importe_siniestros_bruto' translates directly from Spanish as 'gross claims amount', where 'importe' means amount/value |
| ES_PROPERTY_RAW | mes | reporting_month | SEMANTIC_TRANSLATION | HIGH | APPROVED | reporting_month | 'mes' is the Spanish word for 'month', and the sample values (1-12) are consistent with calendar month integers. In the  |
| ES_PROPERTY_RAW | moneda | currency | SEMANTIC_TRANSLATION | HIGH | APPROVED | currency | 'moneda' is the Spanish word for 'currency', and the sample value 'EUR' confirms it stores currency codes. This is a dir |
| ES_PROPERTY_RAW | num_polizas_canceladas | cancelled_policies_count | SEMANTIC_TRANSLATION | HIGH | APPROVED | cancelled_policies_count | 'num_polizas_canceladas' directly translates from Spanish as 'number of cancelled policies', which is a precise conceptu |
| ES_PROPERTY_RAW | num_polizas_nuevas | new_policies_count | SEMANTIC_TRANSLATION | HIGH | APPROVED | new_policies_count | 'num_polizas_nuevas' translates directly from Spanish as 'number of new policies', which is the conceptual equivalent of |
| ES_PROPERTY_RAW | num_polizas_renovadas | renewed_policies_count | SEMANTIC_TRANSLATION | HIGH | APPROVED | renewed_policies_count | 'num_polizas_renovadas' translates directly from Spanish as 'number of renewed policies', which is a conceptual equivale |
| ES_PROPERTY_RAW | num_siniestros_declarados | claims_reported_count | SEMANTIC_TRANSLATION | HIGH | APPROVED | claims_reported_count | 'num_siniestros_declarados' directly translates from Spanish as 'number of declared/reported claims', which is the conce |
| ES_PROPERTY_RAW | num_siniestros_pagados | claims_paid_count | SEMANTIC_TRANSLATION | HIGH | APPROVED | claims_paid_count | 'num_siniestros_pagados' directly translates from Spanish as 'number of paid claims'. The integer sample values (2, 7, 4 |
| ES_PROPERTY_RAW | prima_bruta | gross_written_premium_eur | SEMANTIC_TRANSLATION | HIGH | APPROVED | gross_written_premium_eur | 'prima_bruta' is Spanish for 'gross premium' (literally 'gross premium/raw premium'), which directly corresponds to gros |
| ES_PROPERTY_RAW | prima_neta | net_written_premium_eur | SEMANTIC_TRANSLATION | HIGH | APPROVED | net_written_premium_eur | 'prima_neta' is Spanish for 'net premium'. In the context of Spanish property insurance reporting, this represents the n |
| ES_PROPERTY_RAW | provincia | region | SEMANTIC_TRANSLATION | HIGH | APPROVED | region | 'Provincia' is a Spanish administrative division (province) and the sample values (Zaragoza, Murcia, Barcelona, Madrid,  |
| ES_PROPERTY_RAW | ratio_siniestralidad | loss_ratio | SEMANTIC_TRANSLATION | HIGH | APPROVED | loss_ratio | 'ratio_siniestralidad' is the Spanish insurance term for loss ratio (claims incurred divided by earned premium). The sam |
| ES_PROPERTY_RAW | segmento_cliente | customer_segment | SEMANTIC_TRANSLATION | HIGH | APPROVED | customer_segment | 'segmento_cliente' directly translates to 'customer segment' in English. The sample values (Particular, Pequeña Empresa, |
| ES_PROPERTY_RAW | tipo_riesgo | risk_type | SEMANTIC_TRANSLATION | HIGH | APPROVED | risk_type | 'tipo_riesgo' directly translates from Spanish as 'type of risk', and the sample values (Inundación=Flood, Robo=Theft, D |
| ES_PROPERTY_RAW | zona_riesgo | risk_zone | SEMANTIC_TRANSLATION | HIGH | APPROVED | risk_zone | 'zona_riesgo' directly translates from Spanish as 'risk zone', and the sample values (Zona A, Zona B, Zona C, Zona D) ar |
| IT_PROPERTY_RAW | anno_riferimento | reporting_year | SEMANTIC_TRANSLATION | HIGH | PENDING |  | anno_riferimento is Italian for 'reference year', which is a direct conceptual equivalent of reporting_year. The sample  |
| IT_PROPERTY_RAW | canale_distributivo | distribution_channel | SEMANTIC_TRANSLATION | HIGH | PENDING |  | 'canale_distributivo' is the Italian equivalent of 'distribution channel'. The sample values (Broker, Agente, Diretto, B |
| IT_PROPERTY_RAW | codice_agenzia_interno | NO_MATCH | NO_MATCH | HIGH | PENDING |  | 'codice_agenzia_interno' translates from Italian as 'internal agency code', referring to an internal identifier for the  |
| IT_PROPERTY_RAW | codice_polizza | policy_number | SEMANTIC_TRANSLATION | HIGH | PENDING |  | 'codice_polizza' is Italian for 'policy code/number' (codice = code/number, polizza = policy), which is the direct seman |
| IT_PROPERTY_RAW | copertura_principale | primary_coverage | SEMANTIC_TRANSLATION | HIGH | PENDING |  | 'copertura_principale' is Italian for 'main/primary coverage', directly equivalent to the Spanish 'cobertura_principal'  |
| IT_PROPERTY_RAW | costo_sinistri_lordo | gross_claims_incurred_eur | SEMANTIC_TRANSLATION | HIGH | PENDING |  | costo_sinistri_lordo translates directly from Italian as 'gross claims cost', which is the conceptual equivalent of gros |
| IT_PROPERTY_RAW | id_riga | record_id | SEMANTIC_TRANSLATION | HIGH | PENDING |  | 'id_riga' translates from Italian as 'row ID' or 'line ID', which is a unique row/record identifier. This is semanticall |
| IT_PROPERTY_RAW | mese_riferimento | reporting_month | SEMANTIC_TRANSLATION | HIGH | PENDING |  | 'mese_riferimento' is Italian for 'reference month', which is the conceptual equivalent of 'reporting_month'. The sample |
| IT_PROPERTY_RAW | num_polizze_cancellate | cancelled_policies_count | SEMANTIC_TRANSLATION | HIGH | PENDING |  | 'num_polizze_cancellate' is Italian for 'number of cancelled policies'. This is the direct Italian equivalent of the pre |
| IT_PROPERTY_RAW | num_polizze_nuove | new_policies_count | SEMANTIC_TRANSLATION | HIGH | PENDING |  | Italian 'num_polizze_nuove' directly translates to 'number of new policies'. 'Polizze' is the Italian plural for 'polici |
| IT_PROPERTY_RAW | num_polizze_rinnovate | renewed_policies_count | SEMANTIC_TRANSLATION | HIGH | PENDING |  | The Italian column 'num_polizze_rinnovate' translates directly to 'number of renewed policies'. 'Polizze' means 'policie |
| IT_PROPERTY_RAW | premi_lordi_contabilizzati | gross_written_premium_eur | SEMANTIC_TRANSLATION | HIGH | PENDING |  | 'premi_lordi_contabilizzati' translates directly from Italian as 'gross written premiums accounted/booked', which is the |
| IT_PROPERTY_RAW | premi_netti | net_written_premium_eur | SEMANTIC_TRANSLATION | HIGH | PENDING |  | The Italian term 'premi_netti' directly translates to 'net premiums' or 'net written premium' in English. The sample val |
| IT_PROPERTY_RAW | provvigioni | commissions_eur | SEMANTIC_TRANSLATION | HIGH | PENDING |  | 'Provvigioni' is the standard Italian term for commissions/agent fees, directly equivalent to 'comisiones' in Spanish wh |
| IT_PROPERTY_RAW | rapporto_sinistri_premi | loss_ratio | SEMANTIC_TRANSLATION | HIGH | PENDING |  | 'rapporto_sinistri_premi' literally translates from Italian as 'claims-to-premium ratio', which is the definition of los |
| IT_PROPERTY_RAW | regione | region | SEMANTIC_TRANSLATION | HIGH | PENDING |  | 'regione' is the Italian word for region, referring to Italian administrative regions (e.g. Sicilia, Puglia, Lombardia,  |
| IT_PROPERTY_RAW | riserva_sinistri | claims_reserve_eur | SEMANTIC_TRANSLATION | HIGH | PENDING |  | Italian 'riserva_sinistri' directly translates to 'claims reserve' in English. 'Riserva' means reserve and 'sinistri' me |
| IT_PROPERTY_RAW | segmento_cliente | customer_segment | DIRECT | HIGH | PENDING |  | 'segmento_cliente' is Italian for 'customer segment'. The sample values (Piccola Impresa, Privato, Grande Impresa, Media |
| IT_PROPERTY_RAW | sinistri_denunciati | claims_reported_count | SEMANTIC_TRANSLATION | HIGH | PENDING |  | 'sinistri_denunciati' is Italian for 'claims reported/declared', which is the count of claims that have been notified/re |
| IT_PROPERTY_RAW | sinistri_pagati | claims_paid_count | SEMANTIC_TRANSLATION | HIGH | PENDING |  | 'sinistri_pagati' is Italian for 'claims paid', directly equivalent to the count of paid claims. The sample values (2, 7 |
| IT_PROPERTY_RAW | spese_gestione | management_expenses_eur | SEMANTIC_TRANSLATION | HIGH | PENDING |  | 'spese_gestione' is Italian for 'management expenses' (spese = expenses, gestione = management), directly equivalent to  |
| IT_PROPERTY_RAW | tipo_rischio | risk_type | DIRECT | HIGH | PENDING |  | "tipo_rischio" is the Italian equivalent of "tipo_riesgo" (Spanish), both translating directly to "risk type" in English |
| IT_PROPERTY_RAW | valuta | currency | SEMANTIC_TRANSLATION | HIGH | PENDING |  | 'valuta' is the Italian word for 'currency', directly equivalent to the Spanish 'moneda' which was previously approved a |
| IT_PROPERTY_RAW | zona_rischio | risk_zone | DIRECT | HIGH | PENDING |  | 'zona_rischio' is the Italian equivalent of 'zona_riesgo' (Spanish), both meaning 'risk zone'. The previously approved m |

## Model usage

```sql
SELECT * FROM agent_marketplace_catalog.halvard_harmonization.ai_mapping_usage_metrics ORDER BY source_system
```

| run_id | source_system | mapping_type | source_field_or_column | candidate_rows | success_rows | failed_rows | low_confidence_rows | estimated_prompt_units | estimated_response_units | estimated_cost_eur | recorded_at |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0667c5c6-3f99-44bb-a19d-6bea58c3c305 | ES_PROPERTY_RAW | COLUMN | ES_PROPERTY_RAW | 24 | 5 | 19 | 0 | 4800.0 | 1920.0 | 0.01344 | 2026-10-06T13:54:15.477Z |
| 71ef0f76-676d-4a29-812c-bf047f476859 | ES_PROPERTY_RAW | COLUMN | ES_PROPERTY_RAW | 24 | 24 | 0 | 0 | 4800.0 | 1920.0 | 0.01344 | 2026-10-06T14:00:07.456Z |
| f1adc006-9f69-487b-981f-81a35e3e67e9 | IT_PROPERTY_RAW | COLUMN | IT_PROPERTY_RAW | 24 | 24 | 0 | 0 | 4800.0 | 1920.0 | 0.01344 | 2026-10-06T14:40:13.028Z |
