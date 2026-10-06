# Dq results

_Collected 2026-10-06 15:53 UTC by scripts/collect_evidence.py from workspace https://fevm-agent-marketplace.cloud.databricks.com._

## Data quality checks (latest run per country)

```sql
SELECT source_system, check_name, check_status, metric_value, left(details, 120) AS details, recorded_at FROM agent_marketplace_catalog.halvard_harmonization.data_quality_results QUALIFY dense_rank() OVER (PARTITION BY source_system ORDER BY recorded_at DESC) = 1 ORDER BY source_system, check_status, check_name
```

| source_system | check_name | check_status | metric_value | details | recorded_at |
|---|---|---|---|---|---|
| ES_PROPERTY_RAW | column_mapping_coverage | PASSED | 100.0 | 14/14 mandatory columns have active dict entries | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | column_mapping_version_present | PASSED | 0.0 | 0 rows without expected mapping_status flag | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | loss_ratio_in_range | PASSED | 0.0 | 0 <= loss_ratio <= 2: 0 violations | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_cancelled_policies_count | PASSED | 0.0 | 0 null cancelled_policies_count values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_claims_paid_count | PASSED | 0.0 | 0 null claims_paid_count values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_claims_reported_count | PASSED | 0.0 | 0 null claims_reported_count values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_commissions_eur | PASSED | 0.0 | 0 null commissions_eur values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_currency | PASSED | 0.0 | 0 null currency values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_customer_segment | PASSED | 0.0 | 0 null customer_segment values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_distribution_channel | PASSED | 0.0 | 0 null distribution_channel values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_gross_claims_incurred_eur | PASSED | 0.0 | 0 null gross_claims_incurred_eur values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_gross_written_premium_eur | PASSED | 0.0 | 0 null gross_written_premium_eur values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_management_expenses_eur | PASSED | 0.0 | 0 null management_expenses_eur values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_net_written_premium_eur | PASSED | 0.0 | 0 null net_written_premium_eur values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_new_policies_count | PASSED | 0.0 | 0 null new_policies_count values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_policy_number | PASSED | 0.0 | 0 null policy_number values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_primary_coverage | PASSED | 0.0 | 0 null primary_coverage values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_record_id | PASSED | 0.0 | 0 null record_id values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_region | PASSED | 0.0 | 0 null region values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_renewed_policies_count | PASSED | 0.0 | 0 null renewed_policies_count values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_reporting_month | PASSED | 0.0 | 0 null reporting_month values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_reporting_year | PASSED | 0.0 | 0 null reporting_year values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_risk_type | PASSED | 0.0 | 0 null risk_type values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | not_null_risk_zone | PASSED | 0.0 | 0 null risk_zone values | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | numeric_claims_paid_lte_reported | PASSED | 0.0 | claims_paid_count <= claims_reported_count: 0 violations | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | numeric_premium_gross_gte_net | PASSED | 0.0 | gross_written_premium_eur >= net_written_premium_eur: 0 violations | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | row_count_parity | PASSED | 10000.0 | raw=10,000, harmonized=10,000 | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | values_in_allowed_list_customer_segment | PASSED | 0.0 | customer_segment values are in the configured allow-list: 0 violations | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | values_in_allowed_list_distribution_channel | PASSED | 0.0 | distribution_channel values are in the configured allow-list: 0 violations | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | values_in_allowed_list_risk_type | PASSED | 0.0 | risk_type values are in the configured allow-list: 0 violations | 2026-10-06T14:20:20.847Z |
| ES_PROPERTY_RAW | values_in_allowed_list_risk_zone | PASSED | 0.0 | risk_zone values are in the configured allow-list: 0 violations | 2026-10-06T14:20:20.847Z |
| IT_PROPERTY_RAW | column_mapping_coverage | PASSED | 100.0 | 14/14 mandatory columns have active dict entries | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | column_mapping_version_present | PASSED | 0.0 | 0 rows without expected mapping_status flag | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | loss_ratio_in_range | PASSED | 0.0 | 0 <= loss_ratio <= 2: 0 violations | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_cancelled_policies_count | PASSED | 0.0 | 0 null cancelled_policies_count values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_claims_paid_count | PASSED | 0.0 | 0 null claims_paid_count values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_claims_reported_count | PASSED | 0.0 | 0 null claims_reported_count values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_commissions_eur | PASSED | 0.0 | 0 null commissions_eur values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_currency | PASSED | 0.0 | 0 null currency values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_customer_segment | PASSED | 0.0 | 0 null customer_segment values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_distribution_channel | PASSED | 0.0 | 0 null distribution_channel values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_gross_claims_incurred_eur | PASSED | 0.0 | 0 null gross_claims_incurred_eur values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_gross_written_premium_eur | PASSED | 0.0 | 0 null gross_written_premium_eur values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_management_expenses_eur | PASSED | 0.0 | 0 null management_expenses_eur values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_net_written_premium_eur | PASSED | 0.0 | 0 null net_written_premium_eur values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_new_policies_count | PASSED | 0.0 | 0 null new_policies_count values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_policy_number | PASSED | 0.0 | 0 null policy_number values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_primary_coverage | PASSED | 0.0 | 0 null primary_coverage values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_record_id | PASSED | 0.0 | 0 null record_id values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_region | PASSED | 0.0 | 0 null region values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_renewed_policies_count | PASSED | 0.0 | 0 null renewed_policies_count values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_reporting_month | PASSED | 0.0 | 0 null reporting_month values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_reporting_year | PASSED | 0.0 | 0 null reporting_year values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_risk_type | PASSED | 0.0 | 0 null risk_type values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | not_null_risk_zone | PASSED | 0.0 | 0 null risk_zone values | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | numeric_claims_paid_lte_reported | PASSED | 0.0 | claims_paid_count <= claims_reported_count: 0 violations | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | numeric_premium_gross_gte_net | PASSED | 0.0 | gross_written_premium_eur >= net_written_premium_eur: 0 violations | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | row_count_parity | PASSED | 10000.0 | raw=10,000, harmonized=10,000 | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | values_in_allowed_list_customer_segment | PASSED | 0.0 | customer_segment values are in the configured allow-list: 0 violations | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | values_in_allowed_list_distribution_channel | PASSED | 0.0 | distribution_channel values are in the configured allow-list: 0 violations | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | values_in_allowed_list_risk_type | PASSED | 0.0 | risk_type values are in the configured allow-list: 0 violations | 2026-10-06T15:37:28.582Z |
| IT_PROPERTY_RAW | values_in_allowed_list_risk_zone | PASSED | 0.0 | risk_zone values are in the configured allow-list: 0 violations | 2026-10-06T15:37:28.582Z |

## Value translations

```sql
SELECT source_system, source_field, raw_value, harmonized_value, approval_status, approved_by FROM agent_marketplace_catalog.halvard_harmonization.value_mapping_dictionary ORDER BY 1, 2, 3
```

| source_system | source_field | raw_value | harmonized_value | approval_status | approved_by |
|---|---|---|---|---|---|
| ES_PROPERTY_RAW | customer_segment | Gran Empresa | Large Enterprise | APPROVED | auto:allowed-list |
| ES_PROPERTY_RAW | customer_segment | Mediana Empresa | Medium Business | APPROVED | auto:allowed-list |
| ES_PROPERTY_RAW | customer_segment | Particular | Individual | APPROVED | auto:allowed-list |
| ES_PROPERTY_RAW | customer_segment | Pequeña Empresa | Small Business | APPROVED | auto:allowed-list |
| ES_PROPERTY_RAW | distribution_channel | Agente | Agent | APPROVED | auto:allowed-list |
| ES_PROPERTY_RAW | distribution_channel | Bancaseguros | Bancassurance | APPROVED | auto:allowed-list |
| ES_PROPERTY_RAW | distribution_channel | Corredor | Broker | APPROVED | auto:allowed-list |
| ES_PROPERTY_RAW | distribution_channel | Digital | Digital | APPROVED | auto:allowed-list |
| ES_PROPERTY_RAW | distribution_channel | Directo | Direct | APPROVED | auto:allowed-list |
| ES_PROPERTY_RAW | risk_type | Daños Eléctricos | Electrical Damage | APPROVED | auto:allowed-list |
| ES_PROPERTY_RAW | risk_type | Daños por Agua | Water Damage | APPROVED | auto:allowed-list |
| ES_PROPERTY_RAW | risk_type | Incendio | Fire | APPROVED | auto:allowed-list |
| ES_PROPERTY_RAW | risk_type | Inundación | Flood | APPROVED | auto:allowed-list |
| ES_PROPERTY_RAW | risk_type | Responsabilidad Civil | Liability | APPROVED | auto:allowed-list |
| ES_PROPERTY_RAW | risk_type | Robo | Theft | APPROVED | auto:allowed-list |
| ES_PROPERTY_RAW | risk_zone | Zona A | Zone A | APPROVED | auto:allowed-list |
| ES_PROPERTY_RAW | risk_zone | Zona B | Zone B | APPROVED | auto:allowed-list |
| ES_PROPERTY_RAW | risk_zone | Zona C | Zone C | APPROVED | auto:allowed-list |
| ES_PROPERTY_RAW | risk_zone | Zona D | Zone D | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | customer_segment | Grande Impresa | Large Enterprise | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | customer_segment | Media Impresa | Medium Business | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | customer_segment | Piccola Impresa | Small Business | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | customer_segment | Privato | Individual | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | distribution_channel | Agente | Agent | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | distribution_channel | Bancassicurazione | Bancassurance | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | distribution_channel | Broker | Broker | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | distribution_channel | Digitale | Digital | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | distribution_channel | Diretto | Direct | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | risk_type | Alluvione | Flood | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | risk_type | Danni Elettrici | Electrical Damage | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | risk_type | Danni Idrici | Water Damage | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | risk_type | Furto | Theft | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | risk_type | Incendio | Fire | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | risk_type | Responsabilità Civile | Liability | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | risk_zone | Zona A | Zone A | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | risk_zone | Zona B | Zone B | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | risk_zone | Zona C | Zone C | APPROVED | auto:allowed-list |
| IT_PROPERTY_RAW | risk_zone | Zona D | Zone D | APPROVED | auto:allowed-list |
