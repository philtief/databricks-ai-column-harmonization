# Harmonized kpis

_Collected 2026-10-06 15:53 UTC by scripts/collect_evidence.py from workspace https://fevm-agent-marketplace.cloud.databricks.com._

## Group KPIs by country (metric view)

```sql
SELECT `Country`, MEASURE(`Gross Written Premium`) AS gwp_eur, MEASURE(`Gross Claims Incurred`) AS claims_eur, round(MEASURE(`Loss Ratio`), 3) AS loss_ratio, round(MEASURE(`Expense Ratio`), 3) AS expense_ratio, round(MEASURE(`Combined Ratio`), 3) AS combined_ratio FROM agent_marketplace_catalog.halvard_harmonization.mv_group_property_kpis GROUP BY ALL ORDER BY 1
```

| Country | gwp_eur | claims_eur | loss_ratio | expense_ratio | combined_ratio |
|---|---|---|---|---|---|
| ES | 4.131192158000003E7 | 2.665773841000001E7 | 0.645 | 0.19 | 0.835 |
| IT | 4.1387675889999874E7 | 2.6728528829999927E7 | 0.646 | 0.189 | 0.835 |

## Combined ratio by country and channel

```sql
SELECT `Country`, `Distribution Channel`, round(MEASURE(`Combined Ratio`), 3) AS combined_ratio FROM agent_marketplace_catalog.halvard_harmonization.mv_group_property_kpis GROUP BY ALL ORDER BY 1, 2
```

| Country | Distribution Channel | combined_ratio |
|---|---|---|
| ES | Agent | 0.832 |
| ES | Bancassurance | 0.838 |
| ES | Broker | 0.83 |
| ES | Digital | 0.875 |
| ES | Direct | 0.834 |
| IT | Agent | 0.833 |
| IT | Bancassurance | 0.851 |
| IT | Broker | 0.827 |
| IT | Digital | 0.857 |
| IT | Direct | 0.833 |

## Harmonized rows per country

```sql
SELECT source_country, count(*) AS rows FROM agent_marketplace_catalog.halvard_harmonization.harmonized_property_monthly GROUP BY 1 ORDER BY 1
```

| source_country | rows |
|---|---|
| ES | 10000 |
| IT | 10000 |
