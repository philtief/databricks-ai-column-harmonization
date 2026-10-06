# Genie benchmark

- timestamp: 2026-10-06T15:47:13.747612+00:00

- space id: 01f1c192d2f31c5bb935f924df7b1bd3

## What is the loss ratio by country for the last 3 months?

Status: SUCCESS

**Answer**

For the last **3 months** in the data, the loss ratio is **64.20%** for **ES** and **65.09%** for **IT**. Across the 2 countries shown, **IT** is slightly higher than **ES** by about **0.89 percentage points**.

**Query description**

You want to see the loss ratio expressed as a percentage for each country over the last three months based on the most recent data available.

**Generated SQL**

```sql
SELECT `source_country` AS `Country`, try_divide(100 * SUM(`gross_claims_incurred_eur`),NULLIF(SUM(`gross_written_premium_eur`), 0)) AS `Loss Ratio` FROM `agent_marketplace_catalog`.`halvard_harmonization`.`harmonized_property_monthly` WHERE `source_country` IS NOT NULL AND make_date(`reporting_year`, `reporting_month`, 1) >= add_months((SELECT MAX(make_date(`reporting_year`, `reporting_month`, 1)) FROM `agent_marketplace_catalog`.`halvard_harmonization`.`harmonized_property_monthly`), -2) AND make_date(`reporting_year`, `reporting_month`, 1) <= (SELECT MAX(make_date(`reporting_year`, `reporting_month`, 1)) FROM `agent_marketplace_catalog`.`halvard_harmonization`.`harmonized_property_monthly`) GROUP BY `source_country` ORDER BY `Country` ASC
```

| Country | Loss Ratio |
|---|---|
| ES | 64.20276196018017 |
| IT | 65.0944655054216 |

## Which distribution channel has the highest combined ratio in Italy?

Status: SUCCESS

**Answer**

In Italy, **Digital** has the highest combined ratio at **0.8572189383300954**. Based on the single returned result, **Digital** is the top-performing distribution channel on this measure in the available data.

**Query description**

You want to find the distribution channel in Italy that has the highest combined ratio.

**Generated SQL**

```sql
WITH ranked_channels AS (
  SELECT
    `Distribution Channel`,
    MEASURE(`Combined Ratio`) AS `combined_ratio`,
    RANK() OVER (ORDER BY MEASURE(`Combined Ratio`) DESC) AS `rank`
  FROM `agent_marketplace_catalog`.`halvard_harmonization`.`mv_group_property_kpis`
  WHERE `Country` = 'IT'
    AND `Distribution Channel` IS NOT NULL
  GROUP BY ALL
)
SELECT
  `Distribution Channel`,
  `combined_ratio`
FROM ranked_channels
WHERE `rank` = 1
```

| Distribution Channel | combined_ratio |
|---|---|
| Digital | 0.8572189383300954 |

## How many columns of the Italian feed were mapped by AI and approved without correction?

Status: SUCCESS

**Answer**

The Italian feed (`source_system` matching `IT_PROPERTY_RAW`) has **23** columns with `review_status = APPROVED`, meaning **23** columns were approved without correction. In the result shown, every counted Italian mapping falls into the approved-without-correction outcome.

**Query description**

You want to see the number of columns from the Italian data feed that were mapped by AI and approved without any corrections.

**Generated SQL**

```sql
SELECT COUNT(*) AS `columns`
FROM `agent_marketplace_catalog`.`halvard_harmonization`.`column_mapping_candidates`
WHERE `source_system` ILIKE '%IT_PROPERTY_RAW%'
  AND `review_status` ILIKE '%APPROVED%'
```

| columns |
|---|
| 23 |

## Show gross written premium by month and country

Status: SUCCESS

**Answer**

Examples include:
- **2025-01** — **ES:** EUR **3,436,249.92**; **IT:** EUR **3,480,610.75**
- **2025-02** — **ES:** EUR **3,402,989.13**; **IT:** EUR **3,488,771.95**
- **2025-03** — **ES:** EUR **3,543,417.97**; **IT:** EUR **3,347,289.03**
- **2025-04** — **ES:** EUR **3,504,533.47**; **IT:** EUR **3,490,669.93**
- **2025-05** — **ES:** EUR **3,452,545.94**; **IT:** EUR **3,355,171.71**
Across all **24 month-country rows** shown for 2025, gross written premium stays within a relatively narrow range, from **EUR 3,302,653.46** (ES, 2025-08) to **EUR 3,553,745.33** (IT, 2025-07).

**Query description**

You want to see the total gross written premium for each month and country.

**Generated SQL**

```sql
SELECT
  `Reporting Period`,
  `Country`,
  MEASURE(`Gross Written Premium`) AS `Gross Written Premium`
FROM `agent_marketplace_catalog`.`halvard_harmonization`.`mv_group_property_kpis`
WHERE `Reporting Period` IS NOT NULL
  AND `Country` IS NOT NULL
GROUP BY ALL
ORDER BY `Reporting Period` ASC, `Country` ASC
```

| Reporting Period | Country | Gross Written Premium |
|---|---|---|
| 2025-01-01 | ES | 3436249.92 |
| 2025-01-01 | IT | 3480610.7499999967 |
| 2025-02-01 | ES | 3402989.13 |
| 2025-02-01 | IT | 3488771.9500000016 |
| 2025-03-01 | ES | 3543417.970000001 |
| 2025-03-01 | IT | 3347289.0300000017 |
| 2025-04-01 | ES | 3504533.470000002 |
| 2025-04-01 | IT | 3490669.9299999955 |
| 2025-05-01 | ES | 3452545.939999994 |
| 2025-05-01 | IT | 3355171.7100000028 |

## Which data quality checks failed in the last publish run?

Status: SUCCESS

**Answer**

No failed data quality checks were returned for the queried last publish run: the result set contains **0 rows**. Based on this data, there are **no failed checks to list** for the run identified by this query.

**Query description**

You want to see all the data quality checks that failed in the most recent publish run.

**Generated SQL**

```sql
WITH `failed_runs` AS (
  SELECT
    `run_id`,
    MAX(`recorded_at`) AS `latest_recorded_at`
  FROM `agent_marketplace_catalog`.`halvard_harmonization`.`data_quality_results`
  WHERE `run_id` IS NOT NULL
    AND `check_status` = 'FAILED'
  GROUP BY `run_id`
),
`latest_failed_run` AS (
  SELECT `run_id`
  FROM `failed_runs`
  ORDER BY `latest_recorded_at` DESC
  LIMIT 1
)
SELECT
  `dqr`.`run_id`,
  `dqr`.`source_system`,
  `dqr`.`check_name`,
  `dqr`.`check_status`,
  `dqr`.`metric_value`,
  `dqr`.`recorded_at`,
  `dqr`.`details`
FROM `agent_marketplace_catalog`.`halvard_harmonization`.`data_quality_results` AS `dqr`
INNER JOIN `latest_failed_run` AS `lfr`
  ON `dqr`.`run_id` = `lfr`.`run_id`
WHERE `dqr`.`check_status` = 'FAILED'
ORDER BY `dqr`.`source_system` ASC, `dqr`.`check_name` ASC, `dqr`.`recorded_at` ASC
```

| run_id | source_system | check_name | check_status | metric_value | recorded_at | details |
|---|---|---|---|---|---|---|

## Which local column feeds net_written_premium_eur in Spain?

Status: SUCCESS

**Answer**

In Spain, **local column `prima_neta`** feeds **`net_written_premium_eur`** in source system **`ES_PROPERTY_RAW`**. The mapping uses **`SEMANTIC_TRANSLATION`** and is recorded in **mapping version `v1`**.

**Query description**

You want to see which local columns from the Spanish property raw data source are mapped to the global column for net written premium in euros.

**Generated SQL**

```sql
SELECT
  `source_system`,
  `local_column_name`,
  `global_column_name`,
  `match_type`,
  `mapping_version`
FROM `agent_marketplace_catalog`.`halvard_harmonization`.`column_mapping_dictionary`
WHERE `source_system` = 'ES_PROPERTY_RAW'
  AND `global_column_name` = 'net_written_premium_eur'
```

| source_system | local_column_name | global_column_name | match_type | mapping_version |
|---|---|---|---|---|
| ES_PROPERTY_RAW | prima_neta | net_written_premium_eur | SEMANTIC_TRANSLATION | v1 |

answered with SQL: 6/6
