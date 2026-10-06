# generate_country_files (job halvard_propose_mappings, run 1121820803949496, task run 252526713850993, SUCCESS)

```python
%run ../notebooks/_shared_utils
```

```python
import json

from harmonization.generator import (
    COUNTRIES,
    column_specs,
    file_name,
    generate_rows,
    month_partitions,
    to_csv,
)

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "your_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "harmonizing_agent", "Schema Name")
dbutils.widgets.text("countries", "ES,IT", "Countries")
dbutils.widgets.text("rows_per_country", "10000", "Rows Per Country")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
countries = [country.strip().upper() for country in dbutils.widgets.get("countries").split(",") if country.strip()]
rows_per_country = int(dbutils.widgets.get("rows_per_country"))

if not catalog_name or not schema_name:
    raise ValueError("catalog_name and schema_name must not be empty")
invalid_countries = sorted(set(countries) - set(COUNTRIES))
if invalid_countries:
    raise ValueError(f"Unknown countries: {invalid_countries}. Valid countries: {sorted(COUNTRIES)}")
if rows_per_country < 0:
    raise ValueError("rows_per_country must not be negative")

landing_root = f"/Volumes/{catalog_name}/{schema_name}/landing"
print(f"Landing zone: {landing_root}")
```

Output:
```text
Landing zone: /Volumes/agent_marketplace_catalog/halvard_harmonization/landing
```

```python
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog_name}`.`{schema_name}`.`landing`")
```

```python
files_written = 0
files_skipped = 0
rows_written_by_country: dict[str, int] = {}

for country in countries:
    country_directory = f"{landing_root}/{country.lower()}"
    dbutils.fs.mkdirs(country_directory)
    existing_files = {file.name.rstrip("/") for file in dbutils.fs.ls(country_directory)}

    specs = column_specs(country)
    columns = [spec.name for spec in specs]
    rows = generate_rows(country, rows_per_country, COUNTRIES[country]["seed"])
    rows_written_by_country[country] = len(rows)

    for yyyymm, month_rows in month_partitions(rows).items():
        relative_path = f"{country_directory}/{file_name(yyyymm)}"
        if file_name(yyyymm) in existing_files:
            files_skipped += 1
            continue
        dbutils.fs.put(relative_path, to_csv(month_rows, columns), overwrite=False)
        files_written += 1

summary = {
    "files_written": files_written,
    "files_skipped": files_skipped,
    "rows_per_country": rows_written_by_country,
}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
```

Output:
```text
{"files_written": 0, "files_skipped": 24, "rows_per_country": {"ES": 10000, "IT": 10000}}
```
