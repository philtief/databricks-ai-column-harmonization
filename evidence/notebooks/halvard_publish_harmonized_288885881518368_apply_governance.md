# apply_governance (job halvard_publish_harmonized, run 288885881518368, task run 436802894952277, SUCCESS)

```python
%run ./_shared_utils
```

```python
dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "agent_marketplace_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "halvard_harmonization", "Schema Name")
dbutils.widgets.text("privileged_principals", "", "Privileged Principals")
dbutils.widgets.text("app_service_principal", "", "App Service Principal")
dbutils.widgets.text("app_name", "", "App name (resolves the app service principal)")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
privileged_principals = [
    principal.strip() for principal in dbutils.widgets.get("privileged_principals").split(",") if principal.strip()
]
app_service_principal = dbutils.widgets.get("app_service_principal").strip()
app_name = dbutils.widgets.get("app_name").strip()
if not app_service_principal and app_name:
    # The jobs get the app name, not its SP id: the app references the publish job, so a direct reference is a cycle.
    from databricks.sdk import WorkspaceClient

    app_service_principal = WorkspaceClient().apps.get(app_name).service_principal_client_id
config = load_harmonization_config()

from harmonization.governance import governance_plan, sql_str

DB = f"`{catalog_name}`.`{schema_name}`"
HARMONIZED_TABLE = f"{DB}.`harmonized_property_monthly`"

current_user = spark.sql("SELECT current_user() AS current_user").first()["current_user"]
if current_user:
    privileged_principals.append(current_user)
if app_service_principal:
    privileged_principals.append(app_service_principal)
privileged_principals = list(dict.fromkeys(privileged_principals))

print(f"Catalog: {catalog_name}, schema: {schema_name}")
print(f"Current user added to privileged principals: {current_user}")
print(f"App service principal: {app_service_principal or '(not supplied)'}")
```

Output:
```text
Catalog: agent_marketplace_catalog, schema: halvard_harmonization
Current user added to privileged principals: philipp.tiefenbacher@databricks.com
App service principal: 83b3161c-5563-4469-8548-7e4b979eeb1d
```

```python
import json

statements = governance_plan(
    catalog=catalog_name,
    schema=schema_name,
    config=config,
    privileged_principals=privileged_principals,
    app_sp=app_service_principal,
)
summary = {
    "catalog_name": catalog_name,
    "schema_name": schema_name,
    "current_user": current_user,
    "app_service_principal": app_service_principal,
    "privileged_principals": privileged_principals,
    "ok": [],
    "skipped": [],
    "failed": [],
}

for description, sql in statements:
    try:
        spark.sql(sql).collect()
        print(f"OK: {description}")
        summary["ok"].append(description)
    except Exception as exc:
        if description.startswith("Grant "):
            print(f"SKIPPED: {description} ({exc})")
            summary["skipped"].append({"description": description, "error": str(exc)})
        else:
            print(f"FAILED: {description} ({exc})")
            summary["failed"].append({"description": description, "error": str(exc)})
            raise RuntimeError(f"Governance statement failed: {description}") from exc
```

```python
print("--- SHOW GRANTS ON SCHEMA ---")
display(spark.sql(f"SHOW GRANTS ON SCHEMA {DB}"))

print("--- TABLE TAGS ---")
table_tags = spark.sql(
    f"""
    SELECT table_name, tag_name, tag_value
    FROM `{catalog_name}`.information_schema.table_tags
    WHERE catalog_name = {sql_str(catalog_name)}
      AND schema_name = {sql_str(schema_name)}
    ORDER BY table_name, tag_name
    """
)
display(table_tags)

print("--- COLUMN TAGS ---")
column_tags = spark.sql(
    f"""
    SELECT table_name, column_name, tag_name, tag_value
    FROM `{catalog_name}`.information_schema.column_tags
    WHERE catalog_name = {sql_str(catalog_name)}
      AND schema_name = {sql_str(schema_name)}
    ORDER BY table_name, column_name, tag_name
    """
)
display(column_tags)

print("--- TABLE ROW FILTER ---")
table_details = spark.sql(f"DESCRIBE TABLE EXTENDED {HARMONIZED_TABLE}").collect()
for row in table_details:
    values = [str(row[column]) for column in row.__fields__ if "row filter" in str(row[column]).lower()]
    if values:
        print(" | ".join(values))

print("--- ROW FILTER FUNCTION DEFINITION ---")
display(spark.sql(f"DESCRIBE FUNCTION EXTENDED {DB}.country_row_filter"))

print("--- HARMONIZED ROWS BY SOURCE COUNTRY ---")
display(
    spark.sql(
        f"""
        SELECT source_country, count(*) AS row_count
        FROM {HARMONIZED_TABLE}
        GROUP BY source_country
        ORDER BY source_country
        """
    )
)

print(json.dumps(summary, sort_keys=True))
dbutils.notebook.exit(json.dumps(summary))
```
