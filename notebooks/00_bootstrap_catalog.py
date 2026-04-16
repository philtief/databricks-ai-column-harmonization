# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Bootstrap Catalog and Schema
# MAGIC
# MAGIC Creates the Unity Catalog catalog and schema required by all downstream notebooks.
# MAGIC
# MAGIC **Target:** `{catalog_name}.harmonizing_agent`

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "pt_catalog", "Catalog Name")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = "harmonizing_agent"

print(f"STEP 1 — Parameters loaded")
print(f"  catalog_name : {catalog_name}")
print(f"  schema_name  : {schema_name}")

# COMMAND ----------

# MAGIC %md ## STEP 2 — Create Catalog

# COMMAND ----------

print(f"STEP 2 — Ensuring catalog '{catalog_name}' exists ...")

# Try to create the catalog. On some workspaces the catalog is pre-created and
# CREATE CATALOG may fail because the metastore has no storage root configured.
# In that case we verify the catalog is already visible and continue.
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog_name}`")
    print(f"  CREATE CATALOG succeeded.")
except Exception as _ce:
    print(f"  [WARN] CREATE CATALOG failed (likely pre-created on this workspace): {_ce}")
    print(f"  Checking if catalog already exists ...")
    existing = [r[0] for r in spark.sql(f"SHOW CATALOGS LIKE '{catalog_name}'").collect()]
    if catalog_name not in existing:
        raise Exception(f"FATAL: Catalog '{catalog_name}' does not exist and could not be created: {_ce}")
    print(f"  Catalog '{catalog_name}' already exists — continuing.")

try:
    spark.sql(f"""
      COMMENT ON CATALOG `{catalog_name}` IS
      'PT Harmonization catalog. Hosts property insurance harmonization objects for the global column-mapping solution.'
    """)
except Exception as _e:
    print(f"  [WARN] Could not set catalog comment (may lack ALTER privilege): {_e}")

print(f"  Catalog '{catalog_name}' is ready.")

# COMMAND ----------

# MAGIC %md ## STEP 3 — Create Schema

# COMMAND ----------

print(f"STEP 3 — Creating schema '{catalog_name}.{schema_name}' if not exists ...")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`")
try:
    spark.sql(f"""
      COMMENT ON SCHEMA `{catalog_name}`.`{schema_name}` IS
      'Single schema for all PT harmonization objects: raw data, harmonized output, column-mapping control tables, ops tables, and views. Pattern: Spain local column names -> global English column names.'
    """)
except Exception as _e:
    print(f"  [WARN] Could not set schema comment: {_e}")

print(f"  Schema '{catalog_name}.{schema_name}' is ready.")

# COMMAND ----------

# MAGIC %md ## STEP 4 — Verify

# COMMAND ----------

print("STEP 4 — Verifying catalog and schema ...")

catalogs_df = spark.sql(f"SHOW CATALOGS LIKE '{catalog_name}'")
schemas_df = spark.sql(f"SHOW SCHEMAS IN `{catalog_name}` LIKE '{schema_name}'")

catalog_count = catalogs_df.count()
schema_count = schemas_df.count()

print(f"  Catalogs matching '{catalog_name}': {catalog_count}")
print(f"  Schemas matching '{schema_name}' in '{catalog_name}': {schema_count}")

if catalog_count == 0:
    raise Exception(f"FATAL: Catalog '{catalog_name}' was not created or is not visible.")
if schema_count == 0:
    raise Exception(f"FATAL: Schema '{catalog_name}.{schema_name}' was not created or is not visible.")

print()
print("=" * 60)
print("  Bootstrap complete.")
print(f"  Catalog : {catalog_name}")
print(f"  Schema  : {catalog_name}.{schema_name}")
print("=" * 60)
