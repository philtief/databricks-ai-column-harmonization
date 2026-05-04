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
dbutils.widgets.text("catalog_name", "your_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "harmonizing_agent", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()

print(f"Config: catalog={catalog_name}, schema={schema_name}")

# COMMAND ----------

# MAGIC %md ## Create Catalog

# COMMAND ----------

# On some workspaces the catalog is pre-created and CREATE CATALOG may fail
# because the metastore has no storage root configured. Verify it exists and continue.
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog_name}`")
    print("CREATE CATALOG succeeded.")
except Exception as _ce:
    print(f"[WARN] CREATE CATALOG failed ({_ce}). Checking if it already exists ...")
    existing = [r[0] for r in spark.sql(f"SHOW CATALOGS LIKE '{catalog_name}'").collect()]
    if catalog_name not in existing:
        raise Exception(f"FATAL: Catalog '{catalog_name}' does not exist and could not be created: {_ce}")
    print(f"Catalog '{catalog_name}' already exists.")

try:
    spark.sql(f"""
      COMMENT ON CATALOG `{catalog_name}` IS
      'Harmonization catalog. Hosts property insurance harmonization objects for the global column-mapping solution.'
    """)
except Exception as _e:
    print(f"[WARN] Could not set catalog comment (may lack ALTER privilege): {_e}")

# COMMAND ----------

# MAGIC %md ## Create Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`")
try:
    spark.sql(f"""
      COMMENT ON SCHEMA `{catalog_name}`.`{schema_name}` IS
      'Single schema for all harmonization objects: raw data, harmonized output, column-mapping control tables, ops tables, and views.'
    """)
except Exception as _e:
    print(f"[WARN] Could not set schema comment: {_e}")

print(f"Schema '{catalog_name}.{schema_name}' is ready.")

# COMMAND ----------

# MAGIC %md ## Verify

# COMMAND ----------

catalogs_df = spark.sql(f"SHOW CATALOGS LIKE '{catalog_name}'")
schemas_df = spark.sql(f"SHOW SCHEMAS IN `{catalog_name}` LIKE '{schema_name}'")

if catalogs_df.count() == 0:
    raise Exception(f"FATAL: Catalog '{catalog_name}' was not created or is not visible.")
if schemas_df.count() == 0:
    raise Exception(f"FATAL: Schema '{catalog_name}.{schema_name}' was not created or is not visible.")

print(f"Bootstrap complete: {catalog_name}.{schema_name}")
