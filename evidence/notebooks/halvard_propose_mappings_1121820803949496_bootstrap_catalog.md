# bootstrap_catalog (job halvard_propose_mappings, run 1121820803949496, task run 934318703246502, SUCCESS)

```python
dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "your_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "harmonizing_agent", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()

print(f"Config: catalog={catalog_name}, schema={schema_name}")
```

Output:
```text
Config: catalog=agent_marketplace_catalog, schema=halvard_harmonization
```

```python
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
```

Output:
```text
[WARN] CREATE CATALOG failed ([RequestId=24da2e4e-74e7-402b-8e06-f8f8cc8b3d0a ErrorClass=INVALID_STATE] Metastore storage root URL does not exist. Default Storage is enabled in your account. You can use the UI to create a new catalog using Default Storage, or please provide a storage location for the catalog (for example 'CREATE CATALOG myCatalog MANAGED LOCATION '<location-path>').

JVM stacktrace:
com.databricks.sql.managedcatalog.UnityCatalogServiceException
	at com.databricks.sql.managedcatalog.client.ErrorDetailsHandlerImpl.wrapServiceException(ErrorDetailsHandler.scala:165)
	at com.databricks.sql.managedcatalog.client.ErrorDetailsHandlerImpl.wrapServiceException$(ErrorDetailsHandler.scala:88)
	at com.databricks.managedcatalog.ManagedCatalogClientImpl.wrapServiceException(ManagedCatalogClientImpl.scala:44)
	at com.databricks.sql.managedcatalog.client.ManagedCatalogClientImpl.recordAndWrapExceptionBase(ManagedCatalogClientImpl.scala:8732)
	at com.databricks.sql.managedcatalog.client.ManagedCatalogClientImpl.recordAndWrapException(ManagedCatalogClientImpl.scala:8682)
	at com.databricks.sql.managedcatalog.client.ManagedCatalogClientImpl.createCatalogProto(ManagedCatalogClientImpl.scala:649)
	at com.databricks.sql.managedcatalog.client.ManagedCatalogClientImpl.$anonfun$createCatalog$5(ManagedCatalogClientImpl.scala:637)
	at com.databricks.sql.managedcatalog.client.ManagedCatalogClientImpl.$anonfun$recordAndWrapExceptionBase$2(ManagedCatalogClientImpl.scala:8754)
	at com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)
	at com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:146)
	at com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)
	at com.databricks.sql.managedcatalog.client.ManagedCatalogClientImpl.$anonfun$recordAndWrapExceptionBase$1(ManagedCatalogClientImpl.scala:8753)
	at com.databricks.sql.managedcatalog.client.ErrorDetailsHandlerImpl.wrapServiceException(ErrorDetailsHandler.scala:96)
	at com.databricks.sql.managedcatalog.client.ErrorDetailsHandlerImpl.wrapServiceException$(ErrorDetailsHandler.scala:88)
	at com.databricks.managedcatalog.ManagedCatalogClientImpl.wrapServiceException(ManagedCatalogClientImpl.scala:44)
	at com.databricks.sql.managedcatalog.client.ManagedCatalogClientImpl.recordAndWrapExceptionBase(ManagedCatalogClientImpl.scala:8732)
	at com.databricks.sql.managedcatalog.client.ManagedCatalogClientImpl.recordAndWrapException(ManagedCatalogClientImpl.scala:8682)
	at com.databricks.sql.managedcatalog.client.ManagedCatalogClientImpl.createCatalog(ManagedCatalogClientImpl.scala:639)
	at com.databricks.sql.managedcatalog.client.ManagedCatalogClientImpl.createCatalog(ManagedCatalogClientImpl.scala:625)
	at com.databricks.sql.managedcatalog.ManagedCatalogCommon.$anonfun$createCatalog$1(ManagedCatalogCommon.scala:554)
	at com.databricks.sql.managedcatalog.ManagedCatalogCommon.withCatalogCacheInvalidated(ManagedCatalogCommon.scala:688)
	at com.databricks.sql.managedcatalog.ManagedCatalogCommon.createCatalog(ManagedCatalogCommon.scala:543)
	at com.databricks.sql.managedcatalog.ProfiledManagedCatalog.$anonfun$createCatalog$2(ProfiledManagedCatalog.scala:158)
	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)
	at org.apache.spark.sql.catalyst.MetricKeyUtils$.measure(MetricKey.scala:3049)
	at com.databricks.sql.managedcatalog.ProfiledManagedCatalog.$anonfun$profile$1(ProfiledManagedCatalog.scala:80)
	at com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)
	at com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:146)
	at com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)
	at com.databricks.sql.managedcatalog.ProfiledManagedCatalog.profile(ProfiledManagedCatalog.scala:79)
	at com.databricks.sql.managedcatalog.ProfiledManagedCatalog.createCatalog(ProfiledManagedCatalog.scala:157)
	at com.databricks.sql.managedcatalog.ManagedCatalogSessionCatalog.createCatalog(ManagedCatalogSessionCatalog.scala:1449)
	at com.databricks.sql.managedcatalog.command.CreateCatalogCommand.run(CatalogCommands.scala:53)
	at org.apache.spark.sql.execution.command.ExecutedCommandExec.$anonfun$sideEffectResult$2(commands.scala:103)
	at org.apache.spark.sql.execution.SparkPlan.runCommandInAetherOrSpark(SparkPlan.scala:231)
	at org.apache.spark.sql.execution.command.ExecutedCommandExec.$anonfun$sideEffectResult$1(commands.scala:103)
	at com.databricks.spark.util.FrameProfiler$.$anonfun$record$1(FrameProfiler.scala:114)
	at com.databricks.spark.util.FrameProfilerExporter$.maybeExportFrameProfiler(FrameProfilerExporter.scala:201)
	at com.databricks.spark.util.FrameProfiler$.record(FrameProfiler.scala:105)
	at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult$lzycompute(commands.scala:100)
	at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult(commands.scala:99)
	at org.apache.spark.sql.execution.command.ExecutedCommandExec.executeCollect(commands.scala:113)
	at org.apache.spark.sql.execution.QueryExecution$.$anonfun$runCommand$12(QueryExecution.scala:2527)
	at com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)
	at org.apache.spark.sql.execution.QueryExecution$.$anonfun$runCommand$11(QueryExecution.scala:2517)
	at com.databricks.util.LexicalThreadLocal$Handle.runWith(LexicalThreadLocal.scala:63)
	at org.apache.spark.sql.execution.QueryExecution$.executeWithCaches$1(QueryExecution.scala:2517)
	at org.apache.spark.sql.execution.QueryExecution$.$anonfun$runCommand$10(QueryExecution.scala:2523)
	at org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:305)
	at org.apache.spark.sql.execution.QueryExecution$.$anonfun$runCommand$9(QueryExecution.scala:2510)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId0$23(SQLExecution.scala:1309)
	at com.databrick
```

```python
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`")
try:
    spark.sql(f"""
      COMMENT ON SCHEMA `{catalog_name}`.`{schema_name}` IS
      'Single schema for all harmonization objects: raw data, harmonized output, column-mapping control tables, ops tables, and views.'
    """)
except Exception as _e:
    print(f"[WARN] Could not set schema comment: {_e}")

print(f"Schema '{catalog_name}.{schema_name}' is ready.")
```

Output:
```text
Schema 'agent_marketplace_catalog.halvard_harmonization' is ready.
```

```python
catalogs_df = spark.sql(f"SHOW CATALOGS LIKE '{catalog_name}'")
schemas_df = spark.sql(f"SHOW SCHEMAS IN `{catalog_name}` LIKE '{schema_name}'")

if catalogs_df.count() == 0:
    raise Exception(f"FATAL: Catalog '{catalog_name}' was not created or is not visible.")
if schemas_df.count() == 0:
    raise Exception(f"FATAL: Schema '{catalog_name}.{schema_name}' was not created or is not visible.")

print(f"Bootstrap complete: {catalog_name}.{schema_name}")

import json

dbutils.notebook.exit(json.dumps({"catalog": catalog_name, "schema": schema_name, "status": "ready"}))
```

Output:
```text
{"catalog": "agent_marketplace_catalog", "schema": "halvard_harmonization", "status": "ready"}
```
