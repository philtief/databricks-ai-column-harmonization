# Uc governance

_Collected 2026-10-06 15:53 UTC by scripts/collect_evidence.py from workspace https://fevm-agent-marketplace.cloud.databricks.com._

## Table tags

```sql
SELECT table_name, tag_name, tag_value FROM agent_marketplace_catalog.information_schema.table_tags WHERE schema_name = 'halvard_harmonization' ORDER BY 1, 2
```

| table_name | tag_name | tag_value |
|---|---|---|
| ai_mapping_usage_metrics | component | control |
| bronze_property_monthly_es | layer | bronze |
| bronze_property_monthly_it | layer | bronze |
| column_mapping_audit | component | control |
| column_mapping_candidates | component | control |
| column_mapping_dictionary | component | control |
| data_quality_results | component | control |
| global_target_columns | component | control |
| harmonized_property_monthly | business_owner | finance |
| harmonized_property_monthly | classification | confidential |
| harmonized_property_monthly | domain | finance |
| harmonized_property_monthly | layer | gold |
| harmonized_property_monthly | line_of_business | property |
| harmonized_property_monthly | steward | group_actuarial |
| mapping_eval_results | component | control |
| source_column_inventory | component | control |
| value_mapping_candidates | component | control |
| value_mapping_dictionary | component | control |
| workflow_run_metrics | component | control |

## Column tags

```sql
SELECT table_name, column_name, tag_name, tag_value FROM agent_marketplace_catalog.information_schema.column_tags WHERE schema_name = 'halvard_harmonization' ORDER BY 1, 2
```

| table_name | column_name | tag_name | tag_value |
|---|---|---|---|
| harmonized_property_monthly | claims_reserve_eur | kpi_type | financial |
| harmonized_property_monthly | commissions_eur | kpi_type | financial |
| harmonized_property_monthly | gross_claims_incurred_eur | kpi_type | financial |
| harmonized_property_monthly | gross_written_premium_eur | kpi_type | financial |
| harmonized_property_monthly | management_expenses_eur | kpi_type | financial |
| harmonized_property_monthly | net_written_premium_eur | kpi_type | financial |

## Grants on the schema

```sql
SHOW GRANTS ON SCHEMA agent_marketplace_catalog.halvard_harmonization
```

| Principal | ActionType | ObjectType | ObjectKey |
|---|---|---|---|
| 83b3161c-5563-4469-8548-7e4b979eeb1d | SELECT | SCHEMA | agent_marketplace_catalog.halvard_harmonization |
| 83b3161c-5563-4469-8548-7e4b979eeb1d | USE SCHEMA | SCHEMA | agent_marketplace_catalog.halvard_harmonization |
| philipp.tiefenbacher@databricks.com | ALL PRIVILEGES | CATALOG | agent_marketplace_catalog |
| philipp.tiefenbacher@databricks.com | MANAGE | CATALOG | agent_marketplace_catalog |
| lukas.grubwieser@databricks.com | USE SCHEMA | CATALOG | agent_marketplace_catalog |
| lukas.grubwieser@databricks.com | READ VOLUME | CATALOG | agent_marketplace_catalog |
| lukas.grubwieser@databricks.com | SELECT | CATALOG | agent_marketplace_catalog |
| lukas.grubwieser@databricks.com | WRITE VOLUME | CATALOG | agent_marketplace_catalog |
| lukas.grubwieser@databricks.com | CREATE MODEL | CATALOG | agent_marketplace_catalog |
| lukas.grubwieser@databricks.com | CREATE VOLUME | CATALOG | agent_marketplace_catalog |
| lukas.grubwieser@databricks.com | CREATE MODEL VERSION | CATALOG | agent_marketplace_catalog |
| lukas.grubwieser@databricks.com | CREATE MATERIALIZED VIEW | CATALOG | agent_marketplace_catalog |
| lukas.grubwieser@databricks.com | CREATE TABLE | CATALOG | agent_marketplace_catalog |
| lukas.grubwieser@databricks.com | MODIFY | CATALOG | agent_marketplace_catalog |
| lukas.grubwieser@databricks.com | APPLY TAG | CATALOG | agent_marketplace_catalog |
| lukas.grubwieser@databricks.com | CREATE FUNCTION | CATALOG | agent_marketplace_catalog |
| lukas.grubwieser@databricks.com | CREATE FLOW | CATALOG | agent_marketplace_catalog |
| lukas.grubwieser@databricks.com | REFRESH | CATALOG | agent_marketplace_catalog |
| lukas.grubwieser@databricks.com | EXECUTE | CATALOG | agent_marketplace_catalog |
| suraj.bang@databricks.com | READ METADATA | METASTORE | metastore_aws_us_east_1 |
| leonie.hollstein@databricks.com | MODIFY | CATALOG | agent_marketplace_catalog |
| leonie.hollstein@databricks.com | REFRESH | CATALOG | agent_marketplace_catalog |
| leonie.hollstein@databricks.com | CREATE FLOW | CATALOG | agent_marketplace_catalog |
| leonie.hollstein@databricks.com | EXECUTE | CATALOG | agent_marketplace_catalog |
| leonie.hollstein@databricks.com | CREATE MATERIALIZED VIEW | CATALOG | agent_marketplace_catalog |
| leonie.hollstein@databricks.com | CREATE VOLUME | CATALOG | agent_marketplace_catalog |
| leonie.hollstein@databricks.com | CREATE TABLE | CATALOG | agent_marketplace_catalog |
| leonie.hollstein@databricks.com | CREATE MODEL | CATALOG | agent_marketplace_catalog |
| leonie.hollstein@databricks.com | WRITE VOLUME | CATALOG | agent_marketplace_catalog |
| leonie.hollstein@databricks.com | CREATE MODEL VERSION | CATALOG | agent_marketplace_catalog |
| leonie.hollstein@databricks.com | CREATE FUNCTION | CATALOG | agent_marketplace_catalog |
| leonie.hollstein@databricks.com | APPLY TAG | CATALOG | agent_marketplace_catalog |
| leonie.hollstein@databricks.com | SELECT | CATALOG | agent_marketplace_catalog |
| leonie.hollstein@databricks.com | READ VOLUME | CATALOG | agent_marketplace_catalog |
| leonie.hollstein@databricks.com | USE SCHEMA | CATALOG | agent_marketplace_catalog |

## Row filter on the harmonized table

```sql
SELECT table_name, filter_name, target_columns FROM agent_marketplace_catalog.information_schema.row_filters WHERE table_schema = 'halvard_harmonization'
```

| table_name | filter_name | target_columns |
|---|---|---|
| harmonized_property_monthly | agent_marketplace_catalog.halvard_harmonization.country_row_filter | source_country |

## Row filter function

```sql
DESCRIBE FUNCTION EXTENDED agent_marketplace_catalog.halvard_harmonization.country_row_filter
```

| function_desc |
|---|
| Function:      agent_marketplace_catalog.halvard_harmonization.country_row_filter |
| Type:          SCALAR |
| Input:         source_country STRING |
| Returns:       BOOLEAN |
| Deterministic: true |
| Data Access:   CONTAINS SQL |
| Configs:       runtime.databricks.sql.functions.counter_diff.enabled=true |
|                runtime.databricks.sql.functions.time_bucket.enabled=true |
|                spark.connect.cloudfetch.transparent.enabled=true |
|                spark.connect.cloudfetch.transparent.prefetchDepth=6 |
|                spark.connect.inline.result.byteLimit.enabled=false |
|                spark.connect.qrc.transparent.enabled=false |
|                spark.connect.resultCachingShadowMode.billingMeter.enabled=true |
|                spark.connect.session.connectML.enabled=true |
|                spark.connect.session.connectML.mlCache.memoryControl.maxModelSize=268435456 |
|                spark.connect.session.planCache.analyzedPlan.enabled=false |
|                spark.connect.session.planCompression.threshold=10485760 |
|                spark.databricks.ai.adaptiveBatch.initialSize=4 |
|                spark.databricks.ai.adaptiveBatch.maxRetries=100 |
|                spark.databricks.ai.adaptiveBatch.partitionJitterEnabled=true |
|                spark.databricks.ai.adaptiveBatch.retriableInvokeStatusCodes=UNAVAILABLE,RESOURCE_EXHAUSTED,DEADLINE_EXCEEDED,ABORTED,CANCELLED |
|                spark.databricks.ai.adaptiveBatch.retryMaxDelayMs=120000 |
|                spark.databricks.ai.grpc.sync.retriableStatusCodes=UNAVAILABLE,RESOURCE_EXHAUSTED,DEADLINE_EXCEEDED,ABORTED,CANCELLED |
|                spark.databricks.docingest.apdfl.sandbox.enabled=false |
|                spark.databricks.docingest.batchEval.size=8 |
|                spark.databricks.docingest.contentElementTypes.excluded= |
|                spark.databricks.docingest.maxDocumentPages=1000 |
|                spark.databricks.docingest.pagination.concurrency=1 |
|                spark.databricks.docingest.pdf.drawFlags=USE_ANNOT_FACES |
|                spark.databricks.docingest.stage2.descriptionModel=databricks-gemma-3-12b |
|                spark.databricks.docingest.version.default=2.0 |
|                spark.databricks.docingest.version.supported=2.0 |
|                spark.databricks.sql.ai.hermes.embeddingBatch.enabled=true |
|                spark.databricks.sql.ai.partitionProcessor.aimdLimiter.enabled=true |
|                spark.databricks.sql.ai.partitionProcessor.aimdLimiter.max=200 |
|                spark.databricks.sql.ai.partitionProcessor.globalMaxInFlightElements=10240 |
|                spark.databricks.sql.ai.partitionProcessor.maxBufferedResults=512 |
|                spark.databricks.sql.ai.partitionProcessor.maxInFlightElements=200 |
|                spark.databricks.sql.ai.partitionProcessor.maxWaitTimeSeconds=172800 |
|                spark.databricks.sql.ai.remoteFunction.grpc.initialRetryInterval=100 |
|                spark.databricks.sql.ai.remoteFunction.grpc.maxRetryInterval=10000 |
|                spark.databricks.sql.ai.remoteFunction.grpc.retryIntervalJitterFactor=1.0 |
|                spark.databricks.sql.ai.remoteFunction.grpc.richArguments.enabled=true |
|                spark.databricks.sql.ai.remoteFunction.hermes.extendedTypeConversion.enabled=true |
|                spark.databricks.sql.expression.aiFunctions.repartition=0 |
|                spark.databricks.sql.functions.aiForecast.enabled=false |
|                spark.databricks.sql.functions.aiFunctions.aclValidateAllFunctions=true |
|                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.clusterSizeBasedGlobalParallelism.scaleFactor=512.0 |
|                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.debugLogEnabled=true |
|                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.dynamicPoolSizeEnabled=false |
|                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.enabled=true |
|                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.maxPoolSize=2048 |
|                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.maxPoolSize.aiParseDocument=64 |
|                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.scaleUpThresholdCurrentQpsIncreaseRatio=0.0 |
|                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.scaleUpThresholdSuccessRatio=0.95 |
|                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.scaleUpThresholdTotalQpsIncreaseRatio=0.0 |
|                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.taskWaitTimeInSeconds=1000 |
|                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.threadKeepAliveTimeInSeconds=600 |
|                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.useDynamicTaskQueueExecutor=false |
|                spark.databricks.sql.functions.aiFunctions.batch.aiQuery.embedding.request.size=40 |
|                spark.databricks.sql.functions.aiFunctions.batch.execution.size=2048 |
|                spark.databricks.sql.functions.aiFunctions.batchInferenceApi.enabled=true |
|                spark.databricks.sql.functions.aiFunctions.batchSession.enabled=true |
|                spark.databricks.sql.functions.aiFunctions.batchSession.terminate.enabled=true |
|                spark.databricks.sql.functions.aiFunctions.createBatchSessionOnExecutor=false |
|                spark.databricks.sql.functions.aiFunctions.decimal.dataType.enabled=true |
|                spark.databricks.sql.functions.aiFunctions.disabled=false |
|                spark.databricks.sql.functions.aiFunctions.embeddingsEndpointName=databricks-gte-large-en |
|                spark.databricks.sql.functions.aiFunctions.extract.tileMetadata.enabled=true |
|                spark.databricks.sql.functions.aiFunctions.grpc.enabled=true |
|                spark.databricks.sql.functions.aiFunctions.hermes.aiQuery.failOnError.enabled=true |
|                spark.databricks.sql.functions.aiFunctions.hermes.aiQuery.files.enabled=true |
|                spark.databricks.sql.functions.aiFunctions.hermes.errorTemplate.enabled=true |
|                spark.databricks.sql.functions.aiFunctions.hermes.partitionProcessorConf.enabled=true |
|                spark.databricks.sql.functions.aiFunctions.hermesEnabledFunctions=ai_classify,ai_extract,ai_prep_search,ai_parse_document,ai_search,ai_query,ai_mask,ai_enrich,ai_transcribe,ai_summarize,ai_fix_grammar,ai_analyze_sentiment,ai_translate,ai_decide |
|                spark.databricks.sql.functions.aiFunctions.includeWorkspaceUrl=false |
|                spark.databricks.sql.functions.aiFunctions.model.parameters.enabled=true |
|                spark.databricks.sql.functions.aiFunctions.modelEndpointTypeParsing.enabled=true |
|                spark.databricks.sql.functions.aiFunctions.multiModality.model.list=databricks-llama-4-maverick,databricks-claude-sonnet-4,databricks-claude-3-7-sonnet,databricks-gemma-3-12b,databricks-gpt-5,databricks-gpt-5-mini,databricks-gpt-5-nano,databricks-gpt-5-1,databricks-gpt-5-2,databricks-gemini-2-5-flash,databricks-gemini-2-5-pro,databricks-gemini-3-flash,databricks-gemini-3-pro,databricks-claude-haiku-4-5 |
|                spark.databricks.sql.functions.aiFunctions.multiModality.useCapabilitiesApi=true |
|                spark.databricks.sql.functions.aiFunctions.nullInputJsonOutput={"response":null,"error_message":null} |
|                spark.databricks.sql.functions.aiFunctions.purposeBuiltFunctions.batch.execution.enabled=true |
|                spark.databricks.sql.functions.aiFunctions.purposeBuiltFunctions.translate.instruction=### Instruction  Translate the provided text to %s, and output only the translated text in the target language in this format: <DBSQLAI>translated text</DBSQLAI>.  If the text is already in the target language, output the provided text.  ### Text  %s |
|                spark.databricks.sql.functions.aiFunctions.registry.enabled=true |
|                spark.databricks.sql.functions.aiFunctions.remoteFunction.grpc.batch.maxConcurrentRequests=2048 |
|                spark.databricks.sql.functions.aiFunctions.remoteHttpClient.maxConnections=2048 |
|                spark.databricks.sql.functions.aiFunctions.remoteHttpClient.timeoutInSeconds=360 |
|                spark.databricks.sql.functions.aiFunctions.safe.inference.enabled=true |
|                spark.databricks.sql.functions.aiFunctions.scalingReasonLogging.enabled=true |
|                spark.databricks.sql.functions.aiFunctions.useDedicatedHttpClient=true |
|                spark.databricks.sql.functions.aiFunctions.useGrpcForSessionManagement=true |
|                spark.databricks.sql.functions.aiFunctions.vectorSearch.initialRetryIntervalMillis=1000 |
|                spark.databricks.sql.functions.aiFunctions.vectorSearch.maxRetryInterval=120000 |
|                spark.databricks.sql.functions.aiFunctions.vectorSearch.retryIntervalMultiplierFactor=2.0 |
|                spark.databricks.sql.functions.aiGen.endpointName=databricks-meta-llama-3-3-70b-instruct |
|                spark.databricks.sql.functions.aiQuery.hybrid.reasoning.foundation.model.list=databricks-claude-3-7-sonnet,databricks-gemini-2-5-pro,databricks-gemini-2-5-flash,databricks-gemini-3-flash |
|                spark.databricks.sql.functions.aiQuery.openAI.oSeries.model.list=o1-mini,o1,o3-mini,o3,o4-mini,gpt-5,gpt-5-mini,gpt-5-nano,GPT-5,GPT-5 Mini,GPT-5 Nano |
|                spark.databricks.sql.functions.aiQuery.temperature.whitelistMode.enabled=true |
|                spark.databricks.sql.functions.aiQuery.ucModelService.enabled=true |
|                spark.databricks.sql.functions.vectorSearch.enabled=true |
|                spark.databricks.sql.functions.vectorSearch.use.indexIdentifierOnly=true |
|                spark.databricks.sql.rowColumnAccess.useEffectivePolicies.enabled=true |
|                spark.sql.analyzer.dontDeduplicateExpressionIfExprIdInOutput=false |
|                spark.sql.ansi.enabled=true |
|                spark.sql.artifact.isolation.enabled=true |
|                spark.sql.connect.ldp.enableLdpPipelinesHandler=true |
|                spark.sql.connect.shuffleDependency.fileCleanup.enabled=true |
|                spark.sql.copyInto.timeout.threadDump.enabled=false |
|                spark.sql.cte.recursion.enabled=true |
|                spark.sql.excel.streaming.enabled=true |
|                spark.sql.fileType.enabled=true |
|                spark.sql.functions.remoteHttpClient.redirectLogging.enabled=false |
|                spark.sql.functions.remoteHttpClient.retryOn400TimeoutError=true |
|                spark.sql.functions.remoteHttpClient.retryOnSocketTimeoutException=true |
|                spark.sql.hive.convertCTAS=true |
|                spark.sql.insertIntoReplaceUsing.disallowMisalignedColumns.enabled=true |
|                spark.sql.insertIntoReplaceUsing.oldDPOCodePath.enabled=true |
|                spark.sql.legacy.blockCreateTempTableUsingProvider=true |
|                spark.sql.legacy.codingErrorAction=true |
|                spark.sql.legacy.createHiveTableByDefault=false |
|                spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled=false |
|                spark.sql.legacy.execution.pythonUDTF.pandas.conversion.enabled=false |
|                spark.sql.legacy.observeMetricsAggregateAllAttempts=true |
|                spark.sql.legacy.useLegacyXMLParser.migrationValidation.enabled=false |
|                spark.sql.orc.compression.codec=snappy |
|                spark.sql.parquet.compression.codec=snappy |
|                spark.sql.path.enabled=false |
|                spark.sql.photon.skipIndexForTextSearch.indexingBitmapAgg.forceSerializedOutput=false |
|                spark.sql.photon.skipIndexForTextSearch.indexingBitmapAndAgg.enabled=false |
|                spark.sql.pivot.emptyBucketReturnsAggregateDefault=false |
|                spark.sql.readSideCharPadding=true |
|                spark.sql.scripting.enabled=true |
|                spark.sql.scripting.maxNumberOfCharacters=1000000 |
|                spark.sql.scripting.maxNumberOfLines=30000 |
|                spark.sql.searchIndex.clusterBy.enabled=true |
|                spark.sql.secondaryIndex.manualRefresh.disabled=false |
|                spark.sql.secondaryIndex.rowIdWatermark.enabled=false |
|                spark.sql.secondaryIndex.showInDescribeTable=true |
|                spark.sql.secondaryIndex.ucIntegration.enabled=true |
|                spark.sql.session.timeZone=Etc/UTC |
|                spark.sql.shuffleDependency.skipMigration.enabled=true |
|                spark.sql.skipIndexForTextSearch.enabled=true |
|                spark.sql.skipIndexForTextSearch.ngramIndex.parameterCalculation=true |
|                spark.sql.skipIndexForTextSearch.patternBucketCount=524288 |
|                spark.sql.skipIndexForTextSearch.rowsBatchSize=32 |
|                spark.sql.skipIndexForTextSearch.v2.enabled=false |
|                spark.sql.sources.commitProtocolClass=com.databricks.sql.transaction.directory.DirectoryAtomicCommitProtocol |
|                spark.sql.sources.default=delta |
|                spark.sql.stableDerivedColumnAlias.enabled=true |
|                spark.sql.standardIndex.costBasedSelection.enabled=true |
|                spark.sql.standardIndex.enabled=true |
|                spark.sql.streaming.checkpoint.fileChecksum.enabled=false |
|                spark.sql.streaming.optimizer.subqueryEarlyUnnesting.enabled=false |
|                spark.sql.streaming.stateStore.providerClass=com.databricks.sql.streaming.state.RocksDBStateStoreProvider |
|                spark.sql.streaming.statefulOperator.stateRebalancing.enabled=false |
|                spark.sql.streaming.stopTimeout=15s |
|                spark.sql.timeType.enabled=true |
|                spark.sql.timestampNanosTypes.enabled=false |
|                spark.sql.validateColumnNamesSync.throwException=true |
|                spark.sql.variable.substitute=false |
|                spark.sql.variant.pushVariantIntoScan=true |
|                spark.sql.variant.pushVariantIntoScan.deferCastError=true |
|                spark.sql.variant.writeShredding.enabled=true |
|                spark.sql.vectorIndex.enabled=false |
|                spark.sql.vectorIndex.numCentroidsScalingFactor=4 |
|                spark.sql.vectorIndex.optimization.enabled=false |
|                spark.sql.vectorIndex.optimization.enabledForCommands=false |
|                spark.sql.windowExec.buffer.in.memory.size.threshold=-1 |
| Owner:         philipp.tiefenbacher@databricks.com |
| Create Time:   Tue Oct 06 15:38:27 UTC 2026 |
| Body:          is_account_group_member('halvard-group-actuarial')  OR is_account_group_member(concat('halvard-steward-', lower(source_country))) OR current_user() IN ('philipp.tiefenbacher@databricks.com', '83b3161c-5563-4469-8548-7e4b979eeb1d') |

## Lineage (system.access.table_lineage)

```sql
SELECT DISTINCT source_table_full_name, target_table_full_name, entity_type FROM system.access.table_lineage WHERE target_table_full_name LIKE 'agent_marketplace_catalog.halvard_harmonization.%' AND source_table_full_name IS NOT NULL AND event_date >= current_date() - 2 ORDER BY 2, 1
```

| source_table_full_name | target_table_full_name | entity_type |
|---|---|---|
| agent_marketplace_catalog.halvard_harmonization.source_column_inventory | agent_marketplace_catalog.halvard_harmonization.column_mapping_candidates | JOB |
| agent_marketplace_catalog.halvard_harmonization.column_mapping_candidates | agent_marketplace_catalog.halvard_harmonization.column_mapping_dictionary | JOB |
| agent_marketplace_catalog.halvard_harmonization.bronze_property_monthly_es | agent_marketplace_catalog.halvard_harmonization.harmonized_property_monthly | JOB |
| agent_marketplace_catalog.halvard_harmonization.bronze_property_monthly_it | agent_marketplace_catalog.halvard_harmonization.harmonized_property_monthly | JOB |
| agent_marketplace_catalog.halvard_harmonization.harmonized_property_monthly | agent_marketplace_catalog.halvard_harmonization.harmonized_property_monthly | JOB |
| agent_marketplace_catalog.halvard_harmonization.harmonized_property_monthly | agent_marketplace_catalog.halvard_harmonization.mv_group_property_kpis |  |
| agent_marketplace_catalog.halvard_harmonization.column_mapping_candidates | agent_marketplace_catalog.halvard_harmonization.vw_column_mapping_coverage | JOB |
| agent_marketplace_catalog.halvard_harmonization.global_target_columns | agent_marketplace_catalog.halvard_harmonization.vw_column_mapping_coverage | JOB |
| agent_marketplace_catalog.halvard_harmonization.column_mapping_candidates | agent_marketplace_catalog.halvard_harmonization.vw_column_mapping_low_conf | JOB |
| agent_marketplace_catalog.halvard_harmonization.column_mapping_candidates | agent_marketplace_catalog.halvard_harmonization.vw_latest_column_mapping_summary | JOB |
| agent_marketplace_catalog.halvard_harmonization.column_mapping_dictionary | agent_marketplace_catalog.halvard_harmonization.vw_latest_column_mapping_summary | JOB |
| agent_marketplace_catalog.halvard_harmonization.column_mapping_candidates | agent_marketplace_catalog.halvard_harmonization.vw_mapping_review_summary | JOB |
| agent_marketplace_catalog.halvard_harmonization.column_mapping_candidates | agent_marketplace_catalog.halvard_harmonization.vw_pending_column_mappings | JOB |
| agent_marketplace_catalog.halvard_harmonization.column_mapping_candidates | agent_marketplace_catalog.halvard_harmonization.vw_publish_readiness | JOB |
