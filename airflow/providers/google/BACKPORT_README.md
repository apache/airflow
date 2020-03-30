<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

## Changelog

### v2020.XX.XX

This is the first released version of the package.

**Limitations**

The following operators have not been released:

 * All operators for BigQuery service.
 * GKEStartPodOperator for Kubernetes Engine service.

We recommend staying with the old versions yet. These operators will be released as soon as possible.

**New operators**:

We have worked intensively on operators that have not appeared in any Airflow release and are available only
through this package. This release includes the following new operators:

* [AutoML](https://cloud.google.com/automl/)
    * airflow.providers.google.cloud.operators.automl.AutoMLBatchPredictOperator
    * airflow.providers.google.cloud.operators.automl.AutoMLCreateDatasetOperator
    * airflow.providers.google.cloud.operators.automl.AutoMLDeleteDatasetOperator
    * airflow.providers.google.cloud.operators.automl.AutoMLDeleteModelOperator
    * airflow.providers.google.cloud.operators.automl.AutoMLDeployModelOperator
    * airflow.providers.google.cloud.operators.automl.AutoMLGetModelOperator
    * airflow.providers.google.cloud.operators.automl.AutoMLImportDataOperator
    * airflow.providers.google.cloud.operators.automl.AutoMLListDatasetOperator
    * airflow.providers.google.cloud.operators.automl.AutoMLPredictOperator
    * airflow.providers.google.cloud.operators.automl.AutoMLTablesListColumnSpecsOperator
    * airflow.providers.google.cloud.operators.automl.AutoMLTablesListTableSpecsOperator
    * airflow.providers.google.cloud.operators.automl.AutoMLTablesUpdateDatasetOperator
    * airflow.providers.google.cloud.operators.automl.AutoMLTrainModelOperator

* [BigQuery Data Transfer Service](https://cloud.google.com/bigquery/transfer/)
    * airflow.providers.google.cloud.operators.bigquery_dts.BigQueryCreateDataTransferOperator
    * airflow.providers.google.cloud.operators.bigquery_dts.BigQueryDataTransferServiceStartTransferRunsOperator
    * airflow.providers.google.cloud.operators.bigquery_dts.BigQueryDeleteDataTransferConfigOperator

* [Cloud Memorystore](https://cloud.google.com/memorystore/)
    * airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreCreateInstanceAndImportOperator
    * airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreCreateInstanceOperator
    * airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreDeleteInstanceOperator
    * airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreExportAndDeleteInstanceOperator
    * airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreExportInstanceOperator
    * airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreFailoverInstanceOperator
    * airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreGetInstanceOperator
    * airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreImportOperator
    * airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreListInstancesOperator
    * airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreScaleInstanceOperator
    * airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreUpdateInstanceOperator

* [Data Catalog](https://cloud.google.com/data-catalog)
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryGroupOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagTemplateFieldOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagTemplateOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryGroupOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateFieldOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetEntryGroupOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetEntryOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetTagTemplateOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogListTagsOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogLookupEntryOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogRenameTagTemplateFieldOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogSearchCatalogOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateEntryOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagTemplateFieldOperator
    * airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagTemplateOperator

* [Dataproc](https://cloud.google.com/dataproc/)
    * airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator
    * airflow.providers.google.cloud.operators.dataproc.DataprocUpdateClusterOperator

* [Data Fusion](https://cloud.google.com/data-fusion/)
    * airflow.providers.google.cloud.operators.datafusion.CloudDataFusionCreateInstanceOperator
    * airflow.providers.google.cloud.operators.datafusion.CloudDataFusionCreatePipelineOperator
    * airflow.providers.google.cloud.operators.datafusion.CloudDataFusionDeleteInstanceOperator
    * airflow.providers.google.cloud.operators.datafusion.CloudDataFusionDeletePipelineOperator
    * airflow.providers.google.cloud.operators.datafusion.CloudDataFusionGetInstanceOperator
    * airflow.providers.google.cloud.operators.datafusion.CloudDataFusionListPipelinesOperator
    * airflow.providers.google.cloud.operators.datafusion.CloudDataFusionRestartInstanceOperator
    * airflow.providers.google.cloud.operators.datafusion.CloudDataFusionStartPipelineOperator
    * airflow.providers.google.cloud.operators.datafusion.CloudDataFusionStopPipelineOperator
    * airflow.providers.google.cloud.operators.datafusion.CloudDataFusionUpdateInstanceOperator

* [Cloud Functions](https://cloud.google.com/functions/)
    * airflow.providers.google.cloud.operators.functions.CloudFunctionInvokeFunctionOperator

* [Cloud Storage (GCS)](https://cloud.google.com/gcs/)
    * airflow.providers.google.cloud.operators.gcs.GCSDeleteBucketOperator
    * airflow.providers.google.cloud.operators.gcs.GcsFileTransformOperator

* [Dataproc](https://cloud.google.com/dataproc/)
    * airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator
    * airflow.providers.google.cloud.operators.dataproc.DataprocUpdateClusterOperator

* [Machine Learning Engine](https://cloud.google.com/ml-engine/)
    * airflow.providers.google.cloud.operators.mlengine.MLEngineCreateModelOperator
    * airflow.providers.google.cloud.operators.mlengine.MLEngineCreateVersionOperator
    * airflow.providers.google.cloud.operators.mlengine.MLEngineDeleteModelOperator
    * airflow.providers.google.cloud.operators.mlengine.MLEngineDeleteVersionOperator
    * airflow.providers.google.cloud.operators.mlengine.MLEngineGetModelOperator
    * airflow.providers.google.cloud.operators.mlengine.MLEngineListVersionsOperator
    * airflow.providers.google.cloud.operators.mlengine.MLEngineSetDefaultVersionOperator
    * airflow.providers.google.cloud.operators.mlengine.MLEngineTrainingJobFailureOperator

* [Cloud Pub/Sub](https://cloud.google.com/pubsub/)
    * airflow.providers.google.cloud.operators.pubsub.PubSubPullOperator

* [Cloud Stackdriver](https://cloud.google.com/stackdriver)
    * airflow.providers.google.cloud.operators.stackdriver.StackdriverDeleteAlertOperator
    * airflow.providers.google.cloud.operators.stackdriver.StackdriverDeleteNotificationChannelOperator
    * airflow.providers.google.cloud.operators.stackdriver.StackdriverDisableAlertPoliciesOperator
    * airflow.providers.google.cloud.operators.stackdriver.StackdriverDisableNotificationChannelsOperator
    * airflow.providers.google.cloud.operators.stackdriver.StackdriverEnableAlertPoliciesOperator
    * airflow.providers.google.cloud.operators.stackdriver.StackdriverEnableNotificationChannelsOperator
    * airflow.providers.google.cloud.operators.stackdriver.StackdriverListAlertPoliciesOperator
    * airflow.providers.google.cloud.operators.stackdriver.StackdriverListNotificationChannelsOperator
    * airflow.providers.google.cloud.operators.stackdriver.StackdriverUpsertAlertOperator
    * airflow.providers.google.cloud.operators.stackdriver.StackdriverUpsertNotificationChannelOperator

* [Cloud Tasks](https://cloud.google.com/tasks/)
    * airflow.providers.google.cloud.operators.tasks.CloudTasksQueueCreateOperator
    * airflow.providers.google.cloud.operators.tasks.CloudTasksQueueDeleteOperator
    * airflow.providers.google.cloud.operators.tasks.CloudTasksQueueGetOperator
    * airflow.providers.google.cloud.operators.tasks.CloudTasksQueuePauseOperator
    * airflow.providers.google.cloud.operators.tasks.CloudTasksQueuePurgeOperator
    * airflow.providers.google.cloud.operators.tasks.CloudTasksQueueResumeOperator
    * airflow.providers.google.cloud.operators.tasks.CloudTasksQueueUpdateOperator
    * airflow.providers.google.cloud.operators.tasks.CloudTasksQueuesListOperator
    * airflow.providers.google.cloud.operators.tasks.CloudTasksTaskCreateOperator
    * airflow.providers.google.cloud.operators.tasks.CloudTasksTaskDeleteOperator
    * airflow.providers.google.cloud.operators.tasks.CloudTasksTaskGetOperator
    * airflow.providers.google.cloud.operators.tasks.CloudTasksTaskRunOperator
    * airflow.providers.google.cloud.operators.tasks.CloudTasksTasksListOperator

* [Cloud Firestore](https://firebase.google.com/docs/firestore)
    * airflow.providers.google.firebase.operators.firestore.CloudFirestoreExportDatabaseOperator

* [Analytics360](https://analytics.google.com/)
    * airflow.providers.google.marketing_platform.operators.analytics.GoogleAnalyticsDataImportUploadOperator
    * airflow.providers.google.marketing_platform.operators.analytics.GoogleAnalyticsDeletePreviousDataUploadsOperator
    * airflow.providers.google.marketing_platform.operators.analytics.GoogleAnalyticsGetAdsLinkOperator
    * airflow.providers.google.marketing_platform.operators.analytics.GoogleAnalyticsListAccountsOperator
    * airflow.providers.google.marketing_platform.operators.analytics.GoogleAnalyticsModifyFileHeadersDataImportOperator
    * airflow.providers.google.marketing_platform.operators.analytics.GoogleAnalyticsRetrieveAdsLinksListOperator

* [Google Campaign Manager](https://developers.google.com/doubleclick-advertisers)
    * airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerBatchInsertConversionsOperator
    * airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerBatchUpdateConversionsOperator
    * airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerDeleteReportOperator
    * airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerDownloadReportOperator
    * airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerInsertReportOperator
    * airflow.providers.google.marketing_platform.operators.campaign_manager.GoogleCampaignManagerRunReportOperator

* [Google Display&Video 360](https://marketingplatform.google.com/about/display-video-360/)
    * airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360CreateReportOperator
    * airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360DeleteReportOperator
    * airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360DownloadReportOperator
    * airflow.providers.google.marketing_platform.operators.display_video.GoogleDisplayVideo360RunReportOperator

* [Google Search Ads 360](https://marketingplatform.google.com/about/search-ads-360/)
    * airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsDownloadReportOperator
    * airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsInsertReportOperator

* [Google Spreadsheet](https://www.google.com/intl/en/sheets/about/)
    * airflow.providers.google.suite.operators.sheets.GoogleSheetsCreateSpreadsheet

* Transfer operators
    * airflow.providers.google.cloud.operators.cassandra_to_gcs.CassandraToGCSOperator
    * airflow.providers.google.cloud.operators.gcs_to_gcs.GCSSynchronizeBuckets
    * airflow.providers.google.cloud.operators.gcs_to_sftp.GCSToSFTPOperator
    * airflow.providers.google.cloud.operators.presto_to_gcs.PrestoToGCSOperator
    * airflow.providers.google.cloud.operators.sftp_to_gcs.SFTPToGCSOperator
    * airflow.providers.google.suite.operators.sheets.GCStoGoogleSheets
    * airflow.providers.google.suite.operators.sheets.GoogleSheetsToGCSOperator
    * airflow.providers.google.ads.operators.ads.GoogleAdsToGcsOperator

**Updated operators**

The operators in Airflow 2.0 have been moved to a new package. The following table showing operators
from Airflow 1.10 and its equivalent from Airflow 2.0:

| Airflow 1.10                                                                                                       | Airflow 2.0                                                                                                             |
|--------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| airflow.contrib.operators.adls_to_gcs.AdlsToGoogleCloudStorageOperator                                             | airflow.providers.google.cloud.operators.adls_to_gcs.ADLSToGCSOperator                                                  |
| airflow.contrib.operators.bigquery_check_operator.BigQueryCheckOperator                                            | airflow.providers.google.cloud.operators.bigquery.BigQueryCheckOperator                                                 |
| airflow.contrib.operators.bigquery_check_operator.BigQueryIntervalCheckOperator                                    | airflow.providers.google.cloud.operators.bigquery.BigQueryIntervalCheckOperator                                         |
| airflow.contrib.operators.bigquery_check_operator.BigQueryValueCheckOperator                                       | airflow.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperator                                            |
| airflow.contrib.operators.bigquery_get_data.BigQueryGetDataOperator                                                | airflow.providers.google.cloud.operators.bigquery.BigQueryGetDataOperator                                               |
| airflow.contrib.operators.bigquery_operator.BigQueryOperator                                                       | airflow.providers.google.cloud.operators.bigquery.BigQueryExecuteQueryOperator                                          |
| airflow.contrib.operators.bigquery_table_delete_operator.BigQueryTableDeleteOperator                               | airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteTableOperator                                           |
| airflow.contrib.operators.bigquery_to_bigquery.BigQueryToBigQueryOperator                                          | airflow.providers.google.cloud.operators.bigquery_to_bigquery.BigQueryToBigQueryOperator                                |
| airflow.contrib.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator                                           | airflow.providers.google.cloud.operators.bigquery_to_gcs.BigQueryToGCSOperator                                          |
| airflow.contrib.operators.bigquery_to_mysql_operator.BigQueryToMySqlOperator                                       | airflow.providers.google.cloud.operators.bigquery_to_mysql.BigQueryToMySqlOperator                                      |
| airflow.contrib.operators.dataflow_operator.DataFlowJavaOperator                                                   | airflow.providers.google.cloud.operators.dataflow.DataflowCreateJavaJobOperator                                         |
| airflow.contrib.operators.dataflow_operator.DataFlowPythonOperator                                                 | airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator                                       |
| airflow.contrib.operators.dataflow_operator.DataflowTemplateOperator                                               | airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator                                     |
| airflow.contrib.operators.dataproc_operator.DataProcHadoopOperator                                                 | airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHadoopJobOperator                                       |
| airflow.contrib.operators.dataproc_operator.DataProcHiveOperator                                                   | airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHiveJobOperator                                         |
| airflow.contrib.operators.dataproc_operator.DataProcJobBaseOperator                                                | airflow.providers.google.cloud.operators.dataproc.DataprocJobBaseOperator                                               |
| airflow.contrib.operators.dataproc_operator.DataProcPigOperator                                                    | airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPigJobOperator                                          |
| airflow.contrib.operators.dataproc_operator.DataProcPySparkOperator                                                | airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPySparkJobOperator                                      |
| airflow.contrib.operators.dataproc_operator.DataProcSparkOperator                                                  | airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkJobOperator                                        |
| airflow.contrib.operators.dataproc_operator.DataProcSparkSqlOperator                                               | airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkSqlJobOperator                                     |
| airflow.contrib.operators.dataproc_operator.DataprocClusterCreateOperator                                          | airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator                                         |
| airflow.contrib.operators.dataproc_operator.DataprocClusterDeleteOperator                                          | airflow.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperator                                         |
| airflow.contrib.operators.dataproc_operator.DataprocClusterScaleOperator                                           | airflow.providers.google.cloud.operators.dataproc.DataprocScaleClusterOperator                                          |
| airflow.contrib.operators.dataproc_operator.DataprocWorkflowTemplateInstantiateInlineOperator                      | airflow.providers.google.cloud.operators.dataproc.DataprocInstantiateInlineWorkflowTemplateOperator                     |
| airflow.contrib.operators.dataproc_operator.DataprocWorkflowTemplateInstantiateOperator                            | airflow.providers.google.cloud.operators.dataproc.DataprocInstantiateWorkflowTemplateOperator                           |
| airflow.contrib.operators.datastore_export_operator.DatastoreExportOperator                                        | airflow.providers.google.cloud.operators.datastore.CloudDatastoreExportEntitiesOperator                                 |
| airflow.contrib.operators.datastore_import_operator.DatastoreImportOperator                                        | airflow.providers.google.cloud.operators.datastore.CloudDatastoreImportEntitiesOperator                                 |
| airflow.contrib.operators.file_to_gcs.FileToGoogleCloudStorageOperator                                             | airflow.providers.google.cloud.operators.local_to_gcs.LocalFilesystemToGCSOperator                                      |
| airflow.contrib.operators.gcp_bigtable_operator.BigtableClusterUpdateOperator                                      | airflow.providers.google.cloud.operators.bigtable.BigtableUpdateClusterOperator                                         |
| airflow.contrib.operators.gcp_bigtable_operator.BigtableInstanceCreateOperator                                     | airflow.providers.google.cloud.operators.bigtable.BigtableCreateInstanceOperator                                        |
| airflow.contrib.operators.gcp_bigtable_operator.BigtableInstanceDeleteOperator                                     | airflow.providers.google.cloud.operators.bigtable.BigtableDeleteInstanceOperator                                        |
| airflow.contrib.operators.gcp_bigtable_operator.BigtableTableCreateOperator                                        | airflow.providers.google.cloud.operators.bigtable.BigtableCreateTableOperator                                           |
| airflow.contrib.operators.gcp_bigtable_operator.BigtableTableDeleteOperator                                        | airflow.providers.google.cloud.operators.bigtable.BigtableDeleteTableOperator                                           |
| airflow.contrib.operators.gcp_cloud_build_operator.CloudBuildCreateBuildOperator                                   | airflow.providers.google.cloud.operators.cloud_build.CloudBuildCreateOperator                                           |
| airflow.contrib.operators.gcp_compute_operator.GceBaseOperator                                                     | airflow.providers.google.cloud.operators.compute.ComputeEngineBaseOperator                                              |
| airflow.contrib.operators.gcp_compute_operator.GceInstanceGroupManagerUpdateTemplateOperator                       | airflow.providers.google.cloud.operators.compute.ComputeEngineInstanceGroupUpdateManagerTemplateOperator                |
| airflow.contrib.operators.gcp_compute_operator.GceInstanceStartOperator                                            | airflow.providers.google.cloud.operators.compute.ComputeEngineStartInstanceOperator                                     |
| airflow.contrib.operators.gcp_compute_operator.GceInstanceStopOperator                                             | airflow.providers.google.cloud.operators.compute.ComputeEngineStopInstanceOperator                                      |
| airflow.contrib.operators.gcp_compute_operator.GceInstanceTemplateCopyOperator                                     | airflow.providers.google.cloud.operators.compute.ComputeEngineCopyInstanceTemplateOperator                              |
| airflow.contrib.operators.gcp_compute_operator.GceSetMachineTypeOperator                                           | airflow.providers.google.cloud.operators.compute.ComputeEngineSetMachineTypeOperator                                    |
| airflow.contrib.operators.gcp_container_operator.GKEClusterCreateOperator                                          | airflow.providers.google.cloud.operators.kubernetes_engine.GKECreateClusterOperator                                     |
| airflow.contrib.operators.gcp_container_operator.GKEClusterDeleteOperator                                          | airflow.providers.google.cloud.operators.kubernetes_engine.GKEDeleteClusterOperator                                     |
| airflow.contrib.operators.gcp_container_operator.GKEPodOperator                                                    | airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator                                          |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPCancelDLPJobOperator                                            | airflow.providers.google.cloud.operators.dlp.CloudDLPCancelDLPJobOperator                                               |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateDLPJobOperator                                            | airflow.providers.google.cloud.operators.dlp.CloudDLPCreateDLPJobOperator                                               |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateDeidentifyTemplateOperator                                | airflow.providers.google.cloud.operators.dlp.CloudDLPCreateDeidentifyTemplateOperator                                   |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateInspectTemplateOperator                                   | airflow.providers.google.cloud.operators.dlp.CloudDLPCreateInspectTemplateOperator                                      |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateJobTriggerOperator                                        | airflow.providers.google.cloud.operators.dlp.CloudDLPCreateJobTriggerOperator                                           |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateStoredInfoTypeOperator                                    | airflow.providers.google.cloud.operators.dlp.CloudDLPCreateStoredInfoTypeOperator                                       |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeidentifyContentOperator                                       | airflow.providers.google.cloud.operators.dlp.CloudDLPDeidentifyContentOperator                                          |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteDeidentifyTemplateOperator                                | airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDeidentifyTemplateOperator                                   |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteDlpJobOperator                                            | airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDLPJobOperator                                               |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteInspectTemplateOperator                                   | airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteInspectTemplateOperator                                      |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteJobTriggerOperator                                        | airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteJobTriggerOperator                                           |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteStoredInfoTypeOperator                                    | airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteStoredInfoTypeOperator                                       |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetDeidentifyTemplateOperator                                   | airflow.providers.google.cloud.operators.dlp.CloudDLPGetDeidentifyTemplateOperator                                      |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetDlpJobOperator                                               | airflow.providers.google.cloud.operators.dlp.CloudDLPGetDLPJobOperator                                                  |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetInspectTemplateOperator                                      | airflow.providers.google.cloud.operators.dlp.CloudDLPGetInspectTemplateOperator                                         |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetJobTripperOperator                                           | airflow.providers.google.cloud.operators.dlp.CloudDLPGetDLPJobTriggerOperator                                           |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetStoredInfoTypeOperator                                       | airflow.providers.google.cloud.operators.dlp.CloudDLPGetStoredInfoTypeOperator                                          |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPInspectContentOperator                                          | airflow.providers.google.cloud.operators.dlp.CloudDLPInspectContentOperator                                             |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPListDeidentifyTemplatesOperator                                 | airflow.providers.google.cloud.operators.dlp.CloudDLPListDeidentifyTemplatesOperator                                    |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPListDlpJobsOperator                                             | airflow.providers.google.cloud.operators.dlp.CloudDLPListDLPJobsOperator                                                |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPListInfoTypesOperator                                           | airflow.providers.google.cloud.operators.dlp.CloudDLPListInfoTypesOperator                                              |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPListInspectTemplatesOperator                                    | airflow.providers.google.cloud.operators.dlp.CloudDLPListInspectTemplatesOperator                                       |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPListJobTriggersOperator                                         | airflow.providers.google.cloud.operators.dlp.CloudDLPListJobTriggersOperator                                            |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPListStoredInfoTypesOperator                                     | airflow.providers.google.cloud.operators.dlp.CloudDLPListStoredInfoTypesOperator                                        |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPRedactImageOperator                                             | airflow.providers.google.cloud.operators.dlp.CloudDLPRedactImageOperator                                                |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPReidentifyContentOperator                                       | airflow.providers.google.cloud.operators.dlp.CloudDLPReidentifyContentOperator                                          |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateDeidentifyTemplateOperator                                | airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateDeidentifyTemplateOperator                                   |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateInspectTemplateOperator                                   | airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateInspectTemplateOperator                                      |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateJobTriggerOperator                                        | airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateJobTriggerOperator                                           |
| airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateStoredInfoTypeOperator                                    | airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateStoredInfoTypeOperator                                       |
| airflow.contrib.operators.gcp_function_operator.GcfFunctionDeleteOperator                                          | airflow.providers.google.cloud.operators.functions.CloudFunctionDeleteFunctionOperator                                  |
| airflow.contrib.operators.gcp_function_operator.GcfFunctionDeployOperator                                          | airflow.providers.google.cloud.operators.functions.CloudFunctionDeployFunctionOperator                                  |
| airflow.contrib.operators.gcp_natural_language_operator.CloudLanguageAnalyzeEntitiesOperator                       | airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeEntitiesOperator                   |
| airflow.contrib.operators.gcp_natural_language_operator.CloudLanguageAnalyzeEntitySentimentOperator                | airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeEntitySentimentOperator            |
| airflow.contrib.operators.gcp_natural_language_operator.CloudLanguageAnalyzeSentimentOperator                      | airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeSentimentOperator                  |
| airflow.contrib.operators.gcp_natural_language_operator.CloudLanguageClassifyTextOperator                          | airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageClassifyTextOperator                      |
| airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseDeleteOperator                          | airflow.providers.google.cloud.operators.spanner.SpannerDeleteDatabaseInstanceOperator                                  |
| airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseDeployOperator                          | airflow.providers.google.cloud.operators.spanner.SpannerDeployDatabaseInstanceOperator                                  |
| airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseQueryOperator                           | airflow.providers.google.cloud.operators.spanner.SpannerQueryDatabaseInstanceOperator                                   |
| airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseUpdateOperator                          | airflow.providers.google.cloud.operators.spanner.SpannerUpdateDatabaseInstanceOperator                                  |
| airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDeleteOperator                                  | airflow.providers.google.cloud.operators.spanner.SpannerDeleteInstanceOperator                                          |
| airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDeployOperator                                  | airflow.providers.google.cloud.operators.spanner.SpannerDeployInstanceOperator                                          |
| airflow.contrib.operators.gcp_speech_to_text_operator.GcpSpeechToTextRecognizeSpeechOperator                       | airflow.providers.google.cloud.operators.speech_to_text.CloudSpeechToTextRecognizeSpeechOperator                        |
| airflow.contrib.operators.gcp_sql_operator.CloudSqlBaseOperator                                                    | airflow.providers.google.cloud.operators.cloud_sql.CloudSQLBaseOperator                                                 |
| airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceCreateOperator                                          | airflow.providers.google.cloud.operators.cloud_sql.CloudSQLCreateInstanceOperator                                       |
| airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDatabaseCreateOperator                                  | airflow.providers.google.cloud.operators.cloud_sql.CloudSQLCreateInstanceDatabaseOperator                               |
| airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDatabaseDeleteOperator                                  | airflow.providers.google.cloud.operators.cloud_sql.CloudSQLDeleteInstanceDatabaseOperator                               |
| airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDatabasePatchOperator                                   | airflow.providers.google.cloud.operators.cloud_sql.CloudSQLPatchInstanceDatabaseOperator                                |
| airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceDeleteOperator                                          | airflow.providers.google.cloud.operators.cloud_sql.CloudSQLDeleteInstanceOperator                                       |
| airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceExportOperator                                          | airflow.providers.google.cloud.operators.cloud_sql.CloudSQLExportInstanceOperator                                       |
| airflow.contrib.operators.gcp_sql_operator.CloudSqlInstanceImportOperator                                          | airflow.providers.google.cloud.operators.cloud_sql.CloudSQLImportInstanceOperator                                       |
| airflow.contrib.operators.gcp_sql_operator.CloudSqlInstancePatchOperator                                           | airflow.providers.google.cloud.operators.cloud_sql.CloudSQLInstancePatchOperator                                        |
| airflow.contrib.operators.gcp_sql_operator.CloudSqlQueryOperator                                                   | airflow.providers.google.cloud.operators.cloud_sql.CloudSQLExecuteQueryOperator                                         |
| airflow.contrib.operators.gcp_text_to_speech_operator.GcpTextToSpeechSynthesizeOperator                            | airflow.providers.google.cloud.operators.text_to_speech.CloudTextToSpeechSynthesizeOperator                             |
| airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobCreateOperator                                | airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceCreateJobOperator       |
| airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobDeleteOperator                                | airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceDeleteJobOperator       |
| airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobUpdateOperator                                | airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceUpdateJobOperator       |
| airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationCancelOperator                          | airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceCancelOperationOperator |
| airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationGetOperator                             | airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceGetOperationOperator    |
| airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationPauseOperator                           | airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServicePauseOperationOperator  |
| airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationResumeOperator                          | airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceResumeOperationOperator |
| airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationsListOperator                           | airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceListOperationsOperator  |
| airflow.contrib.operators.gcp_transfer_operator.GoogleCloudStorageToGoogleCloudStorageTransferOperator             | airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceGCSToGCSOperator        |
| airflow.contrib.operators.gcp_translate_operator.CloudTranslateTextOperator                                        | airflow.providers.google.cloud.operators.translate.CloudTranslateTextOperator                                           |
| airflow.contrib.operators.gcp_translate_speech_operator.GcpTranslateSpeechOperator                                 | airflow.providers.google.cloud.operators.translate_speech.GcpTranslateSpeechOperator                                    |
| airflow.contrib.operators.gcp_video_intelligence_operator.CloudVideoIntelligenceDetectVideoExplicitContentOperator | airflow.providers.google.cloud.operators.video_intelligence.CloudVideoIntelligenceDetectVideoExplicitContentOperator    |
| airflow.contrib.operators.gcp_video_intelligence_operator.CloudVideoIntelligenceDetectVideoLabelsOperator          | airflow.providers.google.cloud.operators.video_intelligence.CloudVideoIntelligenceDetectVideoLabelsOperator             |
| airflow.contrib.operators.gcp_video_intelligence_operator.CloudVideoIntelligenceDetectVideoShotsOperator           | airflow.providers.google.cloud.operators.video_intelligence.CloudVideoIntelligenceDetectVideoShotsOperator              |
| airflow.contrib.operators.gcp_vision_operator.CloudVisionAddProductToProductSetOperator                            | airflow.providers.google.cloud.operators.vision.CloudVisionAddProductToProductSetOperator                               |
| airflow.contrib.operators.gcp_vision_operator.CloudVisionAnnotateImageOperator                                     | airflow.providers.google.cloud.operators.vision.CloudVisionImageAnnotateOperator                                        |
| airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectDocumentTextOperator                                | airflow.providers.google.cloud.operators.vision.CloudVisionTextDetectOperator                                           |
| airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectImageLabelsOperator                                 | airflow.providers.google.cloud.operators.vision.CloudVisionDetectImageLabelsOperator                                    |
| airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectImageSafeSearchOperator                             | airflow.providers.google.cloud.operators.vision.CloudVisionDetectImageSafeSearchOperator                                |
| airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectTextOperator                                        | airflow.providers.google.cloud.operators.vision.CloudVisionDetectTextOperator                                           |
| airflow.contrib.operators.gcp_vision_operator.CloudVisionProductCreateOperator                                     | airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductOperator                                        |
| airflow.contrib.operators.gcp_vision_operator.CloudVisionProductDeleteOperator                                     | airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductOperator                                        |
| airflow.contrib.operators.gcp_vision_operator.CloudVisionProductGetOperator                                        | airflow.providers.google.cloud.operators.vision.CloudVisionGetProductOperator                                           |
| airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetCreateOperator                                  | airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductSetOperator                                     |
| airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetDeleteOperator                                  | airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductSetOperator                                     |
| airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetGetOperator                                     | airflow.providers.google.cloud.operators.vision.CloudVisionGetProductSetOperator                                        |
| airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetUpdateOperator                                  | airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductSetOperator                                     |
| airflow.contrib.operators.gcp_vision_operator.CloudVisionProductUpdateOperator                                     | airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductOperator                                        |
| airflow.contrib.operators.gcp_vision_operator.CloudVisionReferenceImageCreateOperator                              | airflow.providers.google.cloud.operators.vision.CloudVisionCreateReferenceImageOperator                                 |
| airflow.contrib.operators.gcp_vision_operator.CloudVisionRemoveProductFromProductSetOperator                       | airflow.providers.google.cloud.operators.vision.CloudVisionRemoveProductFromProductSetOperator                          |
| airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageBucketCreateAclEntryOperator                          | airflow.providers.google.cloud.operators.gcs.GCSBucketCreateAclEntryOperator                                            |
| airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageObjectCreateAclEntryOperator                          | airflow.providers.google.cloud.operators.gcs.GCSObjectCreateAclEntryOperator                                            |
| airflow.contrib.operators.gcs_delete_operator.GoogleCloudStorageDeleteOperator                                     | airflow.providers.google.cloud.operators.gcs.GCSDeleteObjectsOperator                                                   |
| airflow.contrib.operators.gcs_download_operator.GoogleCloudStorageDownloadOperator                                 | airflow.providers.google.cloud.operators.gcs.GCSToLocalOperator                                                         |
| airflow.contrib.operators.gcs_list_operator.GoogleCloudStorageListOperator                                         | airflow.providers.google.cloud.operators.gcs.GCSListObjectsOperator                                                     |
| airflow.contrib.operators.gcs_operator.GoogleCloudStorageCreateBucketOperator                                      | airflow.providers.google.cloud.operators.gcs.GCSCreateBucketOperator                                                    |
| airflow.contrib.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator                                           | airflow.providers.google.cloud.operators.gcs_to_bigquery.GCSToBigQueryOperator                                          |
| airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator                                | airflow.providers.google.cloud.operators.gcs_to_gcs.GCSToGCSOperator                                                    |
| airflow.contrib.operators.gcs_to_gdrive_operator.GCSToGoogleDriveOperator                                          | airflow.providers.google.suite.operators.gcs_to_gdrive.GCSToGoogleDriveOperator                                         |
| airflow.contrib.operators.mlengine_operator.MLEngineBatchPredictionOperator                                        | airflow.providers.google.cloud.operators.mlengine.MLEngineStartBatchPredictionJobOperator                               |
| airflow.contrib.operators.mlengine_operator.MLEngineModelOperator                                                  | airflow.providers.google.cloud.operators.mlengine.MLEngineManageModelOperator                                           |
| airflow.contrib.operators.mlengine_operator.MLEngineTrainingOperator                                               | airflow.providers.google.cloud.operators.mlengine.MLEngineStartTrainingJobOperator                                      |
| airflow.contrib.operators.mlengine_operator.MLEngineVersionOperator                                                | airflow.providers.google.cloud.operators.mlengine.MLEngineManageVersionOperator                                         |
| airflow.contrib.operators.mssql_to_gcs.MsSqlToGoogleCloudStorageOperator                                           | airflow.providers.google.cloud.operators.mssql_to_gcs.MSSQLToGCSOperator                                                |
| airflow.contrib.operators.mysql_to_gcs.MySqlToGoogleCloudStorageOperator                                           | airflow.providers.google.cloud.operators.mysql_to_gcs.MySQLToGCSOperator                                                |
| airflow.contrib.operators.postgres_to_gcs_operator.PostgresToGoogleCloudStorageOperator                            | airflow.providers.google.cloud.operators.postgres_to_gcs.PostgresToGCSOperator                                          |
| airflow.contrib.operators.pubsub_operator.PubSubPublishOperator                                                    | airflow.providers.google.cloud.operators.pubsub.PubSubPublishMessageOperator                                            |
| airflow.contrib.operators.pubsub_operator.PubSubSubscriptionCreateOperator                                         | airflow.providers.google.cloud.operators.pubsub.PubSubCreateSubscriptionOperator                                        |
| airflow.contrib.operators.pubsub_operator.PubSubSubscriptionDeleteOperator                                         | airflow.providers.google.cloud.operators.pubsub.PubSubDeleteSubscriptionOperator                                        |
| airflow.contrib.operators.pubsub_operator.PubSubTopicCreateOperator                                                | airflow.providers.google.cloud.operators.pubsub.PubSubCreateTopicOperator                                               |
| airflow.contrib.operators.pubsub_operator.PubSubTopicDeleteOperator                                                | airflow.providers.google.cloud.operators.pubsub.PubSubDeleteTopicOperator                                               |
| airflow.contrib.operators.s3_to_gcs_operator.S3ToGCSOperator                                                       | airflow.providers.google.cloud.operators.s3_to_gcs.S3ToGCSOperator                                                      |
| airflow.contrib.operators.s3_to_gcs_transfer_operator.CloudDataTransferServiceS3ToGCSOperator                      | airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceS3ToGCSOperator         |
| airflow.contrib.operators.sql_to_gcs.BaseSQLToGoogleCloudStorageOperator                                           | airflow.providers.google.cloud.operators.sql_to_gcs.BaseSQLToGCSOperator                                                |
