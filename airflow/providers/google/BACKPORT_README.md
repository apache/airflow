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

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [v2020.XX.XX](#v2020xxxx)
  - [Limitations](#limitations)
  - [New operators](#new-operators)
    - [Google Cloud Platform operators](#google-cloud-platform-operators)
    - [Google Marketing Platform operators](#google-marketing-platform-operators)
    - [Google Suite operators](#google-suite-operators)
    - [Google Ads operators](#google-ads-operators)
    - [Google Firebase operators](#google-firebase-operators)
  - [Updated operators](#updated-operators)
    - [Google Cloud Platform operators](#google-cloud-platform-operators-1)
    - [Google Suite operators](#google-suite-operators-1)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### v2020.XX.XX

This is the first released version of the package.

#### Limitations

The following operators have not been released:

 * All operators for BigQuery service.
 * GKEStartPodOperator for Kubernetes Engine service.

We recommend staying with the old versions yet. These operators will be released as soon as possible.

#### New operators

We have worked intensively on operators that have not appeared in any Airflow release and are available only
through this package. This release includes the following new operators:


##### Google Cloud Platform operators

All operators in `airflow.providers.google.cloud.operators` package

* [AutoML](https://cloud.google.com/automl/)

    All operators in `automl` module.

    * AutoMLBatchPredictOperator
    * AutoMLCreateDatasetOperator
    * AutoMLDeleteDatasetOperator
    * AutoMLDeleteModelOperator
    * AutoMLDeployModelOperator
    * AutoMLGetModelOperator
    * AutoMLImportDataOperator
    * AutoMLListDatasetOperator
    * AutoMLPredictOperator
    * AutoMLTablesListColumnSpecsOperator
    * AutoMLTablesListTableSpecsOperator
    * AutoMLTablesUpdateDatasetOperator
    * AutoMLTrainModelOperator

* [BigQuery Data Transfer Service](https://cloud.google.com/bigquery/transfer/)

    All operators in `bigquery_dts` module

    * BigQueryCreateDataTransferOperator
    * BigQueryDataTransferServiceStartTransferRunsOperator
    * BigQueryDeleteDataTransferConfigOperator

* [Cloud Memorystore](https://cloud.google.com/memorystore/)

    All operators in `cloud_memorystore` module.

    * CloudMemorystoreCreateInstanceAndImportOperator
    * CloudMemorystoreCreateInstanceOperator
    * CloudMemorystoreDeleteInstanceOperator
    * CloudMemorystoreExportAndDeleteInstanceOperator
    * CloudMemorystoreExportInstanceOperator
    * CloudMemorystoreFailoverInstanceOperator
    * CloudMemorystoreGetInstanceOperator
    * CloudMemorystoreImportOperator
    * CloudMemorystoreListInstancesOperator
    * CloudMemorystoreScaleInstanceOperator
    * CloudMemorystoreUpdateInstanceOperator

* [Data Catalog](https://cloud.google.com/data-catalog)

    All operators in `datacatalog` module.

    * CloudDataCatalogCreateEntryGroupOperator
    * CloudDataCatalogCreateEntryOperator
    * CloudDataCatalogCreateTagOperator
    * CloudDataCatalogCreateTagTemplateFieldOperator
    * CloudDataCatalogCreateTagTemplateOperator
    * CloudDataCatalogDeleteEntryGroupOperator
    * CloudDataCatalogDeleteEntryOperator
    * CloudDataCatalogDeleteTagOperator
    * CloudDataCatalogDeleteTagTemplateFieldOperator
    * CloudDataCatalogDeleteTagTemplateOperator
    * CloudDataCatalogGetEntryGroupOperator
    * CloudDataCatalogGetEntryOperator
    * CloudDataCatalogGetTagTemplateOperator
    * CloudDataCatalogListTagsOperator
    * CloudDataCatalogLookupEntryOperator
    * CloudDataCatalogRenameTagTemplateFieldOperator
    * CloudDataCatalogSearchCatalogOperator
    * CloudDataCatalogUpdateEntryOperator
    * CloudDataCatalogUpdateTagOperator
    * CloudDataCatalogUpdateTagTemplateFieldOperator
    * CloudDataCatalogUpdateTagTemplateOperator

* [Dataproc](https://cloud.google.com/dataproc/)

    All operators in `dataproc` module.

    * DataprocSubmitJobOperator
    * DataprocUpdateClusterOperator

* [Data Fusion](https://cloud.google.com/data-fusion/)

    All operators in `datafusion` module.

    * CloudDataFusionCreateInstanceOperator
    * CloudDataFusionCreatePipelineOperator
    * CloudDataFusionDeleteInstanceOperator
    * CloudDataFusionDeletePipelineOperator
    * CloudDataFusionGetInstanceOperator
    * CloudDataFusionListPipelinesOperator
    * CloudDataFusionRestartInstanceOperator
    * CloudDataFusionStartPipelineOperator
    * CloudDataFusionStopPipelineOperator
    * CloudDataFusionUpdateInstanceOperator

* [Cloud Functions](https://cloud.google.com/functions/)

    All operators in `functions` module.

    * CloudFunctionInvokeFunctionOperator

* [Cloud Storage (GCS)](https://cloud.google.com/gcs/)

    All operators in `gcs` module.

    * GCSDeleteBucketOperator
    * GCSFileTransformOperator

* [Dataproc](https://cloud.google.com/dataproc/)

    All operators in `dataproc` module.

    * DataprocSubmitJobOperator
    * DataprocUpdateClusterOperator

* [Machine Learning Engine](https://cloud.google.com/ml-engine/)

    All operators in `mlengine` module.

    * MLEngineCreateModelOperator
    * MLEngineCreateVersionOperator
    * MLEngineDeleteModelOperator
    * MLEngineDeleteVersionOperator
    * MLEngineGetModelOperator
    * MLEngineListVersionsOperator
    * MLEngineSetDefaultVersionOperator
    * MLEngineTrainingCancelJobOperator

* [Cloud Pub/Sub](https://cloud.google.com/pubsub/)

    All operators in  `pubsub` module.

    * PubSubPullOperator

* [Cloud Stackdriver](https://cloud.google.com/stackdriver)

    All operators in `stackdriver` module.

    * StackdriverDeleteAlertOperator
    * StackdriverDeleteNotificationChannelOperator
    * StackdriverDisableAlertPoliciesOperator
    * StackdriverDisableNotificationChannelsOperator
    * StackdriverEnableAlertPoliciesOperator
    * StackdriverEnableNotificationChannelsOperator
    * StackdriverListAlertPoliciesOperator
    * StackdriverListNotificationChannelsOperator
    * StackdriverUpsertAlertOperator
    * StackdriverUpsertNotificationChannelOperator

* [Cloud Tasks](https://cloud.google.com/tasks/)

    All operators in `tasks` module.

    * CloudTasksQueueCreateOperator
    * CloudTasksQueueDeleteOperator
    * CloudTasksQueueGetOperator
    * CloudTasksQueuePauseOperator
    * CloudTasksQueuePurgeOperator
    * CloudTasksQueueResumeOperator
    * CloudTasksQueueUpdateOperator
    * CloudTasksQueuesListOperator
    * CloudTasksTaskCreateOperator
    * CloudTasksTaskDeleteOperator
    * CloudTasksTaskGetOperator
    * CloudTasksTaskRunOperator
    * CloudTasksTasksListOperator

* Transfer Google Cloud operators:

    * cassandra_to_gcs.CassandraToGCSOperator
    * gcs_to_gcs.GCSSynchronizeBuckets
    * gcs_to_sftp.GCSToSFTPOperator
    * presto_to_gcs.PrestoToGCSOperator
    * sftp_to_gcs.SFTPToGCSOperator
    * sheets_to_gcs.GoogleSheetsToGCSOperator

##### Google Marketing Platform operators

All operators in `airflow.providers.google.marketing_platform.operators` package

* [Analytics360](https://analytics.google.com/)

    All operators in `analytics` module.

    * GoogleAnalyticsDataImportUploadOperator
    * GoogleAnalyticsDeletePreviousDataUploadsOperator
    * GoogleAnalyticsGetAdsLinkOperator
    * GoogleAnalyticsListAccountsOperator
    * GoogleAnalyticsModifyFileHeadersDataImportOperator
    * GoogleAnalyticsRetrieveAdsLinksListOperator

* [Google Campaign Manager](https://developers.google.com/doubleclick-advertisers)

    All operators in `campaign_manager` module.

    * GoogleCampaignManagerBatchInsertConversionsOperator
    * GoogleCampaignManagerBatchUpdateConversionsOperator
    * GoogleCampaignManagerDeleteReportOperator
    * GoogleCampaignManagerDownloadReportOperator
    * GoogleCampaignManagerInsertReportOperator
    * GoogleCampaignManagerRunReportOperator

* [Google Display&Video 360](https://marketingplatform.google.com/about/display-video-360/)

    All operators in `display_video` module.

    * GoogleDisplayVideo360CreateReportOperator
    * GoogleDisplayVideo360DeleteReportOperator
    * GoogleDisplayVideo360DownloadReportOperator
    * GoogleDisplayVideo360RunReportOperator

* [Google Search Ads 360](https://marketingplatform.google.com/about/search-ads-360/)

    All operators in `search_ads` module.

    * GoogleSearchAdsDownloadReportOperator
    * GoogleSearchAdsInsertReportOperator

##### Google Suite operators

All operators in `airflow.providers.google.suite.operators` sub-package:

* [Google Spreadsheet](https://www.google.com/intl/en/sheets/about/)

    All operators in `sheets` module.

    * GoogleSheetsCreateSpreadsheet

* Transfer Google Suite operators:

    * gcs_to_sheets.GCSToGoogleSheetsOperator

##### Google Ads operators

All operators in `airflow.providers.google.ads.operators` package

* [Google Ads](https://ads.google.com/)

    All operators in `ads` module.

    * GoogleAdsToGcsOperator

##### Google Firebase operators

All operators in `airflow.providers.google.firebase.operators` package

* [Cloud Firestore](https://firebase.google.com/docs/firestore)

    All operators in `firestore` module.

    * CloudFirestoreExportDatabaseOperator

#### Updated operators

The operators in Airflow 2.0 have been moved to a new package. The following table showing operators
from Airflow 1.10 and its equivalent from Airflow 2.0:

##### Google Cloud Platform operators

| Airflow 1.10  (`airflow.contrib.operators` package)                                      | Airflow 2.0 (`airflow.providers.google.cloud.operators` package)               |
|------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| adls_to_gcs.AdlsToGoogleCloudStorageOperator                                             | adls_to_gcs.ADLSToGCSOperator                                                  |
| bigquery_check_operator.BigQueryCheckOperator                                            | bigquery.BigQueryCheckOperator                                                 |
| bigquery_check_operator.BigQueryIntervalCheckOperator                                    | bigquery.BigQueryIntervalCheckOperator                                         |
| bigquery_check_operator.BigQueryValueCheckOperator                                       | bigquery.BigQueryValueCheckOperator                                            |
| bigquery_get_data.BigQueryGetDataOperator                                                | bigquery.BigQueryGetDataOperator                                               |
| bigquery_operator.BigQueryOperator                                                       | bigquery.BigQueryExecuteQueryOperator                                          |
| bigquery_table_delete_operator.BigQueryTableDeleteOperator                               | bigquery.BigQueryDeleteTableOperator                                           |
| bigquery_to_bigquery.BigQueryToBigQueryOperator                                          | bigquery_to_bigquery.BigQueryToBigQueryOperator                                |
| bigquery_to_gcs.BigQueryToCloudStorageOperator                                           | bigquery_to_gcs.BigQueryToGCSOperator                                          |
| bigquery_to_mysql_operator.BigQueryToMySqlOperator                                       | bigquery_to_mysql.BigQueryToMySqlOperator                                      |
| dataflow_operator.DataFlowJavaOperator                                                   | dataflow.DataflowCreateJavaJobOperator                                         |
| dataflow_operator.DataFlowPythonOperator                                                 | dataflow.DataflowCreatePythonJobOperator                                       |
| dataflow_operator.DataflowTemplateOperator                                               | dataflow.DataflowTemplatedJobStartOperator                                     |
| dataproc_operator.DataProcHadoopOperator                                                 | dataproc.DataprocSubmitHadoopJobOperator                                       |
| dataproc_operator.DataProcHiveOperator                                                   | dataproc.DataprocSubmitHiveJobOperator                                         |
| dataproc_operator.DataProcJobBaseOperator                                                | dataproc.DataprocJobBaseOperator                                               |
| dataproc_operator.DataProcPigOperator                                                    | dataproc.DataprocSubmitPigJobOperator                                          |
| dataproc_operator.DataProcPySparkOperator                                                | dataproc.DataprocSubmitPySparkJobOperator                                      |
| dataproc_operator.DataProcSparkOperator                                                  | dataproc.DataprocSubmitSparkJobOperator                                        |
| dataproc_operator.DataProcSparkSqlOperator                                               | dataproc.DataprocSubmitSparkSqlJobOperator                                     |
| dataproc_operator.DataprocClusterCreateOperator                                          | dataproc.DataprocCreateClusterOperator                                         |
| dataproc_operator.DataprocClusterDeleteOperator                                          | dataproc.DataprocDeleteClusterOperator                                         |
| dataproc_operator.DataprocClusterScaleOperator                                           | dataproc.DataprocScaleClusterOperator                                          |
| dataproc_operator.DataprocWorkflowTemplateInstantiateInlineOperator                      | dataproc.DataprocInstantiateInlineWorkflowTemplateOperator                     |
| dataproc_operator.DataprocWorkflowTemplateInstantiateOperator                            | dataproc.DataprocInstantiateWorkflowTemplateOperator                           |
| datastore_export_operator.DatastoreExportOperator                                        | datastore.CloudDatastoreExportEntitiesOperator                                 |
| datastore_import_operator.DatastoreImportOperator                                        | datastore.CloudDatastoreImportEntitiesOperator                                 |
| file_to_gcs.FileToGoogleCloudStorageOperator                                             | local_to_gcs.LocalFilesystemToGCSOperator                                      |
| gcp_bigtable_operator.BigtableClusterUpdateOperator                                      | bigtable.BigtableUpdateClusterOperator                                         |
| gcp_bigtable_operator.BigtableInstanceCreateOperator                                     | bigtable.BigtableCreateInstanceOperator                                        |
| gcp_bigtable_operator.BigtableInstanceDeleteOperator                                     | bigtable.BigtableDeleteInstanceOperator                                        |
| gcp_bigtable_operator.BigtableTableCreateOperator                                        | bigtable.BigtableCreateTableOperator                                           |
| gcp_bigtable_operator.BigtableTableDeleteOperator                                        | bigtable.BigtableDeleteTableOperator                                           |
| gcp_cloud_build_operator.CloudBuildCreateBuildOperator                                   | cloud_build.CloudBuildCreateOperator                                           |
| gcp_compute_operator.GceBaseOperator                                                     | compute.ComputeEngineBaseOperator                                              |
| gcp_compute_operator.GceInstanceGroupManagerUpdateTemplateOperator                       | compute.ComputeEngineInstanceGroupUpdateManagerTemplateOperator                |
| gcp_compute_operator.GceInstanceStartOperator                                            | compute.ComputeEngineStartInstanceOperator                                     |
| gcp_compute_operator.GceInstanceStopOperator                                             | compute.ComputeEngineStopInstanceOperator                                      |
| gcp_compute_operator.GceInstanceTemplateCopyOperator                                     | compute.ComputeEngineCopyInstanceTemplateOperator                              |
| gcp_compute_operator.GceSetMachineTypeOperator                                           | compute.ComputeEngineSetMachineTypeOperator                                    |
| gcp_container_operator.GKEClusterCreateOperator                                          | kubernetes_engine.GKECreateClusterOperator                                     |
| gcp_container_operator.GKEClusterDeleteOperator                                          | kubernetes_engine.GKEDeleteClusterOperator                                     |
| gcp_container_operator.GKEPodOperator                                                    | kubernetes_engine.GKEStartPodOperator                                          |
| gcp_dlp_operator.CloudDLPCancelDLPJobOperator                                            | dlp.CloudDLPCancelDLPJobOperator                                               |
| gcp_dlp_operator.CloudDLPCreateDLPJobOperator                                            | dlp.CloudDLPCreateDLPJobOperator                                               |
| gcp_dlp_operator.CloudDLPCreateDeidentifyTemplateOperator                                | dlp.CloudDLPCreateDeidentifyTemplateOperator                                   |
| gcp_dlp_operator.CloudDLPCreateInspectTemplateOperator                                   | dlp.CloudDLPCreateInspectTemplateOperator                                      |
| gcp_dlp_operator.CloudDLPCreateJobTriggerOperator                                        | dlp.CloudDLPCreateJobTriggerOperator                                           |
| gcp_dlp_operator.CloudDLPCreateStoredInfoTypeOperator                                    | dlp.CloudDLPCreateStoredInfoTypeOperator                                       |
| gcp_dlp_operator.CloudDLPDeidentifyContentOperator                                       | dlp.CloudDLPDeidentifyContentOperator                                          |
| gcp_dlp_operator.CloudDLPDeleteDeidentifyTemplateOperator                                | dlp.CloudDLPDeleteDeidentifyTemplateOperator                                   |
| gcp_dlp_operator.CloudDLPDeleteDlpJobOperator                                            | dlp.CloudDLPDeleteDLPJobOperator                                               |
| gcp_dlp_operator.CloudDLPDeleteInspectTemplateOperator                                   | dlp.CloudDLPDeleteInspectTemplateOperator                                      |
| gcp_dlp_operator.CloudDLPDeleteJobTriggerOperator                                        | dlp.CloudDLPDeleteJobTriggerOperator                                           |
| gcp_dlp_operator.CloudDLPDeleteStoredInfoTypeOperator                                    | dlp.CloudDLPDeleteStoredInfoTypeOperator                                       |
| gcp_dlp_operator.CloudDLPGetDeidentifyTemplateOperator                                   | dlp.CloudDLPGetDeidentifyTemplateOperator                                      |
| gcp_dlp_operator.CloudDLPGetDlpJobOperator                                               | dlp.CloudDLPGetDLPJobOperator                                                  |
| gcp_dlp_operator.CloudDLPGetInspectTemplateOperator                                      | dlp.CloudDLPGetInspectTemplateOperator                                         |
| gcp_dlp_operator.CloudDLPGetJobTripperOperator                                           | dlp.CloudDLPGetDLPJobTriggerOperator                                           |
| gcp_dlp_operator.CloudDLPGetStoredInfoTypeOperator                                       | dlp.CloudDLPGetStoredInfoTypeOperator                                          |
| gcp_dlp_operator.CloudDLPInspectContentOperator                                          | dlp.CloudDLPInspectContentOperator                                             |
| gcp_dlp_operator.CloudDLPListDeidentifyTemplatesOperator                                 | dlp.CloudDLPListDeidentifyTemplatesOperator                                    |
| gcp_dlp_operator.CloudDLPListDlpJobsOperator                                             | dlp.CloudDLPListDLPJobsOperator                                                |
| gcp_dlp_operator.CloudDLPListInfoTypesOperator                                           | dlp.CloudDLPListInfoTypesOperator                                              |
| gcp_dlp_operator.CloudDLPListInspectTemplatesOperator                                    | dlp.CloudDLPListInspectTemplatesOperator                                       |
| gcp_dlp_operator.CloudDLPListJobTriggersOperator                                         | dlp.CloudDLPListJobTriggersOperator                                            |
| gcp_dlp_operator.CloudDLPListStoredInfoTypesOperator                                     | dlp.CloudDLPListStoredInfoTypesOperator                                        |
| gcp_dlp_operator.CloudDLPRedactImageOperator                                             | dlp.CloudDLPRedactImageOperator                                                |
| gcp_dlp_operator.CloudDLPReidentifyContentOperator                                       | dlp.CloudDLPReidentifyContentOperator                                          |
| gcp_dlp_operator.CloudDLPUpdateDeidentifyTemplateOperator                                | dlp.CloudDLPUpdateDeidentifyTemplateOperator                                   |
| gcp_dlp_operator.CloudDLPUpdateInspectTemplateOperator                                   | dlp.CloudDLPUpdateInspectTemplateOperator                                      |
| gcp_dlp_operator.CloudDLPUpdateJobTriggerOperator                                        | dlp.CloudDLPUpdateJobTriggerOperator                                           |
| gcp_dlp_operator.CloudDLPUpdateStoredInfoTypeOperator                                    | dlp.CloudDLPUpdateStoredInfoTypeOperator                                       |
| gcp_function_operator.GcfFunctionDeleteOperator                                          | functions.CloudFunctionDeleteFunctionOperator                                  |
| gcp_function_operator.GcfFunctionDeployOperator                                          | functions.CloudFunctionDeployFunctionOperator                                  |
| gcp_natural_language_operator.CloudLanguageAnalyzeEntitiesOperator                       | natural_language.CloudNaturalLanguageAnalyzeEntitiesOperator                   |
| gcp_natural_language_operator.CloudLanguageAnalyzeEntitySentimentOperator                | natural_language.CloudNaturalLanguageAnalyzeEntitySentimentOperator            |
| gcp_natural_language_operator.CloudLanguageAnalyzeSentimentOperator                      | natural_language.CloudNaturalLanguageAnalyzeSentimentOperator                  |
| gcp_natural_language_operator.CloudLanguageClassifyTextOperator                          | natural_language.CloudNaturalLanguageClassifyTextOperator                      |
| gcp_spanner_operator.CloudSpannerInstanceDatabaseDeleteOperator                          | spanner.SpannerDeleteDatabaseInstanceOperator                                  |
| gcp_spanner_operator.CloudSpannerInstanceDatabaseDeployOperator                          | spanner.SpannerDeployDatabaseInstanceOperator                                  |
| gcp_spanner_operator.CloudSpannerInstanceDatabaseQueryOperator                           | spanner.SpannerQueryDatabaseInstanceOperator                                   |
| gcp_spanner_operator.CloudSpannerInstanceDatabaseUpdateOperator                          | spanner.SpannerUpdateDatabaseInstanceOperator                                  |
| gcp_spanner_operator.CloudSpannerInstanceDeleteOperator                                  | spanner.SpannerDeleteInstanceOperator                                          |
| gcp_spanner_operator.CloudSpannerInstanceDeployOperator                                  | spanner.SpannerDeployInstanceOperator                                          |
| gcp_speech_to_text_operator.GcpSpeechToTextRecognizeSpeechOperator                       | speech_to_text.CloudSpeechToTextRecognizeSpeechOperator                        |
| gcp_sql_operator.CloudSqlBaseOperator                                                    | cloud_sql.CloudSQLBaseOperator                                                 |
| gcp_sql_operator.CloudSqlInstanceCreateOperator                                          | cloud_sql.CloudSQLCreateInstanceOperator                                       |
| gcp_sql_operator.CloudSqlInstanceDatabaseCreateOperator                                  | cloud_sql.CloudSQLCreateInstanceDatabaseOperator                               |
| gcp_sql_operator.CloudSqlInstanceDatabaseDeleteOperator                                  | cloud_sql.CloudSQLDeleteInstanceDatabaseOperator                               |
| gcp_sql_operator.CloudSqlInstanceDatabasePatchOperator                                   | cloud_sql.CloudSQLPatchInstanceDatabaseOperator                                |
| gcp_sql_operator.CloudSqlInstanceDeleteOperator                                          | cloud_sql.CloudSQLDeleteInstanceOperator                                       |
| gcp_sql_operator.CloudSqlInstanceExportOperator                                          | cloud_sql.CloudSQLExportInstanceOperator                                       |
| gcp_sql_operator.CloudSqlInstanceImportOperator                                          | cloud_sql.CloudSQLImportInstanceOperator                                       |
| gcp_sql_operator.CloudSqlInstancePatchOperator                                           | cloud_sql.CloudSQLInstancePatchOperator                                        |
| gcp_sql_operator.CloudSqlQueryOperator                                                   | cloud_sql.CloudSQLExecuteQueryOperator                                         |
| gcp_text_to_speech_operator.GcpTextToSpeechSynthesizeOperator                            | text_to_speech.CloudTextToSpeechSynthesizeOperator                             |
| gcp_transfer_operator.GcpTransferServiceJobCreateOperator                                | cloud_storage_transfer_service.CloudDataTransferServiceCreateJobOperator       |
| gcp_transfer_operator.GcpTransferServiceJobDeleteOperator                                | cloud_storage_transfer_service.CloudDataTransferServiceDeleteJobOperator       |
| gcp_transfer_operator.GcpTransferServiceJobUpdateOperator                                | cloud_storage_transfer_service.CloudDataTransferServiceUpdateJobOperator       |
| gcp_transfer_operator.GcpTransferServiceOperationCancelOperator                          | cloud_storage_transfer_service.CloudDataTransferServiceCancelOperationOperator |
| gcp_transfer_operator.GcpTransferServiceOperationGetOperator                             | cloud_storage_transfer_service.CloudDataTransferServiceGetOperationOperator    |
| gcp_transfer_operator.GcpTransferServiceOperationPauseOperator                           | cloud_storage_transfer_service.CloudDataTransferServicePauseOperationOperator  |
| gcp_transfer_operator.GcpTransferServiceOperationResumeOperator                          | cloud_storage_transfer_service.CloudDataTransferServiceResumeOperationOperator |
| gcp_transfer_operator.GcpTransferServiceOperationsListOperator                           | cloud_storage_transfer_service.CloudDataTransferServiceListOperationsOperator  |
| gcp_transfer_operator.GoogleCloudStorageToGoogleCloudStorageTransferOperator             | cloud_storage_transfer_service.CloudDataTransferServiceGCSToGCSOperator        |
| gcp_translate_operator.CloudTranslateTextOperator                                        | translate.CloudTranslateTextOperator                                           |
| gcp_translate_speech_operator.GcpTranslateSpeechOperator                                 | translate_speech.CloudTranslateSpeechOperator                                  |
| gcp_video_intelligence_operator.CloudVideoIntelligenceDetectVideoExplicitContentOperator | video_intelligence.CloudVideoIntelligenceDetectVideoExplicitContentOperator    |
| gcp_video_intelligence_operator.CloudVideoIntelligenceDetectVideoLabelsOperator          | video_intelligence.CloudVideoIntelligenceDetectVideoLabelsOperator             |
| gcp_video_intelligence_operator.CloudVideoIntelligenceDetectVideoShotsOperator           | video_intelligence.CloudVideoIntelligenceDetectVideoShotsOperator              |
| gcp_vision_operator.CloudVisionAddProductToProductSetOperator                            | vision.CloudVisionAddProductToProductSetOperator                               |
| gcp_vision_operator.CloudVisionAnnotateImageOperator                                     | vision.CloudVisionImageAnnotateOperator                                        |
| gcp_vision_operator.CloudVisionDetectDocumentTextOperator                                | vision.CloudVisionTextDetectOperator                                           |
| gcp_vision_operator.CloudVisionDetectImageLabelsOperator                                 | vision.CloudVisionDetectImageLabelsOperator                                    |
| gcp_vision_operator.CloudVisionDetectImageSafeSearchOperator                             | vision.CloudVisionDetectImageSafeSearchOperator                                |
| gcp_vision_operator.CloudVisionDetectTextOperator                                        | vision.CloudVisionDetectTextOperator                                           |
| gcp_vision_operator.CloudVisionProductCreateOperator                                     | vision.CloudVisionCreateProductOperator                                        |
| gcp_vision_operator.CloudVisionProductDeleteOperator                                     | vision.CloudVisionDeleteProductOperator                                        |
| gcp_vision_operator.CloudVisionProductGetOperator                                        | vision.CloudVisionGetProductOperator                                           |
| gcp_vision_operator.CloudVisionProductSetCreateOperator                                  | vision.CloudVisionCreateProductSetOperator                                     |
| gcp_vision_operator.CloudVisionProductSetDeleteOperator                                  | vision.CloudVisionDeleteProductSetOperator                                     |
| gcp_vision_operator.CloudVisionProductSetGetOperator                                     | vision.CloudVisionGetProductSetOperator                                        |
| gcp_vision_operator.CloudVisionProductSetUpdateOperator                                  | vision.CloudVisionUpdateProductSetOperator                                     |
| gcp_vision_operator.CloudVisionProductUpdateOperator                                     | vision.CloudVisionUpdateProductOperator                                        |
| gcp_vision_operator.CloudVisionReferenceImageCreateOperator                              | vision.CloudVisionCreateReferenceImageOperator                                 |
| gcp_vision_operator.CloudVisionRemoveProductFromProductSetOperator                       | vision.CloudVisionRemoveProductFromProductSetOperator                          |
| gcs_acl_operator.GoogleCloudStorageBucketCreateAclEntryOperator                          | gcs.GCSBucketCreateAclEntryOperator                                            |
| gcs_acl_operator.GoogleCloudStorageObjectCreateAclEntryOperator                          | gcs.GCSObjectCreateAclEntryOperator                                            |
| gcs_delete_operator.GoogleCloudStorageDeleteOperator                                     | gcs.GCSDeleteObjectsOperator                                                   |
| gcs_download_operator.GoogleCloudStorageDownloadOperator                                 | gcs.GCSToLocalOperator                                                         |
| gcs_list_operator.GoogleCloudStorageListOperator                                         | gcs.GCSListObjectsOperator                                                     |
| gcs_operator.GoogleCloudStorageCreateBucketOperator                                      | gcs.GCSCreateBucketOperator                                                    |
| gcs_to_bq.GoogleCloudStorageToBigQueryOperator                                           | gcs_to_bigquery.GCSToBigQueryOperator                                          |
| gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator                                | gcs_to_gcs.GCSToGCSOperator                                                    |
| mlengine_operator.MLEngineBatchPredictionOperator                                        | mlengine.MLEngineStartBatchPredictionJobOperator                               |
| mlengine_operator.MLEngineModelOperator                                                  | mlengine.MLEngineManageModelOperator                                           |
| mlengine_operator.MLEngineTrainingOperator                                               | mlengine.MLEngineStartTrainingJobOperator                                      |
| mlengine_operator.MLEngineVersionOperator                                                | mlengine.MLEngineManageVersionOperator                                         |
| mssql_to_gcs.MsSqlToGoogleCloudStorageOperator                                           | mssql_to_gcs.MSSQLToGCSOperator                                                |
| mysql_to_gcs.MySqlToGoogleCloudStorageOperator                                           | mysql_to_gcs.MySQLToGCSOperator                                                |
| postgres_to_gcs_operator.PostgresToGoogleCloudStorageOperator                            | postgres_to_gcs.PostgresToGCSOperator                                          |
| pubsub_operator.PubSubPublishOperator                                                    | pubsub.PubSubPublishMessageOperator                                            |
| pubsub_operator.PubSubSubscriptionCreateOperator                                         | pubsub.PubSubCreateSubscriptionOperator                                        |
| pubsub_operator.PubSubSubscriptionDeleteOperator                                         | pubsub.PubSubDeleteSubscriptionOperator                                        |
| pubsub_operator.PubSubTopicCreateOperator                                                | pubsub.PubSubCreateTopicOperator                                               |
| pubsub_operator.PubSubTopicDeleteOperator                                                | pubsub.PubSubDeleteTopicOperator                                               |
| s3_to_gcs_operator.S3ToGCSOperator                                                       | s3_to_gcs.S3ToGCSOperator                                                      |
| s3_to_gcs_transfer_operator.CloudDataTransferServiceS3ToGCSOperator                      | cloud_storage_transfer_service.CloudDataTransferServiceS3ToGCSOperator         |
| sql_to_gcs.BaseSQLToGoogleCloudStorageOperator                                           | sql_to_gcs.BaseSQLToGCSOperator                                                |

##### Google Suite operators

| Airflow 1.10  (`airflow.contrib.operators` package)                                      | Airflow 2.0 (`airflow.providers.google.suite.operators` package)               |
|------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| gcs_to_gdrive_operator.GCSToGoogleDriveOperator                                          | gcs_to_gdrive.GCSToGoogleDriveOperator                                         |
