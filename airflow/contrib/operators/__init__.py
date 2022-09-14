#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""This package is deprecated. Please use `airflow.operators` or `airflow.providers.*.operators`."""
from __future__ import annotations

import warnings

from airflow.exceptions import RemovedInAirflow3Warning
from airflow.utils.deprecation_tools import add_deprecated_classes

warnings.warn(
    "This package is deprecated. Please use `airflow.operators` or `airflow.providers.*.operators`.",
    RemovedInAirflow3Warning,
    stacklevel=2,
)

__deprecated_classes = {
    'adls_list_operator': {
        'ADLSListOperator': 'airflow.providers.microsoft.azure.operators.adls.ADLSListOperator',
        'AzureDataLakeStorageListOperator': (
            'airflow.providers.microsoft.azure.operators.adls.ADLSListOperator'
        ),
    },
    'adls_to_gcs': {
        'ADLSToGCSOperator': 'airflow.providers.google.cloud.transfers.adls_to_gcs.ADLSToGCSOperator',
        'AdlsToGoogleCloudStorageOperator': (
            'airflow.providers.google.cloud.transfers.adls_to_gcs.ADLSToGCSOperator'
        ),
    },
    'aws_athena_operator': {
        'AWSAthenaOperator': 'airflow.providers.amazon.aws.operators.athena.AthenaOperator',
    },
    'aws_sqs_publish_operator': {
        'SqsPublishOperator': 'airflow.providers.amazon.aws.operators.sqs.SqsPublishOperator',
        'SQSPublishOperator': 'airflow.providers.amazon.aws.operators.sqs.SqsPublishOperator',
    },
    'awsbatch_operator': {
        'BatchProtocol': 'airflow.providers.amazon.aws.hooks.batch_client.BatchProtocol',
        'BatchOperator': 'airflow.providers.amazon.aws.operators.batch.BatchOperator',
        'AWSBatchOperator': 'airflow.providers.amazon.aws.operators.batch.BatchOperator',
    },
    'azure_container_instances_operator': {
        'AzureContainerInstancesOperator': (
            'airflow.providers.microsoft.azure.operators.container_instances.AzureContainerInstancesOperator'
        ),
    },
    'azure_cosmos_operator': {
        'AzureCosmosInsertDocumentOperator': (
            'airflow.providers.microsoft.azure.operators.cosmos.AzureCosmosInsertDocumentOperator'
        ),
    },
    'bigquery_check_operator': {
        'BigQueryCheckOperator': 'airflow.providers.google.cloud.operators.bigquery.BigQueryCheckOperator',
        'BigQueryIntervalCheckOperator': (
            'airflow.providers.google.cloud.operators.bigquery.BigQueryIntervalCheckOperator'
        ),
        'BigQueryValueCheckOperator': (
            'airflow.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperator'
        ),
    },
    'bigquery_get_data': {
        'BigQueryGetDataOperator': (
            'airflow.providers.google.cloud.operators.bigquery.BigQueryGetDataOperator'
        ),
    },
    'bigquery_operator': {
        'BigQueryCreateEmptyDatasetOperator': (
            'airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyDatasetOperator'
        ),
        'BigQueryCreateEmptyTableOperator': (
            'airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyTableOperator'
        ),
        'BigQueryCreateExternalTableOperator': (
            'airflow.providers.google.cloud.operators.bigquery.BigQueryCreateExternalTableOperator'
        ),
        'BigQueryDeleteDatasetOperator': (
            'airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteDatasetOperator'
        ),
        'BigQueryExecuteQueryOperator': (
            'airflow.providers.google.cloud.operators.bigquery.BigQueryExecuteQueryOperator'
        ),
        'BigQueryGetDatasetOperator': (
            'airflow.providers.google.cloud.operators.bigquery.BigQueryGetDatasetOperator'
        ),
        'BigQueryGetDatasetTablesOperator': (
            'airflow.providers.google.cloud.operators.bigquery.BigQueryGetDatasetTablesOperator'
        ),
        'BigQueryPatchDatasetOperator': (
            'airflow.providers.google.cloud.operators.bigquery.BigQueryPatchDatasetOperator'
        ),
        'BigQueryUpdateDatasetOperator': (
            'airflow.providers.google.cloud.operators.bigquery.BigQueryUpdateDatasetOperator'
        ),
        'BigQueryUpsertTableOperator': (
            'airflow.providers.google.cloud.operators.bigquery.BigQueryUpsertTableOperator'
        ),
        'BigQueryOperator': 'airflow.providers.google.cloud.operators.bigquery.BigQueryExecuteQueryOperator',
    },
    'bigquery_table_delete_operator': {
        'BigQueryDeleteTableOperator': (
            'airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteTableOperator'
        ),
        'BigQueryTableDeleteOperator': (
            'airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteTableOperator'
        ),
    },
    'bigquery_to_bigquery': {
        'BigQueryToBigQueryOperator': (
            'airflow.providers.google.cloud.transfers.bigquery_to_bigquery.BigQueryToBigQueryOperator'
        ),
    },
    'bigquery_to_gcs': {
        'BigQueryToGCSOperator': (
            'airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryToGCSOperator'
        ),
        'BigQueryToCloudStorageOperator': (
            'airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryToGCSOperator'
        ),
    },
    'bigquery_to_mysql_operator': {
        'BigQueryToMySqlOperator': (
            'airflow.providers.google.cloud.transfers.bigquery_to_mysql.BigQueryToMySqlOperator'
        ),
    },
    'cassandra_to_gcs': {
        'CassandraToGCSOperator': (
            'airflow.providers.google.cloud.transfers.cassandra_to_gcs.CassandraToGCSOperator'
        ),
        'CassandraToGoogleCloudStorageOperator': (
            'airflow.providers.google.cloud.transfers.cassandra_to_gcs.CassandraToGCSOperator'
        ),
    },
    'databricks_operator': {
        'DatabricksRunNowOperator': (
            'airflow.providers.databricks.operators.databricks.DatabricksRunNowOperator'
        ),
        'DatabricksSubmitRunOperator': (
            'airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator'
        ),
    },
    'dataflow_operator': {
        'DataflowCreateJavaJobOperator': (
            'airflow.providers.google.cloud.operators.dataflow.DataflowCreateJavaJobOperator'
        ),
        'DataflowCreatePythonJobOperator': (
            'airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator'
        ),
        'DataflowTemplatedJobStartOperator': (
            'airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator'
        ),
        'DataFlowJavaOperator': (
            'airflow.providers.google.cloud.operators.dataflow.DataflowCreateJavaJobOperator'
        ),
        'DataFlowPythonOperator': (
            'airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator'
        ),
        'DataflowTemplateOperator': (
            'airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator'
        ),
    },
    'dataproc_operator': {
        'DataprocCreateClusterOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator'
        ),
        'DataprocDeleteClusterOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperator'
        ),
        'DataprocInstantiateInlineWorkflowTemplateOperator':
            'airflow.providers.google.cloud.operators.dataproc.'
            'DataprocInstantiateInlineWorkflowTemplateOperator',
        'DataprocInstantiateWorkflowTemplateOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocInstantiateWorkflowTemplateOperator'
        ),
        'DataprocJobBaseOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocJobBaseOperator'
        ),
        'DataprocScaleClusterOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocScaleClusterOperator'
        ),
        'DataprocSubmitHadoopJobOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHadoopJobOperator'
        ),
        'DataprocSubmitHiveJobOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHiveJobOperator'
        ),
        'DataprocSubmitPigJobOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPigJobOperator'
        ),
        'DataprocSubmitPySparkJobOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPySparkJobOperator'
        ),
        'DataprocSubmitSparkJobOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkJobOperator'
        ),
        'DataprocSubmitSparkSqlJobOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkSqlJobOperator'
        ),
        'DataprocClusterCreateOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator'
        ),
        'DataprocClusterDeleteOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperator'
        ),
        'DataprocClusterScaleOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocScaleClusterOperator'
        ),
        'DataProcHadoopOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHadoopJobOperator'
        ),
        'DataProcHiveOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHiveJobOperator'
        ),
        'DataProcJobBaseOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocJobBaseOperator'
        ),
        'DataProcPigOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPigJobOperator'
        ),
        'DataProcPySparkOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPySparkJobOperator'
        ),
        'DataProcSparkOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkJobOperator'
        ),
        'DataProcSparkSqlOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkSqlJobOperator'
        ),
        'DataprocWorkflowTemplateInstantiateInlineOperator':
            'airflow.providers.google.cloud.operators.dataproc.'
            'DataprocInstantiateInlineWorkflowTemplateOperator',
        'DataprocWorkflowTemplateInstantiateOperator': (
            'airflow.providers.google.cloud.operators.dataproc.DataprocInstantiateWorkflowTemplateOperator'
        ),
    },
    'datastore_export_operator': {
        'CloudDatastoreExportEntitiesOperator': (
            'airflow.providers.google.cloud.operators.datastore.CloudDatastoreExportEntitiesOperator'
        ),
        'DatastoreExportOperator': (
            'airflow.providers.google.cloud.operators.datastore.CloudDatastoreExportEntitiesOperator'
        ),
    },
    'datastore_import_operator': {
        'CloudDatastoreImportEntitiesOperator': (
            'airflow.providers.google.cloud.operators.datastore.CloudDatastoreImportEntitiesOperator'
        ),
        'DatastoreImportOperator': (
            'airflow.providers.google.cloud.operators.datastore.CloudDatastoreImportEntitiesOperator'
        ),
    },
    'dingding_operator': {
        'DingdingOperator': 'airflow.providers.dingding.operators.dingding.DingdingOperator',
    },
    'discord_webhook_operator': {
        'DiscordWebhookOperator': (
            'airflow.providers.discord.operators.discord_webhook.DiscordWebhookOperator'
        ),
    },
    'docker_swarm_operator': {
        'DockerSwarmOperator': 'airflow.providers.docker.operators.docker_swarm.DockerSwarmOperator',
    },
    'druid_operator': {
        'DruidOperator': 'airflow.providers.apache.druid.operators.druid.DruidOperator',
    },
    'dynamodb_to_s3': {
        'DynamoDBToS3Operator': 'airflow.providers.amazon.aws.transfers.dynamodb_to_s3.DynamoDBToS3Operator',
    },
    'ecs_operator': {
        'EcsProtocol': 'airflow.providers.amazon.aws.hooks.ecs.EcsProtocol',
        'EcsRunTaskOperator': 'airflow.providers.amazon.aws.operators.ecs.EcsRunTaskOperator',
        'EcsOperator': 'airflow.providers.amazon.aws.operators.ecs.EcsRunTaskOperator',
    },
    'file_to_gcs': {
        'LocalFilesystemToGCSOperator': (
            'airflow.providers.google.cloud.transfers.local_to_gcs.LocalFilesystemToGCSOperator'
        ),
        'FileToGoogleCloudStorageOperator': (
            'airflow.providers.google.cloud.transfers.local_to_gcs.LocalFilesystemToGCSOperator'
        ),
    },
    'file_to_wasb': {
        'LocalFilesystemToWasbOperator': (
            'airflow.providers.microsoft.azure.transfers.local_to_wasb.LocalFilesystemToWasbOperator'
        ),
        'FileToWasbOperator': (
            'airflow.providers.microsoft.azure.transfers.local_to_wasb.LocalFilesystemToWasbOperator'
        ),
    },
    'gcp_bigtable_operator': {
        'BigtableCreateInstanceOperator': (
            'airflow.providers.google.cloud.operators.bigtable.BigtableCreateInstanceOperator'
        ),
        'BigtableCreateTableOperator': (
            'airflow.providers.google.cloud.operators.bigtable.BigtableCreateTableOperator'
        ),
        'BigtableDeleteInstanceOperator': (
            'airflow.providers.google.cloud.operators.bigtable.BigtableDeleteInstanceOperator'
        ),
        'BigtableDeleteTableOperator': (
            'airflow.providers.google.cloud.operators.bigtable.BigtableDeleteTableOperator'
        ),
        'BigtableUpdateClusterOperator': (
            'airflow.providers.google.cloud.operators.bigtable.BigtableUpdateClusterOperator'
        ),
        'BigtableTableReplicationCompletedSensor': (
            'airflow.providers.google.cloud.sensors.bigtable.BigtableTableReplicationCompletedSensor'
        ),
        'BigtableClusterUpdateOperator': (
            'airflow.providers.google.cloud.operators.bigtable.BigtableUpdateClusterOperator'
        ),
        'BigtableInstanceCreateOperator': (
            'airflow.providers.google.cloud.operators.bigtable.BigtableCreateInstanceOperator'
        ),
        'BigtableInstanceDeleteOperator': (
            'airflow.providers.google.cloud.operators.bigtable.BigtableDeleteInstanceOperator'
        ),
        'BigtableTableCreateOperator': (
            'airflow.providers.google.cloud.operators.bigtable.BigtableCreateTableOperator'
        ),
        'BigtableTableDeleteOperator': (
            'airflow.providers.google.cloud.operators.bigtable.BigtableDeleteTableOperator'
        ),
        'BigtableTableWaitForReplicationSensor': (
            'airflow.providers.google.cloud.sensors.bigtable.BigtableTableReplicationCompletedSensor'
        ),
    },
    'gcp_cloud_build_operator': {
        'CloudBuildCreateBuildOperator': (
            'airflow.providers.google.cloud.operators.cloud_build.CloudBuildCreateBuildOperator'
        ),
    },
    'gcp_compute_operator': {
        'ComputeEngineBaseOperator': (
            'airflow.providers.google.cloud.operators.compute.ComputeEngineBaseOperator'
        ),
        'ComputeEngineCopyInstanceTemplateOperator': (
            'airflow.providers.google.cloud.operators.compute.ComputeEngineCopyInstanceTemplateOperator'
        ),
        'ComputeEngineInstanceGroupUpdateManagerTemplateOperator':
            'airflow.providers.google.cloud.operators.compute.'
            'ComputeEngineInstanceGroupUpdateManagerTemplateOperator',
        'ComputeEngineSetMachineTypeOperator': (
            'airflow.providers.google.cloud.operators.compute.ComputeEngineSetMachineTypeOperator'
        ),
        'ComputeEngineStartInstanceOperator': (
            'airflow.providers.google.cloud.operators.compute.ComputeEngineStartInstanceOperator'
        ),
        'ComputeEngineStopInstanceOperator': (
            'airflow.providers.google.cloud.operators.compute.ComputeEngineStopInstanceOperator'
        ),
        'GceBaseOperator': 'airflow.providers.google.cloud.operators.compute.ComputeEngineBaseOperator',
        'GceInstanceGroupManagerUpdateTemplateOperator':
            'airflow.providers.google.cloud.operators.compute.'
            'ComputeEngineInstanceGroupUpdateManagerTemplateOperator',
        'GceInstanceStartOperator': (
            'airflow.providers.google.cloud.operators.compute.ComputeEngineStartInstanceOperator'
        ),
        'GceInstanceStopOperator': (
            'airflow.providers.google.cloud.operators.compute.ComputeEngineStopInstanceOperator'
        ),
        'GceInstanceTemplateCopyOperator': (
            'airflow.providers.google.cloud.operators.compute.ComputeEngineCopyInstanceTemplateOperator'
        ),
        'GceSetMachineTypeOperator': (
            'airflow.providers.google.cloud.operators.compute.ComputeEngineSetMachineTypeOperator'
        ),
    },
    'gcp_container_operator': {
        'GKECreateClusterOperator': (
            'airflow.providers.google.cloud.operators.kubernetes_engine.GKECreateClusterOperator'
        ),
        'GKEDeleteClusterOperator': (
            'airflow.providers.google.cloud.operators.kubernetes_engine.GKEDeleteClusterOperator'
        ),
        'GKEStartPodOperator': (
            'airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator'
        ),
        'GKEClusterCreateOperator': (
            'airflow.providers.google.cloud.operators.kubernetes_engine.GKECreateClusterOperator'
        ),
        'GKEClusterDeleteOperator': (
            'airflow.providers.google.cloud.operators.kubernetes_engine.GKEDeleteClusterOperator'
        ),
        'GKEPodOperator': 'airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator',
    },
    'gcp_dlp_operator': {
        'CloudDLPCancelDLPJobOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPCancelDLPJobOperator'
        ),
        'CloudDLPCreateDeidentifyTemplateOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPCreateDeidentifyTemplateOperator'
        ),
        'CloudDLPCreateDLPJobOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPCreateDLPJobOperator'
        ),
        'CloudDLPCreateInspectTemplateOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPCreateInspectTemplateOperator'
        ),
        'CloudDLPCreateJobTriggerOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPCreateJobTriggerOperator'
        ),
        'CloudDLPCreateStoredInfoTypeOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPCreateStoredInfoTypeOperator'
        ),
        'CloudDLPDeidentifyContentOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPDeidentifyContentOperator'
        ),
        'CloudDLPDeleteDeidentifyTemplateOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDeidentifyTemplateOperator'
        ),
        'CloudDLPDeleteDLPJobOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDLPJobOperator'
        ),
        'CloudDLPDeleteInspectTemplateOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteInspectTemplateOperator'
        ),
        'CloudDLPDeleteJobTriggerOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteJobTriggerOperator'
        ),
        'CloudDLPDeleteStoredInfoTypeOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteStoredInfoTypeOperator'
        ),
        'CloudDLPGetDeidentifyTemplateOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPGetDeidentifyTemplateOperator'
        ),
        'CloudDLPGetDLPJobOperator': 'airflow.providers.google.cloud.operators.dlp.CloudDLPGetDLPJobOperator',
        'CloudDLPGetDLPJobTriggerOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPGetDLPJobTriggerOperator'
        ),
        'CloudDLPGetInspectTemplateOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPGetInspectTemplateOperator'
        ),
        'CloudDLPGetStoredInfoTypeOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPGetStoredInfoTypeOperator'
        ),
        'CloudDLPInspectContentOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPInspectContentOperator'
        ),
        'CloudDLPListDeidentifyTemplatesOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPListDeidentifyTemplatesOperator'
        ),
        'CloudDLPListDLPJobsOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPListDLPJobsOperator'
        ),
        'CloudDLPListInfoTypesOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPListInfoTypesOperator'
        ),
        'CloudDLPListInspectTemplatesOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPListInspectTemplatesOperator'
        ),
        'CloudDLPListJobTriggersOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPListJobTriggersOperator'
        ),
        'CloudDLPListStoredInfoTypesOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPListStoredInfoTypesOperator'
        ),
        'CloudDLPRedactImageOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPRedactImageOperator'
        ),
        'CloudDLPReidentifyContentOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPReidentifyContentOperator'
        ),
        'CloudDLPUpdateDeidentifyTemplateOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateDeidentifyTemplateOperator'
        ),
        'CloudDLPUpdateInspectTemplateOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateInspectTemplateOperator'
        ),
        'CloudDLPUpdateJobTriggerOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateJobTriggerOperator'
        ),
        'CloudDLPUpdateStoredInfoTypeOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateStoredInfoTypeOperator'
        ),
        'CloudDLPDeleteDlpJobOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDLPJobOperator'
        ),
        'CloudDLPGetDlpJobOperator': 'airflow.providers.google.cloud.operators.dlp.CloudDLPGetDLPJobOperator',
        'CloudDLPGetJobTripperOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPGetDLPJobTriggerOperator'
        ),
        'CloudDLPListDlpJobsOperator': (
            'airflow.providers.google.cloud.operators.dlp.CloudDLPListDLPJobsOperator'
        ),
    },
    'gcp_function_operator': {
        'CloudFunctionDeleteFunctionOperator': (
            'airflow.providers.google.cloud.operators.functions.CloudFunctionDeleteFunctionOperator'
        ),
        'CloudFunctionDeployFunctionOperator': (
            'airflow.providers.google.cloud.operators.functions.CloudFunctionDeployFunctionOperator'
        ),
        'GcfFunctionDeleteOperator': (
            'airflow.providers.google.cloud.operators.functions.CloudFunctionDeleteFunctionOperator'
        ),
        'GcfFunctionDeployOperator': (
            'airflow.providers.google.cloud.operators.functions.CloudFunctionDeployFunctionOperator'
        ),
    },
    'gcp_natural_language_operator': {
        'CloudNaturalLanguageAnalyzeEntitiesOperator':
            'airflow.providers.google.cloud.operators.natural_language.'
            'CloudNaturalLanguageAnalyzeEntitiesOperator',
        'CloudNaturalLanguageAnalyzeEntitySentimentOperator':
            'airflow.providers.google.cloud.operators.natural_language.'
            'CloudNaturalLanguageAnalyzeEntitySentimentOperator',
        'CloudNaturalLanguageAnalyzeSentimentOperator':
            'airflow.providers.google.cloud.operators.natural_language.'
            'CloudNaturalLanguageAnalyzeSentimentOperator',
        'CloudNaturalLanguageClassifyTextOperator':
            'airflow.providers.google.cloud.operators.natural_language.'
            'CloudNaturalLanguageClassifyTextOperator',
        'CloudLanguageAnalyzeEntitiesOperator':
            'airflow.providers.google.cloud.operators.natural_language.'
            'CloudNaturalLanguageAnalyzeEntitiesOperator',
        'CloudLanguageAnalyzeEntitySentimentOperator':
            'airflow.providers.google.cloud.operators.natural_language.'
            'CloudNaturalLanguageAnalyzeEntitySentimentOperator',
        'CloudLanguageAnalyzeSentimentOperator':
            'airflow.providers.google.cloud.operators.natural_language.'
            'CloudNaturalLanguageAnalyzeSentimentOperator',
        'CloudLanguageClassifyTextOperator':
            'airflow.providers.google.cloud.operators.natural_language.'
            'CloudNaturalLanguageClassifyTextOperator',
    },
    'gcp_spanner_operator': {
        'SpannerDeleteDatabaseInstanceOperator': (
            'airflow.providers.google.cloud.operators.spanner.SpannerDeleteDatabaseInstanceOperator'
        ),
        'SpannerDeleteInstanceOperator': (
            'airflow.providers.google.cloud.operators.spanner.SpannerDeleteInstanceOperator'
        ),
        'SpannerDeployDatabaseInstanceOperator': (
            'airflow.providers.google.cloud.operators.spanner.SpannerDeployDatabaseInstanceOperator'
        ),
        'SpannerDeployInstanceOperator': (
            'airflow.providers.google.cloud.operators.spanner.SpannerDeployInstanceOperator'
        ),
        'SpannerQueryDatabaseInstanceOperator': (
            'airflow.providers.google.cloud.operators.spanner.SpannerQueryDatabaseInstanceOperator'
        ),
        'SpannerUpdateDatabaseInstanceOperator': (
            'airflow.providers.google.cloud.operators.spanner.SpannerUpdateDatabaseInstanceOperator'
        ),
        'CloudSpannerInstanceDatabaseDeleteOperator': (
            'airflow.providers.google.cloud.operators.spanner.SpannerDeleteDatabaseInstanceOperator'
        ),
        'CloudSpannerInstanceDatabaseDeployOperator': (
            'airflow.providers.google.cloud.operators.spanner.SpannerDeployDatabaseInstanceOperator'
        ),
        'CloudSpannerInstanceDatabaseQueryOperator': (
            'airflow.providers.google.cloud.operators.spanner.SpannerQueryDatabaseInstanceOperator'
        ),
        'CloudSpannerInstanceDatabaseUpdateOperator': (
            'airflow.providers.google.cloud.operators.spanner.SpannerUpdateDatabaseInstanceOperator'
        ),
        'CloudSpannerInstanceDeleteOperator': (
            'airflow.providers.google.cloud.operators.spanner.SpannerDeleteInstanceOperator'
        ),
        'CloudSpannerInstanceDeployOperator': (
            'airflow.providers.google.cloud.operators.spanner.SpannerDeployInstanceOperator'
        ),
    },
    'gcp_speech_to_text_operator': {
        'CloudSpeechToTextRecognizeSpeechOperator': (
            'airflow.providers.google.cloud.operators.speech_to_text.CloudSpeechToTextRecognizeSpeechOperator'
        ),
        'GcpSpeechToTextRecognizeSpeechOperator': (
            'airflow.providers.google.cloud.operators.speech_to_text.CloudSpeechToTextRecognizeSpeechOperator'
        ),
    },
    'gcp_sql_operator': {
        'CloudSQLBaseOperator': 'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLBaseOperator',
        'CloudSQLCreateInstanceDatabaseOperator': (
            'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLCreateInstanceDatabaseOperator'
        ),
        'CloudSQLCreateInstanceOperator': (
            'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLCreateInstanceOperator'
        ),
        'CloudSQLDeleteInstanceDatabaseOperator': (
            'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLDeleteInstanceDatabaseOperator'
        ),
        'CloudSQLDeleteInstanceOperator': (
            'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLDeleteInstanceOperator'
        ),
        'CloudSQLExecuteQueryOperator': (
            'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLExecuteQueryOperator'
        ),
        'CloudSQLExportInstanceOperator': (
            'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLExportInstanceOperator'
        ),
        'CloudSQLImportInstanceOperator': (
            'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLImportInstanceOperator'
        ),
        'CloudSQLInstancePatchOperator': (
            'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLInstancePatchOperator'
        ),
        'CloudSQLPatchInstanceDatabaseOperator': (
            'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLPatchInstanceDatabaseOperator'
        ),
        'CloudSqlBaseOperator': 'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLBaseOperator',
        'CloudSqlInstanceCreateOperator': (
            'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLCreateInstanceOperator'
        ),
        'CloudSqlInstanceDatabaseCreateOperator': (
            'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLCreateInstanceDatabaseOperator'
        ),
        'CloudSqlInstanceDatabaseDeleteOperator': (
            'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLDeleteInstanceDatabaseOperator'
        ),
        'CloudSqlInstanceDatabasePatchOperator': (
            'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLPatchInstanceDatabaseOperator'
        ),
        'CloudSqlInstanceDeleteOperator': (
            'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLDeleteInstanceOperator'
        ),
        'CloudSqlInstanceExportOperator': (
            'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLExportInstanceOperator'
        ),
        'CloudSqlInstanceImportOperator': (
            'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLImportInstanceOperator'
        ),
        'CloudSqlInstancePatchOperator': (
            'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLInstancePatchOperator'
        ),
        'CloudSqlQueryOperator': (
            'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLExecuteQueryOperator'
        ),
    },
    'gcp_tasks_operator': {
        'CloudTasksQueueCreateOperator': (
            'airflow.providers.google.cloud.operators.tasks.CloudTasksQueueCreateOperator'
        ),
        'CloudTasksQueueDeleteOperator': (
            'airflow.providers.google.cloud.operators.tasks.CloudTasksQueueDeleteOperator'
        ),
        'CloudTasksQueueGetOperator': (
            'airflow.providers.google.cloud.operators.tasks.CloudTasksQueueGetOperator'
        ),
        'CloudTasksQueuePauseOperator': (
            'airflow.providers.google.cloud.operators.tasks.CloudTasksQueuePauseOperator'
        ),
        'CloudTasksQueuePurgeOperator': (
            'airflow.providers.google.cloud.operators.tasks.CloudTasksQueuePurgeOperator'
        ),
        'CloudTasksQueueResumeOperator': (
            'airflow.providers.google.cloud.operators.tasks.CloudTasksQueueResumeOperator'
        ),
        'CloudTasksQueuesListOperator': (
            'airflow.providers.google.cloud.operators.tasks.CloudTasksQueuesListOperator'
        ),
        'CloudTasksQueueUpdateOperator': (
            'airflow.providers.google.cloud.operators.tasks.CloudTasksQueueUpdateOperator'
        ),
        'CloudTasksTaskCreateOperator': (
            'airflow.providers.google.cloud.operators.tasks.CloudTasksTaskCreateOperator'
        ),
        'CloudTasksTaskDeleteOperator': (
            'airflow.providers.google.cloud.operators.tasks.CloudTasksTaskDeleteOperator'
        ),
        'CloudTasksTaskGetOperator': (
            'airflow.providers.google.cloud.operators.tasks.CloudTasksTaskGetOperator'
        ),
        'CloudTasksTaskRunOperator': (
            'airflow.providers.google.cloud.operators.tasks.CloudTasksTaskRunOperator'
        ),
        'CloudTasksTasksListOperator': (
            'airflow.providers.google.cloud.operators.tasks.CloudTasksTasksListOperator'
        ),
    },
    'gcp_text_to_speech_operator': {
        'CloudTextToSpeechSynthesizeOperator': (
            'airflow.providers.google.cloud.operators.text_to_speech.CloudTextToSpeechSynthesizeOperator'
        ),
        'GcpTextToSpeechSynthesizeOperator': (
            'airflow.providers.google.cloud.operators.text_to_speech.CloudTextToSpeechSynthesizeOperator'
        ),
    },
    'gcp_transfer_operator': {
        'CloudDataTransferServiceCancelOperationOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceCancelOperationOperator',
        'CloudDataTransferServiceCreateJobOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceCreateJobOperator',
        'CloudDataTransferServiceDeleteJobOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceDeleteJobOperator',
        'CloudDataTransferServiceGCSToGCSOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceGCSToGCSOperator',
        'CloudDataTransferServiceGetOperationOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceGetOperationOperator',
        'CloudDataTransferServiceListOperationsOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceListOperationsOperator',
        'CloudDataTransferServicePauseOperationOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServicePauseOperationOperator',
        'CloudDataTransferServiceResumeOperationOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceResumeOperationOperator',
        'CloudDataTransferServiceS3ToGCSOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceS3ToGCSOperator',
        'CloudDataTransferServiceUpdateJobOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceUpdateJobOperator',
        'GcpTransferServiceJobCreateOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceCreateJobOperator',
        'GcpTransferServiceJobDeleteOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceDeleteJobOperator',
        'GcpTransferServiceJobUpdateOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceUpdateJobOperator',
        'GcpTransferServiceOperationCancelOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceCancelOperationOperator',
        'GcpTransferServiceOperationGetOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceGetOperationOperator',
        'GcpTransferServiceOperationPauseOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServicePauseOperationOperator',
        'GcpTransferServiceOperationResumeOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceResumeOperationOperator',
        'GcpTransferServiceOperationsListOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceListOperationsOperator',
        'GoogleCloudStorageToGoogleCloudStorageTransferOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceGCSToGCSOperator',
        'S3ToGoogleCloudStorageTransferOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceS3ToGCSOperator',
    },
    'gcp_translate_operator': {
        'CloudTranslateTextOperator': (
            'airflow.providers.google.cloud.operators.translate.CloudTranslateTextOperator'
        ),
    },
    'gcp_translate_speech_operator': {
        'CloudTranslateSpeechOperator': (
            'airflow.providers.google.cloud.operators.translate_speech.CloudTranslateSpeechOperator'
        ),
        'GcpTranslateSpeechOperator': (
            'airflow.providers.google.cloud.operators.translate_speech.CloudTranslateSpeechOperator'
        ),
    },
    'gcp_video_intelligence_operator': {
        'CloudVideoIntelligenceDetectVideoExplicitContentOperator':
            'airflow.providers.google.cloud.operators.video_intelligence.'
            'CloudVideoIntelligenceDetectVideoExplicitContentOperator',
        'CloudVideoIntelligenceDetectVideoLabelsOperator':
            'airflow.providers.google.cloud.operators.video_intelligence.'
            'CloudVideoIntelligenceDetectVideoLabelsOperator',
        'CloudVideoIntelligenceDetectVideoShotsOperator':
            'airflow.providers.google.cloud.operators.video_intelligence.'
            'CloudVideoIntelligenceDetectVideoShotsOperator',
    },
    'gcp_vision_operator': {
        'CloudVisionAddProductToProductSetOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionAddProductToProductSetOperator'
        ),
        'CloudVisionCreateProductOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductOperator'
        ),
        'CloudVisionCreateProductSetOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductSetOperator'
        ),
        'CloudVisionCreateReferenceImageOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionCreateReferenceImageOperator'
        ),
        'CloudVisionDeleteProductOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductOperator'
        ),
        'CloudVisionDeleteProductSetOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductSetOperator'
        ),
        'CloudVisionDetectImageLabelsOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionDetectImageLabelsOperator'
        ),
        'CloudVisionDetectImageSafeSearchOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionDetectImageSafeSearchOperator'
        ),
        'CloudVisionDetectTextOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionDetectTextOperator'
        ),
        'CloudVisionGetProductOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionGetProductOperator'
        ),
        'CloudVisionGetProductSetOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionGetProductSetOperator'
        ),
        'CloudVisionImageAnnotateOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionImageAnnotateOperator'
        ),
        'CloudVisionRemoveProductFromProductSetOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionRemoveProductFromProductSetOperator'
        ),
        'CloudVisionTextDetectOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionTextDetectOperator'
        ),
        'CloudVisionUpdateProductOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductOperator'
        ),
        'CloudVisionUpdateProductSetOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductSetOperator'
        ),
        'CloudVisionAnnotateImageOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionImageAnnotateOperator'
        ),
        'CloudVisionDetectDocumentTextOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionTextDetectOperator'
        ),
        'CloudVisionProductCreateOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductOperator'
        ),
        'CloudVisionProductDeleteOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductOperator'
        ),
        'CloudVisionProductGetOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionGetProductOperator'
        ),
        'CloudVisionProductSetCreateOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductSetOperator'
        ),
        'CloudVisionProductSetDeleteOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductSetOperator'
        ),
        'CloudVisionProductSetGetOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionGetProductSetOperator'
        ),
        'CloudVisionProductSetUpdateOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductSetOperator'
        ),
        'CloudVisionProductUpdateOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductOperator'
        ),
        'CloudVisionReferenceImageCreateOperator': (
            'airflow.providers.google.cloud.operators.vision.CloudVisionCreateReferenceImageOperator'
        ),
    },
    'gcs_acl_operator': {
        'GCSBucketCreateAclEntryOperator': (
            'airflow.providers.google.cloud.operators.gcs.GCSBucketCreateAclEntryOperator'
        ),
        'GCSObjectCreateAclEntryOperator': (
            'airflow.providers.google.cloud.operators.gcs.GCSObjectCreateAclEntryOperator'
        ),
        'GoogleCloudStorageBucketCreateAclEntryOperator': (
            'airflow.providers.google.cloud.operators.gcs.GCSBucketCreateAclEntryOperator'
        ),
        'GoogleCloudStorageObjectCreateAclEntryOperator': (
            'airflow.providers.google.cloud.operators.gcs.GCSObjectCreateAclEntryOperator'
        ),
    },
    'gcs_delete_operator': {
        'GCSDeleteObjectsOperator': 'airflow.providers.google.cloud.operators.gcs.GCSDeleteObjectsOperator',
        'GoogleCloudStorageDeleteOperator': (
            'airflow.providers.google.cloud.operators.gcs.GCSDeleteObjectsOperator'
        ),
    },
    'gcs_download_operator': {
        'GCSToLocalFilesystemOperator': (
            'airflow.providers.google.cloud.transfers.gcs_to_local.GCSToLocalFilesystemOperator'
        ),
        'GoogleCloudStorageDownloadOperator': (
            'airflow.providers.google.cloud.transfers.gcs_to_local.GCSToLocalFilesystemOperator'
        ),
    },
    'gcs_list_operator': {
        'GCSListObjectsOperator': 'airflow.providers.google.cloud.operators.gcs.GCSListObjectsOperator',
        'GoogleCloudStorageListOperator': (
            'airflow.providers.google.cloud.operators.gcs.GCSListObjectsOperator'
        ),
    },
    'gcs_operator': {
        'GCSCreateBucketOperator': 'airflow.providers.google.cloud.operators.gcs.GCSCreateBucketOperator',
        'GoogleCloudStorageCreateBucketOperator': (
            'airflow.providers.google.cloud.operators.gcs.GCSCreateBucketOperator'
        ),
    },
    'gcs_to_bq': {
        'GCSToBigQueryOperator': (
            'airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSToBigQueryOperator'
        ),
        'GoogleCloudStorageToBigQueryOperator': (
            'airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSToBigQueryOperator'
        ),
    },
    'gcs_to_gcs': {
        'GCSToGCSOperator': 'airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSToGCSOperator',
        'GoogleCloudStorageToGoogleCloudStorageOperator': (
            'airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSToGCSOperator'
        ),
    },
    'gcs_to_gdrive_operator': {
        'GCSToGoogleDriveOperator': (
            'airflow.providers.google.suite.transfers.gcs_to_gdrive.GCSToGoogleDriveOperator'
        ),
    },
    'gcs_to_s3': {
        'GCSToS3Operator': 'airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSToS3Operator',
        'GoogleCloudStorageToS3Operator': 'airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSToS3Operator',
    },
    'grpc_operator': {
        'GrpcOperator': 'airflow.providers.grpc.operators.grpc.GrpcOperator',
    },
    'hive_to_dynamodb': {
        'HiveToDynamoDBOperator': (
            'airflow.providers.amazon.aws.transfers.hive_to_dynamodb.HiveToDynamoDBOperator'
        ),
    },
    'imap_attachment_to_s3_operator': {
        'ImapAttachmentToS3Operator': (
            'airflow.providers.amazon.aws.transfers.imap_attachment_to_s3.ImapAttachmentToS3Operator'
        ),
    },
    'jenkins_job_trigger_operator': {
        'JenkinsJobTriggerOperator': (
            'airflow.providers.jenkins.operators.jenkins_job_trigger.JenkinsJobTriggerOperator'
        ),
    },
    'jira_operator': {
        'JiraOperator': 'airflow.providers.atlassian.jira.operators.jira.JiraOperator',
    },
    'kubernetes_pod_operator': {
        'KubernetesPodOperator': (
            'airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator'
        ),
    },
    'mlengine_operator': {
        'MLEngineManageModelOperator': (
            'airflow.providers.google.cloud.operators.mlengine.MLEngineManageModelOperator'
        ),
        'MLEngineManageVersionOperator': (
            'airflow.providers.google.cloud.operators.mlengine.MLEngineManageVersionOperator'
        ),
        'MLEngineStartBatchPredictionJobOperator': (
            'airflow.providers.google.cloud.operators.mlengine.MLEngineStartBatchPredictionJobOperator'
        ),
        'MLEngineStartTrainingJobOperator': (
            'airflow.providers.google.cloud.operators.mlengine.MLEngineStartTrainingJobOperator'
        ),
        'MLEngineBatchPredictionOperator': (
            'airflow.providers.google.cloud.operators.mlengine.MLEngineStartBatchPredictionJobOperator'
        ),
        'MLEngineModelOperator': (
            'airflow.providers.google.cloud.operators.mlengine.MLEngineManageModelOperator'
        ),
        'MLEngineTrainingOperator': (
            'airflow.providers.google.cloud.operators.mlengine.MLEngineStartTrainingJobOperator'
        ),
        'MLEngineVersionOperator': (
            'airflow.providers.google.cloud.operators.mlengine.MLEngineManageVersionOperator'
        ),
    },
    'mongo_to_s3': {
        'MongoToS3Operator': 'airflow.providers.amazon.aws.transfers.mongo_to_s3.MongoToS3Operator',
    },
    'mssql_to_gcs': {
        'MSSQLToGCSOperator': 'airflow.providers.google.cloud.transfers.mssql_to_gcs.MSSQLToGCSOperator',
        'MsSqlToGoogleCloudStorageOperator': (
            'airflow.providers.google.cloud.transfers.mssql_to_gcs.MSSQLToGCSOperator'
        ),
    },
    'mysql_to_gcs': {
        'MySQLToGCSOperator': 'airflow.providers.google.cloud.transfers.mysql_to_gcs.MySQLToGCSOperator',
        'MySqlToGoogleCloudStorageOperator': (
            'airflow.providers.google.cloud.transfers.mysql_to_gcs.MySQLToGCSOperator'
        ),
    },
    'opsgenie_alert_operator': {
        'OpsgenieCreateAlertOperator': (
            'airflow.providers.opsgenie.operators.opsgenie.OpsgenieCreateAlertOperator'
        ),
        'OpsgenieAlertOperator': 'airflow.providers.opsgenie.operators.opsgenie.OpsgenieCreateAlertOperator',
    },
    'oracle_to_azure_data_lake_transfer': {
        'OracleToAzureDataLakeOperator':
            'airflow.providers.microsoft.azure.transfers.'
            'oracle_to_azure_data_lake.OracleToAzureDataLakeOperator',
    },
    'oracle_to_oracle_transfer': {
        'OracleToOracleOperator': (
            'airflow.providers.oracle.transfers.oracle_to_oracle.OracleToOracleOperator'
        ),
        'OracleToOracleTransfer': (
            'airflow.providers.oracle.transfers.oracle_to_oracle.OracleToOracleOperator'
        ),
    },
    'postgres_to_gcs_operator': {
        'PostgresToGCSOperator': (
            'airflow.providers.google.cloud.transfers.postgres_to_gcs.PostgresToGCSOperator'
        ),
        'PostgresToGoogleCloudStorageOperator': (
            'airflow.providers.google.cloud.transfers.postgres_to_gcs.PostgresToGCSOperator'
        ),
    },
    'pubsub_operator': {
        'PubSubCreateSubscriptionOperator': (
            'airflow.providers.google.cloud.operators.pubsub.PubSubCreateSubscriptionOperator'
        ),
        'PubSubCreateTopicOperator': (
            'airflow.providers.google.cloud.operators.pubsub.PubSubCreateTopicOperator'
        ),
        'PubSubDeleteSubscriptionOperator': (
            'airflow.providers.google.cloud.operators.pubsub.PubSubDeleteSubscriptionOperator'
        ),
        'PubSubDeleteTopicOperator': (
            'airflow.providers.google.cloud.operators.pubsub.PubSubDeleteTopicOperator'
        ),
        'PubSubPublishMessageOperator': (
            'airflow.providers.google.cloud.operators.pubsub.PubSubPublishMessageOperator'
        ),
        'PubSubPublishOperator': (
            'airflow.providers.google.cloud.operators.pubsub.PubSubPublishMessageOperator'
        ),
        'PubSubSubscriptionCreateOperator': (
            'airflow.providers.google.cloud.operators.pubsub.PubSubCreateSubscriptionOperator'
        ),
        'PubSubSubscriptionDeleteOperator': (
            'airflow.providers.google.cloud.operators.pubsub.PubSubDeleteSubscriptionOperator'
        ),
        'PubSubTopicCreateOperator': (
            'airflow.providers.google.cloud.operators.pubsub.PubSubCreateTopicOperator'
        ),
        'PubSubTopicDeleteOperator': (
            'airflow.providers.google.cloud.operators.pubsub.PubSubDeleteTopicOperator'
        ),
    },
    'qubole_check_operator': {
        'QuboleCheckOperator': 'airflow.providers.qubole.operators.qubole_check.QuboleCheckOperator',
        'QuboleValueCheckOperator': (
            'airflow.providers.qubole.operators.qubole_check.QuboleValueCheckOperator'
        ),
    },
    'qubole_operator': {
        'QuboleOperator': 'airflow.providers.qubole.operators.qubole.QuboleOperator',
    },
    'redis_publish_operator': {
        'RedisPublishOperator': 'airflow.providers.redis.operators.redis_publish.RedisPublishOperator',
    },
    's3_to_gcs_operator': {
        'S3ToGCSOperator': 'airflow.providers.google.cloud.transfers.s3_to_gcs.S3ToGCSOperator',
    },
    's3_to_gcs_transfer_operator': {
        'CloudDataTransferServiceS3ToGCSOperator':
            'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
            'CloudDataTransferServiceS3ToGCSOperator',
    },
    's3_to_sftp_operator': {
        'S3ToSFTPOperator': 'airflow.providers.amazon.aws.transfers.s3_to_sftp.S3ToSFTPOperator',
    },
    'segment_track_event_operator': {
        'SegmentTrackEventOperator': (
            'airflow.providers.segment.operators.segment_track_event.SegmentTrackEventOperator'
        ),
    },
    'sftp_operator': {
        'SFTPOperator': 'airflow.providers.sftp.operators.sftp.SFTPOperator',
    },
    'sftp_to_s3_operator': {
        'SFTPToS3Operator': 'airflow.providers.amazon.aws.transfers.sftp_to_s3.SFTPToS3Operator',
    },
    'slack_webhook_operator': {
        'SlackWebhookOperator': 'airflow.providers.slack.operators.slack_webhook.SlackWebhookOperator',
    },
    'snowflake_operator': {
        'SnowflakeOperator': 'airflow.providers.snowflake.operators.snowflake.SnowflakeOperator',
    },
    'sns_publish_operator': {
        'SnsPublishOperator': 'airflow.providers.amazon.aws.operators.sns.SnsPublishOperator',
    },
    'spark_jdbc_operator': {
        'SparkJDBCOperator': 'airflow.providers.apache.spark.operators.spark_jdbc.SparkJDBCOperator',
        'SparkSubmitOperator': 'airflow.providers.apache.spark.operators.spark_jdbc.SparkSubmitOperator',
    },
    'spark_sql_operator': {
        'SparkSqlOperator': 'airflow.providers.apache.spark.operators.spark_sql.SparkSqlOperator',
    },
    'spark_submit_operator': {
        'SparkSubmitOperator': 'airflow.providers.apache.spark.operators.spark_submit.SparkSubmitOperator',
    },
    'sql_to_gcs': {
        'BaseSQLToGCSOperator': 'airflow.providers.google.cloud.transfers.sql_to_gcs.BaseSQLToGCSOperator',
        'BaseSQLToGoogleCloudStorageOperator': (
            'airflow.providers.google.cloud.transfers.sql_to_gcs.BaseSQLToGCSOperator'
        ),
    },
    'sqoop_operator': {
        'SqoopOperator': 'airflow.providers.apache.sqoop.operators.sqoop.SqoopOperator',
    },
    'ssh_operator': {
        'SSHOperator': 'airflow.providers.ssh.operators.ssh.SSHOperator',
    },
    'vertica_operator': {
        'VerticaOperator': 'airflow.providers.vertica.operators.vertica.VerticaOperator',
    },
    'vertica_to_hive': {
        'VerticaToHiveOperator': (
            'airflow.providers.apache.hive.transfers.vertica_to_hive.VerticaToHiveOperator'
        ),
        'VerticaToHiveTransfer': (
            'airflow.providers.apache.hive.transfers.vertica_to_hive.VerticaToHiveOperator'
        ),
    },
    'vertica_to_mysql': {
        'VerticaToMySqlOperator': 'airflow.providers.mysql.transfers.vertica_to_mysql.VerticaToMySqlOperator',
        'VerticaToMySqlTransfer': 'airflow.providers.mysql.transfers.vertica_to_mysql.VerticaToMySqlOperator',
    },
    'wasb_delete_blob_operator': {
        'WasbDeleteBlobOperator': (
            'airflow.providers.microsoft.azure.operators.wasb_delete_blob.WasbDeleteBlobOperator'
        ),
    },
    'winrm_operator': {
        'WinRMOperator': 'airflow.providers.microsoft.winrm.operators.winrm.WinRMOperator',
    },
}

add_deprecated_classes(__deprecated_classes, __name__)
