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
"""This package is deprecated. Please use `airflow.utils`."""

import warnings

from airflow.utils.deprecation_tools import add_deprecated_classes

warnings.warn("This module is deprecated. Please use `airflow.utils`.", DeprecationWarning, stacklevel=2)

__deprecated_classes = {
    'aws_athena_hook': {
        'AWSAthenaHook': 'airflow.providers.amazon.aws.hooks.athena.AthenaHook',
    },
    'aws_datasync_hook': {
        'AWSDataSyncHook': 'airflow.providers.amazon.aws.hooks.datasync.DataSyncHook',
    },
    'aws_dynamodb_hook': {
        'AwsDynamoDBHook': 'airflow.providers.amazon.aws.hooks.dynamodb.DynamoDBHook',
    },
    'aws_firehose_hook': {
        'FirehoseHook': 'airflow.providers.amazon.aws.hooks.kinesis.FirehoseHook',
    },
    'aws_glue_catalog_hook': {
        'AwsGlueCatalogHook': 'airflow.providers.amazon.aws.hooks.glue_catalog.GlueCatalogHook',
    },
    'aws_hook': {
        'AwsBaseHook': 'airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook',
        '_parse_s3_config': 'airflow.providers.amazon.aws.hooks.base_aws._parse_s3_config',
        'boto3': 'airflow.providers.amazon.aws.hooks.base_aws.boto3',
    },
    'aws_lambda_hook': {
        'AwsLambdaHook': 'airflow.providers.amazon.aws.hooks.lambda_function.LambdaHook',
    },
    'aws_logs_hook': {
        'AwsLogsHook': 'airflow.providers.amazon.aws.hooks.logs.AwsLogsHook',
    },
    'aws_sns_hook': {
        'AwsSnsHook': 'airflow.providers.amazon.aws.hooks.sns.SnsHook',
    },
    'aws_sqs_hook': {
        'SqsHook': 'airflow.providers.amazon.aws.hooks.sqs.SqsHook',
    },
    'azure_container_instance_hook': {
        'AzureContainerInstanceHook':
            'airflow.providers.microsoft.azure.hooks.container_instance.AzureContainerInstanceHook',
    },
    'azure_container_registry_hook': {
        'AzureContainerRegistryHook':
            'airflow.providers.microsoft.azure.hooks.container_registry.AzureContainerRegistryHook',
    },
    'azure_container_volume_hook': {
        'AzureContainerVolumeHook':
            'airflow.providers.microsoft.azure.hooks.container_volume.AzureContainerVolumeHook',
    },
    'azure_cosmos_hook': {
        'AzureCosmosDBHook': 'airflow.providers.microsoft.azure.hooks.cosmos.AzureCosmosDBHook',
    },
    'azure_data_lake_hook': {
        'AzureDataLakeHook': 'airflow.providers.microsoft.azure.hooks.data_lake.AzureDataLakeHook',
    },
    'azure_fileshare_hook': {
        'AzureFileShareHook': 'airflow.providers.microsoft.azure.hooks.fileshare.AzureFileShareHook',
    },
    'bigquery_hook': {
        'BigQueryBaseCursor': 'airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor',
        'BigQueryConnection': 'airflow.providers.google.cloud.hooks.bigquery.BigQueryConnection',
        'BigQueryCursor': 'airflow.providers.google.cloud.hooks.bigquery.BigQueryCursor',
        'BigQueryHook': 'airflow.providers.google.cloud.hooks.bigquery.BigQueryHook',
        'GbqConnector': 'airflow.providers.google.cloud.hooks.bigquery.GbqConnector',
    },
    'cassandra_hook': {
        'CassandraHook': 'airflow.providers.apache.cassandra.hooks.cassandra.CassandraHook',
    },
    'cloudant_hook': {
        'CloudantHook': 'airflow.providers.cloudant.hooks.cloudant.CloudantHook',
    },
    'databricks_hook': {
        'CANCEL_RUN_ENDPOINT': 'airflow.providers.databricks.hooks.databricks.CANCEL_RUN_ENDPOINT',
        'GET_RUN_ENDPOINT': 'airflow.providers.databricks.hooks.databricks.GET_RUN_ENDPOINT',
        'RESTART_CLUSTER_ENDPOINT': 'airflow.providers.databricks.hooks.databricks.RESTART_CLUSTER_ENDPOINT',
        'RUN_LIFE_CYCLE_STATES': 'airflow.providers.databricks.hooks.databricks.RUN_LIFE_CYCLE_STATES',
        'RUN_NOW_ENDPOINT': 'airflow.providers.databricks.hooks.databricks.RUN_NOW_ENDPOINT',
        'START_CLUSTER_ENDPOINT': 'airflow.providers.databricks.hooks.databricks.START_CLUSTER_ENDPOINT',
        'SUBMIT_RUN_ENDPOINT': 'airflow.providers.databricks.hooks.databricks.SUBMIT_RUN_ENDPOINT',
        'TERMINATE_CLUSTER_ENDPOINT':
            'airflow.providers.databricks.hooks.databricks.TERMINATE_CLUSTER_ENDPOINT',
        'DatabricksHook': 'airflow.providers.databricks.hooks.databricks.DatabricksHook',
        'RunState': 'airflow.providers.databricks.hooks.databricks.RunState',
    },
    'datadog_hook': {
        'DatadogHook': 'airflow.providers.datadog.hooks.datadog.DatadogHook',
    },
    'datastore_hook': {
        'DatastoreHook': 'airflow.providers.google.cloud.hooks.datastore.DatastoreHook',
    },
    'dingding_hook': {
        'DingdingHook': 'airflow.providers.dingding.hooks.dingding.DingdingHook',
        'requests': 'airflow.providers.dingding.hooks.dingding.requests',
    },
    'discord_webhook_hook': {
        'DiscordWebhookHook': 'airflow.providers.discord.hooks.discord_webhook.DiscordWebhookHook',
    },
    'emr_hook': {
        'EmrHook': 'airflow.providers.amazon.aws.hooks.emr.EmrHook',
    },
    'fs_hook': {
        'FSHook': 'airflow.hooks.filesystem.FSHook',
    },
    'ftp_hook': {
        'FTPHook': 'airflow.providers.ftp.hooks.ftp.FTPHook',
        'FTPSHook': 'airflow.providers.ftp.hooks.ftp.FTPSHook',
    },
    'gcp_api_base_hook': {
        'GoogleBaseHook': 'airflow.providers.google.common.hooks.base_google.GoogleBaseHook',
    },
    'gcp_bigtable_hook': {
        'BigtableHook': 'airflow.providers.google.cloud.hooks.bigtable.BigtableHook',
    },
    'gcp_cloud_build_hook': {
        'CloudBuildHook': 'airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook',
    },
    'gcp_compute_hook': {
        'ComputeEngineHook': 'airflow.providers.google.cloud.hooks.compute.ComputeEngineHook',
    },
    'gcp_container_hook': {
        'GKEHook': 'airflow.providers.google.cloud.hooks.kubernetes_engine.GKEHook',
    },
    'gcp_dataflow_hook': {
        'DataflowHook': 'airflow.providers.google.cloud.hooks.dataflow.DataflowHook',
    },
    'gcp_dataproc_hook': {
        'DataprocHook': 'airflow.providers.google.cloud.hooks.dataproc.DataprocHook',
    },
    'gcp_dlp_hook': {
        'CloudDLPHook': 'airflow.providers.google.cloud.hooks.dlp.CloudDLPHook',
        'DlpJob': 'airflow.providers.google.cloud.hooks.dlp.DlpJob',
    },
    'gcp_function_hook': {
        'CloudFunctionsHook': 'airflow.providers.google.cloud.hooks.functions.CloudFunctionsHook',
    },
    'gcp_kms_hook': {
        'CloudKMSHook': 'airflow.providers.google.cloud.hooks.kms.CloudKMSHook',
    },
    'gcp_mlengine_hook': {
        'MLEngineHook': 'airflow.providers.google.cloud.hooks.mlengine.MLEngineHook',
    },
    'gcp_natural_language_hook': {
        'CloudNaturalLanguageHook':
            'airflow.providers.google.cloud.hooks.natural_language.CloudNaturalLanguageHook',
    },
    'gcp_pubsub_hook': {
        'PubSubException': 'airflow.providers.google.cloud.hooks.pubsub.PubSubException',
        'PubSubHook': 'airflow.providers.google.cloud.hooks.pubsub.PubSubHook',
    },
    'gcp_spanner_hook': {
        'SpannerHook': 'airflow.providers.google.cloud.hooks.spanner.SpannerHook',
    },
    'gcp_speech_to_text_hook': {
        'CloudSpeechToTextHook': 'airflow.providers.google.cloud.hooks.speech_to_text.CloudSpeechToTextHook',
    },
    'gcp_sql_hook': {
        'CloudSQLDatabaseHook': 'airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook',
        'CloudSQLHook': 'airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook',
    },
    'gcp_tasks_hook': {
        'CloudTasksHook': 'airflow.providers.google.cloud.hooks.tasks.CloudTasksHook',
    },
    'gcp_text_to_speech_hook': {
        'CloudTextToSpeechHook': 'airflow.providers.google.cloud.hooks.text_to_speech.CloudTextToSpeechHook',
    },
    'gcp_transfer_hook': {
        'CloudDataTransferServiceHook':
            'airflow.providers.google.cloud.hooks.cloud_storage_transfer_service.'
            'CloudDataTransferServiceHook',
    },
    'gcp_translate_hook': {
        'CloudTranslateHook': 'airflow.providers.google.cloud.hooks.translate.CloudTranslateHook',
    },
    'gcp_video_intelligence_hook': {
        'CloudVideoIntelligenceHook':
            'airflow.providers.google.cloud.hooks.video_intelligence.CloudVideoIntelligenceHook',
    },
    'gcp_vision_hook': {
        'CloudVisionHook': 'airflow.providers.google.cloud.hooks.vision.CloudVisionHook',
    },
    'gcs_hook': {
        'GCSHook': 'airflow.providers.google.cloud.hooks.gcs.GCSHook',
    },
    'gdrive_hook': {
        'GoogleDriveHook': 'airflow.providers.google.suite.hooks.drive.GoogleDriveHook',
    },
    'grpc_hook': {
        'GrpcHook': 'airflow.providers.grpc.hooks.grpc.GrpcHook',
    },
    'imap_hook': {
        'ImapHook': 'airflow.providers.imap.hooks.imap.ImapHook',
        'Mail': 'airflow.providers.imap.hooks.imap.Mail',
        'MailPart': 'airflow.providers.imap.hooks.imap.MailPart',
    },
    'jenkins_hook': {
        'JenkinsHook': 'airflow.providers.jenkins.hooks.jenkins.JenkinsHook',
    },
    'jira_hook': {
        'JiraHook': 'airflow.providers.atlassian.jira.hooks.jira.JiraHook',
    },
    'mongo_hook': {
        'MongoHook': 'airflow.providers.mongo.hooks.mongo.MongoHook',
    },
    'openfaas_hook': {
        'OK_STATUS_CODE': 'airflow.providers.openfaas.hooks.openfaas.OK_STATUS_CODE',
        'OpenFaasHook': 'airflow.providers.openfaas.hooks.openfaas.OpenFaasHook',
        'requests': 'airflow.providers.openfaas.hooks.openfaas.requests',
    },
    'opsgenie_alert_hook': {
        'OpsgenieAlertHook': 'airflow.providers.opsgenie.hooks.opsgenie.OpsgenieAlertHook',
    },
    'pagerduty_hook': {
        'PagerdutyHook': 'airflow.providers.pagerduty.hooks.pagerduty.PagerdutyHook',
    },
    'pinot_hook': {
        'PinotAdminHook': 'airflow.providers.apache.pinot.hooks.pinot.PinotAdminHook',
        'PinotDbApiHook': 'airflow.providers.apache.pinot.hooks.pinot.PinotDbApiHook',
    },
    'qubole_check_hook': {
        'QuboleCheckHook': 'airflow.providers.qubole.hooks.qubole_check.QuboleCheckHook',
    },
    'qubole_hook': {
        'QuboleHook': 'airflow.providers.qubole.hooks.qubole.QuboleHook',
    },
    'redis_hook': {
        'RedisHook': 'airflow.providers.redis.hooks.redis.RedisHook',
    },
    'redshift_hook': {
        'RedshiftHook': 'airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook',
    },
    'sagemaker_hook': {
        'LogState': 'airflow.providers.amazon.aws.hooks.sagemaker.LogState',
        'Position': 'airflow.providers.amazon.aws.hooks.sagemaker.Position',
        'SageMakerHook': 'airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook',
        'argmin': 'airflow.providers.amazon.aws.hooks.sagemaker.argmin',
        'secondary_training_status_changed':
            'airflow.providers.amazon.aws.hooks.sagemaker.secondary_training_status_changed',
        'secondary_training_status_message':
            'airflow.providers.amazon.aws.hooks.sagemaker.secondary_training_status_message',
    },
    'salesforce_hook': {
        'SalesforceHook': 'airflow.providers.salesforce.hooks.salesforce.SalesforceHook',
        'pd': 'airflow.providers.salesforce.hooks.salesforce.pd',
    },
    'segment_hook': {
        'SegmentHook': 'airflow.providers.segment.hooks.segment.SegmentHook',
        'analytics': 'airflow.providers.segment.hooks.segment.analytics',
    },
    'sftp_hook': {
        'SFTPHook': 'airflow.providers.sftp.hooks.sftp.SFTPHook',
    },
    'slack_webhook_hook': {
        'SlackWebhookHook': 'airflow.providers.slack.hooks.slack_webhook.SlackWebhookHook',
    },
    'snowflake_hook': {
        'SnowflakeHook': 'airflow.providers.snowflake.hooks.snowflake.SnowflakeHook',
    },
    'spark_jdbc_hook': {
        'SparkJDBCHook': 'airflow.providers.apache.spark.hooks.spark_jdbc.SparkJDBCHook',
    },
    'spark_sql_hook': {
        'SparkSqlHook': 'airflow.providers.apache.spark.hooks.spark_sql.SparkSqlHook',
    },
    'spark_submit_hook': {
        'SparkSubmitHook': 'airflow.providers.apache.spark.hooks.spark_submit.SparkSubmitHook',
    },
    'sqoop_hook': {
        'SqoopHook': 'airflow.providers.apache.sqoop.hooks.sqoop.SqoopHook',
    },
    'ssh_hook': {
        'SSHHook': 'airflow.providers.ssh.hooks.ssh.SSHHook',
    },
    'vertica_hook': {
        'VerticaHook': 'airflow.providers.vertica.hooks.vertica.VerticaHook',
    },
    'wasb_hook': {
        'WasbHook': 'airflow.providers.microsoft.azure.hooks.wasb.WasbHook',
    },
    'winrm_hook': {
        'WinRMHook': 'airflow.providers.microsoft.winrm.hooks.winrm.WinRMHook',
    },
}

add_deprecated_classes(__deprecated_classes, __name__)
