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
"""This package is deprecated. Please use `airflow.sensors` or `airflow.providers.*.sensors`."""
from __future__ import annotations

import warnings

from airflow.exceptions import RemovedInAirflow3Warning
from airflow.utils.deprecation_tools import add_deprecated_classes

warnings.warn(
    "This package is deprecated. Please use `airflow.sensors` or `airflow.providers.*.sensors`.",
    RemovedInAirflow3Warning,
    stacklevel=2,
)

__deprecated_classes = {
    'aws_athena_sensor': {
        'AthenaSensor': 'airflow.providers.amazon.aws.sensors.athena.AthenaSensor',
    },
    'aws_glue_catalog_partition_sensor': {
        'AwsGlueCatalogPartitionSensor': (
            'airflow.providers.amazon.aws.sensors.glue_catalog_partition.GlueCatalogPartitionSensor'
        ),
    },
    'aws_redshift_cluster_sensor': {
        'AwsRedshiftClusterSensor': (
            'airflow.providers.amazon.aws.sensors.redshift_cluster.RedshiftClusterSensor'
        ),
    },
    'aws_sqs_sensor': {
        'SqsSensor': 'airflow.providers.amazon.aws.sensors.sqs.SqsSensor',
        'SQSSensor': 'airflow.providers.amazon.aws.sensors.sqs.SqsSensor',
    },
    'azure_cosmos_sensor': {
        'AzureCosmosDocumentSensor': (
            'airflow.providers.microsoft.azure.sensors.cosmos.AzureCosmosDocumentSensor'
        ),
    },
    'bash_sensor': {
        'STDOUT': 'airflow.sensors.bash.STDOUT',
        'BashSensor': 'airflow.sensors.bash.BashSensor',
        'Popen': 'airflow.sensors.bash.Popen',
        'TemporaryDirectory': 'airflow.sensors.bash.TemporaryDirectory',
        'gettempdir': 'airflow.sensors.bash.gettempdir',
    },
    'bigquery_sensor': {
        'BigQueryTableExistenceSensor': (
            'airflow.providers.google.cloud.sensors.bigquery.BigQueryTableExistenceSensor'
        ),
        'BigQueryTableSensor': 'airflow.providers.google.cloud.sensors.bigquery.BigQueryTableExistenceSensor',
    },
    'cassandra_record_sensor': {
        'CassandraRecordSensor': 'airflow.providers.apache.cassandra.sensors.record.CassandraRecordSensor',
    },
    'cassandra_table_sensor': {
        'CassandraTableSensor': 'airflow.providers.apache.cassandra.sensors.table.CassandraTableSensor',
    },
    'celery_queue_sensor': {
        'CeleryQueueSensor': 'airflow.providers.celery.sensors.celery_queue.CeleryQueueSensor',
    },
    'datadog_sensor': {
        'DatadogSensor': 'airflow.providers.datadog.sensors.datadog.DatadogSensor',
    },
    'file_sensor': {
        'FileSensor': 'airflow.sensors.filesystem.FileSensor',
    },
    'ftp_sensor': {
        'FTPSensor': 'airflow.providers.ftp.sensors.ftp.FTPSensor',
        'FTPSSensor': 'airflow.providers.ftp.sensors.ftp.FTPSSensor',
    },
    'gcp_transfer_sensor': {
        'CloudDataTransferServiceJobStatusSensor':
            'airflow.providers.google.cloud.sensors.cloud_storage_transfer_service.'
            'CloudDataTransferServiceJobStatusSensor',
        'GCPTransferServiceWaitForJobStatusSensor':
            'airflow.providers.google.cloud.sensors.cloud_storage_transfer_service.'
            'CloudDataTransferServiceJobStatusSensor',
    },
    'gcs_sensor': {
        'GCSObjectExistenceSensor': 'airflow.providers.google.cloud.sensors.gcs.GCSObjectExistenceSensor',
        'GCSObjectsWithPrefixExistenceSensor': (
            'airflow.providers.google.cloud.sensors.gcs.GCSObjectsWithPrefixExistenceSensor'
        ),
        'GCSObjectUpdateSensor': 'airflow.providers.google.cloud.sensors.gcs.GCSObjectUpdateSensor',
        'GCSUploadSessionCompleteSensor': (
            'airflow.providers.google.cloud.sensors.gcs.GCSUploadSessionCompleteSensor'
        ),
        'GoogleCloudStorageObjectSensor': (
            'airflow.providers.google.cloud.sensors.gcs.GCSObjectExistenceSensor'
        ),
        'GoogleCloudStorageObjectUpdatedSensor': (
            'airflow.providers.google.cloud.sensors.gcs.GCSObjectUpdateSensor'
        ),
        'GoogleCloudStoragePrefixSensor': (
            'airflow.providers.google.cloud.sensors.gcs.GCSObjectsWithPrefixExistenceSensor'
        ),
        'GoogleCloudStorageUploadSessionCompleteSensor': (
            'airflow.providers.google.cloud.sensors.gcs.GCSUploadSessionCompleteSensor'
        ),
    },
    'hdfs_sensor': {
        'HdfsFolderSensor': 'airflow.providers.apache.hdfs.sensors.hdfs.HdfsFolderSensor',
        'HdfsRegexSensor': 'airflow.providers.apache.hdfs.sensors.hdfs.HdfsRegexSensor',
        'HdfsSensorFolder': 'airflow.providers.apache.hdfs.sensors.hdfs.HdfsFolderSensor',
        'HdfsSensorRegex': 'airflow.providers.apache.hdfs.sensors.hdfs.HdfsRegexSensor',
    },
    'imap_attachment_sensor': {
        'ImapAttachmentSensor': 'airflow.providers.imap.sensors.imap_attachment.ImapAttachmentSensor',
    },
    'jira_sensor': {
        'JiraSensor': 'airflow.providers.atlassian.jira.sensors.jira.JiraSensor',
        'JiraTicketSensor': 'airflow.providers.atlassian.jira.sensors.jira.JiraTicketSensor',
    },
    'mongo_sensor': {
        'MongoSensor': 'airflow.providers.mongo.sensors.mongo.MongoSensor',
    },
    'pubsub_sensor': {
        'PubSubPullSensor': 'airflow.providers.google.cloud.sensors.pubsub.PubSubPullSensor',
    },
    'python_sensor': {
        'PythonSensor': 'airflow.sensors.python.PythonSensor',
    },
    'qubole_sensor': {
        'QuboleFileSensor': 'airflow.providers.qubole.sensors.qubole.QuboleFileSensor',
        'QubolePartitionSensor': 'airflow.providers.qubole.sensors.qubole.QubolePartitionSensor',
        'QuboleSensor': 'airflow.providers.qubole.sensors.qubole.QuboleSensor',
    },
    'redis_key_sensor': {
        'RedisKeySensor': 'airflow.providers.redis.sensors.redis_key.RedisKeySensor',
    },
    'redis_pub_sub_sensor': {
        'RedisPubSubSensor': 'airflow.providers.redis.sensors.redis_pub_sub.RedisPubSubSensor',
    },
    'sagemaker_training_sensor': {
        'SageMakerHook': 'airflow.providers.amazon.aws.sensors.sagemaker.SageMakerHook',
        'SageMakerTrainingSensor': 'airflow.providers.amazon.aws.sensors.sagemaker.SageMakerTrainingSensor',
    },
    'sftp_sensor': {
        'SFTPSensor': 'airflow.providers.sftp.sensors.sftp.SFTPSensor',
    },
    'wasb_sensor': {
        'WasbBlobSensor': 'airflow.providers.microsoft.azure.sensors.wasb.WasbBlobSensor',
        'WasbPrefixSensor': 'airflow.providers.microsoft.azure.sensors.wasb.WasbPrefixSensor',
    },
    'weekday_sensor': {
        'DayOfWeekSensor': 'airflow.sensors.weekday.DayOfWeekSensor',
    },
}

add_deprecated_classes(__deprecated_classes, __name__)
