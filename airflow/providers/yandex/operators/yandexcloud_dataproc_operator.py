# -*- coding: utf-8 -*-
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
#

from airflow.providers.yandex.hooks.yandexcloud_dataproc_hook import DataprocHook
from airflow.providers.yandex.operators.yandexcloud_base_operator import YandexCloudBaseOperator
from airflow.utils.decorators import apply_defaults


class DataprocCreateClusterOperator(YandexCloudBaseOperator):
    """Creates Yandex.Cloud Data Proc cluster."""

    # pylint: disable=too-many-instance-attributes
    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-locals

    @apply_defaults
    def __init__(self,
                 folder_id=None,
                 connection_id=None,
                 cluster_name=None,
                 cluster_description='',
                 cluster_image_version='1.1',
                 ssh_public_keys=None,
                 subnet_id=None,
                 services=('HDFS', 'YARN', 'MAPREDUCE', 'HIVE', 'SPARK'),
                 s3_bucket=None,
                 zone='ru-central1-b',
                 service_account_id=None,
                 masternode_resource_preset='s2.small',
                 masternode_disk_size=15,
                 masternode_disk_type='network-ssd',
                 datanode_resource_preset='s2.small',
                 datanode_disk_size=15,
                 datanode_disk_type='network-ssd',
                 datanode_count=2,
                 computenode_resource_preset='s2.small',
                 computenode_disk_size=15,
                 computenode_disk_type='network-ssd',
                 computenode_count=0,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.folder_id = folder_id
        self.connection_id = connection_id
        self.cluster_name = cluster_name
        self.cluster_description = cluster_description
        self.cluster_image_version = cluster_image_version
        self.ssh_public_keys = ssh_public_keys
        self.subnet_id = subnet_id
        self.services = services
        self.s3_bucket = s3_bucket
        self.zone = zone
        self.service_account_id = service_account_id
        self.masternode_resource_preset = masternode_resource_preset
        self.masternode_disk_size = masternode_disk_size
        self.masternode_disk_type = masternode_disk_type
        self.datanode_resource_preset = datanode_resource_preset
        self.datanode_disk_size = datanode_disk_size
        self.datanode_disk_type = datanode_disk_type
        self.datanode_count = datanode_count
        self.computenode_resource_preset = computenode_resource_preset
        self.computenode_disk_size = computenode_disk_size
        self.computenode_disk_type = computenode_disk_type
        self.computenode_count = computenode_count

    def execute(self, context):
        self.hook = DataprocHook(
            connection_id=self.connection_id,
        )
        cluster_id = self.hook.create_cluster(
            folder_id=self.folder_id,
            cluster_name=self.cluster_name,
            cluster_description=self.cluster_description,
            cluster_image_version=self.cluster_image_version,
            ssh_public_keys=self.ssh_public_keys,
            subnet_id=self.subnet_id,
            services=self.services,
            s3_bucket=self.s3_bucket,
            zone=self.zone,
            service_account_id=self.service_account_id,
            masternode_resource_preset=self.masternode_resource_preset,
            masternode_disk_size=self.masternode_disk_size,
            masternode_disk_type=self.masternode_disk_type,
            datanode_resource_preset=self.datanode_resource_preset,
            datanode_disk_size=self.datanode_disk_size,
            datanode_disk_type=self.datanode_disk_type,
            datanode_count=self.datanode_count,
            computenode_resource_preset=self.computenode_resource_preset,
            computenode_disk_size=self.computenode_disk_size,
            computenode_disk_type=self.computenode_disk_type,
            computenode_count=self.computenode_count,
        )
        context['task_instance'].xcom_push(key='cluster_id', value=cluster_id)
        context['task_instance'].xcom_push(key='yandexcloud_connection_id', value=self.connection_id)


class DataprocDeleteClusterOperator(YandexCloudBaseOperator):
    """Deletes Yandex.Cloud Data Proc cluster."""
    @apply_defaults
    def __init__(self,
                 connection_id=None,
                 cluster_id=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.cluster_id = cluster_id

    def execute(self, context):
        cluster_id = self.cluster_id or context['task_instance'].xcom_pull(key='cluster_id')
        connection_id = self.connection_id or context['task_instance'].xcom_pull(
            key='yandexcloud_connection_id'
        )
        self.hook = DataprocHook(
            connection_id=connection_id,
        )
        self.hook.delete_cluster(cluster_id)


class DataprocRunHiveJobOperator(YandexCloudBaseOperator):
    """Runs Hive job in Data Proc cluster."""

    # pylint: disable=too-many-arguments

    @apply_defaults
    def __init__(self,
                 query=None,
                 query_file_uri=None,
                 script_variables=None,
                 continue_on_failure=False,
                 properties=None,
                 name='Hive job',
                 cluster_id=None,
                 connection_id=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query
        self.query_file_uri = query_file_uri
        self.script_variables = script_variables
        self.continue_on_failure = continue_on_failure
        self.properties = properties
        self.name = name
        self.cluster_id = cluster_id
        self.connection_id = connection_id

    def execute(self, context):
        cluster_id = self.cluster_id or context['task_instance'].xcom_pull(key='cluster_id')
        connection_id = self.connection_id or context['task_instance'].xcom_pull(
            key='yandexcloud_connection_id'
        )
        self.hook = DataprocHook(
            connection_id=connection_id,
        )
        self.hook.run_hive_job(
            query=self.query,
            query_file_uri=self.query_file_uri,
            script_variables=self.script_variables,
            continue_on_failure=self.continue_on_failure,
            properties=self.properties,
            name=self.name,
            cluster_id=cluster_id,
        )


class DataprocRunMapReduceJobOperator(YandexCloudBaseOperator):
    """Runs Mapreduce job in Data Proc cluster."""

    # pylint: disable=too-many-arguments

    @apply_defaults
    def __init__(self,
                 main_class=None,
                 main_jar_file_uri=None,
                 jar_file_uris=None,
                 archive_uris=None,
                 file_uris=None,
                 arguments=None,
                 properties=None,
                 name='Mapreduce job',
                 cluster_id=None,
                 connection_id=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.main_class = main_class
        self.main_jar_file_uri = main_jar_file_uri
        self.jar_file_uris = jar_file_uris
        self.archive_uris = archive_uris
        self.file_uris = file_uris
        self.arguments = arguments
        self.properties = properties
        self.name = name
        self.cluster_id = cluster_id
        self.connection_id = connection_id

    def execute(self, context):
        cluster_id = self.cluster_id or context['task_instance'].xcom_pull(key='cluster_id')
        connection_id = self.connection_id or context['task_instance'].xcom_pull(
            key='yandexcloud_connection_id'
        )
        self.hook = DataprocHook(
            connection_id=connection_id,
        )
        self.hook.run_mapreduce_job(
            main_class=self.main_class,
            main_jar_file_uri=self.main_jar_file_uri,
            jar_file_uris=self.jar_file_uris,
            archive_uris=self.archive_uris,
            file_uris=self.file_uris,
            arguments=self.arguments,
            properties=self.properties,
            name=self.name,
            cluster_id=cluster_id,
        )


class DataprocRunSparkJobOperator(YandexCloudBaseOperator):
    """Runs Spark job in Data Proc cluster."""

    # pylint: disable=too-many-arguments

    @apply_defaults
    def __init__(self,
                 main_class=None,
                 main_jar_file_uri=None,
                 jar_file_uris=None,
                 archive_uris=None,
                 file_uris=None,
                 arguments=None,
                 properties=None,
                 name='Spark job',
                 cluster_id=None,
                 connection_id=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.main_class = main_class
        self.main_jar_file_uri = main_jar_file_uri
        self.jar_file_uris = jar_file_uris
        self.archive_uris = archive_uris
        self.file_uris = file_uris
        self.arguments = arguments
        self.properties = properties
        self.name = name
        self.cluster_id = cluster_id
        self.connection_id = connection_id

    def execute(self, context):
        cluster_id = self.cluster_id or context['task_instance'].xcom_pull(key='cluster_id')
        connection_id = self.connection_id or context['task_instance'].xcom_pull(
            key='yandexcloud_connection_id'
        )
        self.hook = DataprocHook(
            connection_id=connection_id,
        )
        self.hook.run_spark_job(
            main_class=self.main_class,
            main_jar_file_uri=self.main_jar_file_uri,
            jar_file_uris=self.jar_file_uris,
            archive_uris=self.archive_uris,
            file_uris=self.file_uris,
            arguments=self.arguments,
            properties=self.properties,
            name=self.name,
            cluster_id=cluster_id,
        )


class DataprocRunPysparkJobOperator(YandexCloudBaseOperator):
    """Runs Pyspark job in Data Proc cluster."""

    # pylint: disable=too-many-arguments

    @apply_defaults
    def __init__(self,
                 main_python_file_uri=None,
                 python_file_uris=None,
                 jar_file_uris=None,
                 archive_uris=None,
                 file_uris=None,
                 arguments=None,
                 properties=None,
                 name='Pyspark job',
                 cluster_id=None,
                 connection_id=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.main_python_file_uri = main_python_file_uri
        self.python_file_uris = python_file_uris
        self.jar_file_uris = jar_file_uris
        self.archive_uris = archive_uris
        self.file_uris = file_uris
        self.arguments = arguments
        self.properties = properties
        self.name = name
        self.cluster_id = cluster_id
        self.connection_id = connection_id

    def execute(self, context):
        cluster_id = self.cluster_id or context['task_instance'].xcom_pull(key='cluster_id')
        connection_id = self.connection_id or context['task_instance'].xcom_pull(
            key='yandexcloud_connection_id'
        )
        self.hook = DataprocHook(
            connection_id=connection_id,
        )
        self.hook.run_pyspark_job(
            main_python_file_uri=self.main_python_file_uri,
            python_file_uris=self.python_file_uris,
            jar_file_uris=self.jar_file_uris,
            archive_uris=self.archive_uris,
            file_uris=self.file_uris,
            arguments=self.arguments,
            properties=self.properties,
            name=self.name,
            cluster_id=cluster_id,
        )
