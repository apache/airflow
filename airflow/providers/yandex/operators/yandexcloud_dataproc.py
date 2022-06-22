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

from typing import Dict, Iterable, Optional, Union

from airflow.models import BaseOperator
from airflow.providers.yandex.hooks.yandexcloud_dataproc import DataprocHook


class DataprocCreateClusterOperator(BaseOperator):
    """Creates Yandex.Cloud Data Proc cluster.

    :param folder_id: ID of the folder in which cluster should be created.
    :type folder_id: Optional[str]
    :param cluster_name: Cluster name. Must be unique inside the folder.
    :type cluster_name: Optional[str]
    :param cluster_description: Cluster description.
    :type cluster_description: str
    :param cluster_image_version: Cluster image version. Use default.
    :type cluster_image_version: str
    :param ssh_public_keys: List of SSH public keys that will be deployed to created compute instances.
    :type ssh_public_keys: Optional[Union[str, Iterable[str]]]
    :param subnet_id: ID of the subnetwork. All Data Proc cluster nodes will use one subnetwork.
    :type subnet_id: str
    :param services: List of services that will be installed to the cluster. Possible options:
        HDFS, YARN, MAPREDUCE, HIVE, TEZ, ZOOKEEPER, HBASE, SQOOP, FLUME, SPARK, SPARK, ZEPPELIN, OOZIE
    :type services: Iterable[str]
    :param s3_bucket: Yandex.Cloud S3 bucket to store cluster logs.
                      Jobs will not work if the bucket is not specified.
    :type s3_bucket: Optional[str]
    :param zone: Availability zone to create cluster in.
                 Currently there are ru-central1-a, ru-central1-b and ru-central1-c.
    :type zone: str
    :param service_account_id: Service account id for the cluster.
                               Service account can be created inside the folder.
    :type service_account_id: Optional[str]
    :param masternode_resource_preset: Resources preset (CPU+RAM configuration)
                                       for the primary node of the cluster.
    :type masternode_resource_preset: str
    :param masternode_disk_size: Masternode storage size in GiB.
    :type masternode_disk_size: int
    :param masternode_disk_type: Masternode storage type. Possible options: network-ssd, network-hdd.
    :type masternode_disk_type: str
    :param datanode_resource_preset: Resources preset (CPU+RAM configuration)
                                     for the data nodes of the cluster.
    :type datanode_resource_preset: str
    :param datanode_disk_size: Datanodes storage size in GiB.
    :type datanode_disk_size: int
    :param datanode_disk_type: Datanodes storage type. Possible options: network-ssd, network-hdd.
    :type datanode_disk_type: str
    :param computenode_resource_preset: Resources preset (CPU+RAM configuration)
                                        for the compute nodes of the cluster.
    :type computenode_resource_preset: str
    :param computenode_disk_size: Computenodes storage size in GiB.
    :type computenode_disk_size: int
    :param computenode_disk_type: Computenodes storage type. Possible options: network-ssd, network-hdd.
    :type computenode_disk_type: str
    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    :type connection_id: Optional[str]
    :type computenode_max_count: int
    :param computenode_max_count: Maximum number of nodes of compute autoscaling subcluster.
    :param computenode_warmup_duration: The warmup time of the instance in seconds. During this time,
                                        traffic is sent to the instance,
                                        but instance metrics are not collected. In seconds.
    :type computenode_warmup_duration: int
    :param computenode_stabilization_duration: Minimum amount of time in seconds for monitoring before
                                   Instance Groups can reduce the number of instances in the group.
                                   During this time, the group size doesn't decrease,
                                   even if the new metric values indicate that it should. In seconds.
    :type computenode_stabilization_duration: int
    :param computenode_preemptible: Preemptible instances are stopped at least once every 24 hours,
                        and can be stopped at any time if their resources are needed by Compute.
    :type computenode_preemptible: bool
    :param computenode_cpu_utilization_target: Defines an autoscaling rule
                                   based on the average CPU utilization of the instance group.
                                   in percents. 10-100.
                                   By default is not set and default autoscaling strategy is used.
    :type computenode_cpu_utilization_target: int
    :param computenode_decommission_timeout: Timeout to gracefully decommission nodes during downscaling.
                                             In seconds.
    :type computenode_decommission_timeout: int
    :param log_group_id: Id of log group to write logs. By default logs will be sent to default log group.
                    To disable cloud log sending set cluster property dataproc:disable_cloud_logging = true
    :type log_group_id: str
    """

    def __init__(
        self,
        *,
        folder_id: Optional[str] = None,
        cluster_name: Optional[str] = None,
        cluster_description: Optional[str] = '',
        cluster_image_version: Optional[str] = None,
        ssh_public_keys: Optional[Union[str, Iterable[str]]] = None,
        subnet_id: Optional[str] = None,
        services: Iterable[str] = ('HDFS', 'YARN', 'MAPREDUCE', 'HIVE', 'SPARK'),
        s3_bucket: Optional[str] = None,
        zone: str = 'ru-central1-b',
        service_account_id: Optional[str] = None,
        masternode_resource_preset: Optional[str] = None,
        masternode_disk_size: Optional[int] = None,
        masternode_disk_type: Optional[str] = None,
        datanode_resource_preset: Optional[str] = None,
        datanode_disk_size: Optional[int] = None,
        datanode_disk_type: Optional[str] = None,
        datanode_count: int = 1,
        computenode_resource_preset: Optional[str] = None,
        computenode_disk_size: Optional[int] = None,
        computenode_disk_type: Optional[str] = None,
        computenode_count: int = 0,
        computenode_max_hosts_count: Optional[int] = None,
        computenode_measurement_duration: Optional[int] = None,
        computenode_warmup_duration: Optional[int] = None,
        computenode_stabilization_duration: Optional[int] = None,
        computenode_preemptible: bool = False,
        computenode_cpu_utilization_target: Optional[int] = None,
        computenode_decommission_timeout: Optional[int] = None,
        connection_id: Optional[str] = None,
        log_group_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.folder_id = folder_id
        self.yandex_conn_id = connection_id
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
        self.computenode_max_hosts_count = computenode_max_hosts_count
        self.computenode_measurement_duration = computenode_measurement_duration
        self.computenode_warmup_duration = computenode_warmup_duration
        self.computenode_stabilization_duration = computenode_stabilization_duration
        self.computenode_preemptible = computenode_preemptible
        self.computenode_cpu_utilization_target = computenode_cpu_utilization_target
        self.computenode_decommission_timeout = computenode_decommission_timeout
        self.log_group_id = log_group_id

        self.hook: Optional[DataprocHook] = None

    def execute(self, context) -> None:
        self.hook = DataprocHook(
            yandex_conn_id=self.yandex_conn_id,
        )
        operation_result = self.hook.client.create_cluster(
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
            computenode_max_hosts_count=self.computenode_max_hosts_count,
            computenode_measurement_duration=self.computenode_measurement_duration,
            computenode_warmup_duration=self.computenode_warmup_duration,
            computenode_stabilization_duration=self.computenode_stabilization_duration,
            computenode_preemptible=self.computenode_preemptible,
            computenode_cpu_utilization_target=self.computenode_cpu_utilization_target,
            computenode_decommission_timeout=self.computenode_decommission_timeout,
            log_group_id=self.log_group_id,
        )
        context['task_instance'].xcom_push(key='cluster_id', value=operation_result.response.id)
        context['task_instance'].xcom_push(key='yandexcloud_connection_id', value=self.yandex_conn_id)


class DataprocDeleteClusterOperator(BaseOperator):
    """Deletes Yandex.Cloud Data Proc cluster.

    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    :type connection_id: Optional[str]
    :param cluster_id: ID of the cluster to remove. (templated)
    :type cluster_id: Optional[str]
    """

    template_fields = ['cluster_id']

    def __init__(
        self, *, connection_id: Optional[str] = None, cluster_id: Optional[str] = None, **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.yandex_conn_id = connection_id
        self.cluster_id = cluster_id
        self.hook: Optional[DataprocHook] = None

    def execute(self, context) -> None:
        cluster_id = self.cluster_id or context['task_instance'].xcom_pull(key='cluster_id')
        yandex_conn_id = self.yandex_conn_id or context['task_instance'].xcom_pull(
            key='yandexcloud_connection_id'
        )
        self.hook = DataprocHook(
            yandex_conn_id=yandex_conn_id,
        )
        self.hook.client.delete_cluster(cluster_id)


class DataprocCreateHiveJobOperator(BaseOperator):
    """Runs Hive job in Data Proc cluster.

    :param query: Hive query.
    :type query: Optional[str]
    :param query_file_uri: URI of the script that contains Hive queries. Can be placed in HDFS or S3.
    :type query_file_uri: Optional[str]
    :param properties: A mapping of property names to values, used to configure Hive.
    :type properties: Optional[Dist[str, str]]
    :param script_variables: Mapping of query variable names to values.
    :type script_variables: Optional[Dist[str, str]]
    :param continue_on_failure: Whether to continue executing queries if a query fails.
    :type continue_on_failure: bool
    :param name: Name of the job. Used for labeling.
    :type name: str
    :param cluster_id: ID of the cluster to run job in.
                       Will try to take the ID from Dataproc Hook object if ot specified. (templated)
    :type cluster_id: Optional[str]
    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    :type connection_id: Optional[str]
    """

    template_fields = ['cluster_id']

    def __init__(
        self,
        *,
        query: Optional[str] = None,
        query_file_uri: Optional[str] = None,
        script_variables: Optional[Dict[str, str]] = None,
        continue_on_failure: bool = False,
        properties: Optional[Dict[str, str]] = None,
        name: str = 'Hive job',
        cluster_id: Optional[str] = None,
        connection_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.query_file_uri = query_file_uri
        self.script_variables = script_variables
        self.continue_on_failure = continue_on_failure
        self.properties = properties
        self.name = name
        self.cluster_id = cluster_id
        self.connection_id = connection_id
        self.hook: Optional[DataprocHook] = None

    def execute(self, context) -> None:
        cluster_id = self.cluster_id or context['task_instance'].xcom_pull(key='cluster_id')
        yandex_conn_id = self.connection_id or context['task_instance'].xcom_pull(
            key='yandexcloud_connection_id'
        )
        self.hook = DataprocHook(
            yandex_conn_id=yandex_conn_id,
        )
        self.hook.client.create_hive_job(
            query=self.query,
            query_file_uri=self.query_file_uri,
            script_variables=self.script_variables,
            continue_on_failure=self.continue_on_failure,
            properties=self.properties,
            name=self.name,
            cluster_id=cluster_id,
        )


class DataprocCreateMapReduceJobOperator(BaseOperator):
    """Runs Mapreduce job in Data Proc cluster.

    :param main_jar_file_uri: URI of jar file with job.
                              Can be placed in HDFS or S3. Can be specified instead of main_class.
    :type main_jar_file_uri: Optional[str]
    :param main_class: Name of the main class of the job. Can be specified instead of main_jar_file_uri.
    :type main_class: Optional[str]
    :param file_uris: URIs of files used in the job. Can be placed in HDFS or S3.
    :type file_uris: Optional[Iterable[str]]
    :param archive_uris: URIs of archive files used in the job. Can be placed in HDFS or S3.
    :type archive_uris: Optional[Iterable[str]]
    :param jar_file_uris: URIs of JAR files used in the job. Can be placed in HDFS or S3.
    :type jar_file_uris: Optional[Iterable[str]]
    :param properties: Properties for the job.
    :type properties: Optional[Dist[str, str]]
    :param args: Arguments to be passed to the job.
    :type args: Optional[Iterable[str]]
    :param name: Name of the job. Used for labeling.
    :type name: str
    :param cluster_id: ID of the cluster to run job in.
                       Will try to take the ID from Dataproc Hook object if ot specified. (templated)
    :type cluster_id: Optional[str]
    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    :type connection_id: Optional[str]
    """

    template_fields = ['cluster_id']

    def __init__(
        self,
        *,
        main_class: Optional[str] = None,
        main_jar_file_uri: Optional[str] = None,
        jar_file_uris: Optional[Iterable[str]] = None,
        archive_uris: Optional[Iterable[str]] = None,
        file_uris: Optional[Iterable[str]] = None,
        args: Optional[Iterable[str]] = None,
        properties: Optional[Dict[str, str]] = None,
        name: str = 'Mapreduce job',
        cluster_id: Optional[str] = None,
        connection_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.main_class = main_class
        self.main_jar_file_uri = main_jar_file_uri
        self.jar_file_uris = jar_file_uris
        self.archive_uris = archive_uris
        self.file_uris = file_uris
        self.args = args
        self.properties = properties
        self.name = name
        self.cluster_id = cluster_id
        self.connection_id = connection_id
        self.hook: Optional[DataprocHook] = None

    def execute(self, context) -> None:
        cluster_id = self.cluster_id or context['task_instance'].xcom_pull(key='cluster_id')
        yandex_conn_id = self.connection_id or context['task_instance'].xcom_pull(
            key='yandexcloud_connection_id'
        )
        self.hook = DataprocHook(
            yandex_conn_id=yandex_conn_id,
        )
        self.hook.client.create_mapreduce_job(
            main_class=self.main_class,
            main_jar_file_uri=self.main_jar_file_uri,
            jar_file_uris=self.jar_file_uris,
            archive_uris=self.archive_uris,
            file_uris=self.file_uris,
            args=self.args,
            properties=self.properties,
            name=self.name,
            cluster_id=cluster_id,
        )


class DataprocCreateSparkJobOperator(BaseOperator):
    """Runs Spark job in Data Proc cluster.

    :param main_jar_file_uri: URI of jar file with job. Can be placed in HDFS or S3.
    :type main_jar_file_uri: Optional[str]
    :param main_class: Name of the main class of the job.
    :type main_class: Optional[str]
    :param file_uris: URIs of files used in the job. Can be placed in HDFS or S3.
    :type file_uris: Optional[Iterable[str]]
    :param archive_uris: URIs of archive files used in the job. Can be placed in HDFS or S3.
    :type archive_uris: Optional[Iterable[str]]
    :param jar_file_uris: URIs of JAR files used in the job. Can be placed in HDFS or S3.
    :type jar_file_uris: Optional[Iterable[str]]
    :param properties: Properties for the job.
    :type properties: Optional[Dist[str, str]]
    :param args: Arguments to be passed to the job.
    :type args: Optional[Iterable[str]]
    :param name: Name of the job. Used for labeling.
    :type name: str
    :param cluster_id: ID of the cluster to run job in.
                       Will try to take the ID from Dataproc Hook object if ot specified. (templated)
    :type cluster_id: Optional[str]
    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    :type connection_id: Optional[str]
    :param packages: List of maven coordinates of jars to include on the driver and executor classpaths.
    :type packages: Optional[Iterable[str]]
    :param repositories: List of additional remote repositories to search for the maven coordinates
                        given with --packages.
    :type repositories: Optional[Iterable[str]]
    :param exclude_packages: List of groupId:artifactId, to exclude while resolving the dependencies
                        provided in --packages to avoid dependency conflicts.
    :type exclude_packages: Optional[Iterable[str]]
    """

    template_fields = ['cluster_id']

    def __init__(
        self,
        *,
        main_class: Optional[str] = None,
        main_jar_file_uri: Optional[str] = None,
        jar_file_uris: Optional[Iterable[str]] = None,
        archive_uris: Optional[Iterable[str]] = None,
        file_uris: Optional[Iterable[str]] = None,
        args: Optional[Iterable[str]] = None,
        properties: Optional[Dict[str, str]] = None,
        name: str = 'Spark job',
        cluster_id: Optional[str] = None,
        connection_id: Optional[str] = None,
        packages: Optional[Iterable[str]] = None,
        repositories: Optional[Iterable[str]] = None,
        exclude_packages: Optional[Iterable[str]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.main_class = main_class
        self.main_jar_file_uri = main_jar_file_uri
        self.jar_file_uris = jar_file_uris
        self.archive_uris = archive_uris
        self.file_uris = file_uris
        self.args = args
        self.properties = properties
        self.name = name
        self.cluster_id = cluster_id
        self.connection_id = connection_id
        self.packages = packages
        self.repositories = repositories
        self.exclude_packages = exclude_packages
        self.hook: Optional[DataprocHook] = None

    def execute(self, context) -> None:
        cluster_id = self.cluster_id or context['task_instance'].xcom_pull(key='cluster_id')
        yandex_conn_id = self.connection_id or context['task_instance'].xcom_pull(
            key='yandexcloud_connection_id'
        )
        self.hook = DataprocHook(
            yandex_conn_id=yandex_conn_id,
        )
        self.hook.client.create_spark_job(
            main_class=self.main_class,
            main_jar_file_uri=self.main_jar_file_uri,
            jar_file_uris=self.jar_file_uris,
            archive_uris=self.archive_uris,
            file_uris=self.file_uris,
            args=self.args,
            properties=self.properties,
            packages=self.packages,
            repositories=self.repositories,
            exclude_packages=self.exclude_packages,
            name=self.name,
            cluster_id=cluster_id,
        )


class DataprocCreatePysparkJobOperator(BaseOperator):
    """Runs Pyspark job in Data Proc cluster.

    :param main_python_file_uri: URI of python file with job. Can be placed in HDFS or S3.
    :type main_python_file_uri: Optional[str]
    :param python_file_uris: URIs of python files used in the job. Can be placed in HDFS or S3.
    :type python_file_uris: Optional[Iterable[str]]
    :param file_uris: URIs of files used in the job. Can be placed in HDFS or S3.
    :type file_uris: Optional[Iterable[str]]
    :param archive_uris: URIs of archive files used in the job. Can be placed in HDFS or S3.
    :type archive_uris: Optional[Iterable[str]]
    :param jar_file_uris: URIs of JAR files used in the job. Can be placed in HDFS or S3.
    :type jar_file_uris: Optional[Iterable[str]]
    :param properties: Properties for the job.
    :type properties: Optional[Dist[str, str]]
    :param args: Arguments to be passed to the job.
    :type args: Optional[Iterable[str]]
    :param name: Name of the job. Used for labeling.
    :type name: str
    :param cluster_id: ID of the cluster to run job in.
                       Will try to take the ID from Dataproc Hook object if ot specified. (templated)
    :type cluster_id: Optional[str]
    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    :type connection_id: Optional[str]
    :param packages: List of maven coordinates of jars to include on the driver and executor classpaths.
    :type packages: Optional[Iterable[str]]
    :param repositories: List of additional remote repositories to search for the maven coordinates
                         given with --packages.
    :type repositories: Optional[Iterable[str]]
    :param exclude_packages: List of groupId:artifactId, to exclude while resolving the dependencies
                         provided in --packages to avoid dependency conflicts.
    :type exclude_packages: Optional[Iterable[str]]
    """

    template_fields = ['cluster_id']

    def __init__(
        self,
        *,
        main_python_file_uri: Optional[str] = None,
        python_file_uris: Optional[Iterable[str]] = None,
        jar_file_uris: Optional[Iterable[str]] = None,
        archive_uris: Optional[Iterable[str]] = None,
        file_uris: Optional[Iterable[str]] = None,
        args: Optional[Iterable[str]] = None,
        properties: Optional[Dict[str, str]] = None,
        name: str = 'Pyspark job',
        cluster_id: Optional[str] = None,
        connection_id: Optional[str] = None,
        packages: Optional[Iterable[str]] = None,
        repositories: Optional[Iterable[str]] = None,
        exclude_packages: Optional[Iterable[str]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.main_python_file_uri = main_python_file_uri
        self.python_file_uris = python_file_uris
        self.jar_file_uris = jar_file_uris
        self.archive_uris = archive_uris
        self.file_uris = file_uris
        self.args = args
        self.properties = properties
        self.name = name
        self.cluster_id = cluster_id
        self.connection_id = connection_id
        self.packages = packages
        self.repositories = repositories
        self.exclude_packages = exclude_packages
        self.hook: Optional[DataprocHook] = None

    def execute(self, context) -> None:
        cluster_id = self.cluster_id or context['task_instance'].xcom_pull(key='cluster_id')
        yandex_conn_id = self.connection_id or context['task_instance'].xcom_pull(
            key='yandexcloud_connection_id'
        )
        self.hook = DataprocHook(
            yandex_conn_id=yandex_conn_id,
        )
        self.hook.client.create_pyspark_job(
            main_python_file_uri=self.main_python_file_uri,
            python_file_uris=self.python_file_uris,
            jar_file_uris=self.jar_file_uris,
            archive_uris=self.archive_uris,
            file_uris=self.file_uris,
            args=self.args,
            properties=self.properties,
            packages=self.packages,
            repositories=self.repositories,
            exclude_packages=self.exclude_packages,
            name=self.name,
            cluster_id=cluster_id,
        )
