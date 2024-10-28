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
from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Sequence

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import BaseOperator
from airflow.providers.yandex.hooks.dataproc import DataprocHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


@dataclass
class InitializationAction:
    """Data for initialization action to be run at start of DataProc cluster."""

    uri: str  # Uri of the executable file
    args: Iterable[str]  # Arguments to the initialization action
    timeout: int  # Execution timeout


class DataprocCreateClusterOperator(BaseOperator):
    """
    Creates Yandex.Cloud Data Proc cluster.

    :param folder_id: ID of the folder in which cluster should be created.
    :param cluster_name: Cluster name. Must be unique inside the folder.
    :param cluster_description: Cluster description.
    :param cluster_image_version: Cluster image version. Use default.
    :param ssh_public_keys: List of SSH public keys that will be deployed to created compute instances.
    :param subnet_id: ID of the subnetwork. All Data Proc cluster nodes will use one subnetwork.
    :param services: List of services that will be installed to the cluster. Possible options:
        HDFS, YARN, MAPREDUCE, HIVE, TEZ, ZOOKEEPER, HBASE, SQOOP, FLUME, SPARK, SPARK, ZEPPELIN, OOZIE
    :param s3_bucket: Yandex.Cloud S3 bucket to store cluster logs.
                      Jobs will not work if the bucket is not specified.
    :param zone: Availability zone to create cluster in.
                 Currently there are ru-central1-a, ru-central1-b and ru-central1-c.
    :param service_account_id: Service account id for the cluster.
                               Service account can be created inside the folder.
    :param masternode_resource_preset: Resources preset (CPU+RAM configuration)
                                       for the primary node of the cluster.
    :param masternode_disk_size: Masternode storage size in GiB.
    :param masternode_disk_type: Masternode storage type. Possible options: network-ssd, network-hdd.
    :param datanode_resource_preset: Resources preset (CPU+RAM configuration)
                                     for the data nodes of the cluster.
    :param datanode_disk_size: Datanodes storage size in GiB.
    :param datanode_disk_type: Datanodes storage type. Possible options: network-ssd, network-hdd.
    :param computenode_resource_preset: Resources preset (CPU+RAM configuration)
                                        for the compute nodes of the cluster.
    :param computenode_disk_size: Computenodes storage size in GiB.
    :param computenode_disk_type: Computenodes storage type. Possible options: network-ssd, network-hdd.
    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    :param computenode_max_count: Maximum number of nodes of compute autoscaling subcluster.
    :param computenode_warmup_duration: The warmup time of the instance in seconds. During this time,
                                        traffic is sent to the instance,
                                        but instance metrics are not collected. In seconds.
    :param computenode_stabilization_duration: Minimum amount of time in seconds for monitoring before
                                   Instance Groups can reduce the number of instances in the group.
                                   During this time, the group size doesn't decrease,
                                   even if the new metric values indicate that it should. In seconds.
    :param computenode_preemptible: Preemptible instances are stopped at least once every 24 hours,
                        and can be stopped at any time if their resources are needed by Compute.
    :param computenode_cpu_utilization_target: Defines an autoscaling rule
                                   based on the average CPU utilization of the instance group.
                                   in percents. 10-100.
                                   By default is not set and default autoscaling strategy is used.
    :param computenode_decommission_timeout: Timeout to gracefully decommission nodes during downscaling.
                                             In seconds
    :param properties: Properties passed to main node software.
                        Docs: https://cloud.yandex.com/docs/data-proc/concepts/settings-list
    :param enable_ui_proxy: Enable UI Proxy feature for forwarding Hadoop components web interfaces
                        Docs: https://cloud.yandex.com/docs/data-proc/concepts/ui-proxy
    :param host_group_ids: Dedicated host groups to place VMs of cluster on.
                        Docs: https://cloud.yandex.com/docs/compute/concepts/dedicated-host
    :param security_group_ids: User security groups.
                        Docs: https://cloud.yandex.com/docs/data-proc/concepts/network#security-groups
    :param log_group_id: Id of log group to write logs. By default logs will be sent to default log group.
                    To disable cloud log sending set cluster property dataproc:disable_cloud_logging = true
                    Docs: https://cloud.yandex.com/docs/data-proc/concepts/logs
    :param initialization_actions: Set of init-actions to run when cluster starts.
                        Docs: https://cloud.yandex.com/docs/data-proc/concepts/init-action
    :param labels: Cluster labels as key:value pairs. No more than 64 per resource.
                        Docs: https://cloud.yandex.com/docs/resource-manager/concepts/labels
    """

    def __init__(
        self,
        *,
        folder_id: str | None = None,
        cluster_name: str | None = None,
        cluster_description: str | None = "",
        cluster_image_version: str | None = None,
        ssh_public_keys: str | Iterable[str] | None = None,
        subnet_id: str | None = None,
        services: Iterable[str] = ("HDFS", "YARN", "MAPREDUCE", "HIVE", "SPARK"),
        s3_bucket: str | None = None,
        zone: str = "ru-central1-b",
        service_account_id: str | None = None,
        masternode_resource_preset: str | None = None,
        masternode_disk_size: int | None = None,
        masternode_disk_type: str | None = None,
        datanode_resource_preset: str | None = None,
        datanode_disk_size: int | None = None,
        datanode_disk_type: str | None = None,
        datanode_count: int = 1,
        computenode_resource_preset: str | None = None,
        computenode_disk_size: int | None = None,
        computenode_disk_type: str | None = None,
        computenode_count: int = 0,
        computenode_max_hosts_count: int | None = None,
        computenode_measurement_duration: int | None = None,
        computenode_warmup_duration: int | None = None,
        computenode_stabilization_duration: int | None = None,
        computenode_preemptible: bool = False,
        computenode_cpu_utilization_target: int | None = None,
        computenode_decommission_timeout: int | None = None,
        connection_id: str | None = None,
        properties: dict[str, str] | None = None,
        enable_ui_proxy: bool = False,
        host_group_ids: Iterable[str] | None = None,
        security_group_ids: Iterable[str] | None = None,
        log_group_id: str | None = None,
        initialization_actions: Iterable[InitializationAction] | None = None,
        labels: dict[str, str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if ssh_public_keys is None:
            ssh_public_keys = []

        if services is None:
            services = []

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
        self.properties = properties
        self.enable_ui_proxy = enable_ui_proxy
        self.host_group_ids = host_group_ids
        self.security_group_ids = security_group_ids
        self.log_group_id = log_group_id
        self.initialization_actions = initialization_actions
        self.labels = labels

        self.hook: DataprocHook | None = None

    def execute(self, context: Context) -> dict:
        self.hook = DataprocHook(
            yandex_conn_id=self.yandex_conn_id,
        )
        operation_result = self.hook.dataproc_client.create_cluster(
            folder_id=self.folder_id,
            cluster_name=self.cluster_name,
            cluster_description=self.cluster_description,
            cluster_image_version=self.cluster_image_version,
            ssh_public_keys=self.ssh_public_keys,
            subnet_id=self.subnet_id,
            services=self.services,
            s3_bucket=self.s3_bucket,
            zone=self.zone,
            service_account_id=self.service_account_id
            or self.hook.default_service_account_id,
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
            properties=self.properties,
            enable_ui_proxy=self.enable_ui_proxy,
            host_group_ids=self.host_group_ids,
            security_group_ids=self.security_group_ids,
            log_group_id=self.log_group_id,
            labels=self.labels,
            initialization_actions=[
                self.hook.sdk.wrappers.InitializationAction(
                    uri=init_action.uri,
                    args=init_action.args,
                    timeout=init_action.timeout,
                )
                for init_action in self.initialization_actions
            ]
            if self.initialization_actions
            else None,
        )
        cluster_id = operation_result.response.id

        context["task_instance"].xcom_push(key="cluster_id", value=cluster_id)
        # Deprecated
        context["task_instance"].xcom_push(
            key="yandexcloud_connection_id", value=self.yandex_conn_id
        )
        return cluster_id

    @property
    def cluster_id(self):
        return self.output


class DataprocBaseOperator(BaseOperator):
    """
    Base class for DataProc operators working with given cluster.

    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    :param cluster_id: ID of the cluster to remove. (templated)
    """

    template_fields: Sequence[str] = ("cluster_id",)

    def __init__(
        self,
        *,
        yandex_conn_id: str | None = None,
        cluster_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.cluster_id = cluster_id
        self.yandex_conn_id = yandex_conn_id

    def _setup(self, context: Context) -> DataprocHook:
        if self.cluster_id is None:
            self.cluster_id = context["task_instance"].xcom_pull(key="cluster_id")
        if self.yandex_conn_id is None:
            xcom_yandex_conn_id = context["task_instance"].xcom_pull(
                key="yandexcloud_connection_id"
            )
            if xcom_yandex_conn_id:
                warnings.warn(
                    "Implicit pass of `yandex_conn_id` is deprecated, please pass it explicitly",
                    AirflowProviderDeprecationWarning,
                    stacklevel=2,
                )
                self.yandex_conn_id = xcom_yandex_conn_id

        return DataprocHook(yandex_conn_id=self.yandex_conn_id)

    def execute(self, context: Context):
        raise NotImplementedError()


class DataprocDeleteClusterOperator(DataprocBaseOperator):
    """
    Deletes Yandex.Cloud Data Proc cluster.

    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    :param cluster_id: ID of the cluster to remove. (templated)
    """

    def __init__(
        self, *, connection_id: str | None = None, cluster_id: str | None = None, **kwargs
    ) -> None:
        super().__init__(yandex_conn_id=connection_id, cluster_id=cluster_id, **kwargs)

    def execute(self, context: Context) -> None:
        hook = self._setup(context)
        hook.dataproc_client.delete_cluster(self.cluster_id)


class DataprocCreateHiveJobOperator(DataprocBaseOperator):
    """
    Runs Hive job in Data Proc cluster.

    :param query: Hive query.
    :param query_file_uri: URI of the script that contains Hive queries. Can be placed in HDFS or S3.
    :param properties: A mapping of property names to values, used to configure Hive.
    :param script_variables: Mapping of query variable names to values.
    :param continue_on_failure: Whether to continue executing queries if a query fails.
    :param name: Name of the job. Used for labeling.
    :param cluster_id: ID of the cluster to run job in.
                       Will try to take the ID from Dataproc Hook object if it's specified. (templated)
    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    """

    def __init__(
        self,
        *,
        query: str | None = None,
        query_file_uri: str | None = None,
        script_variables: dict[str, str] | None = None,
        continue_on_failure: bool = False,
        properties: dict[str, str] | None = None,
        name: str = "Hive job",
        cluster_id: str | None = None,
        connection_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(yandex_conn_id=connection_id, cluster_id=cluster_id, **kwargs)
        self.query = query
        self.query_file_uri = query_file_uri
        self.script_variables = script_variables
        self.continue_on_failure = continue_on_failure
        self.properties = properties
        self.name = name

    def execute(self, context: Context) -> None:
        hook = self._setup(context)
        hook.dataproc_client.create_hive_job(
            query=self.query,
            query_file_uri=self.query_file_uri,
            script_variables=self.script_variables,
            continue_on_failure=self.continue_on_failure,
            properties=self.properties,
            name=self.name,
            cluster_id=self.cluster_id,
        )


class DataprocCreateMapReduceJobOperator(DataprocBaseOperator):
    """
    Runs Mapreduce job in Data Proc cluster.

    :param main_jar_file_uri: URI of jar file with job.
                              Can be placed in HDFS or S3. Can be specified instead of main_class.
    :param main_class: Name of the main class of the job. Can be specified instead of main_jar_file_uri.
    :param file_uris: URIs of files used in the job. Can be placed in HDFS or S3.
    :param archive_uris: URIs of archive files used in the job. Can be placed in HDFS or S3.
    :param jar_file_uris: URIs of JAR files used in the job. Can be placed in HDFS or S3.
    :param properties: Properties for the job.
    :param args: Arguments to be passed to the job.
    :param name: Name of the job. Used for labeling.
    :param cluster_id: ID of the cluster to run job in.
                       Will try to take the ID from Dataproc Hook object if it's specified. (templated)
    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    """

    def __init__(
        self,
        *,
        main_class: str | None = None,
        main_jar_file_uri: str | None = None,
        jar_file_uris: Iterable[str] | None = None,
        archive_uris: Iterable[str] | None = None,
        file_uris: Iterable[str] | None = None,
        args: Iterable[str] | None = None,
        properties: dict[str, str] | None = None,
        name: str = "Mapreduce job",
        cluster_id: str | None = None,
        connection_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(yandex_conn_id=connection_id, cluster_id=cluster_id, **kwargs)
        self.main_class = main_class
        self.main_jar_file_uri = main_jar_file_uri
        self.jar_file_uris = jar_file_uris
        self.archive_uris = archive_uris
        self.file_uris = file_uris
        self.args = args
        self.properties = properties
        self.name = name

    def execute(self, context: Context) -> None:
        hook = self._setup(context)
        hook.dataproc_client.create_mapreduce_job(
            main_class=self.main_class,
            main_jar_file_uri=self.main_jar_file_uri,
            jar_file_uris=self.jar_file_uris,
            archive_uris=self.archive_uris,
            file_uris=self.file_uris,
            args=self.args,
            properties=self.properties,
            name=self.name,
            cluster_id=self.cluster_id,
        )


class DataprocCreateSparkJobOperator(DataprocBaseOperator):
    """
    Runs Spark job in Data Proc cluster.

    :param main_jar_file_uri: URI of jar file with job. Can be placed in HDFS or S3.
    :param main_class: Name of the main class of the job.
    :param file_uris: URIs of files used in the job. Can be placed in HDFS or S3.
    :param archive_uris: URIs of archive files used in the job. Can be placed in HDFS or S3.
    :param jar_file_uris: URIs of JAR files used in the job. Can be placed in HDFS or S3.
    :param properties: Properties for the job.
    :param args: Arguments to be passed to the job.
    :param name: Name of the job. Used for labeling.
    :param cluster_id: ID of the cluster to run job in.
                       Will try to take the ID from Dataproc Hook object if it's specified. (templated)
    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    :param packages: List of maven coordinates of jars to include on the driver and executor classpaths.
    :param repositories: List of additional remote repositories to search for the maven coordinates
                        given with --packages.
    :param exclude_packages: List of groupId:artifactId, to exclude while resolving the dependencies
                        provided in --packages to avoid dependency conflicts.
    """

    def __init__(
        self,
        *,
        main_class: str | None = None,
        main_jar_file_uri: str | None = None,
        jar_file_uris: Iterable[str] | None = None,
        archive_uris: Iterable[str] | None = None,
        file_uris: Iterable[str] | None = None,
        args: Iterable[str] | None = None,
        properties: dict[str, str] | None = None,
        name: str = "Spark job",
        cluster_id: str | None = None,
        connection_id: str | None = None,
        packages: Iterable[str] | None = None,
        repositories: Iterable[str] | None = None,
        exclude_packages: Iterable[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(yandex_conn_id=connection_id, cluster_id=cluster_id, **kwargs)
        self.main_class = main_class
        self.main_jar_file_uri = main_jar_file_uri
        self.jar_file_uris = jar_file_uris
        self.archive_uris = archive_uris
        self.file_uris = file_uris
        self.args = args
        self.properties = properties
        self.name = name
        self.packages = packages
        self.repositories = repositories
        self.exclude_packages = exclude_packages

    def execute(self, context: Context) -> None:
        hook = self._setup(context)
        hook.dataproc_client.create_spark_job(
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
            cluster_id=self.cluster_id,
        )


class DataprocCreatePysparkJobOperator(DataprocBaseOperator):
    """
    Runs Pyspark job in Data Proc cluster.

    :param main_python_file_uri: URI of python file with job. Can be placed in HDFS or S3.
    :param python_file_uris: URIs of python files used in the job. Can be placed in HDFS or S3.
    :param file_uris: URIs of files used in the job. Can be placed in HDFS or S3.
    :param archive_uris: URIs of archive files used in the job. Can be placed in HDFS or S3.
    :param jar_file_uris: URIs of JAR files used in the job. Can be placed in HDFS or S3.
    :param properties: Properties for the job.
    :param args: Arguments to be passed to the job.
    :param name: Name of the job. Used for labeling.
    :param cluster_id: ID of the cluster to run job in.
                       Will try to take the ID from Dataproc Hook object if it's specified. (templated)
    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    :param packages: List of maven coordinates of jars to include on the driver and executor classpaths.
    :param repositories: List of additional remote repositories to search for the maven coordinates
                         given with --packages.
    :param exclude_packages: List of groupId:artifactId, to exclude while resolving the dependencies
                         provided in --packages to avoid dependency conflicts.
    """

    def __init__(
        self,
        *,
        main_python_file_uri: str | None = None,
        python_file_uris: Iterable[str] | None = None,
        jar_file_uris: Iterable[str] | None = None,
        archive_uris: Iterable[str] | None = None,
        file_uris: Iterable[str] | None = None,
        args: Iterable[str] | None = None,
        properties: dict[str, str] | None = None,
        name: str = "Pyspark job",
        cluster_id: str | None = None,
        connection_id: str | None = None,
        packages: Iterable[str] | None = None,
        repositories: Iterable[str] | None = None,
        exclude_packages: Iterable[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(yandex_conn_id=connection_id, cluster_id=cluster_id, **kwargs)
        self.main_python_file_uri = main_python_file_uri
        self.python_file_uris = python_file_uris
        self.jar_file_uris = jar_file_uris
        self.archive_uris = archive_uris
        self.file_uris = file_uris
        self.args = args
        self.properties = properties
        self.name = name
        self.packages = packages
        self.repositories = repositories
        self.exclude_packages = exclude_packages

    def execute(self, context: Context) -> None:
        hook = self._setup(context)
        hook.dataproc_client.create_pyspark_job(
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
            cluster_id=self.cluster_id,
        )
