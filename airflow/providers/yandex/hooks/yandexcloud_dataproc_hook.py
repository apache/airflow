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

import random
from datetime import datetime

import yandex.cloud.dataproc.v1.cluster_pb2 as cluster_pb
import yandex.cloud.dataproc.v1.cluster_service_pb2 as cluster_service_pb
import yandex.cloud.dataproc.v1.cluster_service_pb2_grpc as cluster_service_grpc_pb
import yandex.cloud.dataproc.v1.common_pb2 as common_pb
import yandex.cloud.dataproc.v1.job_pb2 as job_pb
import yandex.cloud.dataproc.v1.job_service_pb2 as job_service_pb
import yandex.cloud.dataproc.v1.job_service_pb2_grpc as job_service_grpc_pb
import yandex.cloud.dataproc.v1.subcluster_pb2 as subcluster_pb
import yandex.cloud.dataproc.v1.subcluster_service_pb2 as subcluster_service_pb
import yandex.cloud.dataproc.v1.subcluster_service_pb2_grpc as subcluster_service_grpc_pb
from google.protobuf.field_mask_pb2 import FieldMask
from six import string_types

from airflow.exceptions import AirflowException
from airflow.providers.yandex.hooks.yandexcloud_base_hook import YandexCloudBaseHook


class DataprocHook(YandexCloudBaseHook):
    """
    A base hook for Yandex.Cloud Data Proc.

    :param connection_id: The connection ID to use when fetching connection info.
    :type connection_id: str
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cluster_id = None

    def _get_operation_result(self, operation, response_type=None, meta_type=None):
        created_at = datetime.fromtimestamp(operation.created_at.seconds)
        message = f'Running Yandex.Cloud operation. ID: {operation.id}. ' + \
                  f'Description: {operation.description}. Created at: {created_at}. ' + \
                  f'Created by: {operation.created_by}.'
        if meta_type:
            unpacked_meta = meta_type()
            operation.metadata.Unpack(unpacked_meta)
            message += f' Meta: {unpacked_meta}.'
        self.log.info(message)
        result = self.wait_for_operation(operation)
        if result.error and result.error.code:
            error_message = f'Error Yandex.Cloud operation. ID: {result.id}. ' + \
                            f'Error code: {result.error.code}. Details: {result.error.details}. ' + \
                            f'Message: {result.error.message}.'
            self.log.error(error_message)
            raise AirflowException(error_message)
        else:
            log_message = f'Done Yandex.Cloud operation. ID: {operation.id}.'
            unpacked_response = None
            if response_type:
                unpacked_response = response_type()
                result.response.Unpack(unpacked_response)
                log_message += f' Response: {unpacked_response}.'
            self.log.info(log_message)
            if unpacked_response:
                return unpacked_response
        return None

    def add_subcluster(
        self,
        cluster_id,
        subcluster_type,
        name,
        subnet_id,
        resource_preset='s2.small',
        disk_size=15,
        disk_type='network-ssd',
        hosts_count=5,
    ):
        """
        Add subcluster to Yandex.Cloud Data Proc cluster.

        :param cluster_id: ID of the cluster.
        :type cluster_id: str
        :param name: Name of the subcluster. Must be unique in the cluster
        :type name: str
        :param subcluster_type: Type of the subcluster. Either "data" or "compute".
        :type subcluster_type: str
        :param subnet_id: Subnet ID of the cluster.
        :type subnet_id: str
        :param resource_preset: Resources preset (CPU+RAM configuration) for the nodes of the cluster.
        :type resource_preset: str
        :param disk_size: Storage size in GiB.
        :type disk_size: int
        :param disk_type: Storage type. Possible options: network-ssd, network-hdd.
        :type disk_type: str
        :param hosts_count: Number of nodes in subcluster.
        :type hosts_count: int
        """
        types = {
            'compute': subcluster_pb.Role.COMPUTENODE,
            'data': subcluster_pb.Role.DATANODE,
        }
        resources = common_pb.Resources(
            resource_preset_id=resource_preset,
            disk_size=disk_size * (1024 ** 3),
            disk_type_id=disk_type,
        ),

        self.log.info(f'Adding subcluster to cluster {cluster_id}')

        req = subcluster_service_pb.CreateSubclusterRequest(
            cluster_id=cluster_id,
            name=name,
            role=types[subcluster_type],
            resources=resources,
            subnet_id=subnet_id,
            hosts_count=hosts_count,
        )

        operation = self.client(subcluster_service_grpc_pb.SubclusterServiceStub).Create(req)
        return self._get_operation_result(operation)

    def change_cluster_description(self, cluster_id, description):
        """
        Changes Yandex.Cloud Data Proc cluster description.

        :param cluster_id: ID of the cluster.
        :type cluster_id: str
        :param description: Description of the cluster.
        :type description: str
        """
        self.log.info(f'Updating cluster {cluster_id}')
        mask = FieldMask(paths=['description'])
        update_req = cluster_service_pb.UpdateClusterRequest(
            cluster_id=cluster_id,
            update_mask=mask,
            description=description,
        )

        operation = self.client(cluster_service_grpc_pb.ClusterServiceStub).Update(update_req)
        return self._get_operation_result(operation)

    def delete_cluster(self, cluster_id):
        """
        Delete Yandex.Cloud Data Proc cluster.
        :param cluster_id: ID of the cluster to remove.
        :type cluster_id: str
        """
        self.log.info(f'Deleting cluster {cluster_id}')
        operation = self.client(cluster_service_grpc_pb.ClusterServiceStub).Delete(
            cluster_service_pb.DeleteClusterRequest(cluster_id=cluster_id))
        return self._get_operation_result(operation)

    def run_hive_job(self,
                     query=None,
                     query_file_uri=None,
                     script_variables=None,
                     continue_on_failure=False,
                     properties=None,
                     cluster_id=None,
                     name='Hive job',
                     ):
        """
        Run Hive job in Yandex.Cloud Data Proc cluster.

        :param query: Hive query.
        :type query: str
        :param query_file_uri: URI of the script that contains Hive queries. Can be placed in HDFS or S3.
        :type query_file_uri: str
        :param properties: A mapping of property names to values, used to configure Hive.
        :type properties: Dist[str, str]
        :param script_variables: Mapping of query variable names to values.
        :type script_variables: Dist[str, str]
        :param continue_on_failure: Whether to continue executing queries if a query fails.
        :type continue_on_failure: boole
        :param cluster_id: ID of the cluster to run job in.
                           Will try to take the ID from Dataproc Hook object if ot specified.
        :type cluster_id: str
        :param name: Name of the job. Used for labeling.
        :type name: str
        """
        cluster_id = cluster_id or self.cluster_id
        if not cluster_id:
            raise AirflowException('Cluster id must be specified.')
        if (query and query_file_uri) or not (query or query_file_uri):
            raise AirflowException('Either query or query_file_uri must be specified.')
        self.log.info(f'Starting Hive job at cluster {cluster_id}')

        hive_job = job_pb.HiveJob(
            query_file_uri=query_file_uri,
            script_variables=script_variables,
            continue_on_failure=continue_on_failure,
            properties=properties,
        )
        if query:
            hive_job = job_pb.HiveJob(
                query_list=job_pb.QueryList(queries=query.split('\n')),
                script_variables=script_variables,
                continue_on_failure=continue_on_failure,
                properties=properties,
            )

        operation = self.client(job_service_grpc_pb.JobServiceStub).Create(
            job_service_pb.CreateJobRequest(
                cluster_id=cluster_id,
                name=name,
                hive_job=hive_job,
            )
        )
        return self._get_operation_result(
            operation,
            response_type=job_pb.Job,
            meta_type=job_service_pb.CreateJobMetadata,
        )

    def run_mapreduce_job(
        self,
        main_class,
        main_jar_file_uri,
        jar_file_uris,
        archive_uris,
        file_uris,
        arguments,
        properties,
        cluster_id=None,
        name='Mapreduce job'
    ):
        """
        Run Mapreduce job in Yandex.Cloud Data Proc cluster.

        :param main_jar_file_uri: URI of jar file with job.
                                  Can be placed in HDFS or S3. Can be specified instead of main_class.
        :type main_class: str
        :param main_class: Name of the main class of the job. Can be specified instead of main_jar_file_uri.
        :type main_class: str
        :param file_uris: URIs of files used in the job. Can be placed in HDFS or S3.
        :type file_uris: List[str]
        :param archive_uris: URIs of archive files used in the job. Can be placed in HDFS or S3.
        :type archive_uris: List[str]
        :param jar_file_uris: URIs of JAR files used in the job. Can be placed in HDFS or S3.
        :type archive_uris: List[str]
        :param properties: Properties for the job.
        :type properties: Dist[str, str]
        :param arguments: Arguments to be passed to the job.
        :type arguments: List[str]
        :param cluster_id: ID of the cluster to run job in.
                           Will try to take the ID from Dataproc Hook object if ot specified.
        :type cluster_id: str
        :param name: Name of the job. Used for labeling.
        :type name: str
        """
        cluster_id = cluster_id or self.cluster_id
        if not cluster_id:
            raise AirflowException('Cluster id must be specified.')
        self.log.info(f'Running Mapreduce job {cluster_id}')
        operation = self.client(job_service_grpc_pb.JobServiceStub).Create(
            job_service_pb.CreateJobRequest(
                cluster_id=cluster_id,
                name=name,
                mapreduce_job=job_pb.MapreduceJob(
                    main_class=main_class,
                    main_jar_file_uri=main_jar_file_uri,
                    jar_file_uris=jar_file_uris,
                    archive_uris=archive_uris,
                    file_uris=file_uris,
                    args=arguments,
                    properties=properties,
                )
            )
        )
        return self._get_operation_result(operation, response_type=job_pb.Job)

    def run_spark_job(self,
                      main_jar_file_uri,
                      main_class,
                      file_uris,
                      archive_uris,
                      jar_file_uris,
                      arguments,
                      properties,
                      cluster_id=None,
                      name='Spark job',
                      ):
        """
        Run Spark job in Yandex.Cloud Data Proc cluster.

        :param main_jar_file_uri: URI of jar file with job. Can be placed in HDFS or S3.
        :type main_class: str
        :param main_class: Name of the main class of the job.
        :type main_class: str
        :param file_uris: URIs of files used in the job. Can be placed in HDFS or S3.
        :type file_uris: List[str]
        :param archive_uris: URIs of archive files used in the job. Can be placed in HDFS or S3.
        :type archive_uris: List[str]
        :param jar_file_uris: URIs of JAR files used in the job. Can be placed in HDFS or S3.
        :type archive_uris: List[str]
        :param properties: Properties for the job.
        :type properties: Dist[str, str]
        :param arguments: Arguments to be passed to the job.
        :type arguments: List[str]
        :param cluster_id: ID of the cluster to run job in.
                           Will try to take the ID from Dataproc Hook object if ot specified.
        :type cluster_id: str
        :param name: Name of the job. Used for labeling.
        :type name: str
        """
        cluster_id = cluster_id or self.cluster_id
        if not cluster_id:
            raise AirflowException('Cluster id must be specified.')
        self.log.info(f'Running Spark job {cluster_id}')
        operation = self.client(job_service_grpc_pb.JobServiceStub).Create(
            job_service_pb.CreateJobRequest(
                cluster_id=cluster_id,
                name=name,
                spark_job=job_pb.SparkJob(
                    main_jar_file_uri=main_jar_file_uri,
                    main_class=main_class,
                    file_uris=file_uris,
                    archive_uris=archive_uris,
                    jar_file_uris=jar_file_uris,
                    args=arguments,
                    properties=properties,
                )
            )
        )
        return self._get_operation_result(operation, response_type=job_pb.Job)

    def run_pyspark_job(self,
                        main_python_file_uri=None,
                        python_file_uris=None,
                        file_uris=None,
                        archive_uris=None,
                        jar_file_uris=None,
                        arguments=None,
                        properties=None,
                        cluster_id=None,
                        name='Pyspark job',
                        ):
        """
        Run Pyspark job in Yandex.Cloud Data Proc cluster.

        :param main_python_file_uri: URI of python file with job. Can be placed in HDFS or S3.
        :type main_python_file_uri: str
        :param python_file_uris: URIs of python files used in the job. Can be placed in HDFS or S3.
        :type python_file_uris: List[str]
        :param file_uris: URIs of files used in the job. Can be placed in HDFS or S3.
        :type file_uris: List[str]
        :param archive_uris: URIs of archive files used in the job. Can be placed in HDFS or S3.
        :type archive_uris: List[str]
        :param jar_file_uris: URIs of JAR files used in the job. Can be placed in HDFS or S3.
        :type archive_uris: List[str]
        :param properties: Properties for the job.
        :type properties: Dist[str, str]
        :param arguments: Arguments to be passed to the job.
        :type arguments: List[str]
        :param cluster_id: ID of the cluster to run job in.
                           Will try to take the ID from Dataproc Hook object if ot specified.
        :type cluster_id: str
        :param name: Name of the job. Used for labeling.
        :type name: str
        """
        cluster_id = cluster_id or self.cluster_id
        if not cluster_id:
            raise AirflowException('Cluster id must be specified.')

        self.log.info(f'Running Pyspark job. Cluster ID: {cluster_id}')
        operation = self.client(job_service_grpc_pb.JobServiceStub).Create(
            job_service_pb.CreateJobRequest(
                cluster_id=cluster_id,
                name=name,
                pyspark_job=job_pb.PysparkJob(
                    main_python_file_uri=main_python_file_uri,
                    python_file_uris=python_file_uris,
                    file_uris=file_uris,
                    archive_uris=archive_uris,
                    jar_file_uris=jar_file_uris,
                    args=arguments,
                    properties=properties,
                )
            )
        )
        return self._get_operation_result(operation, response_type=job_pb.Job)

    def create_cluster(
        self,
        folder_id=None,
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
    ):
        """
        Create Yandex.Cloud Data Proc cluster.

        :param folder_id: ID of the folder in which cluster should be created.
        :type folder_id: str
        :param cluster_name: Cluster name. Must be unique inside the folder.
        :type folder_id: str
        :param cluster_description: Cluster description.
        :type cluster_description: str
        :param cluster_image_version: Cluster image version. Use default.
        :type cluster_image_version: str
        :param ssh_public_keys: List of SSH public keys that will be deployed to created compute instances.
        :type ssh_public_keys: List[str]
        :param subnet_id: ID of the subnetwork. All Data Proc cluster nodes will use one subnetwork.
        :type subnet_id: str
        :param services: List of services that will be installed to the cluster. Possible options:
            HDFS, YARN, MAPREDUCE, HIVE, TEZ, ZOOKEEPER, HBASE, SQOOP, FLUME, SPARK, SPARK, ZEPPELIN, OOZIE
        :type services: List[str]
        :param s3_bucket: Yandex.Cloud S3 bucket to store cluster logs.
                          Jobs will not work if the bicket is not specified.
        :type s3_bucket: str
        :param zone: Availability zone to create cluster in.
                     Currently there are ru-central1-a, ru-central1-b and ru-central1-c.
        :type zone: str
        :param service_account_id: Service account id for the cluster.
                                   Service account can be created inside the folder.
        :type service_account_id: str
        :param masternode_resource_preset: Resources preset (CPU+RAM configuration)
                                           for the master node of the cluster.
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

        :return: Cluster ID
        :rtype: str
        """

        # pylint: disable=too-many-arguments
        # pylint: disable=too-many-locals

        folder_id = folder_id or self.default_folder_id
        if not folder_id:
            raise AirflowException('Folder ID must be specified to create cluster.')

        if not cluster_name:
            random_int = random.randint(0, 999)
            cluster_name = f'dataproc-{random_int}'

        if not subnet_id:
            network_id = self.find_network(folder_id)
            subnet_id = self.find_subnet(folder_id, zone, network_id)

        if not service_account_id:
            service_account_id = self.find_service_account_id(folder_id)

        if not ssh_public_keys:
            if self.default_public_ssh_key:
                ssh_public_keys = (self.default_public_ssh_key, )
            else:
                raise AirflowException('Public ssh keys must be specified.')
        elif isinstance(ssh_public_keys, string_types):
            ssh_public_keys = [ssh_public_keys]

        subclusters = [
            cluster_service_pb.CreateSubclusterConfigSpec(
                name='master',
                role=subcluster_pb.Role.MASTERNODE,
                resources=common_pb.Resources(
                    resource_preset_id=masternode_resource_preset,
                    disk_size=masternode_disk_size * (1024 ** 3),
                    disk_type_id=masternode_disk_type,
                ),
                subnet_id=subnet_id,
                hosts_count=1,
            ),
            cluster_service_pb.CreateSubclusterConfigSpec(
                name='data',
                role=subcluster_pb.Role.DATANODE,
                resources=common_pb.Resources(
                    resource_preset_id=datanode_resource_preset,
                    disk_size=datanode_disk_size * (1024 ** 3),
                    disk_type_id=datanode_disk_type,
                ),
                subnet_id=subnet_id,
                hosts_count=datanode_count,
            ),
        ]

        if computenode_count:
            subclusters.append(
                cluster_service_pb.CreateSubclusterConfigSpec(
                    name='compute',
                    role=subcluster_pb.Role.DATANODE,
                    resources=common_pb.Resources(
                        resource_preset_id=computenode_resource_preset,
                        disk_size=computenode_disk_size * (1024 ** 3),
                        disk_type_id=computenode_disk_type,
                    ),
                    subnet_id=subnet_id,
                    hosts_count=computenode_count,
                )
            )

        request = cluster_service_pb.CreateClusterRequest(
            folder_id=folder_id,
            name=cluster_name,
            description=cluster_description,
            config_spec=cluster_service_pb.CreateClusterConfigSpec(
                version_id=cluster_image_version,
                hadoop=cluster_pb.HadoopConfig(
                    services=services,
                    ssh_public_keys=ssh_public_keys,
                ),
                subclusters_spec=subclusters,
            ),
            zone_id=zone,
            service_account_id=service_account_id,
            bucket=s3_bucket,
        )

        operation = self.client(cluster_service_grpc_pb.ClusterServiceStub).Create(request)
        unpacked_response = self._get_operation_result(
            operation,
            response_type=cluster_pb.Cluster,
            meta_type=cluster_service_pb.CreateClusterMetadata
        )

        if not unpacked_response:
            raise RuntimeError('Cluster was not created.')
        self.cluster_id = unpacked_response.id  # pylint: disable=no-member
        return self.cluster_id
