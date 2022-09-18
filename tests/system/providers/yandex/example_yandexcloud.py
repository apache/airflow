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

import os
from datetime import datetime

import yandex.cloud.dataproc.v1.cluster_pb2 as cluster_pb
import yandex.cloud.dataproc.v1.cluster_service_pb2 as cluster_service_pb
import yandex.cloud.dataproc.v1.cluster_service_pb2_grpc as cluster_service_grpc_pb
import yandex.cloud.dataproc.v1.common_pb2 as common_pb
import yandex.cloud.dataproc.v1.job_pb2 as job_pb
import yandex.cloud.dataproc.v1.job_service_pb2 as job_service_pb
import yandex.cloud.dataproc.v1.job_service_pb2_grpc as job_service_grpc_pb
import yandex.cloud.dataproc.v1.subcluster_pb2 as subcluster_pb
from google.protobuf.json_format import MessageToDict

from airflow import DAG
from airflow.decorators import task
from airflow.providers.yandex.hooks.yandex import YandexCloudBaseHook

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = 'example_yandexcloud_hook'

# Fill it with your identifiers
YC_S3_BUCKET_NAME = ''  # Fill to use S3 instead of HFDS
YC_FOLDER_ID = None  # Fill to override default YC folder from connection data
YC_ZONE_NAME = 'ru-central1-b'
YC_SUBNET_ID = None  # Fill if you have more than one VPC subnet in given folder and zone
YC_SERVICE_ACCOUNT_ID = None  # Fill if you have more than one YC service account in given folder


def create_cluster_request(
    folder_id: str,
    cluster_name: str,
    cluster_desc: str,
    zone: str,
    subnet_id: str,
    service_account_id: str,
    ssh_public_key: str,
    resources: common_pb.Resources,
):
    return cluster_service_pb.CreateClusterRequest(
        folder_id=folder_id,
        name=cluster_name,
        description=cluster_desc,
        bucket=YC_S3_BUCKET_NAME,
        config_spec=cluster_service_pb.CreateClusterConfigSpec(
            hadoop=cluster_pb.HadoopConfig(
                services=('SPARK', 'YARN'),
                ssh_public_keys=[ssh_public_key],
            ),
            subclusters_spec=[
                cluster_service_pb.CreateSubclusterConfigSpec(
                    name='master',
                    role=subcluster_pb.Role.MASTERNODE,
                    resources=resources,
                    subnet_id=subnet_id,
                    hosts_count=1,
                ),
                cluster_service_pb.CreateSubclusterConfigSpec(
                    name='compute',
                    role=subcluster_pb.Role.COMPUTENODE,
                    resources=resources,
                    subnet_id=subnet_id,
                    hosts_count=1,
                ),
            ],
        ),
        zone_id=zone,
        service_account_id=service_account_id,
    )


@task
def create_cluster(
    yandex_conn_id: str | None = None,
    folder_id: str | None = None,
    network_id: str | None = None,
    subnet_id: str | None = None,
    zone: str = YC_ZONE_NAME,
    service_account_id: str | None = None,
    ssh_public_key: str | None = None,
    *,
    dag: DAG | None = None,
    ts_nodash: str | None = None,
) -> str:
    hook = YandexCloudBaseHook(yandex_conn_id=yandex_conn_id)
    folder_id = folder_id or hook.default_folder_id
    if subnet_id is None:
        network_id = network_id or hook.sdk.helpers.find_network_id(folder_id)
        subnet_id = hook.sdk.helpers.find_subnet_id(folder_id=folder_id, zone_id=zone, network_id=network_id)
    service_account_id = service_account_id or hook.sdk.helpers.find_service_account_id()
    ssh_public_key = ssh_public_key or hook.default_public_ssh_key

    dag_id = dag and dag.dag_id or 'dag'

    request = create_cluster_request(
        folder_id=folder_id,
        subnet_id=subnet_id,
        zone=zone,
        cluster_name=f'airflow_{dag_id}_{ts_nodash}'[:62],
        cluster_desc='Created via Airflow custom hook task',
        service_account_id=service_account_id,
        ssh_public_key=ssh_public_key,
        resources=common_pb.Resources(
            resource_preset_id='s2.micro',
            disk_type_id='network-ssd',
        ),
    )
    operation = hook.sdk.client(cluster_service_grpc_pb.ClusterServiceStub).Create(request)
    operation_result = hook.sdk.wait_operation_and_get_result(
        operation, response_type=cluster_pb.Cluster, meta_type=cluster_service_pb.CreateClusterMetadata
    )
    return operation_result.response.id


@task
def run_spark_job(
    cluster_id: str,
    yandex_conn_id: str | None = None,
):
    hook = YandexCloudBaseHook(yandex_conn_id=yandex_conn_id)

    request = job_service_pb.CreateJobRequest(
        cluster_id=cluster_id,
        name='Spark job: Find total urban population in distribution by country',
        spark_job=job_pb.SparkJob(
            main_jar_file_uri='file:///usr/lib/spark/examples/jars/spark-examples.jar',
            main_class='org.apache.spark.examples.SparkPi',
            args=['1000'],
        ),
    )
    operation = hook.sdk.client(job_service_grpc_pb.JobServiceStub).Create(request)
    operation_result = hook.sdk.wait_operation_and_get_result(
        operation, response_type=job_pb.Job, meta_type=job_service_pb.CreateJobMetadata
    )
    return MessageToDict(operation_result.response)


@task(trigger_rule='all_done')
def delete_cluster(
    cluster_id: str,
    yandex_conn_id: str | None = None,
):
    hook = YandexCloudBaseHook(yandex_conn_id=yandex_conn_id)

    operation = hook.sdk.client(cluster_service_grpc_pb.ClusterServiceStub).Delete(
        cluster_service_pb.DeleteClusterRequest(cluster_id=cluster_id)
    )
    hook.sdk.wait_operation_and_get_result(
        operation,
        meta_type=cluster_service_pb.DeleteClusterMetadata,
    )


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
) as dag:
    cluster_id = create_cluster(
        folder_id=YC_FOLDER_ID,
        subnet_id=YC_SUBNET_ID,
        zone=YC_ZONE_NAME,
        service_account_id=YC_SERVICE_ACCOUNT_ID,
    )
    spark_job = run_spark_job(cluster_id=cluster_id)
    delete_task = delete_cluster(cluster_id=cluster_id)

    spark_job >> delete_task

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
