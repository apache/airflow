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
"""
Example Airflow DAG that shows how to check Hive partitions existence with Dataproc Metastore Sensor.

Note that Metastore service must be configured to use gRPC endpoints.
"""

from __future__ import annotations

import datetime
import os

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.gcs import _parse_gcs_url
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.dataproc_metastore import (
    DataprocMetastoreCreateServiceOperator,
    DataprocMetastoreDeleteServiceOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteBucketOperator
from airflow.providers.google.cloud.sensors.dataproc_metastore import (
    MetastoreHivePartitionSensor,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "hive_partition_sensor"
PROJECT_ID = (
    os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
)
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
REGION = "europe-west1"
NETWORK = "default"

METASTORE_SERVICE_ID = f"metastore-{DAG_ID}-{ENV_ID}".replace("_", "-")
METASTORE_TIMEOUT = 2400
METASTORE_SERVICE = {
    "name": METASTORE_SERVICE_ID,
    "hive_metastore_config": {
        "endpoint_protocol": "GRPC",
    },
    "network": f"projects/{PROJECT_ID}/global/networks/{NETWORK}",
}
METASTORE_SERVICE_QFN = (
    f"projects/{PROJECT_ID}/locations/{REGION}/services/{METASTORE_SERVICE_ID}"
)
DATAPROC_CLUSTER_NAME = f"cluster-{DAG_ID}-{ENV_ID}".replace("_", "-")
DATAPROC_CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "metastore_config": {
        "dataproc_metastore_service": METASTORE_SERVICE_QFN,
    },
    "gce_cluster_config": {
        "service_account_scopes": [
            "https://www.googleapis.com/auth/cloud-platform",
        ],
    },
}

TABLE_NAME = "transactions_partitioned"
COLUMN = "TransactionType"
PARTITION_1 = f"{COLUMN}=credit".lower()
PARTITION_2 = f"{COLUMN}=debit".lower()
SOURCE_DATA_BUCKET = "airflow-system-tests-resources"
SOURCE_DATA_PATH = "dataproc/hive"
SOURCE_DATA_FILE_NAME = "part-00000.parquet"
EXTERNAL_TABLE_BUCKET = (
    "{{task_instance.xcom_pull(task_ids='get_hive_warehouse_bucket_task', key='bucket')}}"
)
QUERY_CREATE_EXTERNAL_TABLE = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS transactions
(SubmissionDate DATE, TransactionAmount DOUBLE, TransactionType STRING)
STORED AS PARQUET
LOCATION 'gs://{EXTERNAL_TABLE_BUCKET}/{SOURCE_DATA_PATH}';
"""
QUERY_CREATE_PARTITIONED_TABLE = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {TABLE_NAME}
(SubmissionDate DATE, TransactionAmount DOUBLE)
PARTITIONED BY ({COLUMN} STRING);
"""
QUERY_COPY_DATA_WITH_PARTITIONS = f"""
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE {TABLE_NAME} PARTITION ({COLUMN})
SELECT SubmissionDate,TransactionAmount,TransactionType FROM transactions;
"""

with DAG(
    DAG_ID,
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example", "dataproc", "metastore", "partition", "hive", "sensor"],
) as dag:
    create_metastore_service = DataprocMetastoreCreateServiceOperator(
        task_id="create_metastore_service",
        region=REGION,
        project_id=PROJECT_ID,
        service=METASTORE_SERVICE,
        service_id=METASTORE_SERVICE_ID,
        timeout=METASTORE_TIMEOUT,
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        cluster_name=DATAPROC_CLUSTER_NAME,
        project_id=PROJECT_ID,
        cluster_config=DATAPROC_CLUSTER_CONFIG,
        region=REGION,
    )

    @task(task_id="get_hive_warehouse_bucket_task")
    def get_hive_warehouse_bucket(**kwargs):
        """Return Hive Metastore Warehouse GCS bucket name."""
        ti = kwargs["ti"]
        metastore_service: dict = ti.xcom_pull(task_ids="create_metastore_service")
        config_overrides: dict = metastore_service["hive_metastore_config"][
            "config_overrides"
        ]
        destination_dir: str = config_overrides["hive.metastore.warehouse.dir"]
        bucket, _ = _parse_gcs_url(destination_dir)
        ti.xcom_push(key="bucket", value=bucket)

    get_hive_warehouse_bucket_task = get_hive_warehouse_bucket()

    copy_source_data = GCSToGCSOperator(
        task_id="copy_source_data",
        source_bucket=SOURCE_DATA_BUCKET,
        source_object=f"{SOURCE_DATA_PATH}/{SOURCE_DATA_FILE_NAME}",
        destination_bucket=EXTERNAL_TABLE_BUCKET,
        destination_object=f"{SOURCE_DATA_PATH}/{SOURCE_DATA_FILE_NAME}",
    )

    create_external_table = DataprocSubmitJobOperator(
        task_id="create_external_table",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
            "hive_job": {"query_list": {"queries": [QUERY_CREATE_EXTERNAL_TABLE]}},
        },
        region=REGION,
        project_id=PROJECT_ID,
    )

    create_partitioned_table = DataprocSubmitJobOperator(
        task_id="create_partitioned_table",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
            "hive_job": {"query_list": {"queries": [QUERY_CREATE_PARTITIONED_TABLE]}},
        },
        region=REGION,
        project_id=PROJECT_ID,
    )

    partition_data = DataprocSubmitJobOperator(
        task_id="partition_data",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
            "hive_job": {"query_list": {"queries": [QUERY_COPY_DATA_WITH_PARTITIONS]}},
        },
        region=REGION,
        project_id=PROJECT_ID,
    )

    # [START how_to_cloud_dataproc_metastore_hive_partition_sensor]
    hive_partition_sensor = MetastoreHivePartitionSensor(
        task_id="hive_partition_sensor",
        service_id=METASTORE_SERVICE_ID,
        region=REGION,
        table=TABLE_NAME,
        partitions=[PARTITION_1, PARTITION_2],
    )
    # [END how_to_cloud_dataproc_metastore_hive_partition_sensor]

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        cluster_name=DATAPROC_CLUSTER_NAME,
        project_id=PROJECT_ID,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_metastore_service = DataprocMetastoreDeleteServiceOperator(
        task_id="delete_metastore_service",
        service_id=METASTORE_SERVICE_ID,
        project_id=PROJECT_ID,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_warehouse_bucket = GCSDeleteBucketOperator(
        task_id="delete_warehouse_bucket",
        bucket_name=EXTERNAL_TABLE_BUCKET,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_metastore_service
        >> create_cluster
        >> get_hive_warehouse_bucket_task
        >> copy_source_data
        >> create_external_table
        >> create_partitioned_table
        # TEST BODY
        >> partition_data
        >> hive_partition_sensor
        # TEST TEARDOWN
        >> [delete_dataproc_cluster, delete_metastore_service, delete_warehouse_bucket]
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
