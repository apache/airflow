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
Example Airflow DAG for Dataproc batch operators.
"""

from __future__ import annotations

import os
from datetime import datetime

from google.api_core.retry import Retry

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateBatchOperator,
    DataprocCreateClusterOperator,
    DataprocDeleteBatchOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from airflow.providers.google.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "dataproc_batch_ps"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("-", "_")
REGION = "us-east4"
CLUSTER_NAME_BASE = f"cluster-{DAG_ID}".replace("_", "-")
CLUSTER_NAME_FULL = CLUSTER_NAME_BASE + f"-{ENV_ID}".replace("_", "-")
CLUSTER_NAME = CLUSTER_NAME_BASE if len(CLUSTER_NAME_FULL) >= 33 else CLUSTER_NAME_FULL
BATCH_ID = f"batch-{ENV_ID}-{DAG_ID}".replace("_", "-")

CLUSTER_GENERATOR_CONFIG_FOR_PHS = ClusterGenerator(
    project_id=PROJECT_ID,
    region=REGION,
    master_machine_type="n1-standard-4",
    worker_machine_type="n1-standard-4",
    num_workers=0,
    properties={
        "spark:spark.history.fs.logDirectory": f"gs://{BUCKET_NAME}",
    },
    enable_component_gateway=True,
).make()
BATCH_CONFIG_WITH_PHS = {
    "spark_batch": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    },
    "environment_config": {
        "peripherals_config": {
            "spark_history_server_config": {
                "dataproc_cluster": f"projects/{PROJECT_ID}/regions/{REGION}/clusters/{CLUSTER_NAME}"
            }
        }
    },
}


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataproc", "batch", "persistent"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    # [START how_to_cloud_dataproc_create_cluster_for_persistent_history_server]
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster_for_phs",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_GENERATOR_CONFIG_FOR_PHS,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        retry=Retry(maximum=100.0, initial=10.0, multiplier=1.0),
        num_retries_if_resource_is_not_ready=3,
    )
    # [END how_to_cloud_dataproc_create_cluster_for_persistent_history_server]

    # [START how_to_cloud_dataproc_create_batch_operator_with_persistent_history_server]
    create_batch = DataprocCreateBatchOperator(
        task_id="create_batch_with_phs",
        project_id=PROJECT_ID,
        region=REGION,
        batch=BATCH_CONFIG_WITH_PHS,
        batch_id=BATCH_ID,
        result_retry=Retry(maximum=100.0, initial=10.0, multiplier=1.0),
        num_retries_if_resource_is_not_ready=3,
    )
    # [END how_to_cloud_dataproc_create_batch_operator_with_persistent_history_server]

    delete_batch = DataprocDeleteBatchOperator(
        task_id="delete_batch",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=BATCH_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        >> create_cluster
        # TEST BODY
        >> create_batch
        # TEST TEARDOWN
        >> delete_batch
        >> delete_cluster
        >> delete_bucket
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
