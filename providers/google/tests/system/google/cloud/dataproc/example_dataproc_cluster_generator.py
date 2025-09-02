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
Example Airflow DAG testing Dataproc
operators for managing a cluster and submitting jobs.
"""

from __future__ import annotations

import os
from datetime import datetime

from google.api_core.retry import Retry

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSSynchronizeBucketsOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "dataproc_cluster_generation"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
INIT_FILE = "pip-install.sh"
GCS_INIT_FILE = f"gs://{RESOURCE_DATA_BUCKET}/dataproc/{INIT_FILE}"

CLUSTER_NAME_BASE = f"cluster-{DAG_ID}".replace("_", "-")
CLUSTER_NAME_FULL = CLUSTER_NAME_BASE + f"-{ENV_ID}".replace("_", "-")
CLUSTER_NAME = CLUSTER_NAME_BASE if len(CLUSTER_NAME_FULL) >= 33 else CLUSTER_NAME_FULL

REGION = "us-east4"
ZONE = "us-east4-a"

# Cluster definition: Generating Cluster Config for DataprocCreateClusterOperator
# [START how_to_cloud_dataproc_create_cluster_generate_cluster_config]
CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    zone=ZONE,
    master_machine_type="n1-standard-4",
    master_disk_size=32,
    worker_machine_type="n1-standard-4",
    worker_disk_size=32,
    num_workers=2,
    storage_bucket=BUCKET_NAME,
    init_actions_uris=[GCS_INIT_FILE],
    metadata={"PIP_PACKAGES": "pyyaml requests pandas openpyxl"},
    num_preemptible_workers=1,
    preemptibility="PREEMPTIBLE",
    internal_ip_only=False,
    cluster_tier="CLUSTER_TIER_STANDARD",
).make()

# [END how_to_cloud_dataproc_create_cluster_generate_cluster_config]


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataproc"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    move_init_file = GCSSynchronizeBucketsOperator(
        task_id="move_init_file",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="dataproc",
        destination_bucket=BUCKET_NAME,
        destination_object="dataproc",
        recursive=True,
    )

    # [START how_to_cloud_dataproc_create_cluster_generate_cluster_config_operator]

    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        cluster_name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        region=REGION,
        cluster_config=CLUSTER_GENERATOR_CONFIG,
        retry=Retry(maximum=100.0, initial=10.0, multiplier=1.0),
        num_retries_if_resource_is_not_ready=3,
    )

    # [END how_to_cloud_dataproc_create_cluster_generate_cluster_config_operator]

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
        >> move_init_file
        # TEST BODY
        >> create_dataproc_cluster
        # TEST TEARDOWN
        >> [delete_cluster, delete_bucket]
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
