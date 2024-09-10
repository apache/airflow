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
Example Airflow DAG for DataprocUpdateClusterOperator.
"""

from __future__ import annotations

import os
from datetime import datetime

from google.api_core.retry import Retry

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocUpdateClusterOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "dataproc_cluster_def"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

CLUSTER_NAME_BASE = f"cluster-{DAG_ID}".replace("_", "-")
CLUSTER_NAME_FULL = CLUSTER_NAME_BASE + f"-{ENV_ID}".replace("_", "-")
CLUSTER_NAME = CLUSTER_NAME_BASE if len(CLUSTER_NAME_FULL) >= 33 else CLUSTER_NAME_FULL
REGION = "europe-north1"


# Cluster definition
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "secondary_worker_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 32,
        },
        "is_preemptible": True,
        "preemptibility": "PREEMPTIBLE",
    },
}

# Update options
# [START how_to_cloud_dataproc_updatemask_cluster_operator]
CLUSTER_UPDATE = {
    "config": {"worker_config": {"num_instances": 3}, "secondary_worker_config": {"num_instances": 3}}
}
UPDATE_MASK = {
    "paths": ["config.worker_config.num_instances", "config.secondary_worker_config.num_instances"]
}
# [END how_to_cloud_dataproc_updatemask_cluster_operator]

TIMEOUT = {"seconds": 1 * 24 * 60 * 60}


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataproc", "deferrable"],
) as dag:
    # [START how_to_cloud_dataproc_create_cluster_operator_async]
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        deferrable=True,
        retry=Retry(maximum=100.0, initial=10.0, multiplier=1.0),
    )
    # [END how_to_cloud_dataproc_create_cluster_operator_async]

    # [START how_to_cloud_dataproc_update_cluster_operator_async]
    update_cluster = DataprocUpdateClusterOperator(
        task_id="update_cluster",
        cluster_name=CLUSTER_NAME,
        cluster=CLUSTER_UPDATE,
        update_mask=UPDATE_MASK,
        graceful_decommission_timeout=TIMEOUT,
        project_id=PROJECT_ID,
        region=REGION,
        deferrable=True,
        retry=Retry(maximum=100.0, initial=10.0, multiplier=1.0),
    )
    # [END how_to_cloud_dataproc_update_cluster_operator_async]

    # [START how_to_cloud_dataproc_delete_cluster_operator_async]
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
        deferrable=True,
    )
    # [END how_to_cloud_dataproc_delete_cluster_operator_async]

    (
        # TEST SETUP
        create_cluster
        # TEST BODY
        >> update_cluster
        # TEST TEARDOWN
        >> delete_cluster
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
