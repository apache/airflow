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
Example Airflow DAG for Google Cloud Managed Service for Apache Kafka testing Cluster operations.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.managed_kafka import (
    ManagedKafkaCreateClusterOperator,
    ManagedKafkaDeleteClusterOperator,
    ManagedKafkaGetClusterOperator,
    ManagedKafkaListClustersOperator,
    ManagedKafkaUpdateClusterOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "managed_kafka_cluster_operations"
LOCATION = "us-central1"

CLUSTER_ID = f"cluster_{DAG_ID}_{ENV_ID}".replace("_", "-")
CLUSTER_CONF = {
    "gcp_config": {
        "access_config": {
            "network_configs": [
                {"subnet": f"projects/{PROJECT_ID}/regions/{LOCATION}/subnetworks/default"},
            ],
        },
    },
    "capacity_config": {
        "vcpu_count": 3,
        "memory_bytes": 3221225472,
    },
}
CLUSTER_TO_UPDATE = {
    "capacity_config": {
        "vcpu_count": 3,
        "memory_bytes": 8589934592,
    }
}
CLUSTER_UPDATE_MASK = {"paths": ["capacity_config"]}


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "managed_kafka", "cluster"],
) as dag:
    # [START how_to_cloud_managed_kafka_create_cluster_operator]
    create_cluster = ManagedKafkaCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster=CLUSTER_CONF,
        cluster_id=CLUSTER_ID,
    )
    # [END how_to_cloud_managed_kafka_create_cluster_operator]

    # [START how_to_cloud_managed_kafka_update_cluster_operator]
    update_cluster = ManagedKafkaUpdateClusterOperator(
        task_id="update_cluster",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id=CLUSTER_ID,
        cluster=CLUSTER_TO_UPDATE,
        update_mask=CLUSTER_UPDATE_MASK,
    )
    # [END how_to_cloud_managed_kafka_update_cluster_operator]

    # [START how_to_cloud_managed_kafka_get_cluster_operator]
    get_cluster = ManagedKafkaGetClusterOperator(
        task_id="get_cluster",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id=CLUSTER_ID,
    )
    # [END how_to_cloud_managed_kafka_get_cluster_operator]

    # [START how_to_cloud_managed_kafka_delete_cluster_operator]
    delete_cluster = ManagedKafkaDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id=CLUSTER_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END how_to_cloud_managed_kafka_delete_cluster_operator]

    # [START how_to_cloud_managed_kafka_list_cluster_operator]
    list_clusters = ManagedKafkaListClustersOperator(
        task_id="list_clusters",
        project_id=PROJECT_ID,
        location=LOCATION,
    )
    # [END how_to_cloud_managed_kafka_list_cluster_operator]

    (
        [
            create_cluster >> update_cluster >> get_cluster >> delete_cluster,
            list_clusters,
        ]
    )

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
