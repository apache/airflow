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
Example Airflow DAG for Google Cloud Managed Service for Apache Kafka testing Topic operations.

Requirements:
    Operator to create a cluster requires GOOGLE_PROVIDER_NETWORK environmental variable
    that will contain the name of the network that will be used for cluster creation.

    Please, note that if you are running this operator in Google Cloud Composer, this value will be set
    automatically and will not require any additional configuration.
    In other cases, the network in which the cluster will be created should be the same as your machine
    is running in.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.managed_kafka import (
    ManagedKafkaCreateClusterOperator,
    ManagedKafkaCreateTopicOperator,
    ManagedKafkaDeleteClusterOperator,
    ManagedKafkaDeleteTopicOperator,
    ManagedKafkaGetTopicOperator,
    ManagedKafkaListTopicsOperator,
    ManagedKafkaUpdateTopicOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
IS_COMPOSER = bool(os.environ.get("COMPOSER_ENVIRONMENT", ""))
NETWORK = os.environ.get("GOOGLE_PROVIDER_NETWORK") if not IS_COMPOSER else "default"
DAG_ID = "managed_kafka_topic_operations"
LOCATION = "us-central1"

CLUSTER_ID = f"cluster_{DAG_ID}_{ENV_ID}".replace("_", "-")
CLUSTER_CONF = {
    "gcp_config": {
        "access_config": {
            "network_configs": [
                {"subnet": f"projects/{PROJECT_ID}/regions/{LOCATION}/subnetworks/{NETWORK}"},
            ],
        },
    },
    "capacity_config": {
        "vcpu_count": 3,
        "memory_bytes": 3221225472,
    },
}
TOPIC_ID = f"topic_{DAG_ID}_{ENV_ID}".replace("_", "-")
TOPIC_CONF = {
    "partition_count": 3,
    "replication_factor": 3,
}
TOPIC_TO_UPDATE = {
    "partition_count": 30,
    "replication_factor": 3,
}
TOPIC_UPDATE_MASK: dict = {"paths": ["partition_count"]}


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "managed_kafka", "topic"],
) as dag:
    create_cluster = ManagedKafkaCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster=CLUSTER_CONF,
        cluster_id=CLUSTER_ID,
    )

    # [START how_to_cloud_managed_kafka_create_topic_operator]
    create_topic = ManagedKafkaCreateTopicOperator(
        task_id="create_topic",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id=CLUSTER_ID,
        topic_id=TOPIC_ID,
        topic=TOPIC_CONF,
    )
    # [END how_to_cloud_managed_kafka_create_topic_operator]

    # [START how_to_cloud_managed_kafka_update_topic_operator]
    update_topic = ManagedKafkaUpdateTopicOperator(
        task_id="update_topic",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id=CLUSTER_ID,
        topic_id=TOPIC_ID,
        topic=TOPIC_TO_UPDATE,
        update_mask=TOPIC_UPDATE_MASK,
    )
    # [END how_to_cloud_managed_kafka_update_topic_operator]

    # [START how_to_cloud_managed_kafka_get_topic_operator]
    get_topic = ManagedKafkaGetTopicOperator(
        task_id="get_topic",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id=CLUSTER_ID,
        topic_id=TOPIC_ID,
    )
    # [END how_to_cloud_managed_kafka_get_topic_operator]

    # [START how_to_cloud_managed_kafka_delete_topic_operator]
    delete_topic = ManagedKafkaDeleteTopicOperator(
        task_id="delete_topic",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id=CLUSTER_ID,
        topic_id=TOPIC_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END how_to_cloud_managed_kafka_delete_topic_operator]

    delete_cluster = ManagedKafkaDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id=CLUSTER_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # [START how_to_cloud_managed_kafka_list_topic_operator]
    list_topics = ManagedKafkaListTopicsOperator(
        task_id="list_topics",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id=CLUSTER_ID,
    )
    # [END how_to_cloud_managed_kafka_list_topic_operator]

    (
        # TEST SETUP
        create_cluster
        # TEST BODY
        >> create_topic
        >> update_topic
        >> get_topic
        >> list_topics
        >> delete_topic
        # TEST TEARDOWN
        >> delete_cluster
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
