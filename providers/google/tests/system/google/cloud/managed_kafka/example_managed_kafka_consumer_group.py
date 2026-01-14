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

import json
import logging
import os
import random
from datetime import datetime
from typing import Any

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from airflow.models.dag import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.google.cloud.operators.managed_kafka import (
    ManagedKafkaCreateClusterOperator,
    ManagedKafkaCreateTopicOperator,
    ManagedKafkaDeleteClusterOperator,
    ManagedKafkaDeleteConsumerGroupOperator,
    ManagedKafkaGetConsumerGroupOperator,
    ManagedKafkaListConsumerGroupsOperator,
    ManagedKafkaUpdateConsumerGroupOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.google.gcp_api_client_helpers import create_airflow_connection

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
IS_COMPOSER = bool(os.environ.get("COMPOSER_ENVIRONMENT", ""))
NETWORK = os.environ.get("GOOGLE_PROVIDER_NETWORK") if not IS_COMPOSER else "default"
DAG_ID = "managed_kafka_consumer_group_operations"
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
CONSUMER_GROUP_ID = f"consumer_group_{DAG_ID}_{ENV_ID}".replace("_", "-")
CONNECTION_ID = f"connection_{DAG_ID}_{ENV_ID}"
PORT = "9092"
BOOTSTRAP_URL = f"bootstrap.{CLUSTER_ID}.{LOCATION}.managedkafka.{PROJECT_ID}.cloud.goog:{PORT}"

log = logging.getLogger(__name__)


def producer():
    """Produce and submit 10 messages"""

    for i in range(10):
        now = datetime.now()
        datetime_string = now.strftime("%Y-%m-%d %H:%M:%S")

        message_data = {"random_id": f"{ENV_ID}_{random.randint(1, 100)}", "date_time": datetime_string}

        yield (
            json.dumps(i),
            json.dumps(message_data),
        )


def consumer(message):
    "Take in consumed messages and print its contents to the logs."

    message_content = json.loads(message.value())
    random_id = message_content["random_id"]
    date_time = message_content["date_time"]
    log.info("id: %s, date_time: %s", random_id, date_time)


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "managed_kafka", "consumer_group"],
) as dag:
    create_cluster = ManagedKafkaCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster=CLUSTER_CONF,
        cluster_id=CLUSTER_ID,
    )

    create_topic = ManagedKafkaCreateTopicOperator(
        task_id="create_topic",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id=CLUSTER_ID,
        topic_id=TOPIC_ID,
        topic=TOPIC_CONF,
    )

    @task
    def create_connection(connection_id: str):
        conn_extra = {
            "bootstrap.servers": BOOTSTRAP_URL,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "OAUTHBEARER",
            "group.id": CONSUMER_GROUP_ID,
        }
        conn_extra_json = json.dumps(conn_extra)
        connection: dict[str, Any] = {"conn_type": "kafka", "extra": conn_extra_json}
        create_airflow_connection(
            connection_id=connection_id,
            connection_conf=connection,
            is_composer=IS_COMPOSER,
        )

    create_connection_task = create_connection(connection_id=CONNECTION_ID)

    # [START how_to_cloud_managed_kafka_produce_to_topic_operator]
    produce_to_topic = ProduceToTopicOperator(
        task_id="produce_to_topic",
        kafka_config_id=CONNECTION_ID,
        topic=TOPIC_ID,
        producer_function=producer,
        poll_timeout=10,
    )
    # [END how_to_cloud_managed_kafka_produce_to_topic_operator]

    # [START how_to_cloud_managed_kafka_consume_from_topic_operator]
    consume_from_topic = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        kafka_config_id=CONNECTION_ID,
        topics=[TOPIC_ID],
        apply_function=consumer,
        poll_timeout=20,
        max_messages=20,
        max_batch_size=20,
    )
    # [END how_to_cloud_managed_kafka_consume_from_topic_operator]

    # [START how_to_cloud_managed_kafka_update_consumer_group_operator]
    update_consumer_group = ManagedKafkaUpdateConsumerGroupOperator(
        task_id="update_consumer_group",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id=CLUSTER_ID,
        consumer_group_id=CONSUMER_GROUP_ID,
        consumer_group={
            "topics": {},
        },
        update_mask={"paths": ["topics"]},
    )
    # [END how_to_cloud_managed_kafka_update_consumer_group_operator]

    # [START how_to_cloud_managed_kafka_get_consumer_group_operator]
    get_consumer_group = ManagedKafkaGetConsumerGroupOperator(
        task_id="get_consumer_group",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id=CLUSTER_ID,
        consumer_group_id=CONSUMER_GROUP_ID,
    )
    # [END how_to_cloud_managed_kafka_get_consumer_group_operator]

    # [START how_to_cloud_managed_kafka_delete_consumer_group_operator]
    delete_consumer_group = ManagedKafkaDeleteConsumerGroupOperator(
        task_id="delete_consumer_group",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id=CLUSTER_ID,
        consumer_group_id=CONSUMER_GROUP_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END how_to_cloud_managed_kafka_delete_consumer_group_operator]

    delete_cluster = ManagedKafkaDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id=CLUSTER_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # [START how_to_cloud_managed_kafka_list_consumer_group_operator]
    list_consumer_groups = ManagedKafkaListConsumerGroupsOperator(
        task_id="list_consumer_groups",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_id=CLUSTER_ID,
    )
    # [END how_to_cloud_managed_kafka_list_consumer_group_operator]

    (
        # TEST SETUP
        create_cluster
        >> create_topic
        >> create_connection_task
        >> produce_to_topic
        >> consume_from_topic
        # TEST BODY
        >> update_consumer_group
        >> get_consumer_group
        >> list_consumer_groups
        >> delete_consumer_group
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
