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
Example Airflow DAG that uses Google PubSub services.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateSubscriptionOperator,
    PubSubCreateTopicOperator,
    PubSubDeleteSubscriptionOperator,
    PubSubDeleteTopicOperator,
    PubSubPublishMessageOperator,
    PubSubPullOperator,
)
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.standard.core.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")

DAG_ID = "pubsub"

TOPIC_ID = f"topic-{DAG_ID}-{ENV_ID}"
MESSAGE = {"data": b"Tool", "attributes": {"name": "wrench", "mass": "1.3kg", "count": "3"}}
MESSAGE_TWO = {"data": b"Tool", "attributes": {"name": "wrench", "mass": "1.2kg", "count": "2"}}

# [START howto_operator_gcp_pubsub_pull_messages_result_cmd]
echo_cmd = """
{% for m in task_instance.xcom_pull('pull_messages') %}
    echo "AckID: {{ m.get('ackId') }}, Base64-Encoded: {{ m.get('message') }}"
{% endfor %}
"""
# [END howto_operator_gcp_pubsub_pull_messages_result_cmd]

with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    # [START howto_operator_gcp_pubsub_create_topic]
    create_topic = PubSubCreateTopicOperator(
        task_id="create_topic", topic=TOPIC_ID, project_id=PROJECT_ID, fail_if_exists=False
    )
    # [END howto_operator_gcp_pubsub_create_topic]

    # [START howto_operator_gcp_pubsub_create_subscription]
    subscribe_task = PubSubCreateSubscriptionOperator(
        task_id="subscribe_task", project_id=PROJECT_ID, topic=TOPIC_ID
    )
    # [END howto_operator_gcp_pubsub_create_subscription]

    # [START howto_operator_gcp_pubsub_pull_message_with_sensor]
    subscription = subscribe_task.output

    pull_messages = PubSubPullSensor(
        task_id="pull_messages",
        ack_messages=True,
        project_id=PROJECT_ID,
        subscription=subscription,
    )
    # [END howto_operator_gcp_pubsub_pull_message_with_sensor]

    # [START howto_operator_gcp_pubsub_pull_messages_result]
    pull_messages_result = BashOperator(task_id="pull_messages_result", bash_command=echo_cmd)
    # [END howto_operator_gcp_pubsub_pull_messages_result]

    # [START howto_operator_gcp_pubsub_pull_message_with_operator]

    pull_messages_operator = PubSubPullOperator(
        task_id="pull_messages_operator",
        ack_messages=True,
        project_id=PROJECT_ID,
        subscription=subscription,
    )
    # [END howto_operator_gcp_pubsub_pull_message_with_operator]

    # [START howto_operator_gcp_pubsub_publish]
    publish_task = PubSubPublishMessageOperator(
        task_id="publish_task",
        project_id=PROJECT_ID,
        topic=TOPIC_ID,
        messages=[MESSAGE, MESSAGE],
    )
    # [END howto_operator_gcp_pubsub_publish]

    publish_task2 = PubSubPublishMessageOperator(
        task_id="publish_task2",
        project_id=PROJECT_ID,
        topic=TOPIC_ID,
        messages=[MESSAGE_TWO, MESSAGE_TWO],
    )

    # [START howto_operator_gcp_pubsub_unsubscribe]
    unsubscribe_task = PubSubDeleteSubscriptionOperator(
        task_id="unsubscribe_task",
        project_id=PROJECT_ID,
        subscription=subscription,
    )
    # [END howto_operator_gcp_pubsub_unsubscribe]
    unsubscribe_task.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_operator_gcp_pubsub_delete_topic]
    delete_topic = PubSubDeleteTopicOperator(task_id="delete_topic", topic=TOPIC_ID, project_id=PROJECT_ID)
    # [END howto_operator_gcp_pubsub_delete_topic]
    delete_topic.trigger_rule = TriggerRule.ALL_DONE

    (
        create_topic
        >> subscribe_task
        >> publish_task
        >> pull_messages
        >> pull_messages_result
        >> publish_task2
        >> pull_messages_operator
        >> unsubscribe_task
        >> delete_topic
    )

    # Task dependencies created via `XComArgs`:
    #   subscribe_task >> pull_messages
    #   subscribe_task >> pull_messages_operator
    #   subscribe_task >> unsubscribe_task

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
