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
import os
from datetime import datetime

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateSubscriptionOperator,
    PubSubCreateTopicOperator,
    PubSubDeleteSubscriptionOperator,
    PubSubDeleteTopicOperator,
    PubSubPublishMessageOperator,
    PubSubPullOperator,
)
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor

START_DATE = datetime(2021, 1, 1)

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-project-id")
TOPIC_FOR_SENSOR_DAG = os.environ.get("GCP_PUBSUB_SENSOR_TOPIC", "PubSubSensorTestTopic")
TOPIC_FOR_OPERATOR_DAG = os.environ.get("GCP_PUBSUB_OPERATOR_TOPIC", "PubSubOperatorTestTopic")
MESSAGE = {"data": b"Tool", "attributes": {"name": "wrench", "mass": "1.3kg", "count": "3"}}

# [START howto_operator_gcp_pubsub_pull_messages_result_cmd]
echo_cmd = """
{% for m in task_instance.xcom_pull('pull_messages') %}
    echo "AckID: {{ m.get('ackId') }}, Base64-Encoded: {{ m.get('message') }}"
{% endfor %}
"""
# [END howto_operator_gcp_pubsub_pull_messages_result_cmd]

with models.DAG(
    "example_gcp_pubsub_sensor",
    schedule_interval='@once',  # Override to match your needs
    start_date=START_DATE,
    catchup=False,
) as example_sensor_dag:
    # [START howto_operator_gcp_pubsub_create_topic]
    create_topic = PubSubCreateTopicOperator(
        task_id="create_topic", topic=TOPIC_FOR_SENSOR_DAG, project_id=GCP_PROJECT_ID, fail_if_exists=False
    )
    # [END howto_operator_gcp_pubsub_create_topic]

    # [START howto_operator_gcp_pubsub_create_subscription]
    subscribe_task = PubSubCreateSubscriptionOperator(
        task_id="subscribe_task", project_id=GCP_PROJECT_ID, topic=TOPIC_FOR_SENSOR_DAG
    )
    # [END howto_operator_gcp_pubsub_create_subscription]

    # [START howto_operator_gcp_pubsub_pull_message_with_sensor]
    subscription = subscribe_task.output

    pull_messages = PubSubPullSensor(
        task_id="pull_messages",
        ack_messages=True,
        project_id=GCP_PROJECT_ID,
        subscription=subscription,
    )
    # [END howto_operator_gcp_pubsub_pull_message_with_sensor]

    # [START howto_operator_gcp_pubsub_pull_messages_result]
    pull_messages_result = BashOperator(task_id="pull_messages_result", bash_command=echo_cmd)
    # [END howto_operator_gcp_pubsub_pull_messages_result]

    # [START howto_operator_gcp_pubsub_publish]
    publish_task = PubSubPublishMessageOperator(
        task_id="publish_task",
        project_id=GCP_PROJECT_ID,
        topic=TOPIC_FOR_SENSOR_DAG,
        messages=[MESSAGE] * 10,
    )
    # [END howto_operator_gcp_pubsub_publish]

    # [START howto_operator_gcp_pubsub_unsubscribe]
    unsubscribe_task = PubSubDeleteSubscriptionOperator(
        task_id="unsubscribe_task",
        project_id=GCP_PROJECT_ID,
        subscription=subscription,
    )
    # [END howto_operator_gcp_pubsub_unsubscribe]

    # [START howto_operator_gcp_pubsub_delete_topic]
    delete_topic = PubSubDeleteTopicOperator(
        task_id="delete_topic", topic=TOPIC_FOR_SENSOR_DAG, project_id=GCP_PROJECT_ID
    )
    # [END howto_operator_gcp_pubsub_delete_topic]

    create_topic >> subscribe_task >> publish_task
    pull_messages >> pull_messages_result >> unsubscribe_task >> delete_topic

    # Task dependencies created via `XComArgs`:
    #   subscribe_task >> pull_messages
    #   subscribe_task >> unsubscribe_task


with models.DAG(
    "example_gcp_pubsub_operator",
    schedule_interval='@once',  # Override to match your needs
    start_date=START_DATE,
    catchup=False,
) as example_operator_dag:
    # [START howto_operator_gcp_pubsub_create_topic]
    create_topic = PubSubCreateTopicOperator(
        task_id="create_topic", topic=TOPIC_FOR_OPERATOR_DAG, project_id=GCP_PROJECT_ID
    )
    # [END howto_operator_gcp_pubsub_create_topic]

    # [START howto_operator_gcp_pubsub_create_subscription]
    subscribe_task = PubSubCreateSubscriptionOperator(
        task_id="subscribe_task", project_id=GCP_PROJECT_ID, topic=TOPIC_FOR_OPERATOR_DAG
    )
    # [END howto_operator_gcp_pubsub_create_subscription]

    # [START howto_operator_gcp_pubsub_pull_message_with_operator]
    subscription = subscribe_task.output

    pull_messages_operator = PubSubPullOperator(
        task_id="pull_messages",
        ack_messages=True,
        project_id=GCP_PROJECT_ID,
        subscription=subscription,
    )
    # [END howto_operator_gcp_pubsub_pull_message_with_operator]

    # [START howto_operator_gcp_pubsub_pull_messages_result]
    pull_messages_result = BashOperator(task_id="pull_messages_result", bash_command=echo_cmd)
    # [END howto_operator_gcp_pubsub_pull_messages_result]

    # [START howto_operator_gcp_pubsub_publish]
    publish_task = PubSubPublishMessageOperator(
        task_id="publish_task",
        project_id=GCP_PROJECT_ID,
        topic=TOPIC_FOR_OPERATOR_DAG,
        messages=[MESSAGE, MESSAGE, MESSAGE],
    )
    # [END howto_operator_gcp_pubsub_publish]

    # [START howto_operator_gcp_pubsub_unsubscribe]
    unsubscribe_task = PubSubDeleteSubscriptionOperator(
        task_id="unsubscribe_task",
        project_id=GCP_PROJECT_ID,
        subscription=subscription,
    )
    # [END howto_operator_gcp_pubsub_unsubscribe]

    # [START howto_operator_gcp_pubsub_delete_topic]
    delete_topic = PubSubDeleteTopicOperator(
        task_id="delete_topic", topic=TOPIC_FOR_OPERATOR_DAG, project_id=GCP_PROJECT_ID
    )
    # [END howto_operator_gcp_pubsub_delete_topic]

    (
        create_topic
        >> subscribe_task
        >> publish_task
        >> pull_messages_operator
        >> pull_messages_result
        >> unsubscribe_task
        >> delete_topic
    )

    # Task dependencies created via `XComArgs`:
    #   subscribe_task >> pull_messages_operator
    #   subscribe_task >> unsubscribe_task
