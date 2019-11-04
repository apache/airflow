# -*- coding: utf-8 -*-
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

import airflow
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubPublishOperator, PubSubSubscriptionCreateOperator, PubSubSubscriptionDeleteOperator,
    PubSubTopicCreateOperator, PubSubTopicDeleteOperator,
)
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-project-id")
TOPIC = "PubSubTestTopic"
MESSAGE = {"data": b"Tool", "attributes": {"name": "wrench", "mass": "1.3kg", "count": "3"}}

default_args = {"start_date": airflow.utils.dates.days_ago(1)}

# [START howto_operator_gcp_pubsub_pull_messages_result_cmd]
echo_cmd = """
{% for m in task_instance.xcom_pull('pull_messages') %}
    echo "AckID: {{ m.get('ackId') }}, Base64-Encoded: {{ m.get('message') }}"
{% endfor %}
"""
# [END howto_operator_gcp_pubsub_pull_messages_result_cmd]

with models.DAG(
    "example_gcp_pubsub",
    default_args=default_args,
    schedule_interval=None,  # Override to match your needs
) as example_dag:
    # [START howto_operator_gcp_pubsub_create_topic]
    create_topic = PubSubTopicCreateOperator(
        task_id="create_topic", topic=TOPIC, project_id=GCP_PROJECT_ID
    )
    # [END howto_operator_gcp_pubsub_create_topic]

    # [START howto_operator_gcp_pubsub_create_subscription]
    subscribe_task = PubSubSubscriptionCreateOperator(
        task_id="subscribe_task", project_id=GCP_PROJECT_ID, topic=TOPIC
    )
    # [END howto_operator_gcp_pubsub_create_subscription]

    # [START howto_operator_gcp_pubsub_pull_message]
    subscription = "{{ task_instance.xcom_pull('subscribe_task') }}"

    pull_messages = PubSubPullSensor(
        task_id="pull_messages",
        ack_messages=True,
        project_id=GCP_PROJECT_ID,
        subscription=subscription,
    )
    # [END howto_operator_gcp_pubsub_pull_message]

    # [START howto_operator_gcp_pubsub_pull_messages_result]
    pull_messages_result = BashOperator(
        task_id="pull_messages_result", bash_command=echo_cmd
    )
    # [END howto_operator_gcp_pubsub_pull_messages_result]

    # [START howto_operator_gcp_pubsub_publish]
    publish_task = PubSubPublishOperator(
        task_id="publish_task",
        project_id=GCP_PROJECT_ID,
        topic=TOPIC,
        messages=[MESSAGE, MESSAGE, MESSAGE],
    )
    # [END howto_operator_gcp_pubsub_publish]

    # [START howto_operator_gcp_pubsub_unsubscribe]
    unsubscribe_task = PubSubSubscriptionDeleteOperator(
        task_id="unsubscribe_task",
        project_id=GCP_PROJECT_ID,
        subscription="{{ task_instance.xcom_pull('subscribe_task') }}",
    )
    # [END howto_operator_gcp_pubsub_unsubscribe]

    # [START howto_operator_gcp_pubsub_delete_topic]
    delete_topic = PubSubTopicDeleteOperator(
        task_id="delete_topic", topic=TOPIC, project_id=GCP_PROJECT_ID
    )
    # [END howto_operator_gcp_pubsub_delete_topic]

    create_topic >> subscribe_task >> publish_task
    subscribe_task >> pull_messages >> pull_messages_result >> unsubscribe_task >> delete_topic
