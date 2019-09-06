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

from airflow.gcp.operators.pubsub import (
    PubSubTopicCreateOperator,
    PubSubSubscriptionDeleteOperator,
    PubSubSubscriptionCreateOperator,
    PubSubPublishOperator,
    PubSubTopicDeleteOperator,
)

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-project-id")
TOPIC = "PubSubTestTopic"
MESSAGE = {"attributes": {"name": "wrench", "mass": "1.3kg", "count": "3"}}

default_args = {"start_date": airflow.utils.dates.days_ago(1)}

with models.DAG(
    "example_gcp_pubsub",
    default_args=default_args,
    schedule_interval=None,  # Override to match your needs
) as example_dag:
    create_topic = PubSubTopicCreateOperator(
        task_id="create_topic", topic=TOPIC, project=GCP_PROJECT_ID
    )

    subscribe_task = PubSubSubscriptionCreateOperator(
        task_id="subscribe_task", topic_project=GCP_PROJECT_ID, topic=TOPIC
    )

    publish_task = PubSubPublishOperator(
        task_id="publish_task", project=GCP_PROJECT_ID, topic=TOPIC, messages=[MESSAGE]
    )

    unsubscribe_task = PubSubSubscriptionDeleteOperator(
        task_id="unsubscribe_task",
        project=GCP_PROJECT_ID,
        subscription="{{ task_instance.xcom_pull('subscribe_task') }}",
    )

    delete_topic = PubSubTopicDeleteOperator(task_id="delete_topic", topic=TOPIC, project=GCP_PROJECT_ID)

    create_topic >> [subscribe_task, publish_task] >> unsubscribe_task >> delete_topic
