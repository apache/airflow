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
This example DAG demonstrates how the PubSub*Operators and PubSubPullSensor
can be used to trigger dependant tasks upon receipt of a Pub/Sub message.

NOTE: project_id must be updated to a GCP project ID accessible with the
      Google Default Credentials on the machine running the workflow
"""
from __future__ import unicode_literals
from base64 import b64encode

import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.pubsub_operator import (
    PubSubTopicCreateOperator, PubSubSubscriptionCreateOperator,
    PubSubPublishOperator, PubSubTopicDeleteOperator,
    PubSubSubscriptionDeleteOperator
)
from airflow.contrib.sensors.pubsub_sensor import PubSubPullSensor
from airflow.utils import dates

project = 'your-project-id'  # Change this to your own GCP project_id
topic = 'example-topic'  # Cloud Pub/Sub topic
subscription = 'subscription-to-example-topic'  # Cloud Pub/Sub subscription
# Sample messages to push/pull
messages = [
    {'data': b64encode(b'Hello World')},
    {'data': b64encode(b'Another message')},
    {'data': b64encode(b'A final message')}
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'project': project,
    'topic': topic,
    'subscription': subscription,
}


echo_template = '''
{% for m in task_instance.xcom_pull(task_ids='pull-messages') %}
    echo "AckID: {{ m.get('ackId') }}, Base64-Encoded: {{ m.get('message') }}"
{% endfor %}
'''

with DAG('pubsub-end-to-end', default_args=default_args,
         schedule_interval=datetime.timedelta(days=1)) as dag:
    t1 = PubSubTopicCreateOperator(task_id='create-topic')
    t2 = PubSubSubscriptionCreateOperator(
        task_id='create-subscription', topic_project=project,
        subscription=subscription)
    t3 = PubSubPublishOperator(
        task_id='publish-messages', messages=messages)
    t4 = PubSubPullSensor(task_id='pull-messages', ack_messages=True)
    t5 = BashOperator(task_id='echo-pulled-messages',
                      bash_command=echo_template)
    t6 = PubSubSubscriptionDeleteOperator(task_id='delete-subscription')
    t7 = PubSubTopicDeleteOperator(task_id='delete-topic')

    t1 >> t2 >> t3
    t2 >> t4 >> t5 >> t6 >> t7
