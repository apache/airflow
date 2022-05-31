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
from datetime import datetime
from os import getenv

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.aws.operators.sqs import SqsPublishOperator
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor

QUEUE_NAME = getenv('QUEUE_NAME', 'Airflow-Example-Queue')


@task(task_id="create_queue")
def create_queue_fn():
    """Create the example queue"""
    hook = SqsHook()
    result = hook.create_queue(queue_name=QUEUE_NAME)
    return result['QueueUrl']


@task(task_id="delete_queue")
def delete_queue_fn(queue_url):
    """Delete the example queue"""
    hook = SqsHook()
    hook.get_conn().delete_queue(QueueUrl=queue_url)


with DAG(
    dag_id='example_sqs',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    create_queue = create_queue_fn()

    # [START howto_operator_sqs]
    publish_to_queue = SqsPublishOperator(
        task_id='publish_to_queue',
        sqs_queue=create_queue,
        message_content="{{ task_instance }}-{{ execution_date }}",
    )
    # [END howto_operator_sqs]

    # [START howto_sensor_sqs]
    read_from_queue = SqsSensor(
        task_id='read_from_queue',
        sqs_queue=create_queue,
    )
    # [END howto_sensor_sqs]

    delete_queue = delete_queue_fn(create_queue)

    chain(create_queue, publish_to_queue, read_from_queue, delete_queue)
