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

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.aws.operators.sqs import SqsPublishOperator
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor

QUEUE_NAME = 'Airflow-Example-Queue'
AWS_CONN_ID = 'aws_default'


@task(task_id="create_queue")
def create_queue_fn():
    """This is a Python function that creates an SQS queue"""
    hook = SqsHook()
    result = hook.create_queue(queue_name=QUEUE_NAME)
    return result['QueueUrl']


@task(task_id="delete_queue")
def delete_queue_fn(queue_url):
    """This is a Python function that deletes an SQS queue"""
    hook = SqsHook()
    hook.get_conn().delete_queue(QueueUrl=queue_url)


with DAG(
    dag_id='example_sqs',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example'],
    catchup=False,
) as dag:
    # [START howto_sqs_operator_and_sensor]

    # Using a task-decorated function to create an SQS queue
    create_queue = create_queue_fn()

    publish_to_queue = SqsPublishOperator(
        task_id='publish_to_queue',
        sqs_queue=create_queue,
        message_content="{{ task_instance }}-{{ execution_date }}",
        message_attributes=None,
        delay_seconds=0,
    )

    read_from_queue = SqsSensor(
        task_id='read_from_queue',
        sqs_queue=create_queue,
        max_messages=5,
        wait_time_seconds=1,
        visibility_timeout=None,
        message_filtering=None,
        message_filtering_match_values=None,
        message_filtering_config=None,
    )

    # Using a task-decorated function to delete the SQS queue we created earlier
    delete_queue = delete_queue_fn(create_queue)

    create_queue >> publish_to_queue >> read_from_queue >> delete_queue
    # [END howto_sqs_operator_and_sensor]
