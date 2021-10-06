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

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.sqs import SQSHook
from airflow.providers.amazon.aws.operators.sqs import SQSPublishOperator
from airflow.providers.amazon.aws.sensors.sqs import SQSSensor
from airflow.utils.dates import days_ago

QUEUE_NAME = 'Airflow-Example-Queue'
AWS_CONN_ID = 'aws_default'


def create_queue_fn(**kwargs):
    hook = SQSHook(aws_conn_id=AWS_CONN_ID)
    result = hook.create_queue(
        queue_name=QUEUE_NAME,
    )
    return result['QueueUrl']


def delete_queue_fn(queue_url):
    hook = SQSHook(aws_conn_id=AWS_CONN_ID)
    hook.get_conn().delete_queue(QueueUrl=queue_url)


with DAG(
    "example_sqs",
    schedule_interval=None,
    start_date=days_ago(1),  # Override to match your needs
) as dag:
    # [START howto_sqs_operator_and_sensor]
    create_queue = PythonOperator(
        task_id='create_queue',
        python_callable=create_queue_fn,
        provide_context=True,
    )

    publish_to_queue = SQSPublishOperator(
        task_id='publish_to_queue',
        sqs_queue="{{ task_instance.xcom_pull('create_queue', key='return_value') }}",
        message_content="{{ task_instance }}-{{ execution_date }}",
        message_attributes=None,
        delay_seconds=0,
        aws_conn_id=AWS_CONN_ID,
    )

    read_from_queue = SQSSensor(
        task_id='read_from_queue',
        sqs_queue="{{ task_instance.xcom_pull('create_queue', key='return_value') }}",
        max_messages=5,
        wait_time_seconds=1,
        visibility_timeout=None,
        message_filtering=None,
        message_filtering_match_values=None,
        message_filtering_config=None,
        aws_conn_id=AWS_CONN_ID,
    )

    delete_queue = PythonOperator(
        task_id='delete_queue',
        python_callable=delete_queue_fn,
        provide_context=True,
        op_kwargs={'queue_url': "{{ task_instance.xcom_pull('create_queue', key='return_value') }}"},
    )

    create_queue >> publish_to_queue >> read_from_queue >> delete_queue
    # [END howto_sqs_operator_and_sensor]
