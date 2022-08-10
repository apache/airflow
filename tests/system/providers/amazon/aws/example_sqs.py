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

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.aws.operators.sqs import SqsPublishOperator
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from tests.system.providers.amazon.aws.utils import set_env_id

ENV_ID = set_env_id()
DAG_ID = 'example_sqs'
QUEUE_NAME = f'{ENV_ID}-example-queue'


@task
def create_queue() -> str:
    """Create the example queue"""
    return SqsHook().create_queue(queue_name=QUEUE_NAME)['QueueUrl']


@task(trigger_rule='all_done')
def delete_queue(queue_url):
    """Delete the example queue"""
    SqsHook().conn.delete_queue(QueueUrl=queue_url)


with DAG(
    dag_id=DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    sqs_queue = create_queue()

    # [START howto_operator_sqs]
    publish_to_queue_1 = SqsPublishOperator(
        task_id='publish_to_queue_1',
        sqs_queue=sqs_queue,
        message_content='{{ task_instance }}-{{ logical_date }}',
    )
    publish_to_queue_2 = SqsPublishOperator(
        task_id='publish_to_queue_2',
        sqs_queue=sqs_queue,
        message_content='{{ task_instance }}-{{ logical_date }}',
    )
    # [END howto_operator_sqs]

    # [START howto_sensor_sqs]
    read_from_queue = SqsSensor(
        task_id='read_from_queue',
        sqs_queue=sqs_queue,
    )
    # Retrieve multiple batches of messages from SQS.
    # The SQS API only returns a maximum of 10 messages per poll.
    read_from_queue_in_batch = SqsSensor(
        task_id='read_from_queue_in_batch',
        sqs_queue=create_queue,
        # Get maximum 10 messages each poll
        max_messages=10,
        # Combine 3 polls before returning results
        num_batches=3,
    )
    # [END howto_sensor_sqs]

    chain(
        # TEST SETUP
        sqs_queue,
        # TEST BODY
        publish_to_queue_1,
        read_from_queue,
        publish_to_queue_2,
        read_from_queue_in_batch,
        # TEST TEARDOWN
        delete_queue(sqs_queue),
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
