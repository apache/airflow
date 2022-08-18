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

import boto3

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import set_env_id

ENV_ID = set_env_id()
DAG_ID = 'example_sns'

SNS_TOPIC_NAME = f'{ENV_ID}-test-topic'


@task
def create_topic() -> str:
    return boto3.client('sns').create_topic(Name=SNS_TOPIC_NAME)['TopicArn']


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_topic(topic_arn) -> None:
    boto3.client('sns').delete_topic(TopicArn=topic_arn)


with DAG(
    dag_id=DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    create_sns_topic = create_topic()

    # [START howto_operator_sns_publish_operator]
    publish_message = SnsPublishOperator(
        task_id='publish_message',
        target_arn=create_sns_topic,
        message='This is a sample message sent to SNS via an Apache Airflow DAG task.',
    )
    # [END howto_operator_sns_publish_operator]

    chain(
        # TEST SETUP
        create_sns_topic,
        # TEST BODY
        publish_message,
        # TEST TEARDOWN
        delete_topic(create_sns_topic),
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
