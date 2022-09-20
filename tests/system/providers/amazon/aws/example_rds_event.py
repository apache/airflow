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
from __future__ import annotations

from datetime import datetime

import boto3

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.rds import (
    RdsCreateDbInstanceOperator,
    RdsCreateEventSubscriptionOperator,
    RdsDeleteDbInstanceOperator,
    RdsDeleteEventSubscriptionOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = 'example_rds_event'

sys_test_context_task = SystemTestContextBuilder().build()


@task
def create_sns_topic(env_id) -> str:
    return boto3.client('sns').create_topic(Name=f'{env_id}-topic')['TopicArn']


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_sns_topic(topic_arn) -> None:
    boto3.client('sns').delete_topic(TopicArn=topic_arn)


with DAG(
    dag_id=DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()

    rds_db_name = f'{test_context[ENV_ID_KEY]}_db'
    rds_instance_name = f'{test_context[ENV_ID_KEY]}-instance'
    rds_subscription_name = f'{test_context[ENV_ID_KEY]}-subscription'

    sns_topic = create_sns_topic(test_context[ENV_ID_KEY])

    create_db_instance = RdsCreateDbInstanceOperator(
        task_id="create_db_instance",
        db_instance_identifier=rds_instance_name,
        db_instance_class="db.t4g.micro",
        engine="postgres",
        rds_kwargs={
            "MasterUsername": "rds_username",
            # NEVER store your production password in plaintext in a DAG like this.
            # Use Airflow Secrets or a secret manager for this in production.
            "MasterUserPassword": "rds_password",
            "AllocatedStorage": 20,
            "DBName": rds_db_name,
        },
    )

    # [START howto_operator_rds_create_event_subscription]
    create_subscription = RdsCreateEventSubscriptionOperator(
        task_id='create_subscription',
        subscription_name=rds_subscription_name,
        sns_topic_arn=sns_topic,
        source_type='db-instance',
        source_ids=[rds_instance_name],
        event_categories=['availability'],
    )
    # [END howto_operator_rds_create_event_subscription]

    # [START howto_operator_rds_delete_event_subscription]
    delete_subscription = RdsDeleteEventSubscriptionOperator(
        task_id='delete_subscription',
        subscription_name=rds_subscription_name,
    )
    # [END howto_operator_rds_delete_event_subscription]
    delete_subscription.trigger_rule = TriggerRule.ALL_DONE

    delete_db_instance = RdsDeleteDbInstanceOperator(
        task_id="delete_db_instance",
        db_instance_identifier=rds_instance_name,
        rds_kwargs={"SkipFinalSnapshot": True},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        test_context,
        sns_topic,
        create_db_instance,
        # TEST BODY
        create_subscription,
        delete_subscription,
        # TEST TEARDOWN
        delete_db_instance,
        delete_sns_topic(sns_topic),
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
