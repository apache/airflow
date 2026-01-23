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

from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain, task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import SystemTestContextBuilder

sys_test_context_task = SystemTestContextBuilder().build()

DAG_ID = "example_sns"


@task
def create_topic(topic_name) -> str:
    return boto3.client("sns").create_topic(Name=topic_name)["TopicArn"]


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_topic(topic_arn) -> None:
    boto3.client("sns").delete_topic(TopicArn=topic_arn)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    sns_topic_name = f"{env_id}-test-topic"

    create_sns_topic = create_topic(sns_topic_name)

    # [START howto_operator_sns_publish_operator]
    publish_message = SnsPublishOperator(
        task_id="publish_message",
        target_arn=create_sns_topic,
        message="This is a sample message sent to SNS via an Apache Airflow DAG task.",
    )
    # [END howto_operator_sns_publish_operator]

    chain(
        # TEST SETUP
        test_context,
        create_sns_topic,
        # TEST BODY
        publish_message,
        # TEST TEARDOWN
        delete_topic(create_sns_topic),
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
