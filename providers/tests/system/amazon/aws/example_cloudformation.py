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

import json
from datetime import datetime

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.cloud_formation import (
    CloudFormationCreateStackOperator,
    CloudFormationDeleteStackOperator,
)
from airflow.providers.amazon.aws.sensors.cloud_formation import (
    CloudFormationCreateStackSensor,
    CloudFormationDeleteStackSensor,
)
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import SystemTestContextBuilder

sys_test_context_task = SystemTestContextBuilder().build()

DAG_ID = "example_cloudformation"

# The CloudFormation template must have at least one resource to
# be usable, this example uses SQS as a free and serverless option.
CLOUDFORMATION_TEMPLATE = {
    "Description": "Stack from Airflow CloudFormation example DAG",
    "Resources": {
        "ExampleQueue": {
            "Type": "AWS::SQS::Queue",
        }
    },
}

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    cloudformation_stack_name = f"{env_id}-stack"
    cloudformation_create_parameters = {
        "StackName": cloudformation_stack_name,
        "TemplateBody": json.dumps(CLOUDFORMATION_TEMPLATE),
        "TimeoutInMinutes": 2,
        "OnFailure": "DELETE",  # Don't leave stacks behind if creation fails.
    }

    # [START howto_operator_cloudformation_create_stack]
    create_stack = CloudFormationCreateStackOperator(
        task_id="create_stack",
        stack_name=cloudformation_stack_name,
        cloudformation_parameters=cloudformation_create_parameters,
    )
    # [END howto_operator_cloudformation_create_stack]

    # [START howto_sensor_cloudformation_create_stack]
    wait_for_stack_create = CloudFormationCreateStackSensor(
        task_id="wait_for_stack_create",
        stack_name=cloudformation_stack_name,
    )
    # [END howto_sensor_cloudformation_create_stack]
    wait_for_stack_create.poke_interval = 10

    # [START howto_operator_cloudformation_delete_stack]
    delete_stack = CloudFormationDeleteStackOperator(
        task_id="delete_stack",
        stack_name=cloudformation_stack_name,
    )
    # [END howto_operator_cloudformation_delete_stack]
    delete_stack.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_sensor_cloudformation_delete_stack]
    wait_for_stack_delete = CloudFormationDeleteStackSensor(
        task_id="wait_for_stack_delete",
        stack_name=cloudformation_stack_name,
    )
    # [END howto_sensor_cloudformation_delete_stack]
    wait_for_stack_delete.poke_interval = 10

    chain(
        # TEST SETUP
        test_context,
        # TEST BODY
        create_stack,
        wait_for_stack_create,
        delete_stack,
        wait_for_stack_delete,
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
