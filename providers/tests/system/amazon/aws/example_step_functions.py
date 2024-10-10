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

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.step_function import StepFunctionHook
from airflow.providers.amazon.aws.operators.step_function import (
    StepFunctionGetExecutionOutputOperator,
    StepFunctionStartExecutionOperator,
)
from airflow.providers.amazon.aws.sensors.step_function import StepFunctionExecutionSensor

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_step_functions"

# Externally fetched variables:
ROLE_ARN_KEY = "ROLE_ARN"

sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

STATE_MACHINE_DEFINITION = {
    "StartAt": "Wait",
    "States": {"Wait": {"Type": "Wait", "Seconds": 7, "Next": "Success"}, "Success": {"Type": "Succeed"}},
}


@task
def create_state_machine(env_id, role_arn):
    # Create a Step Functions State Machine and return the ARN for use by
    # downstream tasks.
    return (
        StepFunctionHook()
        .get_conn()
        .create_state_machine(
            name=f"{DAG_ID}_{env_id}",
            definition=json.dumps(STATE_MACHINE_DEFINITION),
            roleArn=role_arn,
        )["stateMachineArn"]
    )


@task
def delete_state_machine(state_machine_arn):
    StepFunctionHook().get_conn().delete_state_machine(stateMachineArn=state_machine_arn)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    # This context contains the ENV_ID and any env variables requested when the
    # task was built above. Access the info as you would any other TaskFlow task.
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    role_arn = test_context[ROLE_ARN_KEY]

    state_machine_arn = create_state_machine(env_id, role_arn)

    # [START howto_operator_step_function_start_execution]
    start_execution = StepFunctionStartExecutionOperator(
        task_id="start_execution", state_machine_arn=state_machine_arn
    )
    # [END howto_operator_step_function_start_execution]

    execution_arn = start_execution.output

    # [START howto_sensor_step_function_execution]
    wait_for_execution = StepFunctionExecutionSensor(
        task_id="wait_for_execution", execution_arn=execution_arn
    )
    # [END howto_sensor_step_function_execution]
    wait_for_execution.poke_interval = 1

    # [START howto_operator_step_function_get_execution_output]
    get_execution_output = StepFunctionGetExecutionOutputOperator(
        task_id="get_execution_output", execution_arn=execution_arn
    )
    # [END howto_operator_step_function_get_execution_output]

    chain(
        # TEST SETUP
        test_context,
        state_machine_arn,
        # TEST BODY
        start_execution,
        wait_for_execution,
        get_execution_output,
        # TEST TEARDOWN
        delete_state_machine(state_machine_arn),
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
