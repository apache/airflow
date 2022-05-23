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
from os import environ

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.step_function import (
    StepFunctionGetExecutionOutputOperator,
    StepFunctionStartExecutionOperator,
)
from airflow.providers.amazon.aws.sensors.step_function import StepFunctionExecutionSensor

STEP_FUNCTIONS_STATE_MACHINE_ARN = environ.get('STEP_FUNCTIONS_STATE_MACHINE_ARN', 'state_machine_arn')

with DAG(
    dag_id='example_step_functions',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    # [START howto_operator_step_function_start_execution]
    start_execution = StepFunctionStartExecutionOperator(
        task_id='start_execution', state_machine_arn=STEP_FUNCTIONS_STATE_MACHINE_ARN
    )
    # [END howto_operator_step_function_start_execution]

    # [START howto_sensor_step_function_execution]
    wait_for_execution = StepFunctionExecutionSensor(
        task_id='wait_for_execution', execution_arn=start_execution.output
    )
    # [END howto_sensor_step_function_execution]

    # [START howto_operator_step_function_get_execution_output]
    get_execution_output = StepFunctionGetExecutionOutputOperator(
        task_id='get_execution_output', execution_arn=start_execution.output
    )
    # [END howto_operator_step_function_get_execution_output]

    chain(start_execution, wait_for_execution, get_execution_output)
