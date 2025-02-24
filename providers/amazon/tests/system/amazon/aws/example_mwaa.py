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

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.mwaa import MwaaTriggerDagRunOperator
from airflow.providers.amazon.aws.sensors.mwaa import MwaaDagRunSensor
from system.amazon.aws.utils import SystemTestContextBuilder

DAG_ID = "example_mwaa"

# Externally fetched variables:
EXISTING_ENVIRONMENT_NAME_KEY = "ENVIRONMENT_NAME"
EXISTING_DAG_ID_KEY = "DAG_ID"

sys_test_context_task = (
    SystemTestContextBuilder()
    # NOTE: Creating a functional MWAA environment is time-consuming and requires
    # manually creating and configuring an S3 bucket for DAG storage and a VPC with
    # private subnets which is out of scope for this demo. To simplify this demo and
    # make it run in a reasonable time, an existing MWAA environment already
    # containing a DAG is required.
    # Here's a quick start guide to create an MWAA environment using AWS CloudFormation:
    # https://docs.aws.amazon.com/mwaa/latest/userguide/quick-start.html
    # If creating the environment using the AWS console, make sure to have a VPC with
    # at least 1 private subnet to be able to select the VPC while going through the
    # environment creation steps in the console wizard.
    # Make sure to set the environment variables with appropriate values
    .add_variable(EXISTING_ENVIRONMENT_NAME_KEY)
    .add_variable(EXISTING_DAG_ID_KEY)
    .build()
)

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_name = test_context[EXISTING_ENVIRONMENT_NAME_KEY]
    trigger_dag_id = test_context[EXISTING_DAG_ID_KEY]

    # [START howto_operator_mwaa_trigger_dag_run]
    trigger_dag_run = MwaaTriggerDagRunOperator(
        task_id="trigger_dag_run",
        env_name=env_name,
        trigger_dag_id=trigger_dag_id,
    )
    # [END howto_operator_mwaa_trigger_dag_run]

    # [START howto_sensor_mwaa_dag_run]
    wait_for_dag_run = MwaaDagRunSensor(
        task_id="wait_for_dag_run",
        external_env_name=env_name,
        external_dag_id=trigger_dag_id,
        external_dag_run_id="{{ task_instance.xcom_pull(task_ids='trigger_dag_run')['RestApiResponse']['dag_run_id'] }}",
        poke_interval=5,
    )
    # [END howto_sensor_mwaa_dag_run]

    chain(
        # TEST SETUP
        test_context,
        # TEST BODY
        trigger_dag_run,
        wait_for_dag_run,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
