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

from airflow.providers.amazon.aws.hooks.mwaa import MwaaHook
from airflow.providers.amazon.aws.hooks.sts import StsHook
from airflow.providers.amazon.aws.operators.mwaa import MwaaTriggerDagRunOperator
from airflow.providers.amazon.aws.sensors.mwaa import MwaaDagRunSensor, MwaaTaskSensor

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain, task, timezone
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]


from system.amazon.aws.utils import SystemTestContextBuilder

DAG_ID = "example_mwaa"

# Externally fetched variables:
EXISTING_ENVIRONMENT_NAME_KEY = "ENVIRONMENT_NAME"
EXISTING_DAG_ID_KEY = "DAG_ID"
EXISTING_TASK_ID_KEY = "TASK_ID"
ROLE_WITHOUT_INVOKE_REST_API_ARN_KEY = "ROLE_WITHOUT_INVOKE_REST_API_ARN"

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
    .add_variable(ROLE_WITHOUT_INVOKE_REST_API_ARN_KEY)
    .add_variable(EXISTING_TASK_ID_KEY)
    .build()
)


@task
def unpause_dag(env_name: str, dag_id: str):
    mwaa_hook = MwaaHook()
    response = mwaa_hook.invoke_rest_api(
        env_name=env_name, path=f"/dags/{dag_id}", method="PATCH", body={"is_paused": False}
    )
    return not response["RestApiResponse"]["is_paused"]


# This task in the system test verifies that the MwaaHook's IAM fallback mechanism continues to work with
# the live MWAA API. This fallback depends on parsing a specific error message from the MWAA API, so we
# want to ensure we find out if the API response format ever changes. Unit tests cover this with mocked
# responses, but this system test validates against the real API.
@task
def test_iam_fallback(role_to_assume_arn, mwaa_env_name):
    assumed_role = StsHook().conn.assume_role(
        RoleArn=role_to_assume_arn, RoleSessionName="MwaaSysTestIamFallback"
    )

    credentials = assumed_role["Credentials"]
    session = boto3.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )

    mwaa_hook = MwaaHook()
    mwaa_hook.conn = session.client("mwaa")
    response = mwaa_hook.invoke_rest_api(
        env_name=mwaa_env_name, path="/dags", method="GET", airflow_version=3
    )
    return "dags" in response["RestApiResponse"]


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_name = test_context[EXISTING_ENVIRONMENT_NAME_KEY]
    trigger_dag_id = test_context[EXISTING_DAG_ID_KEY]
    task_id = test_context[EXISTING_TASK_ID_KEY]
    restricted_role_arn = test_context[ROLE_WITHOUT_INVOKE_REST_API_ARN_KEY]

    # [START howto_operator_mwaa_trigger_dag_run]
    trigger_dag_run = MwaaTriggerDagRunOperator(
        task_id="trigger_dag_run",
        env_name=env_name,
        logical_date=datetime.now(timezone.utc).isoformat(),
        trigger_dag_id=trigger_dag_id,
        wait_for_completion=True,
        airflow_version=3,
    )
    # [END howto_operator_mwaa_trigger_dag_run]

    # [START howto_sensor_mwaa_task]
    wait_for_task = MwaaTaskSensor(
        task_id="wait_for_task",
        external_env_name=env_name,
        external_dag_id=trigger_dag_id,
        external_task_id=task_id,
        poke_interval=5,
    )
    # [END howto_sensor_mwaa_task]

    # [START howto_sensor_mwaa_dag_run]
    wait_for_dag_run = MwaaDagRunSensor(
        task_id="wait_for_dag_run",
        external_env_name=env_name,
        external_dag_id=trigger_dag_id,
        external_dag_run_id="{{ task_instance.xcom_pull(task_ids='trigger_dag_run')['RestApiResponse']['dag_run_id'] }}",
        poke_interval=5,
    )
    # [END howto_sensor_mwaa_dag_run]

    trigger_dag_run_dont_wait = MwaaTriggerDagRunOperator(
        task_id="trigger_dag_run_dont_wait",
        env_name=env_name,
        logical_date=datetime.now(timezone.utc).isoformat(),
        trigger_dag_id=trigger_dag_id,
        wait_for_completion=False,
        airflow_version=3,
    )

    wait_for_task_concurrent = MwaaTaskSensor(
        task_id="wait_for_task_concurrent",
        external_env_name=env_name,
        external_dag_id=trigger_dag_id,
        external_task_id=task_id,
        poke_interval=5,
    )

    test_context >> [
        unpause_dag(env_name, trigger_dag_id),
        test_iam_fallback(restricted_role_arn, env_name),
        trigger_dag_run,
        trigger_dag_run_dont_wait,
    ]
    chain(trigger_dag_run, wait_for_task, wait_for_dag_run)
    chain(trigger_dag_run_dont_wait, wait_for_task_concurrent)

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
