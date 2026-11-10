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

from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.operators.athena_spark import AthenaSparkOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.sensors.athena_spark import AthenaSparkSensor

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

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_athena_spark"

# Athena rejects a PySpark work group without an execution role, so the role is preconfigured
# test infrastructure. The results bucket and the work group are created here.
EXECUTION_ROLE_ARN_KEY = "EXECUTION_ROLE_ARN"

sys_test_context_task = SystemTestContextBuilder().add_variable(EXECUTION_ROLE_ARN_KEY).build()


@task
def create_work_group(work_group: str, execution_role_arn: str, bucket_name: str) -> None:
    client = boto3.client("athena")
    client.create_work_group(
        Name=work_group,
        Configuration={
            "ExecutionRole": execution_role_arn,
            "ResultConfiguration": {"OutputLocation": f"s3://{bucket_name}/"},
            "EngineVersion": {"SelectedEngineVersion": "PySpark engine version 3"},
        },
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_work_group(work_group: str) -> None:
    client = boto3.client("athena")
    client.delete_work_group(WorkGroup=work_group, RecursiveDeleteOption=True)


@task
def start_athena_spark_session(work_group: str) -> str:
    client = boto3.client("athena")
    response = client.start_session(
        WorkGroup=work_group,
        EngineConfiguration={"MaxConcurrentDpus": 20},
    )
    return response["SessionId"]


@task
def wait_for_athena_spark_session(session_id: str) -> str:
    AthenaHook().get_waiter("session_idle").wait(
        SessionId=session_id,
        WaiterConfig={"Delay": 10, "MaxAttempts": 60},
    )
    return session_id


@task(trigger_rule=TriggerRule.ALL_DONE)
def stop_athena_spark_session(session_id: str) -> None:
    client = boto3.client("athena")
    client.terminate_session(SessionId=session_id)


@task
def start_athena_spark_calculation(session_id: str) -> str:
    client = boto3.client("athena")
    response = client.start_calculation_execution(
        SessionId=session_id,
        CodeBlock="print('hello from the athena spark sensor test')",
    )
    return response["CalculationExecutionId"]


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]

    work_group = f"{env_id}-athena-spark"
    bucket_name = f"{env_id}-athena-spark-bucket"

    create_bucket = S3CreateBucketOperator(task_id="create_bucket", bucket_name=bucket_name)

    setup_work_group = create_work_group(work_group, test_context[EXECUTION_ROLE_ARN_KEY], bucket_name)

    session_id = start_athena_spark_session(work_group)
    idle_session_id = wait_for_athena_spark_session(session_id)

    # [START howto_operator_athena_spark]
    run_spark_calculation = AthenaSparkOperator(
        task_id="run_spark_calculation",
        session_id=idle_session_id,
        code_block="print('hello from athena spark')",
        waiter_delay=30,
        waiter_max_attempts=120,
    )
    # [END howto_operator_athena_spark]

    calculation_execution_id = start_athena_spark_calculation(idle_session_id)

    # [START howto_sensor_athena_spark]
    await_spark_calculation = AthenaSparkSensor(
        task_id="await_spark_calculation",
        calculation_execution_id=calculation_execution_id,
        poke_interval=30,
        timeout=3600,
    )
    # [END howto_sensor_athena_spark]

    stop_session = stop_athena_spark_session(session_id)

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=bucket_name,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        test_context,
        create_bucket,
        setup_work_group,
        session_id,
        idle_session_id,
        # TEST BODY
        run_spark_calculation,
        calculation_execution_id,
        await_spark_calculation,
        # TEST TEARDOWN
        stop_session,
        delete_work_group(work_group),
        delete_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
