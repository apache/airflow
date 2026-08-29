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

import os
import time
from datetime import datetime

import boto3

from airflow.providers.amazon.aws.operators.athena_spark import AthenaSparkOperator

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

DAG_ID = "example_athena_spark"

# The Spark workgroup is preconfigured test infrastructure; this DAG creates only the session.
# Test runners can override the default by exporting ATHENA_SPARK_WORK_GROUP.
ATHENA_SPARK_WORK_GROUP = os.environ.get("ATHENA_SPARK_WORK_GROUP", "airflow-athena-spark-test")


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
    client = boto3.client("athena")

    while True:
        response = client.get_session(SessionId=session_id)
        state = response["Status"]["State"]

        if state == "IDLE":
            return session_id

        if state in {"TERMINATED", "DEGRADED", "FAILED"}:
            reason = response["Status"].get("StateChangeReason", "Unknown")
            raise RuntimeError(f"Athena Spark session failed with state {state}. Reason: {reason}")

        time.sleep(10)


@task(trigger_rule=TriggerRule.ALL_DONE)
def stop_athena_spark_session(session_id: str) -> None:
    client = boto3.client("athena")
    client.terminate_session(SessionId=session_id)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()

    session_id = start_athena_spark_session(ATHENA_SPARK_WORK_GROUP)
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

    stop_session = stop_athena_spark_session(session_id)

    chain(
        test_context,
        session_id,
        idle_session_id,
        run_spark_calculation,
        stop_session,
    )

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
