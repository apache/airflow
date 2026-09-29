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

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest import mock

import boto3
import pytest
from aiobotocore.session import get_session
from aiobotocore.stub import AioStubber
from botocore.stub import Stubber

from airflow.providers.amazon.aws.hooks.ecs import EcsHook
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.common.compat.sdk import DAG
from airflow.utils.state import DagRunState, TaskInstanceState

from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

TASK_ARN = "arn:aws:ecs:us-east-1:123456789012:task/probe/00000000000000000000000000000001"
DATE = datetime(2026, 1, 1, tzinfo=timezone.utc)
CLIENT_KWARGS = {
    "region_name": "us-east-1",
    "aws_access_key_id": "testing",
    "aws_secret_access_key": "testing",
}


@asynccontextmanager
async def create_async_client(service, response):
    async with get_session().create_client(service, **CLIENT_KWARGS) as client:
        with AioStubber(client) as stubber:
            if service == "ecs":
                stubber.add_response("describe_tasks", response, {"cluster": "probe", "tasks": [TASK_ARN]})
            yield client
            stubber.assert_no_pending_responses()


@pytest.mark.db_test
@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Exercises the Task SDK Execution API boundary")
@pytest.mark.parametrize("deferred", [False, True], ids=["synchronous", "inline_trigger_resume"])
@pytest.mark.parametrize(
    ("scenario", "codes", "fail_codes", "skip_codes", "expected_state", "expected_attempts"),
    [
        pytest.param("selected", [2], 2, None, TaskInstanceState.FAILED, 1, id="selected_no_retry"),
        pytest.param("legacy", [2], None, None, TaskInstanceState.FAILED, 3, id="legacy_default_retries"),
        pytest.param(
            "policy", [2], 2, None, TaskInstanceState.FAILED, 1, id="selected_bypasses_retry_policy"
        ),
        pytest.param("unmatched", [3], 2, None, TaskInstanceState.FAILED, 3, id="unmatched_retries"),
        pytest.param("missing", [None], 1, None, TaskInstanceState.FAILED, 3, id="missing_code_retries"),
        pytest.param("service", [2], 2, None, TaskInstanceState.FAILED, 3, id="service_error_retries"),
        pytest.param("skip", [3], 2, 3, TaskInstanceState.SKIPPED, 1, id="unmatched_skip"),
        pytest.param("success", [0], [0, 2], None, TaskInstanceState.SUCCESS, 1, id="zero_success"),
        pytest.param("overlap", [2], 2, 2, TaskInstanceState.FAILED, 1, id="fail_over_skip"),
        pytest.param("siblings", [3, 2], 2, 3, TaskInstanceState.FAILED, 1, id="sibling_skip_before_failure"),
    ],
)
def test_exit_code_policy_through_task_runner(
    testing_dag_bundle, deferred, scenario, codes, fail_codes, skip_codes, expected_state, expected_attempts
):
    retries = []
    failures = []

    def on_retry(context):
        retries.append(context["ti"].try_number)

    def on_failure(context):
        failures.append(context["ti"].try_number)

    containers = []
    for index, code in enumerate(codes):
        container = {"name": f"container_{index}", "lastStatus": "STOPPED"}
        if code is not None:
            container["exitCode"] = code
        containers.append(container)
    response = {
        "tasks": [{"taskArn": TASK_ARN, "lastStatus": "STOPPED", "containers": containers}],
        "failures": [],
    }
    options = {}
    if fail_codes is not None:
        options["fail_on_exit_code"] = fail_codes
    if scenario == "policy":
        policies = pytest.importorskip("airflow.sdk.definitions.retry_policy")
        options["retry_policy"] = policies.ExceptionRetryPolicy(
            rules=[policies.RetryRule(exception=Exception, action=policies.RetryAction.RETRY)]
        )
    with DAG(dag_id=f"ecs_retry_{scenario}_{deferred}", schedule=None, start_date=DATE) as dag:
        operator = EcsRunTaskOperator(
            task_id="probe",
            cluster="probe",
            task_definition="probe",
            overrides={},
            aws_conn_id=None,
            region_name="us-east-1",
            do_xcom_push=True,
            deferrable=deferred,
            waiter_delay=0,
            waiter_max_attempts=1,
            skip_on_exit_code=skip_codes,
            stop_task_on_failure=False,
            retries=2,
            retry_delay=timedelta(0),
            on_retry_callback=on_retry,
            on_failure_callback=on_failure,
            **options,
        )
    sync_dag_to_db(dag)

    async def get_ecs_client(hook):
        return create_async_client("ecs", response)

    async def get_logs_client(hook):
        return create_async_client("logs", response)

    client = boto3.client("ecs", **CLIENT_KWARGS)
    try:
        with (
            Stubber(client) as stubber,
            mock.patch.object(EcsHook, "conn", client),
            mock.patch.object(EcsHook, "get_async_conn", autospec=True, side_effect=get_ecs_client),
            mock.patch.object(AwsLogsHook, "get_async_conn", autospec=True, side_effect=get_logs_client),
        ):
            for _ in range(expected_attempts):
                if scenario == "service":
                    stubber.add_client_error("run_task", service_error_code="AccessDeniedException")
                    continue
                stubber.add_response(
                    "run_task",
                    {"tasks": [{"taskArn": TASK_ARN}], "failures": []},
                    {
                        "cluster": "probe",
                        "taskDefinition": "probe",
                        "overrides": {},
                        "startedBy": operator.owner,
                        "launchType": "EC2",
                    },
                )
                if not deferred:
                    stubber.add_response(
                        "describe_tasks", response, {"cluster": "probe", "tasks": [TASK_ARN]}
                    )
                stubber.add_response("describe_tasks", response, {"cluster": "probe", "tasks": [TASK_ARN]})
            run = dag.test(logical_date=DATE, run_after=DATE)
            stubber.assert_no_pending_responses()
        ti = run.get_task_instance("probe")
        assert ti.state == expected_state
        assert ti.try_number == expected_attempts
        assert ti.max_tries == 2
        assert retries == list(range(1, expected_attempts))
        assert failures == ([expected_attempts] if expected_state == TaskInstanceState.FAILED else [])
        assert run.state == (
            DagRunState.FAILED if expected_state == TaskInstanceState.FAILED else DagRunState.SUCCESS
        )
        if scenario != "service":
            assert ti.xcom_pull(task_ids="probe", key="ecs_task_arn") == TASK_ARN
    finally:
        client.close()
