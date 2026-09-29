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

from copy import deepcopy
from unittest import mock

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from airflow.providers.amazon.aws.exceptions import EcsTaskFailToStart
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.amazon.aws.utils.task_log_fetcher import AwsTaskLogFetcher
from airflow.providers.common.compat.sdk import (
    AirflowException,
    AirflowFailException,
    AirflowSkipException,
    TaskDeferred,
)

TASK_ARN = "arn:aws:ecs:us-east-1:123456789012:task/probe/00000000000000000000000000000001"


class ExitCodeContainer:
    def __init__(self, code):
        self.code = code

    def __contains__(self, code):
        return code == self.code


@pytest.fixture
def ecs_client():
    client = boto3.client(
        "ecs", region_name="us-east-1", aws_access_key_id="testing", aws_secret_access_key="testing"
    )
    try:
        yield client
    finally:
        client.close()


def create_operator(ecs_client, **kwargs):
    operator = EcsRunTaskOperator(
        task_id="probe",
        cluster="probe",
        task_definition="probe",
        overrides={},
        aws_conn_id=None,
        region_name="us-east-1",
        do_xcom_push=False,
        waiter_delay=0,
        waiter_max_attempts=1,
        **kwargs,
    )
    operator.hook.__dict__["conn"] = ecs_client
    return operator


def create_response(codes):
    containers = []
    for index, code in enumerate(codes):
        container = {"name": f"container_{index}", "lastStatus": "STOPPED"}
        if code is not None:
            container["exitCode"] = code
        containers.append(container)
    return {
        "tasks": [{"taskArn": TASK_ARN, "lastStatus": "STOPPED", "containers": containers}],
        "failures": [],
    }


def add_start_response(stubber, operator):
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


@pytest.mark.parametrize("deferred", [False, True], ids=["synchronous", "deferred_resume"])
@pytest.mark.parametrize(
    ("fail_codes", "skip_codes", "codes", "exception"),
    [
        pytest.param(None, None, [2], AirflowException, id="default_retry"),
        pytest.param([], None, [2], AirflowException, id="empty_retry"),
        pytest.param(2, None, [2], AirflowFailException, id="selected_int"),
        pytest.param([2, 3], None, [3], AirflowFailException, id="selected_list"),
        pytest.param({2, 3}, None, [2], AirflowFailException, id="selected_set"),
        pytest.param((2, 3), None, [3], AirflowFailException, id="selected_tuple"),
        pytest.param(ExitCodeContainer(2), None, [2], AirflowFailException, id="non_iterable_container"),
        pytest.param(2, None, [1], AirflowException, id="unmatched_retry"),
        pytest.param(1, None, [None], AirflowException, id="missing_not_inferred_as_one"),
        pytest.param([0, 2], None, [0], None, id="zero_remains_success"),
        pytest.param(2, 3, [3], AirflowSkipException, id="unmatched_skip"),
        pytest.param(2, 2, [2], AirflowFailException, id="fail_over_skip_overlap"),
        pytest.param(2, 3, [3, 2], AirflowFailException, id="skip_before_selected"),
        pytest.param(2, 3, [2, 3], AirflowFailException, id="skip_after_selected"),
        pytest.param(2, None, [1, 2], AirflowFailException, id="unmatched_before_selected"),
        pytest.param(2, None, [2, 1], AirflowFailException, id="unmatched_after_selected"),
        pytest.param(2, None, [None, 2], AirflowFailException, id="missing_before_selected"),
        pytest.param(2, None, [2, None], AirflowFailException, id="missing_after_selected"),
    ],
)
def test_execution_exit_code_policy(ecs_client, deferred, fail_codes, skip_codes, codes, exception):
    kwargs = {"deferrable": deferred, "fail_on_exit_code": fail_codes, "skip_on_exit_code": skip_codes}
    operator = create_operator(ecs_client, **kwargs)
    response = create_response(codes)
    original = deepcopy(response)
    with Stubber(ecs_client) as stubber:
        add_start_response(stubber, operator)
        if deferred:
            with pytest.raises(TaskDeferred) as raised:
                operator.execute({})
            path, fields = raised.value.trigger.serialize()
            trigger = type(raised.value.trigger)(**fields)
            assert trigger.serialize() == (path, fields)
            operator = create_operator(ecs_client, **kwargs)
            execute = operator.execute_complete
            execute_kwargs = {"event": {"status": "success", "task_arn": TASK_ARN, "cluster": "probe"}}
        else:
            stubber.add_response("describe_tasks", response, {"cluster": "probe", "tasks": [TASK_ARN]})
            execute = operator.execute
            execute_kwargs = {}
        stubber.add_response("describe_tasks", response, {"cluster": "probe", "tasks": [TASK_ARN]})
        if exception is None:
            assert execute({}, **execute_kwargs) is None
        else:
            with pytest.raises(exception) as raised:
                execute({}, **execute_kwargs)
            assert type(raised.value) is exception
        stubber.assert_no_pending_responses()
    assert response == original


@pytest.mark.parametrize("failure", ["failed_to_start", "host_terminated", "api_failure", "access_denied"])
def test_no_retry_preserves_task_and_service_errors(ecs_client, failure):
    operator = create_operator(ecs_client, fail_on_exit_code=2)
    operator.arn = TASK_ARN
    response = create_response([2])
    expected = AirflowException
    if failure == "failed_to_start":
        response["tasks"][0].update(stopCode="TaskFailedToStart", stoppedReason="COPY_PROBE_123")
        expected = EcsTaskFailToStart
    elif failure == "host_terminated":
        response["tasks"][0]["stoppedReason"] = "Host EC2 (instance i-1234567890abcdef) terminated."
    elif failure == "api_failure":
        response["failures"] = [{"arn": TASK_ARN, "reason": "MISSING"}]
    with Stubber(ecs_client) as stubber:
        if failure == "access_denied":
            stubber.add_client_error(
                "describe_tasks",
                service_error_code="AccessDeniedException",
                expected_params={"cluster": "probe", "tasks": [TASK_ARN]},
            )
            expected = ClientError
        else:
            stubber.add_response("describe_tasks", response, {"cluster": "probe", "tasks": [TASK_ARN]})
        with pytest.raises(expected) as raised:
            operator._check_success_task()
        if failure == "access_denied":
            assert raised.value.response["Error"]["Code"] == "AccessDeniedException"
        else:
            assert type(raised.value) is expected
        stubber.assert_no_pending_responses()


def test_no_retry_preserves_cloudwatch_log_tail(ecs_client):
    operator = create_operator(ecs_client, fail_on_exit_code=2)
    operator.arn = TASK_ARN
    fetcher = mock.Mock(spec=AwsTaskLogFetcher)
    fetcher.get_last_log_messages.return_value = ["COPY_PROBE_123", "business failure"]
    operator.task_log_fetcher = fetcher
    with Stubber(ecs_client) as stubber:
        stubber.add_response(
            "describe_tasks", create_response([2]), {"cluster": "probe", "tasks": [TASK_ARN]}
        )
        with pytest.raises(AirflowFailException, match="COPY_PROBE_123\nbusiness failure"):
            operator._check_success_task()
        fetcher.get_last_log_messages.assert_called_once_with(operator.number_logs_exception)
        stubber.assert_no_pending_responses()


def test_no_retry_does_not_inspect_exit_codes_without_waiting(ecs_client):
    operator = create_operator(ecs_client, fail_on_exit_code=2, wait_for_completion=False, deferrable=False)
    with Stubber(ecs_client) as stubber:
        add_start_response(stubber, operator)
        assert operator.execute({}) is None
        stubber.assert_no_pending_responses()


def test_no_retry_requires_a_stopped_container(ecs_client):
    operator = create_operator(ecs_client, fail_on_exit_code=2)
    operator.arn = TASK_ARN
    response = create_response([2])
    response["tasks"][0]["containers"][0]["lastStatus"] = "RUNNING"
    with Stubber(ecs_client) as stubber:
        stubber.add_response("describe_tasks", response, {"cluster": "probe", "tasks": [TASK_ARN]})
        assert operator._check_success_task() is None
        stubber.assert_no_pending_responses()
