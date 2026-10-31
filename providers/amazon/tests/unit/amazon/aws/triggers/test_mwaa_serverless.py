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

from unittest import mock
from unittest.mock import AsyncMock

import pytest

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.triggers.mwaa_serverless import MwaaServerlessWorkflowRunCompletedTrigger
from airflow.triggers.base import TriggerEvent

from unit.amazon.aws.utils.test_waiter import assert_expected_waiter_type

WORKFLOW_ARN = "arn:aws:airflow-serverless:us-east-1:123456789012:workflow/test-workflow"
RUN_ID = "run-abc123"
TRIGGER_KWARGS = {"workflow_arn": WORKFLOW_ARN, "run_id": RUN_ID}


def _acceptor_states(trigger: MwaaServerlessWorkflowRunCompletedTrigger) -> dict[str, str]:
    return {a["expected"]: a["state"] for a in trigger.waiter_config_overrides["acceptors"]}


class TestMwaaServerlessWorkflowRunCompletedTrigger:
    def test_default_acceptors(self):
        trigger = MwaaServerlessWorkflowRunCompletedTrigger(**TRIGGER_KWARGS)

        assert _acceptor_states(trigger) == {
            "SUCCESS": "success",
            "FAILED": "failure",
            "TIMEOUT": "failure",
            "STOPPED": "failure",
            "STARTING": "retry",
            "QUEUED": "retry",
            "RUNNING": "retry",
            "STOPPING": "retry",
        }
        assert {a["argument"] for a in trigger.waiter_config_overrides["acceptors"]} == {"RunDetail.RunState"}

    def test_custom_states_acceptors(self):
        trigger = MwaaServerlessWorkflowRunCompletedTrigger(
            **TRIGGER_KWARGS, success_states={"SUCCESS", "STOPPED"}, failure_states={"FAILED"}
        )

        acceptor_states = _acceptor_states(trigger)
        assert acceptor_states["STOPPED"] == "success"
        assert acceptor_states["FAILED"] == "failure"
        assert acceptor_states["TIMEOUT"] == "retry"

    def test_default_failure_states_exclude_success_states(self):
        trigger = MwaaServerlessWorkflowRunCompletedTrigger(
            **TRIGGER_KWARGS, success_states={"SUCCESS", "STOPPED"}
        )

        assert trigger.failure_states == {"FAILED", "TIMEOUT"}
        assert _acceptor_states(trigger)["STOPPED"] == "success"

    def test_overlapping_states_raise(self):
        with pytest.raises(ValueError, match=r"success_states and failure_states"):
            MwaaServerlessWorkflowRunCompletedTrigger(
                **TRIGGER_KWARGS, success_states={"SUCCESS", "STOPPED"}, failure_states={"STOPPED"}
            )

    def test_serialization(self):
        trigger = MwaaServerlessWorkflowRunCompletedTrigger(
            **TRIGGER_KWARGS,
            success_states={"SUCCESS"},
            failure_states={"FAILED"},
            waiter_delay=10,
            waiter_max_attempts=5,
            aws_conn_id="my_conn",
            region_name="eu-west-1",
        )

        classpath, kwargs = trigger.serialize()

        assert classpath == (
            "airflow.providers.amazon.aws.triggers.mwaa_serverless.MwaaServerlessWorkflowRunCompletedTrigger"
        )
        assert kwargs == {
            "workflow_arn": WORKFLOW_ARN,
            "run_id": RUN_ID,
            "success_states": ["SUCCESS"],
            "failure_states": ["FAILED"],
            "waiter_delay": 10,
            "waiter_max_attempts": 5,
            "aws_conn_id": "my_conn",
            "region_name": "eu-west-1",
        }
        assert MwaaServerlessWorkflowRunCompletedTrigger(**kwargs).serialize() == (classpath, kwargs)

    def test_hook_uses_mwaa_serverless_client(self):
        hook = MwaaServerlessWorkflowRunCompletedTrigger(**TRIGGER_KWARGS, aws_conn_id="my_conn").hook()

        assert isinstance(hook, AwsBaseHook)
        assert hook.client_type == "mwaa-serverless"
        assert hook.aws_conn_id == "my_conn"

    @pytest.mark.asyncio
    @mock.patch.object(AwsBaseHook, "get_waiter")
    @mock.patch.object(AwsBaseHook, "get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.return_value.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = MwaaServerlessWorkflowRunCompletedTrigger(**TRIGGER_KWARGS)

        response = await trigger.run().asend(None)

        assert response == TriggerEvent({"status": "success", "run_id": RUN_ID})
        assert_expected_waiter_type(mock_get_waiter, "workflow_run_complete")
        mock_get_waiter().wait.assert_called_once()
        assert mock_get_waiter().wait.call_args.kwargs["WorkflowArn"] == WORKFLOW_ARN
        assert mock_get_waiter().wait.call_args.kwargs["RunId"] == RUN_ID
