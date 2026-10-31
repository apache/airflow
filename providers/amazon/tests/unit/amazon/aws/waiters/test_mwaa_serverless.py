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

import boto3
import botocore
import pytest

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

WAITER_NAME = "workflow_run_complete"
WORKFLOW_ARN = "arn:aws:airflow-serverless:us-east-1:123456789012:workflow/test-workflow"
RUN_ID = "run-abc123"


def _run(state: str) -> dict:
    return {"RunDetail": {"RunState": state}}


class TestMwaaServerlessCustomWaiters:
    @pytest.fixture(autouse=True)
    def mock_conn(self, monkeypatch):
        self.client = boto3.client("mwaa-serverless", region_name="us-east-1")
        monkeypatch.setattr(AwsBaseHook, "conn", self.client)
        self.hook = AwsBaseHook(client_type="mwaa-serverless")

    @pytest.fixture
    def mock_get_workflow_run(self):
        with mock.patch.object(self.client, "get_workflow_run") as getter:
            yield getter

    def test_service_waiters(self):
        assert WAITER_NAME in self.hook.list_waiters()

    def test_run_success(self, mock_get_workflow_run):
        mock_get_workflow_run.return_value = _run("SUCCESS")

        self.hook.get_waiter(WAITER_NAME).wait(WorkflowArn=WORKFLOW_ARN, RunId=RUN_ID)

    @pytest.mark.parametrize("state", ["FAILED", "TIMEOUT", "STOPPED"])
    def test_run_failed(self, state, mock_get_workflow_run):
        mock_get_workflow_run.return_value = _run(state)

        with pytest.raises(botocore.exceptions.WaiterError):
            self.hook.get_waiter(WAITER_NAME).wait(WorkflowArn=WORKFLOW_ARN, RunId=RUN_ID)

    def test_run_wait(self, mock_get_workflow_run):
        mock_get_workflow_run.side_effect = [
            _run("STARTING"),
            _run("QUEUED"),
            _run("RUNNING"),
            _run("STOPPING"),
            _run("SUCCESS"),
        ]

        self.hook.get_waiter(WAITER_NAME).wait(
            WorkflowArn=WORKFLOW_ARN, RunId=RUN_ID, WaiterConfig={"Delay": 0.01, "MaxAttempts": 5}
        )

        assert mock_get_workflow_run.call_count == 5
