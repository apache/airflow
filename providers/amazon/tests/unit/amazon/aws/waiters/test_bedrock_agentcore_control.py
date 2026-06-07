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

from airflow.providers.amazon.aws.hooks.bedrock import BedrockAgentCoreControlHook


class TestBedrockAgentCoreControlCustomWaiters:
    def test_service_waiters(self):
        assert "agent_runtime_ready" in BedrockAgentCoreControlHook().list_waiters()


class TestBedrockAgentCoreControlCustomWaitersBase:
    @pytest.fixture(autouse=True)
    def mock_conn(self, monkeypatch):
        self.client = boto3.client("bedrock-agentcore-control", region_name="us-east-1")
        monkeypatch.setattr(BedrockAgentCoreControlHook, "conn", self.client)


class TestAgentRuntimeReadyWaiter(TestBedrockAgentCoreControlCustomWaitersBase):
    WAITER_NAME = "agent_runtime_ready"
    WAITER_ARGS = {"agentRuntimeId": "runtime_id", "agentRuntimeVersion": "1"}
    SUCCESS_STATES = ["READY"]
    FAILURE_STATES = ["CREATE_FAILED", "UPDATE_FAILED", "DELETING"]
    INTERMEDIATE_STATES = ["CREATING", "UPDATING"]

    @pytest.fixture
    def mock_getter(self):
        with mock.patch.object(self.client, "get_agent_runtime") as getter:
            yield getter

    @pytest.mark.parametrize("state", SUCCESS_STATES)
    def test_agent_runtime_ready_complete(self, state, mock_getter):
        mock_getter.return_value = {"status": state}

        BedrockAgentCoreControlHook().get_waiter(self.WAITER_NAME).wait(**self.WAITER_ARGS)

    @pytest.mark.parametrize("state", FAILURE_STATES)
    def test_agent_runtime_ready_failed(self, state, mock_getter):
        mock_getter.return_value = {"status": state}

        with pytest.raises(botocore.exceptions.WaiterError):
            BedrockAgentCoreControlHook().get_waiter(self.WAITER_NAME).wait(**self.WAITER_ARGS)

    @pytest.mark.parametrize("state", INTERMEDIATE_STATES)
    def test_agent_runtime_ready_wait(self, state, mock_getter):
        wait = {"status": state}
        success = {"status": "READY"}
        mock_getter.side_effect = [wait, wait, success]

        BedrockAgentCoreControlHook().get_waiter(self.WAITER_NAME).wait(
            **self.WAITER_ARGS,
            WaiterConfig={"Delay": 0.01, "MaxAttempts": 3},
        )
