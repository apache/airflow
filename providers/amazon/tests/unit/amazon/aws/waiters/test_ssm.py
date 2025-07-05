#!/usr/bin/env python
#
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
import pytest
from botocore.exceptions import WaiterError

from airflow.providers.amazon.aws.hooks.ssm import SsmHook
from airflow.providers.amazon.aws.sensors.ssm import SsmRunCommandCompletedSensor


class TestRunCommandInstanceCompleteWaiter:
    WAITER_NAME = "run_command_instance_complete"

    @pytest.fixture(autouse=True)
    def mock_conn(self, monkeypatch):
        self.client = boto3.client("ssm")
        monkeypatch.setattr(SsmHook, "conn", self.client)

    @pytest.fixture
    def mock_get_command(self):
        with mock.patch.object(self.client, "get_command_invocation") as mock_getter:
            yield mock_getter

    def test_service_waiters(self):
        assert self.WAITER_NAME in SsmHook().list_waiters()

    @pytest.mark.parametrize("state", SsmRunCommandCompletedSensor.SUCCESS_STATES)
    def test_instance_completed(self, state, mock_get_command):
        mock_get_command.return_value = {"Status": state}
        SsmHook().get_waiter(self.WAITER_NAME).wait()

    @pytest.mark.parametrize("state", SsmRunCommandCompletedSensor.FAILURE_STATES)
    def test_instance_failed(self, state, mock_get_command):
        mock_get_command.return_value = {"Status": state}
        with pytest.raises(WaiterError, match="Waiter encountered a terminal failure state"):
            SsmHook().get_waiter(self.WAITER_NAME).wait()

    @pytest.mark.parametrize(
        "intermediate_state",
        [state for state in SsmRunCommandCompletedSensor.INTERMEDIATE_STATES if state != "Cancelling"],
    )
    def test_instance_wait_until_success(self, intermediate_state, mock_get_command):
        final_state = SsmRunCommandCompletedSensor.SUCCESS_STATES[0]
        mock_get_command.side_effect = [
            {"Status": intermediate_state},
            {"Status": intermediate_state},
            {"Status": final_state},
        ]

        SsmHook().get_waiter(self.WAITER_NAME).wait(WaiterConfig={"Delay": 0.01})

    def test_instance_wait_until_failure(self, mock_get_command):
        intermediate_state = "Cancelling"
        final_state = SsmRunCommandCompletedSensor.FAILURE_STATES[0]
        mock_get_command.side_effect = [
            {"Status": intermediate_state},
            {"Status": intermediate_state},
            {"Status": final_state},
        ]

        with pytest.raises(WaiterError, match="Waiter encountered a terminal failure state"):
            SsmHook().get_waiter(self.WAITER_NAME).wait(WaiterConfig={"Delay": 0.01})
