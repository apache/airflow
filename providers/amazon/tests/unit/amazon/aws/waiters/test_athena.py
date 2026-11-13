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

from airflow.providers.amazon.aws.hooks.athena import AthenaHook


class TestAthenaSparkCustomWaiters:
    def test_calculation_terminal_waiter(self):
        assert "spark_calculation_terminal" in AthenaHook().list_waiters()


class TestAthenaSparkCalculationTerminalWaiter:
    WAITER_NAME = "spark_calculation_terminal"
    CALCULATION_EXECUTION_ID = "calc-exec-123"

    @pytest.fixture(autouse=True)
    def mock_conn(self, monkeypatch):
        self.client = boto3.client("athena")
        monkeypatch.setattr(AthenaHook, "conn", self.client)

    @pytest.fixture
    def mock_get_calculation(self):
        with mock.patch.object(self.client, "get_calculation_execution") as mock_getter:
            yield mock_getter

    @pytest.mark.parametrize("state", ["COMPLETED", "FAILED", "CANCELED"])
    def test_terminal_state_ends_wait(self, state, mock_get_calculation):
        mock_get_calculation.return_value = {"Status": {"State": state}}

        AthenaHook().get_waiter(self.WAITER_NAME).wait(
            CalculationExecutionId=self.CALCULATION_EXECUTION_ID,
            WaiterConfig={"Delay": 0, "MaxAttempts": 1},
        )

        mock_get_calculation.assert_called_once_with(
            CalculationExecutionId=self.CALCULATION_EXECUTION_ID,
        )

    @pytest.mark.parametrize(
        "intermediate",
        ["CREATING", "CREATED", "QUEUED", "RUNNING", "CANCELING"],
    )
    def test_nonterminal_state_retries_then_completes(self, intermediate, mock_get_calculation):
        mock_get_calculation.side_effect = [
            {"Status": {"State": intermediate}},
            {"Status": {"State": intermediate}},
            {"Status": {"State": "COMPLETED"}},
        ]

        AthenaHook().get_waiter(self.WAITER_NAME).wait(
            CalculationExecutionId=self.CALCULATION_EXECUTION_ID,
            WaiterConfig={"Delay": 0, "MaxAttempts": 3},
        )

        assert mock_get_calculation.call_args_list == [
            mock.call(CalculationExecutionId=self.CALCULATION_EXECUTION_ID),
            mock.call(CalculationExecutionId=self.CALCULATION_EXECUTION_ID),
            mock.call(CalculationExecutionId=self.CALCULATION_EXECUTION_ID),
        ]

    def test_running_state_exhausts_attempts(self, mock_get_calculation):
        mock_get_calculation.return_value = {"Status": {"State": "RUNNING"}}

        with pytest.raises(botocore.exceptions.WaiterError, match="Max attempts exceeded"):
            AthenaHook().get_waiter(self.WAITER_NAME).wait(
                CalculationExecutionId=self.CALCULATION_EXECUTION_ID,
                WaiterConfig={"Delay": 0, "MaxAttempts": 3},
            )

        assert mock_get_calculation.call_count == 3
