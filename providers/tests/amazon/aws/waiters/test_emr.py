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

from typing import Sequence
from unittest import mock

import boto3
import pytest
from botocore.exceptions import WaiterError

from airflow.providers.amazon.aws.hooks.emr import EmrHook


class TestCustomEmrServiceWaiters:
    JOBFLOW_ID = "test_jobflow_id"
    STEP_ID1 = "test_step_id_1"
    STEP_ID2 = "test_step_id_2"

    @pytest.fixture(autouse=True)
    def setup_test_cases(self, monkeypatch):
        self.client = boto3.client("emr", region_name="eu-west-3")
        monkeypatch.setattr(EmrHook, "conn", self.client)

    @pytest.fixture
    def mock_list_steps(self):
        """Mock ``EmrHook.Client.list_steps`` method."""
        with mock.patch.object(self.client, "list_steps") as m:
            yield m

    def test_service_waiters(self):
        hook_waiters = EmrHook(aws_conn_id=None).list_waiters()
        assert "steps_wait_for_terminal" in hook_waiters

    @staticmethod
    def list_steps(step_records: Sequence[tuple[str, str]]):
        """
        Helper function to generate minimal ListSteps response.
        https://docs.aws.amazon.com/emr/latest/APIReference/API_ListSteps.html
        """
        return {
            "Steps": [
                {
                    "Id": step_record[0],
                    "Status": {
                        "State": step_record[1],
                    },
                }
                for step_record in step_records
            ],
        }

    def test_steps_succeeded(self, mock_list_steps):
        """Test steps succeeded"""
        mock_list_steps.side_effect = [
            self.list_steps([(self.STEP_ID1, "PENDING"), (self.STEP_ID2, "RUNNING")]),
            self.list_steps([(self.STEP_ID1, "RUNNING"), (self.STEP_ID2, "COMPLETED")]),
            self.list_steps([(self.STEP_ID1, "COMPLETED"), (self.STEP_ID2, "COMPLETED")]),
        ]
        waiter = EmrHook(aws_conn_id=None).get_waiter("steps_wait_for_terminal")
        waiter.wait(
            ClusterId=self.JOBFLOW_ID,
            StepIds=[self.STEP_ID1, self.STEP_ID2],
            WaiterConfig={"Delay": 0.01, "MaxAttempts": 3},
        )

    def test_steps_failed(self, mock_list_steps):
        """Test steps failed"""
        mock_list_steps.side_effect = [
            self.list_steps([(self.STEP_ID1, "PENDING"), (self.STEP_ID2, "RUNNING")]),
            self.list_steps([(self.STEP_ID1, "RUNNING"), (self.STEP_ID2, "COMPLETED")]),
            self.list_steps([(self.STEP_ID1, "FAILED"), (self.STEP_ID2, "COMPLETED")]),
        ]
        waiter = EmrHook(aws_conn_id=None).get_waiter("steps_wait_for_terminal")

        with pytest.raises(WaiterError, match="Waiter encountered a terminal failure state"):
            waiter.wait(
                ClusterId=self.JOBFLOW_ID,
                StepIds=[self.STEP_ID1, self.STEP_ID2],
                WaiterConfig={"Delay": 0.01, "MaxAttempts": 3},
            )
