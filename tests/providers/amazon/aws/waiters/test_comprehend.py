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

from airflow.providers.amazon.aws.hooks.comprehend import ComprehendHook
from airflow.providers.amazon.aws.sensors.comprehend import (
    ComprehendStartPiiEntitiesDetectionJobCompletedSensor,
)


class TestComprehendCustomWaiters:
    def test_service_waiters(self):
        assert "pii_entities_detection_job_complete" in ComprehendHook().list_waiters()


class TestComprehendCustomWaitersBase:
    @pytest.fixture(autouse=True)
    def mock_conn(self, monkeypatch):
        self.client = boto3.client("comprehend")
        monkeypatch.setattr(ComprehendHook, "conn", self.client)


class TestComprehendStartPiiEntitiesDetectionJobCompleteWaiter(TestComprehendCustomWaitersBase):
    WAITER_NAME = "pii_entities_detection_job_complete"

    @pytest.fixture
    def mock_get_job(self):
        with mock.patch.object(self.client, "describe_pii_entities_detection_job") as mock_getter:
            yield mock_getter

    @pytest.mark.parametrize("state", ComprehendStartPiiEntitiesDetectionJobCompletedSensor.SUCCESS_STATES)
    def test_pii_entities_detection_job_complete(self, state, mock_get_job):
        mock_get_job.return_value = {"PiiEntitiesDetectionJobProperties": {"JobStatus": state}}

        ComprehendHook().get_waiter(self.WAITER_NAME).wait(JobId="job_id")

    @pytest.mark.parametrize("state", ComprehendStartPiiEntitiesDetectionJobCompletedSensor.FAILURE_STATES)
    def test_pii_entities_detection_job_failed(self, state, mock_get_job):
        mock_get_job.return_value = {"PiiEntitiesDetectionJobProperties": {"JobStatus": state}}

        with pytest.raises(botocore.exceptions.WaiterError):
            ComprehendHook().get_waiter(self.WAITER_NAME).wait(JobId="job_id")

    def test_pii_entities_detection_job_wait(self, mock_get_job):
        wait = {"PiiEntitiesDetectionJobProperties": {"JobStatus": "IN_PROGRESS"}}
        success = {"PiiEntitiesDetectionJobProperties": {"JobStatus": "COMPLETED"}}
        mock_get_job.side_effect = [wait, wait, success]

        ComprehendHook().get_waiter(self.WAITER_NAME).wait(
            JobId="job_id", WaiterConfig={"Delay": 0.01, "MaxAttempts": 3}
        )
