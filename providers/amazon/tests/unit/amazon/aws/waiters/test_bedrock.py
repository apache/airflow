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

import inspect
import re
import sys
from unittest import mock

import boto3
import botocore
import pytest

from airflow.providers.amazon.aws.hooks.bedrock import BedrockHook
from airflow.providers.amazon.aws.sensors.bedrock import (
    BedrockBatchInferenceSensor,
    BedrockCustomizeModelCompletedSensor,
    BedrockProvisionModelThroughputCompletedSensor,
)


class TestBedrockCustomWaiters:
    def test_service_waiters(self):
        """Ensure that all custom Bedrock waiters have unit tests."""

        def _class_tests_a_waiter(class_name: str) -> bool:
            """Check if the class name starts with 'Test' and ends with 'Waiter'."""
            return bool(re.match(r"^Test[A-Za-z]+Waiter$", class_name))

        # Collect WAITER_NAME from each waiter test class in this module.
        test_classes = inspect.getmembers(sys.modules[__name__], inspect.isclass)
        waiters_tested = [cls.WAITER_NAME for (name, cls) in test_classes if _class_tests_a_waiter(name)]

        assert sorted(BedrockHook()._list_custom_waiters()) == sorted(waiters_tested)


class TestBedrockCustomWaitersBase:
    @pytest.fixture(autouse=True)
    def mock_conn(self, monkeypatch):
        self.client = boto3.client("bedrock")
        monkeypatch.setattr(BedrockHook, "conn", self.client)


class TestModelCustomizationJobCompleteWaiter(TestBedrockCustomWaitersBase):
    WAITER_NAME = "model_customization_job_complete"

    @pytest.fixture
    def mock_get_job(self):
        with mock.patch.object(self.client, "get_model_customization_job") as mock_getter:
            yield mock_getter

    @pytest.mark.parametrize("state", BedrockCustomizeModelCompletedSensor.SUCCESS_STATES)
    def test_model_customization_job_complete(self, state, mock_get_job):
        mock_get_job.return_value = {"status": state}

        BedrockHook().get_waiter(self.WAITER_NAME).wait(jobIdentifier="job_id")

    @pytest.mark.parametrize("state", BedrockCustomizeModelCompletedSensor.FAILURE_STATES)
    def test_model_customization_job_failed(self, state, mock_get_job):
        mock_get_job.return_value = {"status": state}

        with pytest.raises(botocore.exceptions.WaiterError):
            BedrockHook().get_waiter(self.WAITER_NAME).wait(jobIdentifier="job_id")

    def test_model_customization_job_wait(self, mock_get_job):
        wait = {"status": "InProgress"}
        success = {"status": "Completed"}
        mock_get_job.side_effect = [wait, wait, success]

        BedrockHook().get_waiter(self.WAITER_NAME).wait(
            jobIdentifier="job_id", WaiterConfig={"Delay": 0.01, "MaxAttempts": 3}
        )


class TestProvisionedModelThroughputCompleteWaiter(TestBedrockCustomWaitersBase):
    WAITER_NAME = "provisioned_model_throughput_complete"

    @pytest.fixture
    def mock_get_job(self):
        with mock.patch.object(self.client, "get_provisioned_model_throughput") as mock_getter:
            yield mock_getter

    @pytest.mark.parametrize("state", BedrockProvisionModelThroughputCompletedSensor.SUCCESS_STATES)
    def test_model_customization_job_complete(self, state, mock_get_job):
        mock_get_job.return_value = {"status": state}

        BedrockHook().get_waiter(self.WAITER_NAME).wait(jobIdentifier="job_id")

    @pytest.mark.parametrize("state", BedrockProvisionModelThroughputCompletedSensor.FAILURE_STATES)
    def test_model_customization_job_failed(self, state, mock_get_job):
        mock_get_job.return_value = {"status": state}

        with pytest.raises(botocore.exceptions.WaiterError):
            BedrockHook().get_waiter(self.WAITER_NAME).wait(jobIdentifier="job_id")

    def test_model_customization_job_wait(self, mock_get_job):
        wait = {"status": "Creating"}
        success = {"status": "InService"}
        mock_get_job.side_effect = [wait, wait, success]

        BedrockHook().get_waiter(self.WAITER_NAME).wait(
            jobIdentifier="job_id", WaiterConfig={"Delay": 0.01, "MaxAttempts": 3}
        )


class TestBatchInferenceCompleteWaiter(TestBedrockCustomWaitersBase):
    WAITER_NAME = "batch_inference_complete"
    SENSOR = BedrockBatchInferenceSensor(
        task_id="task_id",
        job_arn="job_arn",
        success_state=BedrockBatchInferenceSensor.SuccessState.COMPLETED,
    )

    @pytest.fixture
    def mock_get_job(self):
        with mock.patch.object(self.client, "get_model_invocation_job") as mock_getter:
            yield mock_getter

    @pytest.mark.parametrize("state", SENSOR.SUCCESS_STATES)
    def test_batch_inference_complete(self, state, mock_get_job):
        mock_get_job.return_value = {"status": state}

        BedrockHook().get_waiter(self.WAITER_NAME).wait(jobIdentifier="job_arn")

    @pytest.mark.parametrize("state", SENSOR.FAILURE_STATES)
    def test_batch_inference_failed(self, state, mock_get_job):
        mock_get_job.return_value = {"status": state}

        with pytest.raises(botocore.exceptions.WaiterError):
            BedrockHook().get_waiter(self.WAITER_NAME).wait(jobIdentifier="job_arn")

    def test_batch_inference_wait(self, mock_get_job):
        wait = {"status": "InProgress"}
        success = {"status": "Completed"}
        mock_get_job.side_effect = [wait, wait, success]

        BedrockHook().get_waiter(self.WAITER_NAME).wait(
            jobIdentifier="job_arn", WaiterConfig={"Delay": 0.01, "MaxAttempts": 3}
        )


class TestBatchInferenceScheduledWaiter(TestBedrockCustomWaitersBase):
    WAITER_NAME = "batch_inference_scheduled"
    SENSOR = BedrockBatchInferenceSensor(
        task_id="task_id",
        job_arn="job_arn",
        success_state=BedrockBatchInferenceSensor.SuccessState.SCHEDULED,
    )

    @pytest.fixture
    def mock_get_job(self):
        with mock.patch.object(self.client, "get_model_invocation_job") as mock_getter:
            yield mock_getter

    @pytest.mark.parametrize("state", SENSOR.SUCCESS_STATES)
    def test_batch_inference_complete(self, state, mock_get_job):
        mock_get_job.return_value = {"status": state}

        BedrockHook().get_waiter(self.WAITER_NAME).wait(jobIdentifier="job_arn")

    @pytest.mark.parametrize("state", SENSOR.FAILURE_STATES)
    def test_batch_inference_failed(self, state, mock_get_job):
        mock_get_job.return_value = {"status": state}

        with pytest.raises(botocore.exceptions.WaiterError):
            BedrockHook().get_waiter(self.WAITER_NAME).wait(jobIdentifier="job_arn")

    def test_batch_inference_wait(self, mock_get_job):
        wait = {"status": "InProgress"}
        success = {"status": "Completed"}
        mock_get_job.side_effect = [wait, wait, success]

        BedrockHook().get_waiter(self.WAITER_NAME).wait(
            jobIdentifier="job_arn", WaiterConfig={"Delay": 0.01, "MaxAttempts": 3}
        )
