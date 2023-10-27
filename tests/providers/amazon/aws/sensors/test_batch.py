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

from typing import TYPE_CHECKING, Any
from unittest import mock

import pytest

from airflow.exceptions import AirflowException, AirflowSkipException, TaskDeferred
from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook
from airflow.providers.amazon.aws.sensors.batch import (
    BatchComputeEnvironmentSensor,
    BatchJobQueueSensor,
    BatchSensor,
)
from airflow.providers.amazon.aws.triggers.batch import BatchJobTrigger

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor


TASK_ID = "batch_job_sensor"
JOB_ID = "8222a1c2-b246-4e19-b1b8-0039bb4407c0"
AWS_REGION = "eu-west-1"
ENVIRONMENT_NAME = "environment_name"
JOB_QUEUE = "job_queue"

SOFT_FAIL_CASES = [
    pytest.param(False, AirflowException, id="not-soft-fail"),
    pytest.param(True, AirflowSkipException, id="soft-fail"),
]


@pytest.fixture(scope="module")
def deferrable_batch_sensor() -> BatchSensor:
    return BatchSensor(task_id="task", job_id=JOB_ID, region_name=AWS_REGION, deferrable=True)


@pytest.fixture
def mock_get_job_description():
    with mock.patch.object(BatchClientHook, "get_job_description") as m:
        yield m


@pytest.fixture
def mock_batch_client():
    with mock.patch.object(BatchClientHook, "client") as m:
        yield m


class BaseBatchSensorsTests:
    """Base test class for Batch Sensors."""

    op_class: type[AwsBaseSensor]
    default_op_kwargs: dict[str, Any]

    def test_base_aws_op_attributes(self):
        op = self.op_class(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = self.op_class(
            **self.default_op_kwargs,
            aws_conn_id="aws-test-custom-conn",
            region_name="eu-west-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        assert op.hook.aws_conn_id == "aws-test-custom-conn"
        assert op.hook._region_name == "eu-west-1"
        assert op.hook._verify is False
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42


class TestBatchSensor(BaseBatchSensorsTests):
    op_class = BatchSensor

    @pytest.fixture(autouse=True)
    def setup_test_cases(self):
        self.default_op_kwargs = dict(
            task_id="test_batch_sensor",
            job_id=JOB_ID,
        )
        self.sensor = self.op_class(**self.default_op_kwargs, aws_conn_id=None)

    def test_poke_on_success_state(self, mock_get_job_description):
        mock_get_job_description.return_value = {"status": "SUCCEEDED"}
        assert self.sensor.poke({}) is True
        mock_get_job_description.assert_called_once_with(JOB_ID)

    @pytest.mark.parametrize("job_status", ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"])
    def test_poke_on_intermediate_state(self, mock_get_job_description, job_status):
        mock_get_job_description.return_value = {"status": job_status}
        assert self.sensor.poke({}) is False
        mock_get_job_description.assert_called_once_with(JOB_ID)

    def test_execute_in_deferrable_mode(self, deferrable_batch_sensor: BatchSensor):
        """
        Asserts that a task is deferred and a BatchSensorTrigger will be fired
        when the BatchSensor is executed in deferrable mode.
        """

        with pytest.raises(TaskDeferred) as exc:
            deferrable_batch_sensor.execute({})
        assert isinstance(exc.value.trigger, BatchJobTrigger), "Trigger is not a BatchJobTrigger"

    def test_execute_failure_in_deferrable_mode(self, deferrable_batch_sensor: BatchSensor):
        """Tests that an AirflowException is raised in case of error event"""

        with pytest.raises(AirflowException):
            deferrable_batch_sensor.execute_complete(context={}, event={"status": "failure"})

    def test_execute_failure_in_deferrable_mode_with_soft_fail(self, deferrable_batch_sensor: BatchSensor):
        """Tests that an AirflowSkipException is raised in case of error event and soft_fail is set to True"""
        deferrable_batch_sensor.soft_fail = True
        with pytest.raises(AirflowSkipException):
            deferrable_batch_sensor.execute_complete(context={}, event={"status": "failure"})

    @pytest.mark.parametrize("soft_fail, expected_exception", SOFT_FAIL_CASES)
    @pytest.mark.parametrize(
        "state, error_message",
        (
            pytest.param(
                BatchClientHook.FAILURE_STATE,
                f"Batch sensor failed. AWS Batch job status: {BatchClientHook.FAILURE_STATE}",
                id="failure",
            ),
            pytest.param(
                "INVALID", "Batch sensor failed. Unknown AWS Batch job status: INVALID", id="unknown"
            ),
        ),
    )
    def test_fail_poke(
        self,
        mock_get_job_description,
        state,
        error_message: str,
        soft_fail,
        expected_exception,
    ):
        mock_get_job_description.return_value = {"status": state}
        batch_sensor = BatchSensor(**self.default_op_kwargs, soft_fail=soft_fail)
        with pytest.raises(expected_exception, match=error_message):
            batch_sensor.poke({})
        mock_get_job_description.assert_called_once_with(JOB_ID)


class TestBatchComputeEnvironmentSensor(BaseBatchSensorsTests):
    op_class = BatchComputeEnvironmentSensor

    @pytest.fixture(autouse=True)
    def setup_test_cases(self):
        self.default_op_kwargs = dict(
            task_id="test_batch_compute_environment_sensor",
            compute_environment=ENVIRONMENT_NAME,
        )
        self.sensor = self.op_class(**self.default_op_kwargs, aws_conn_id=None)

    @staticmethod
    def _generate_status(status: str | None):
        if status:
            return {"computeEnvironments": [{"status": status}]}
        return {"computeEnvironments": []}

    def test_poke_no_environment(self, mock_batch_client):
        mock_batch_client.describe_compute_environments.return_value = {"computeEnvironments": []}
        with pytest.raises(AirflowException) as ctx:
            self.sensor.poke({})
        mock_batch_client.describe_compute_environments.assert_called_once_with(
            computeEnvironments=[ENVIRONMENT_NAME],
        )
        assert "not found" in str(ctx.value)

    def test_poke_valid(self, mock_batch_client):
        mock_batch_client.describe_compute_environments.return_value = self._generate_status("VALID")
        assert self.sensor.poke({}) is True
        mock_batch_client.describe_compute_environments.assert_called_once_with(
            computeEnvironments=[ENVIRONMENT_NAME],
        )

    def test_poke_running(self, mock_batch_client):
        mock_batch_client.describe_compute_environments.return_value = self._generate_status("CREATING")
        assert self.sensor.poke({}) is False
        mock_batch_client.describe_compute_environments.assert_called_once_with(
            computeEnvironments=[ENVIRONMENT_NAME],
        )

    @pytest.mark.parametrize("soft_fail, expected_exception", SOFT_FAIL_CASES)
    @pytest.mark.parametrize(
        "status, error_message",
        (
            pytest.param(
                "INVALID",
                "AWS Batch compute environment failed. AWS Batch compute environment status: INVALID",
                id="invalid-status",
            ),
            pytest.param(
                "UNKNOWN_STATUS",
                "AWS Batch compute environment failed. AWS Batch compute environment status: UNKNOWN_STATUS",
                id="unknown-status",
            ),
            pytest.param(None, "AWS Batch compute environment .* not found", id="no-ce-exists"),
        ),
    )
    def test_fail_poke(
        self,
        mock_batch_client,
        status,
        error_message,
        soft_fail,
        expected_exception,
    ):
        mock_batch_client.describe_compute_environments.return_value = self._generate_status(status)
        sensor = BatchComputeEnvironmentSensor(
            **self.default_op_kwargs, aws_conn_id=None, soft_fail=soft_fail
        )
        sensor.soft_fail = soft_fail
        with pytest.raises(expected_exception, match=error_message):
            sensor.poke({})


class TestBatchJobQueueSensor(BaseBatchSensorsTests):
    op_class = BatchJobQueueSensor

    @pytest.fixture(autouse=True)
    def setup_test_cases(self):
        self.default_op_kwargs = dict(
            task_id="test_batch_compute_environment_sensor",
            job_queue=JOB_QUEUE,
        )
        self.sensor = self.op_class(**self.default_op_kwargs, aws_conn_id=None)

    @staticmethod
    def _generate_status(status: str | None):
        if status:
            return {"jobQueues": [{"status": status}]}
        return {"jobQueues": []}

    def test_poke_no_queue_with_treat_non_existing_as_deleted(self, mock_batch_client):
        mock_batch_client.describe_job_queues.return_value = self._generate_status(None)
        sensor = BatchJobQueueSensor(
            **self.default_op_kwargs, aws_conn_id=None, treat_non_existing_as_deleted=True
        )
        assert sensor.poke({}) is True
        mock_batch_client.describe_job_queues.assert_called_once_with(
            jobQueues=[JOB_QUEUE],
        )

    def test_poke_valid(self, mock_batch_client):
        mock_batch_client.describe_job_queues.return_value = self._generate_status("VALID")
        assert self.sensor.poke({}) is True
        mock_batch_client.describe_job_queues.assert_called_once_with(
            jobQueues=[JOB_QUEUE],
        )

    def test_poke_running(self, mock_batch_client):
        mock_batch_client.describe_job_queues.return_value = self._generate_status("CREATING")
        assert self.sensor.poke({}) is False
        mock_batch_client.describe_job_queues.assert_called_once_with(
            jobQueues=[JOB_QUEUE],
        )

    @pytest.mark.parametrize("soft_fail, expected_exception", SOFT_FAIL_CASES)
    @pytest.mark.parametrize(
        "status",
        [
            pytest.param(None, id="no-job-queue-exists"),
            pytest.param("INVALID", id="invalid"),
            pytest.param("UNKNOWN_STATUS", id="unknown-status"),
        ],
    )
    def test_fail_poke(self, mock_batch_client, status, soft_fail, expected_exception):
        mock_batch_client.describe_job_queues.return_value = self._generate_status(status)
        sensor = BatchJobQueueSensor(
            **self.default_op_kwargs,
            aws_conn_id=None,
            treat_non_existing_as_deleted=False,
            soft_fail=soft_fail,
        )
        message = "AWS Batch job queue"
        with pytest.raises(expected_exception, match=message):
            sensor.poke({})
