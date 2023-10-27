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

import pytest

from airflow.exceptions import AirflowException, AirflowSkipException, TaskDeferred
from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook
from airflow.providers.amazon.aws.sensors.batch import (
    BatchComputeEnvironmentSensor,
    BatchJobQueueSensor,
    BatchSensor,
)
from airflow.providers.amazon.aws.triggers.batch import BatchJobTrigger

TASK_ID = "batch_job_sensor"
JOB_ID = "8222a1c2-b246-4e19-b1b8-0039bb4407c0"
AWS_REGION = "eu-west-1"
ENVIRONMENT_NAME = "environment_name"
JOB_QUEUE = "job_queue"


@pytest.fixture(scope="module")
def batch_sensor() -> BatchSensor:
    return BatchSensor(
        task_id="batch_job_sensor",
        job_id=JOB_ID,
    )


@pytest.fixture(scope="module")
def deferrable_batch_sensor() -> BatchSensor:
    return BatchSensor(task_id="task", job_id=JOB_ID, region_name=AWS_REGION, deferrable=True)


class TestBatchSensor:
    @mock.patch.object(BatchClientHook, "get_job_description")
    def test_poke_on_success_state(self, mock_get_job_description, batch_sensor: BatchSensor):
        mock_get_job_description.return_value = {"status": "SUCCEEDED"}
        assert batch_sensor.poke({}) is True
        mock_get_job_description.assert_called_once_with(JOB_ID)

    @mock.patch.object(BatchClientHook, "get_job_description")
    def test_poke_on_failure_state(self, mock_get_job_description, batch_sensor: BatchSensor):
        mock_get_job_description.return_value = {"status": "FAILED"}
        with pytest.raises(AirflowException, match="Batch sensor failed. AWS Batch job status: FAILED"):
            batch_sensor.poke({})

        mock_get_job_description.assert_called_once_with(JOB_ID)

    @mock.patch.object(BatchClientHook, "get_job_description")
    def test_poke_on_invalid_state(self, mock_get_job_description, batch_sensor: BatchSensor):
        mock_get_job_description.return_value = {"status": "INVALID"}
        with pytest.raises(
            AirflowException, match="Batch sensor failed. Unknown AWS Batch job status: INVALID"
        ):
            batch_sensor.poke({})

        mock_get_job_description.assert_called_once_with(JOB_ID)

    @pytest.mark.parametrize("job_status", ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"])
    @mock.patch.object(BatchClientHook, "get_job_description")
    def test_poke_on_intermediate_state(
        self, mock_get_job_description, job_status, batch_sensor: BatchSensor
    ):
        print(job_status)
        mock_get_job_description.return_value = {"status": job_status}
        assert batch_sensor.poke({}) is False
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

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @pytest.mark.parametrize(
        "state, error_message",
        (
            (
                BatchClientHook.FAILURE_STATE,
                f"Batch sensor failed. AWS Batch job status: {BatchClientHook.FAILURE_STATE}",
            ),
            ("unknown_state", "Batch sensor failed. Unknown AWS Batch job status: unknown_state"),
        ),
    )
    @mock.patch.object(BatchClientHook, "get_job_description")
    def test_fail_poke(
        self,
        mock_get_job_description,
        batch_sensor: BatchSensor,
        state,
        error_message,
        soft_fail,
        expected_exception,
    ):
        mock_get_job_description.return_value = {"status": state}
        batch_sensor.soft_fail = soft_fail
        with pytest.raises(expected_exception, match=error_message):
            batch_sensor.poke({})


@pytest.fixture(scope="module")
def batch_compute_environment_sensor() -> BatchComputeEnvironmentSensor:
    return BatchComputeEnvironmentSensor(
        task_id="test_batch_compute_environment_sensor",
        compute_environment=ENVIRONMENT_NAME,
    )


class TestBatchComputeEnvironmentSensor:
    @mock.patch.object(BatchClientHook, "client")
    def test_poke_no_environment(
        self, mock_batch_client, batch_compute_environment_sensor: BatchComputeEnvironmentSensor
    ):
        mock_batch_client.describe_compute_environments.return_value = {"computeEnvironments": []}
        with pytest.raises(AirflowException) as ctx:
            batch_compute_environment_sensor.poke({})
        mock_batch_client.describe_compute_environments.assert_called_once_with(
            computeEnvironments=[ENVIRONMENT_NAME],
        )
        assert "not found" in str(ctx.value)

    @mock.patch.object(BatchClientHook, "client")
    def test_poke_valid(
        self, mock_batch_client, batch_compute_environment_sensor: BatchComputeEnvironmentSensor
    ):
        mock_batch_client.describe_compute_environments.return_value = {
            "computeEnvironments": [{"status": "VALID"}]
        }
        assert batch_compute_environment_sensor.poke({}) is True
        mock_batch_client.describe_compute_environments.assert_called_once_with(
            computeEnvironments=[ENVIRONMENT_NAME],
        )

    @mock.patch.object(BatchClientHook, "client")
    def test_poke_running(
        self, mock_batch_client, batch_compute_environment_sensor: BatchComputeEnvironmentSensor
    ):
        mock_batch_client.describe_compute_environments.return_value = {
            "computeEnvironments": [
                {
                    "status": "CREATING",
                }
            ]
        }
        assert batch_compute_environment_sensor.poke({}) is False
        mock_batch_client.describe_compute_environments.assert_called_once_with(
            computeEnvironments=[ENVIRONMENT_NAME],
        )

    @mock.patch.object(BatchClientHook, "client")
    def test_poke_invalid(
        self, mock_batch_client, batch_compute_environment_sensor: BatchComputeEnvironmentSensor
    ):
        mock_batch_client.describe_compute_environments.return_value = {
            "computeEnvironments": [
                {
                    "status": "INVALID",
                }
            ]
        }
        with pytest.raises(AirflowException) as ctx:
            batch_compute_environment_sensor.poke({})
        mock_batch_client.describe_compute_environments.assert_called_once_with(
            computeEnvironments=[ENVIRONMENT_NAME],
        )
        assert "AWS Batch compute environment failed" in str(ctx.value)

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @pytest.mark.parametrize(
        "compute_env, error_message",
        (
            (
                [{"status": "unknown_status"}],
                "AWS Batch compute environment failed. AWS Batch compute environment status:",
            ),
            ([], "AWS Batch compute environment"),
        ),
    )
    @mock.patch.object(BatchClientHook, "client")
    def test_fail_poke(
        self,
        mock_batch_client,
        batch_compute_environment_sensor: BatchComputeEnvironmentSensor,
        compute_env,
        error_message,
        soft_fail,
        expected_exception,
    ):
        mock_batch_client.describe_compute_environments.return_value = {"computeEnvironments": compute_env}
        batch_compute_environment_sensor.soft_fail = soft_fail
        with pytest.raises(expected_exception, match=error_message):
            batch_compute_environment_sensor.poke({})


@pytest.fixture(scope="module")
def batch_job_queue_sensor() -> BatchJobQueueSensor:
    return BatchJobQueueSensor(
        task_id="test_batch_job_queue_sensor",
        job_queue=JOB_QUEUE,
    )


class TestBatchJobQueueSensor:
    @mock.patch.object(BatchClientHook, "client")
    def test_poke_no_queue(self, mock_batch_client, batch_job_queue_sensor: BatchJobQueueSensor):
        mock_batch_client.describe_job_queues.return_value = {"jobQueues": []}
        with pytest.raises(AirflowException) as ctx:
            batch_job_queue_sensor.poke({})
        mock_batch_client.describe_job_queues.assert_called_once_with(
            jobQueues=[JOB_QUEUE],
        )
        assert "not found" in str(ctx.value)

    @mock.patch.object(BatchClientHook, "client")
    def test_poke_no_queue_with_treat_non_existing_as_deleted(
        self, mock_batch_client, batch_job_queue_sensor: BatchJobQueueSensor
    ):
        batch_job_queue_sensor.treat_non_existing_as_deleted = True
        mock_batch_client.describe_job_queues.return_value = {"jobQueues": []}
        assert batch_job_queue_sensor.poke({}) is True
        mock_batch_client.describe_job_queues.assert_called_once_with(
            jobQueues=[JOB_QUEUE],
        )

    @mock.patch.object(BatchClientHook, "client")
    def test_poke_valid(self, mock_batch_client, batch_job_queue_sensor: BatchJobQueueSensor):
        mock_batch_client.describe_job_queues.return_value = {"jobQueues": [{"status": "VALID"}]}
        assert batch_job_queue_sensor.poke({}) is True
        mock_batch_client.describe_job_queues.assert_called_once_with(
            jobQueues=[JOB_QUEUE],
        )

    @mock.patch.object(BatchClientHook, "client")
    def test_poke_running(self, mock_batch_client, batch_job_queue_sensor: BatchJobQueueSensor):
        mock_batch_client.describe_job_queues.return_value = {
            "jobQueues": [
                {
                    "status": "CREATING",
                }
            ]
        }
        assert batch_job_queue_sensor.poke({}) is False
        mock_batch_client.describe_job_queues.assert_called_once_with(
            jobQueues=[JOB_QUEUE],
        )

    @mock.patch.object(BatchClientHook, "client")
    def test_poke_invalid(self, mock_batch_client, batch_job_queue_sensor: BatchJobQueueSensor):
        mock_batch_client.describe_job_queues.return_value = {
            "jobQueues": [
                {
                    "status": "INVALID",
                }
            ]
        }
        with pytest.raises(AirflowException) as ctx:
            batch_job_queue_sensor.poke({})
        mock_batch_client.describe_job_queues.assert_called_once_with(
            jobQueues=[JOB_QUEUE],
        )
        assert "AWS Batch job queue failed" in str(ctx.value)

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @pytest.mark.parametrize("job_queue", ([], [{"status": "UNKNOWN_STATUS"}]))
    @mock.patch.object(BatchClientHook, "client")
    def test_fail_poke(
        self,
        mock_batch_client,
        batch_job_queue_sensor: BatchJobQueueSensor,
        job_queue,
        soft_fail,
        expected_exception,
    ):
        mock_batch_client.describe_job_queues.return_value = {"jobQueues": job_queue}
        batch_job_queue_sensor.treat_non_existing_as_deleted = False
        batch_job_queue_sensor.soft_fail = soft_fail
        message = "AWS Batch job queue"
        with pytest.raises(expected_exception, match=message):
            batch_job_queue_sensor.poke({})
