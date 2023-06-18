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
from botocore.exceptions import WaiterError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.emr import EmrHook
from airflow.providers.amazon.aws.triggers.emr import EmrCreateJobFlowTrigger, EmrTerminateJobFlowTrigger
from airflow.triggers.base import TriggerEvent

TEST_JOB_FLOW_ID = "test-job-flow-id"
TEST_POLL_INTERVAL = 10
TEST_MAX_ATTEMPTS = 10
TEST_AWS_CONN_ID = "test-aws-id"


class TestEmrCreateJobFlowTrigger:
    def test_emr_create_job_flow_trigger_serialize(self):
        """Test serialize method to make sure all parameters are being serialized correctly."""
        emr_create_job_flow_trigger = EmrCreateJobFlowTrigger(
            job_flow_id=TEST_JOB_FLOW_ID,
            aws_conn_id=TEST_AWS_CONN_ID,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPTS,
        )
        class_path, args = emr_create_job_flow_trigger.serialize()
        assert class_path == "airflow.providers.amazon.aws.triggers.emr.EmrCreateJobFlowTrigger"
        assert args["job_flow_id"] == TEST_JOB_FLOW_ID
        assert args["aws_conn_id"] == TEST_AWS_CONN_ID
        assert args["poll_interval"] == str(TEST_POLL_INTERVAL)
        assert args["max_attempts"] == str(TEST_MAX_ATTEMPTS)

    @pytest.mark.asyncio
    @mock.patch.object(EmrHook, "get_waiter")
    @mock.patch.object(EmrHook, "async_conn")
    async def test_emr_create_job_flow_trigger_run(self, mock_async_conn, mock_get_waiter):
        """
        Test run method, with basic success case to assert TriggerEvent contains the
        correct payload.
        """
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock

        mock_get_waiter().wait = AsyncMock()

        emr_create_job_flow_trigger = EmrCreateJobFlowTrigger(
            job_flow_id=TEST_JOB_FLOW_ID,
            aws_conn_id=TEST_AWS_CONN_ID,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPTS,
        )

        generator = emr_create_job_flow_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {
                "status": "success",
                "message": "JobFlow completed successfully",
                "job_flow_id": TEST_JOB_FLOW_ID,
            }
        )

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EmrHook, "get_waiter")
    @mock.patch.object(EmrHook, "async_conn")
    async def test_emr_create_job_flow_trigger_run_multiple_attempts(
        self, mock_async_conn, mock_get_waiter, mock_sleep
    ):
        """
        Test run method with multiple attempts to make sure the waiter retries
        are working as expected.
        """
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"Cluster": {"Status": {"State": "STARTING", "StateChangeReason": "test-reason"}}},
        )
        mock_get_waiter().wait.side_effect = AsyncMock(side_effect=[error, error, True])
        mock_sleep.return_value = True

        emr_create_job_flow_trigger = EmrCreateJobFlowTrigger(
            job_flow_id=TEST_JOB_FLOW_ID,
            aws_conn_id=TEST_AWS_CONN_ID,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPTS,
        )

        generator = emr_create_job_flow_trigger.run()
        response = await generator.asend(None)

        assert mock_get_waiter().wait.call_count == 3
        assert response == TriggerEvent(
            {
                "status": "success",
                "message": "JobFlow completed successfully",
                "job_flow_id": TEST_JOB_FLOW_ID,
            }
        )

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EmrHook, "get_waiter")
    @mock.patch.object(EmrHook, "async_conn")
    async def test_emr_create_job_flow_trigger_run_attempts_exceeded(
        self, mock_async_conn, mock_get_waiter, mock_sleep
    ):
        """
        Test run method with max_attempts set to 2 to test the Trigger yields
        the correct TriggerEvent in the case of max_attempts being exceeded.
        """
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"Cluster": {"Status": {"State": "STARTING", "StateChangeReason": "test-reason"}}},
        )
        mock_get_waiter().wait.side_effect = AsyncMock(side_effect=[error, error, True])
        mock_sleep.return_value = True

        emr_create_job_flow_trigger = EmrCreateJobFlowTrigger(
            job_flow_id=TEST_JOB_FLOW_ID,
            aws_conn_id=TEST_AWS_CONN_ID,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=2,
        )

        with pytest.raises(AirflowException) as exc:
            generator = emr_create_job_flow_trigger.run()
            await generator.asend(None)

        assert str(exc.value) == "JobFlow creation failed - max attempts reached: 2"
        assert mock_get_waiter().wait.call_count == 2

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EmrHook, "get_waiter")
    @mock.patch.object(EmrHook, "async_conn")
    async def test_emr_create_job_flow_trigger_run_attempts_failed(
        self, mock_async_conn, mock_get_waiter, mock_sleep
    ):
        """
        Test run method with a failure case to test Trigger yields the correct
        failure TriggerEvent.
        """
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error_starting = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"Cluster": {"Status": {"State": "STARTING", "StateChangeReason": "test-reason"}}},
        )
        error_failed = WaiterError(
            name="test_name",
            reason="Waiter encountered a terminal failure state:",
            last_response={
                "Cluster": {"Status": {"State": "TERMINATED_WITH_ERRORS", "StateChangeReason": "test-reason"}}
            },
        )
        mock_get_waiter().wait.side_effect = AsyncMock(
            side_effect=[error_starting, error_starting, error_failed]
        )
        mock_sleep.return_value = True

        emr_create_job_flow_trigger = EmrCreateJobFlowTrigger(
            job_flow_id=TEST_JOB_FLOW_ID,
            aws_conn_id=TEST_AWS_CONN_ID,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPTS,
        )

        with pytest.raises(AirflowException) as exc:
            generator = emr_create_job_flow_trigger.run()
            await generator.asend(None)

        assert str(exc.value) == f"JobFlow creation failed: {error_failed}"
        assert mock_get_waiter().wait.call_count == 3


class TestEmrTerminateJobFlowTrigger:
    def test_emr_terminate_job_flow_trigger_serialize(self):
        emr_terminate_job_flow_trigger = EmrTerminateJobFlowTrigger(
            job_flow_id=TEST_JOB_FLOW_ID,
            aws_conn_id=TEST_AWS_CONN_ID,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPTS,
        )
        class_path, args = emr_terminate_job_flow_trigger.serialize()
        assert class_path == "airflow.providers.amazon.aws.triggers.emr.EmrTerminateJobFlowTrigger"
        assert args["job_flow_id"] == TEST_JOB_FLOW_ID
        assert args["aws_conn_id"] == TEST_AWS_CONN_ID
        assert args["poll_interval"] == str(TEST_POLL_INTERVAL)
        assert args["max_attempts"] == str(TEST_MAX_ATTEMPTS)

    @pytest.mark.asyncio
    @mock.patch.object(EmrHook, "get_waiter")
    @mock.patch.object(EmrHook, "async_conn")
    async def test_emr_terminate_job_flow_trigger_run(self, mock_async_conn, mock_get_waiter):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock

        mock_get_waiter().wait = AsyncMock()

        emr_terminate_job_flow_trigger = EmrTerminateJobFlowTrigger(
            job_flow_id=TEST_JOB_FLOW_ID,
            aws_conn_id=TEST_AWS_CONN_ID,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPTS,
        )

        generator = emr_terminate_job_flow_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {
                "status": "success",
                "message": "JobFlow terminated successfully",
            }
        )

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EmrHook, "get_waiter")
    @mock.patch.object(EmrHook, "async_conn")
    async def test_emr_terminate_job_flow_trigger_run_multiple_attempts(
        self, mock_async_conn, mock_get_waiter, mock_sleep
    ):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={
                "Cluster": {"Status": {"State": "TERMINATING", "StateChangeReason": "test-reason"}}
            },
        )
        mock_get_waiter().wait.side_effect = AsyncMock(side_effect=[error, error, True])
        mock_sleep.return_value = True

        emr_terminate_job_flow_trigger = EmrTerminateJobFlowTrigger(
            job_flow_id=TEST_JOB_FLOW_ID,
            aws_conn_id=TEST_AWS_CONN_ID,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPTS,
        )

        generator = emr_terminate_job_flow_trigger.run()
        response = await generator.asend(None)

        assert mock_get_waiter().wait.call_count == 3
        assert response == TriggerEvent(
            {
                "status": "success",
                "message": "JobFlow terminated successfully",
            }
        )

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EmrHook, "get_waiter")
    @mock.patch.object(EmrHook, "async_conn")
    async def test_emr_terminate_job_flow_trigger_run_attempts_exceeded(
        self, mock_async_conn, mock_get_waiter, mock_sleep
    ):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={
                "Cluster": {"Status": {"State": "TERMINATING", "StateChangeReason": "test-reason"}}
            },
        )
        mock_get_waiter().wait.side_effect = AsyncMock(side_effect=[error, error, True])
        mock_sleep.return_value = True

        emr_terminate_job_flow_trigger = EmrTerminateJobFlowTrigger(
            job_flow_id=TEST_JOB_FLOW_ID,
            aws_conn_id=TEST_AWS_CONN_ID,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=2,
        )
        with pytest.raises(AirflowException) as exc:
            generator = emr_terminate_job_flow_trigger.run()
            await generator.asend(None)

        assert str(exc.value) == "JobFlow termination failed - max attempts reached: 2"
        assert mock_get_waiter().wait.call_count == 2

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EmrHook, "get_waiter")
    @mock.patch.object(EmrHook, "async_conn")
    async def test_emr_terminate_job_flow_trigger_run_attempts_failed(
        self, mock_async_conn, mock_get_waiter, mock_sleep
    ):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error_starting = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={
                "Cluster": {"Status": {"State": "TERMINATING", "StateChangeReason": "test-reason"}}
            },
        )
        error_failed = WaiterError(
            name="test_name",
            reason="Waiter encountered a terminal failure state:",
            last_response={
                "Cluster": {"Status": {"State": "TERMINATED_WITH_ERRORS", "StateChangeReason": "test-reason"}}
            },
        )
        mock_get_waiter().wait.side_effect = AsyncMock(
            side_effect=[error_starting, error_starting, error_failed]
        )
        mock_sleep.return_value = True

        emr_terminate_job_flow_trigger = EmrTerminateJobFlowTrigger(
            job_flow_id=TEST_JOB_FLOW_ID,
            aws_conn_id=TEST_AWS_CONN_ID,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempts=TEST_MAX_ATTEMPTS,
        )
        with pytest.raises(AirflowException) as exc:
            generator = emr_terminate_job_flow_trigger.run()
            await generator.asend(None)

        assert str(exc.value) == f"JobFlow termination failed: {error_failed}"
        assert mock_get_waiter().wait.call_count == 3
