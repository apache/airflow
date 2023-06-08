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

from airflow.providers.amazon.aws.hooks.emr import EmrHook
from airflow.providers.amazon.aws.triggers.emr import EmrAddStepsTrigger
from airflow.triggers.base import TriggerEvent

TEST_JOB_FLOW_ID = "test_job_flow_id"
TEST_STEP_IDS = ["step1", "step2"]
TEST_AWS_CONN_ID = "test-aws-id"
TEST_MAX_ATTEMPTS = 10
TEST_POLL_INTERVAL = 10


class TestEmrAddStepsTrigger:
    def test_emr_add_steps_trigger_serialize(self):
        emr_add_steps_trigger = EmrAddStepsTrigger(
            job_flow_id=TEST_JOB_FLOW_ID,
            step_ids=TEST_STEP_IDS,
            aws_conn_id=TEST_AWS_CONN_ID,
            max_attempts=TEST_MAX_ATTEMPTS,
            poll_interval=TEST_POLL_INTERVAL,
        )
        class_path, args = emr_add_steps_trigger.serialize()
        assert class_path == "airflow.providers.amazon.aws.triggers.emr.EmrAddStepsTrigger"
        assert args["job_flow_id"] == TEST_JOB_FLOW_ID
        assert args["step_ids"] == TEST_STEP_IDS
        assert args["poll_interval"] == str(TEST_POLL_INTERVAL)
        assert args["max_attempts"] == str(TEST_MAX_ATTEMPTS)
        assert args["aws_conn_id"] == TEST_AWS_CONN_ID

    @pytest.mark.asyncio
    @mock.patch.object(EmrHook, "async_conn")
    async def test_emr_add_steps_trigger_run(self, mock_async_conn):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        a_mock.get_waiter().wait = AsyncMock()

        emr_add_steps_trigger = EmrAddStepsTrigger(
            job_flow_id=TEST_JOB_FLOW_ID,
            step_ids=TEST_STEP_IDS,
            aws_conn_id=TEST_AWS_CONN_ID,
            max_attempts=TEST_MAX_ATTEMPTS,
            poll_interval=TEST_POLL_INTERVAL,
        )

        generator = emr_add_steps_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {"status": "success", "message": "Steps completed", "step_ids": TEST_STEP_IDS}
        )

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EmrHook, "async_conn")
    async def test_emr_add_steps_trigger_run_multiple_attempts(self, mock_async_conn, mock_sleep):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"Step": {"Status": {"State": "Running", "StateChangeReason": "test_reason"}}},
        )
        a_mock.get_waiter().wait.side_effect = AsyncMock(side_effect=[error, error, True, error, error, True])
        mock_sleep.return_value = True

        emr_add_steps_trigger = EmrAddStepsTrigger(
            job_flow_id=TEST_JOB_FLOW_ID,
            step_ids=TEST_STEP_IDS,
            aws_conn_id=TEST_AWS_CONN_ID,
            max_attempts=TEST_MAX_ATTEMPTS,
            poll_interval=TEST_POLL_INTERVAL,
        )

        generator = emr_add_steps_trigger.run()
        response = await generator.asend(None)

        assert a_mock.get_waiter().wait.call_count == 6
        assert response == TriggerEvent(
            {"status": "success", "message": "Steps completed", "step_ids": TEST_STEP_IDS}
        )

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EmrHook, "async_conn")
    async def test_emr_add_steps_trigger_run_attempts_exceeded(self, mock_async_conn, mock_sleep):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"Step": {"Status": {"State": "Running", "StateChangeReason": "test_reason"}}},
        )
        a_mock.get_waiter().wait.side_effect = AsyncMock(side_effect=[error, error, True])
        mock_sleep.return_value = True

        emr_add_steps_trigger = EmrAddStepsTrigger(
            job_flow_id=TEST_JOB_FLOW_ID,
            step_ids=[TEST_STEP_IDS[0]],
            aws_conn_id=TEST_AWS_CONN_ID,
            max_attempts=2,
            poll_interval=TEST_POLL_INTERVAL,
        )

        generator = emr_add_steps_trigger.run()
        response = await generator.asend(None)

        assert a_mock.get_waiter().wait.call_count == 2
        assert response == TriggerEvent(
            {"status": "failure", "message": "Steps failed: max attempts reached"}
        )

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EmrHook, "async_conn")
    async def test_emr_add_steps_trigger_run_attempts_failed(self, mock_async_conn, mock_sleep):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error_running = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"Step": {"Status": {"State": "Running", "StateChangeReason": "test_reason"}}},
        )
        error_failed = WaiterError(
            name="test_name",
            reason="Waiter encountered a terminal failure state:",
            last_response={"Step": {"Status": {"State": "FAILED", "StateChangeReason": "test_reason"}}},
        )
        a_mock.get_waiter().wait.side_effect = AsyncMock(
            side_effect=[error_running, error_running, error_failed]
        )
        mock_sleep.return_value = True

        emr_add_steps_trigger = EmrAddStepsTrigger(
            job_flow_id=TEST_JOB_FLOW_ID,
            step_ids=[TEST_STEP_IDS[0]],
            aws_conn_id=TEST_AWS_CONN_ID,
            max_attempts=TEST_MAX_ATTEMPTS,
            poll_interval=TEST_POLL_INTERVAL,
        )

        generator = emr_add_steps_trigger.run()
        response = await generator.asend(None)

        assert a_mock.get_waiter().wait.call_count == 3
        assert response == TriggerEvent(
            {"status": "failure", "message": f"Step {TEST_STEP_IDS[0]} failed: {error_failed}"}
        )
