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

from airflow.providers.amazon.aws.hooks.redshift_data import (
    ABORTED_STATE,
    FAILED_STATE,
    RedshiftDataHook,
    RedshiftDataQueryAbortedError,
    RedshiftDataQueryFailedError,
)
from airflow.providers.amazon.aws.triggers.redshift_data import RedshiftDataTrigger
from airflow.triggers.base import TriggerEvent

TEST_CONN_ID = "aws_default"
TEST_TASK_ID = "123"
POLL_INTERVAL = 4.0


class TestRedshiftDataTrigger:
    def test_redshift_data_trigger_serialization(self):
        """
        Asserts that the RedshiftDataTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = RedshiftDataTrigger(
            statement_id=[],
            task_id=TEST_TASK_ID,
            aws_conn_id=TEST_CONN_ID,
            poll_interval=POLL_INTERVAL,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.redshift_data.RedshiftDataTrigger"
        assert kwargs == {
            "statement_id": [],
            "task_id": TEST_TASK_ID,
            "poll_interval": POLL_INTERVAL,
            "aws_conn_id": TEST_CONN_ID,
            "region_name": None,
            "botocore_config": None,
            "verify": None,
            "cancel_on_kill": True,
        }

    def test_redshift_data_trigger_serialization_cancel_on_kill_false(self):
        """cancel_on_kill=False round-trips through serialization."""
        trigger = RedshiftDataTrigger(
            statement_id="uuid",
            task_id=TEST_TASK_ID,
            aws_conn_id=TEST_CONN_ID,
            poll_interval=POLL_INTERVAL,
            cancel_on_kill=False,
        )
        _, kwargs = trigger.serialize()
        assert kwargs["cancel_on_kill"] is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("return_value", "response"),
        [
            (
                True,
                TriggerEvent({"status": "success", "statement_id": "uuid"}),
            ),
            (
                False,
                TriggerEvent(
                    {"status": "error", "message": f"{TEST_TASK_ID} failed", "statement_id": "uuid"}
                ),
            ),
        ],
    )
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.check_query_is_finished_async"
    )
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.is_still_running",
        return_value=False,
    )
    async def test_redshift_data_trigger_run(
        self, mocked_is_still_running, mock_check_query_is_finised_async, return_value, response
    ):
        """
        Tests that RedshiftDataTrigger only fires once the query execution reaches a successful state.
        """
        mock_check_query_is_finised_async.return_value = return_value
        trigger = RedshiftDataTrigger(
            statement_id="uuid",
            task_id=TEST_TASK_ID,
            poll_interval=POLL_INTERVAL,
            aws_conn_id=TEST_CONN_ID,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert response == actual

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("raised_exception", "expected_response"),
        [
            (
                RedshiftDataQueryFailedError("Failed"),
                {
                    "status": "error",
                    "statement_id": "uuid",
                    "message": "Failed",
                    "type": FAILED_STATE,
                },
            ),
            (
                RedshiftDataQueryAbortedError("Aborted"),
                {
                    "status": "error",
                    "statement_id": "uuid",
                    "message": "Aborted",
                    "type": ABORTED_STATE,
                },
            ),
            (
                Exception(f"{TEST_TASK_ID} failed"),
                {"status": "error", "statement_id": "uuid", "message": f"{TEST_TASK_ID} failed"},
            ),
        ],
    )
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.check_query_is_finished_async"
    )
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.is_still_running",
        return_value=False,
    )
    async def test_redshift_data_trigger_exception(
        self, mocked_is_still_running, mock_check_query_is_finised_async, raised_exception, expected_response
    ):
        """
        Test that RedshiftDataTrigger fires the correct event in case of an error.
        """
        mock_check_query_is_finised_async.side_effect = raised_exception

        trigger = RedshiftDataTrigger(
            statement_id="uuid",
            task_id=TEST_TASK_ID,
            poll_interval=POLL_INTERVAL,
            aws_conn_id=TEST_CONN_ID,
        )
        task = [i async for i in trigger.run()]
        assert len(task) == 1
        assert TriggerEvent(expected_response) in task

    @pytest.mark.asyncio
    @mock.patch.object(RedshiftDataHook, "get_async_conn")
    async def test_on_kill_cancels_the_statement(self, mock_get_async_conn):
        """on_kill() cancels the running statement when enabled and a statement_id is set."""
        mock_client = mock.AsyncMock()
        mock_cm = mock.AsyncMock()
        mock_cm.__aenter__.return_value = mock_client
        mock_get_async_conn.return_value = mock_cm

        trigger = RedshiftDataTrigger(
            statement_id="uuid",
            task_id=TEST_TASK_ID,
            poll_interval=POLL_INTERVAL,
            aws_conn_id=TEST_CONN_ID,
        )
        await trigger.on_kill()

        mock_client.cancel_statement.assert_awaited_once_with(Id="uuid")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("cancel_on_kill", "statement_id"),
        [
            pytest.param(False, "uuid", id="disabled"),
            pytest.param(True, "", id="no-statement-id"),
        ],
    )
    @mock.patch.object(RedshiftDataHook, "get_async_conn")
    async def test_on_kill_does_not_cancel(self, mock_get_async_conn, cancel_on_kill, statement_id):
        """on_kill() is a no-op (no connection opened) when disabled or without a statement_id."""
        trigger = RedshiftDataTrigger(
            statement_id=statement_id,
            task_id=TEST_TASK_ID,
            poll_interval=POLL_INTERVAL,
            aws_conn_id=TEST_CONN_ID,
            cancel_on_kill=cancel_on_kill,
        )
        await trigger.on_kill()

        mock_get_async_conn.assert_not_called()

    @pytest.mark.asyncio
    @mock.patch.object(RedshiftDataHook, "get_async_conn")
    async def test_on_kill_swallows_cancel_errors(self, mock_get_async_conn):
        """on_kill() logs and swallows exceptions raised while cancelling."""
        mock_client = mock.AsyncMock()
        mock_client.cancel_statement.side_effect = Exception("AWS API error")
        mock_cm = mock.AsyncMock()
        mock_cm.__aenter__.return_value = mock_client
        mock_get_async_conn.return_value = mock_cm

        trigger = RedshiftDataTrigger(
            statement_id="uuid",
            task_id=TEST_TASK_ID,
            poll_interval=POLL_INTERVAL,
            aws_conn_id=TEST_CONN_ID,
        )
        await trigger.on_kill()

        mock_client.cancel_statement.assert_awaited_once_with(Id="uuid")
