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

import logging
from typing import Any
from unittest import mock
from unittest.mock import AsyncMock

import pytest
from botocore.exceptions import WaiterError

from airflow.providers.amazon.aws.utils.waiter_with_logging import _LazyStatusFormatter, async_wait, wait
from airflow.providers.common.compat.sdk import AirflowException


def generate_response(state: str) -> dict[str, Any]:
    return {
        "Status": {
            "State": state,
        },
    }


class TestWaiter:
    @mock.patch("time.sleep")
    def test_wait(self, mock_sleep):
        mock_sleep.return_value = True
        mock_waiter = mock.MagicMock()
        error = WaiterError(
            name="test_waiter",
            reason="test_reason",
            last_response=generate_response("Pending"),
        )
        mock_waiter.wait.side_effect = [error, error, True]
        wait(
            waiter=mock_waiter,
            waiter_delay=123,
            waiter_max_attempts=456,
            args={"test_arg": "test_value"},
            failure_message="test failure message",
            status_message="test status message",
            status_args=["Status.State"],
        )

        mock_waiter.wait.assert_called_with(
            **{"test_arg": "test_value"},
            WaiterConfig={
                "MaxAttempts": 1,
            },
        )
        assert mock_waiter.wait.call_count == 3
        mock_sleep.assert_called_with(123)

    @pytest.mark.asyncio
    async def test_async_wait(self):
        mock_waiter = mock.MagicMock()
        error = WaiterError(
            name="test_waiter",
            reason="test_reason",
            last_response=generate_response("Pending"),
        )
        mock_waiter.wait = AsyncMock()
        mock_waiter.wait.side_effect = [error, error, True]

        await async_wait(
            waiter=mock_waiter,
            waiter_delay=0,
            waiter_max_attempts=456,
            args={"test_arg": "test_value"},
            failure_message="test failure message",
            status_message="test status message",
            status_args=["Status.State"],
        )

        mock_waiter.wait.assert_called_with(
            **{"test_arg": "test_value"},
            WaiterConfig={
                "MaxAttempts": 1,
            },
        )
        assert mock_waiter.wait.call_count == 3

    @pytest.mark.asyncio
    async def test_async_wait_with_unknown_failure(self):
        mock_waiter = mock.MagicMock()
        service_exception = WaiterError(
            name="test_waiter",
            reason="An error occurred",
            last_response={
                "Error": {
                    "Message": "Not authorized to perform: states:DescribeExecution on resource",
                    "Code": "AccessDeniedException",
                }
            },
        )
        mock_waiter.wait = AsyncMock()
        mock_waiter.wait.side_effect = [service_exception]
        with pytest.raises(AirflowException) as exc:
            await async_wait(
                waiter=mock_waiter,
                waiter_delay=0,
                waiter_max_attempts=456,
                args={"test_arg": "test_value"},
                failure_message="test failure message",
                status_message="test status message",
                status_args=["Status.State"],
            )

        mock_waiter.wait.assert_called_with(
            **{"test_arg": "test_value"},
            WaiterConfig={
                "MaxAttempts": 1,
            },
        )
        assert "An error occurred" in str(exc)
        assert mock_waiter.wait.call_count == 1

    @mock.patch("time.sleep")
    def test_wait_max_attempts_exceeded(self, mock_sleep):
        mock_sleep.return_value = True
        mock_waiter = mock.MagicMock()
        error = WaiterError(
            name="test_waiter",
            reason="test_reason",
            last_response=generate_response("Pending"),
        )
        mock_waiter.wait.side_effect = [error, error, error]
        with pytest.raises(AirflowException) as exc:
            wait(
                waiter=mock_waiter,
                waiter_delay=123,
                waiter_max_attempts=2,
                args={"test_arg": "test_value"},
                failure_message="test failure message",
                status_message="test status message",
                status_args=["Status.State"],
            )
        assert "Waiter error: max attempts reached" in str(exc)
        mock_waiter.wait.assert_called_with(
            **{"test_arg": "test_value"},
            WaiterConfig={
                "MaxAttempts": 1,
            },
        )

        assert mock_waiter.wait.call_count == 2
        mock_sleep.assert_called_with(123)

    @mock.patch("time.sleep")
    def test_wait_with_failure(self, mock_sleep):
        mock_sleep.return_value = True
        mock_waiter = mock.MagicMock()
        error = WaiterError(
            name="test_waiter",
            reason="test_reason",
            last_response=generate_response("Pending"),
        )
        failure_error = WaiterError(
            name="test_waiter",
            reason="terminal failure in waiter",
            last_response=generate_response("Failure"),
        )
        mock_waiter.wait.side_effect = [error, error, error, failure_error]

        with pytest.raises(AirflowException) as exc:
            wait(
                waiter=mock_waiter,
                waiter_delay=123,
                waiter_max_attempts=10,
                args={"test_arg": "test_value"},
                failure_message="test failure message",
                status_message="test status message",
                status_args=["Status.State"],
            )

        assert "test failure message" in str(exc)
        mock_waiter.wait.assert_called_with(
            **{"test_arg": "test_value"},
            WaiterConfig={
                "MaxAttempts": 1,
            },
        )
        assert mock_waiter.wait.call_count == 4

    @mock.patch("time.sleep")
    def test_wait_with_unknown_failure(self, mock_sleep):
        mock_sleep.return_value = True
        mock_waiter = mock.MagicMock()
        service_exception = WaiterError(
            name="test_waiter",
            reason="An error occurred",
            last_response={
                "Error": {
                    "Message": "Not authorized to perform: states:DescribeExecution on resource",
                    "Code": "AccessDeniedException",
                }
            },
        )
        mock_waiter.wait.side_effect = [service_exception]

        with pytest.raises(AirflowException) as exc:
            wait(
                waiter=mock_waiter,
                waiter_delay=123,
                waiter_max_attempts=10,
                args={"test_arg": "test_value"},
                failure_message="test failure message",
                status_message="test status message",
                status_args=["Status.State"],
            )

        assert "An error occurred" in str(exc)
        mock_waiter.wait.assert_called_with(
            **{"test_arg": "test_value"},
            WaiterConfig={
                "MaxAttempts": 1,
            },
        )
        assert mock_waiter.wait.call_count == 1

    @mock.patch("time.sleep")
    def test_wait_with_list_response(self, mock_sleep):
        mock_sleep.return_value = True
        mock_waiter = mock.MagicMock()
        error = WaiterError(
            name="test_waiter",
            reason="test_reason",
            last_response={
                "Clusters": [
                    {
                        "Status": "Pending",
                    },
                    {
                        "Status": "Pending",
                    },
                ]
            },
        )
        mock_waiter.wait.side_effect = [error, error, True]
        wait(
            waiter=mock_waiter,
            waiter_delay=123,
            waiter_max_attempts=456,
            args={"test_arg": "test_value"},
            failure_message="test failure message",
            status_message="test status message",
            status_args=["Clusters[0].Status"],
        )

        mock_waiter.wait.assert_called_with(
            **{"test_arg": "test_value"},
            WaiterConfig={
                "MaxAttempts": 1,
            },
        )
        mock_waiter.wait.call_count == 3
        mock_sleep.assert_called_with(123)

    @mock.patch("time.sleep")
    def test_wait_with_incorrect_args(self, mock_sleep):
        mock_sleep.return_value = True
        mock_waiter = mock.MagicMock()
        error = WaiterError(
            name="test_waiter",
            reason="test_reason",
            last_response={
                "Clusters": [
                    {
                        "Status": "Pending",
                    },
                    {
                        "Status": "Pending",
                    },
                ]
            },
        )
        mock_waiter.wait.side_effect = [error, error, True]
        wait(
            waiter=mock_waiter,
            waiter_delay=123,
            waiter_max_attempts=456,
            args={"test_arg": "test_value"},
            failure_message="test failure message",
            status_message="test status message",
            status_args=["Clusters[0].State"],  # this does not exist in the response
        )

        mock_waiter.wait.assert_called_with(
            **{"test_arg": "test_value"},
            WaiterConfig={
                "MaxAttempts": 1,
            },
        )
        assert mock_waiter.wait.call_count == 3
        mock_sleep.assert_called_with(123)

    @mock.patch("time.sleep")
    def test_wait_with_multiple_args(self, mock_sleep):
        mock_sleep.return_value = True
        mock_waiter = mock.MagicMock()
        error = WaiterError(
            name="test_waiter",
            reason="test_reason",
            last_response={
                "Clusters": [
                    {
                        "Status": "Pending",
                        "StatusDetails": "test_details",
                        "ClusterName": "test_name",
                    },
                ]
            },
        )
        mock_waiter.wait.side_effect = [error, error, True]
        wait(
            waiter=mock_waiter,
            waiter_delay=123,
            waiter_max_attempts=456,
            args={"test_arg": "test_value"},
            failure_message="test failure message",
            status_message="test status message",
            status_args=["Clusters[0].Status", "Clusters[0].StatusDetails", "Clusters[0].ClusterName"],
        )
        assert mock_waiter.wait.call_count == 3
        mock_sleep.assert_called_with(123)

    @mock.patch.object(_LazyStatusFormatter, "__str__")
    def test_status_formatting_not_done_if_higher_log_level(self, status_format_mock: mock.MagicMock):
        status_format_mock.return_value = "test_status"
        mock_waiter = mock.MagicMock()
        error = WaiterError(
            name="test_waiter",
            reason="test_reason",
            last_response=generate_response("Pending"),
        )
        logger = logging.getLogger(wait.__module__)
        level = logger.getEffectiveLevel()
        logger.setLevel(logging.WARNING)
        try:
            mock_waiter.wait.side_effect = [error, error, True]

            wait(
                waiter=mock_waiter,
                waiter_delay=0,
                waiter_max_attempts=456,
                args={"test_arg": "test_value"},
                failure_message="test failure message",
                status_message="test status message",
                status_args=["Status.State"],
            )
        finally:
            logger.setLevel(level)
        status_format_mock.assert_not_called()
