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

from unittest import mock

import pytest
from google.api_core.exceptions import GoogleAPICallError
from google.api_core.operation import Operation

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.common.hooks.operation_helpers import OperationHelper


@pytest.fixture
def operation():
    return mock.create_autospec(Operation, instance=True)


class TestWaitForOperationResult:
    def test_returns_operation_result(self, operation):
        assert OperationHelper.wait_for_operation_result(operation=operation) is operation.result.return_value

    def test_forwards_polling_arguments(self, operation):
        polling = mock.MagicMock()
        retry = mock.MagicMock()

        OperationHelper.wait_for_operation_result(
            operation=operation, timeout=30, polling=polling, retry=retry
        )

        operation.result.assert_called_once_with(timeout=30, polling=polling, retry=retry)

    def test_wraps_google_api_call_error(self, operation):
        api_error = GoogleAPICallError("boom")
        operation.result.side_effect = api_error

        with pytest.raises(AirflowException, match="Google API error on operation result call") as exc_info:
            OperationHelper.wait_for_operation_result(operation=operation)

        assert exc_info.value.__cause__ is api_error
        operation.exception.assert_not_called()

    def test_raises_operation_exception_for_other_errors(self, operation):
        operation.result.side_effect = ValueError("unexpected")
        operation.exception.return_value = RuntimeError("operation failed")

        with pytest.raises(AirflowException, match="operation failed"):
            OperationHelper.wait_for_operation_result(operation=operation, timeout=15)

        operation.exception.assert_called_once_with(timeout=15)


class TestWaitForOperation:
    @pytest.mark.parametrize(
        ("timeout", "expected_timeout"),
        [
            pytest.param(30.7, 30, id="float-truncated-to-int"),
            pytest.param(30, 30, id="int-unchanged"),
            pytest.param(None, None, id="none-preserved"),
        ],
    )
    def test_normalises_timeout(self, operation, timeout, expected_timeout):
        OperationHelper().wait_for_operation(operation=operation, timeout=timeout)

        operation.result.assert_called_once_with(timeout=expected_timeout, polling=None, retry=None)

    def test_returns_operation_result(self, operation):
        result = OperationHelper().wait_for_operation(operation=operation)

        assert result is operation.result.return_value
