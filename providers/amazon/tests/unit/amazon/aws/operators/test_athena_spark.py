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

from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.operators.athena_spark import AthenaSparkOperator
from airflow.providers.common.compat.sdk import AirflowException

CALC_ID = "calc-exec-123"
SESSION_ID = "session-456"
CODE_BLOCK = "1 + 1"


@pytest.fixture
def operator():
    return AthenaSparkOperator(
        task_id="test_athena_spark",
        session_id=SESSION_ID,
        code_block=CODE_BLOCK,
        poll_interval=0,
        max_polling_attempts=5,
    )


@pytest.fixture
def context():
    return {"ti": None}


def _exec_info(state: str, submission_time=None, completion_time=None):
    return {
        "Status": {
            "State": state,
            "SubmissionDateTime": submission_time,
            "CompletionDateTime": completion_time,
        }
    }


class TestAthenaSparkOperator:
    def test_init(self, operator):
        assert operator.session_id == SESSION_ID
        assert operator.code_block == CODE_BLOCK
        assert operator.poll_interval == 0
        assert operator.max_polling_attempts == 5
        assert operator._calculation_execution_id is None

    def test_template_fields(self):
        assert "session_id" in AthenaSparkOperator.template_fields
        assert "code_block" in AthenaSparkOperator.template_fields
        assert "description" in AthenaSparkOperator.template_fields

    @mock.patch.object(AthenaHook, "get_calculation_info", return_value={})
    @mock.patch.object(AthenaHook, "get_calculation_state_change_reason", return_value=None)
    @mock.patch.object(AthenaHook, "check_calculation_status", return_value="COMPLETED")
    @mock.patch.object(AthenaHook, "start_calculation", return_value=CALC_ID)
    def test_execute_success_immediate_completed(
        self, mock_start, mock_check, mock_reason, mock_info, operator, context
    ):
        mock_info.return_value = _exec_info("COMPLETED")
        result = operator.execute(context)
        mock_start.assert_called_once_with(
            session_id=SESSION_ID,
            code_block=CODE_BLOCK,
            description=None,
            client_request_token=None,
        )
        assert result["calculation_execution_id"] == CALC_ID
        assert result["state"] == "COMPLETED"
        assert result["session_id"] == SESSION_ID

    @mock.patch.object(AthenaHook, "get_calculation_info", return_value={})
    @mock.patch.object(AthenaHook, "get_calculation_state_change_reason", return_value="Job failed")
    @mock.patch.object(AthenaHook, "check_calculation_status", return_value="FAILED")
    @mock.patch.object(AthenaHook, "start_calculation", return_value=CALC_ID)
    def test_execute_failure_raises(self, mock_start, mock_check, mock_reason, mock_info, operator, context):
        mock_info.return_value = _exec_info("FAILED")
        with pytest.raises(AirflowException, match="FAILED"):
            operator.execute(context)
        mock_reason.assert_called()

    @mock.patch.object(AthenaHook, "get_calculation_info", return_value={})
    @mock.patch.object(AthenaHook, "get_calculation_state_change_reason", return_value="Canceled")
    @mock.patch.object(AthenaHook, "check_calculation_status", return_value="CANCELED")
    @mock.patch.object(AthenaHook, "start_calculation", return_value=CALC_ID)
    def test_execute_cancelled_raises(
        self, mock_start, mock_check, mock_reason, mock_info, operator, context
    ):
        mock_info.return_value = _exec_info("CANCELED")
        with pytest.raises(AirflowException, match="CANCELED"):
            operator.execute(context)

    @mock.patch.object(AthenaHook, "get_calculation_info", return_value={})
    @mock.patch.object(AthenaHook, "get_calculation_state_change_reason", return_value=None)
    @mock.patch.object(AthenaHook, "check_calculation_status", side_effect=["RUNNING", "COMPLETED"])
    @mock.patch.object(AthenaHook, "start_calculation", return_value=CALC_ID)
    def test_execute_poll_then_success(
        self, mock_start, mock_check, mock_reason, mock_info, operator, context
    ):
        mock_info.return_value = _exec_info("COMPLETED")
        result = operator.execute(context)
        assert mock_check.call_count == 2
        assert result["state"] == "COMPLETED"

    @mock.patch.object(AthenaHook, "check_calculation_status", return_value="RUNNING")
    @mock.patch.object(AthenaHook, "start_calculation", return_value=CALC_ID)
    def test_execute_poll_timeout(self, mock_start, mock_check, operator, context):
        operator.max_polling_attempts = 2
        with pytest.raises(AirflowException, match="timed out"):
            operator.execute(context)
        assert mock_check.call_count >= 2

    @mock.patch.object(AthenaHook, "check_calculation_status", return_value=None)
    @mock.patch.object(AthenaHook, "start_calculation", return_value=CALC_ID)
    def test_execute_malformed_status_raises(self, mock_start, mock_check, operator, context):
        with pytest.raises(AirflowException, match="Malformed or missing status"):
            operator.execute(context)

    @mock.patch.object(AthenaHook, "stop_calculation")
    def test_on_kill_calls_stop_calculation(self, mock_stop, operator):
        operator._calculation_execution_id = CALC_ID
        operator.on_kill()
        mock_stop.assert_called_once_with(CALC_ID)

    @mock.patch.object(AthenaHook, "stop_calculation")
    def test_on_kill_no_op_when_no_calc_id(self, mock_stop, operator):
        operator.on_kill()
        mock_stop.assert_not_called()
