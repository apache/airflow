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
from moto import mock_aws

from airflow.exceptions import TaskDeferred
from airflow.models import DAG
from airflow.providers.amazon.aws.exceptions import WaiterMaxAttemptsError
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.operators.athena_spark import AthenaSparkOperator
from airflow.providers.amazon.aws.triggers.athena import AthenaSparkTrigger

from tests_common.test_utils.compat import timezone
from unit.amazon.aws.utils.test_template_fields import validate_template_fields

TEST_DAG_ID = "unit_tests"
DEFAULT_DATE = timezone.datetime(2018, 1, 1)
ATHENA_CALCULATION_ID = "calc-exec-123"

MOCK_DATA = {
    "task_id": "test_athena_spark_operator",
    "session_id": "session-456",
    "code_block": "1 + 1",
    "description": "Test Spark calculation",
    "client_request_token": "eac427d0-1c6d-4dfb-96aa-2835d3ac6595",
}


def _calculation_info(state: str, submission_time=None, completion_time=None):
    return {
        "CalculationExecutionId": ATHENA_CALCULATION_ID,
        "SessionId": MOCK_DATA["session_id"],
        "WorkingDirectory": "s3://test-bucket/spark/",
        "Status": {
            "State": state,
            "SubmissionDateTime": submission_time,
            "CompletionDateTime": completion_time,
        },
        "Result": {
            "StdOutS3Uri": "s3://test-bucket/spark/stdout",
            "StdErrorS3Uri": "s3://test-bucket/spark/stderr",
            "ResultS3Uri": "s3://test-bucket/spark/results",
            "ResultType": "application/vnd.aws.athena.v1+json",
        },
    }


@mock_aws
class TestAthenaSparkOperator:
    @pytest.fixture(autouse=True)
    def _setup_test_cases(self):
        args = {
            "owner": "airflow",
            "start_date": DEFAULT_DATE,
        }

        self.dag = DAG(TEST_DAG_ID, default_args=args, schedule="@once")
        self.default_op_kwargs = dict(
            task_id=MOCK_DATA["task_id"],
            session_id=MOCK_DATA["session_id"],
            code_block=MOCK_DATA["code_block"],
            client_request_token=MOCK_DATA["client_request_token"],
            waiter_delay=0,
            waiter_max_attempts=3,
            deferrable=False,
        )
        self.athena = AthenaSparkOperator(**self.default_op_kwargs, aws_conn_id=None, dag=self.dag)

    def test_base_aws_op_attributes(self):
        op = AthenaSparkOperator(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None
        assert op.hook.log_query is True

        op = AthenaSparkOperator(
            **self.default_op_kwargs,
            aws_conn_id="aws-test-custom-conn",
            region_name="eu-west-1",
            verify=False,
            botocore_config={"read_timeout": 42},
            log_query=False,
        )
        assert op.hook.aws_conn_id == "aws-test-custom-conn"
        assert op.hook._region_name == "eu-west-1"
        assert op.hook._verify is False
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42
        assert op.hook.log_query is False

    def test_init(self):
        assert self.athena.task_id == MOCK_DATA["task_id"]
        assert self.athena.session_id == MOCK_DATA["session_id"]
        assert self.athena.code_block == MOCK_DATA["code_block"]
        assert self.athena.client_request_token == MOCK_DATA["client_request_token"]
        assert self.athena.waiter_delay == 0
        assert self.athena.waiter_max_attempts == 3
        assert self.athena.deferrable is False
        assert self.athena._calculation_execution_id is None

    @mock.patch.object(AthenaHook, "get_spark_calculation_info")
    @mock.patch.object(AthenaHook, "get_spark_calculation_state_change_reason", return_value=None)
    @mock.patch.object(AthenaHook, "poll_spark_calculation_status", return_value="COMPLETED")
    @mock.patch.object(AthenaHook, "start_spark_calculation", return_value=ATHENA_CALCULATION_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_execute_success(
        self,
        mock_conn,
        mock_start_spark_calculation,
        mock_poll_spark_calculation_status,
        mock_get_spark_calculation_state_change_reason,
        mock_get_spark_calculation_info,
    ):
        mock_get_spark_calculation_info.return_value = _calculation_info("COMPLETED")

        result = self.athena.execute({})

        mock_start_spark_calculation.assert_called_once_with(
            session_id=MOCK_DATA["session_id"],
            code_block=MOCK_DATA["code_block"],
            description=None,
            client_request_token=MOCK_DATA["client_request_token"],
        )

        mock_poll_spark_calculation_status.assert_called_once_with(
            ATHENA_CALCULATION_ID,
            waiter_delay=self.athena.waiter_delay,
            waiter_max_attempts=self.athena.waiter_max_attempts,
        )
        mock_get_spark_calculation_state_change_reason.assert_called_once_with(ATHENA_CALCULATION_ID)
        mock_get_spark_calculation_info.assert_called_once_with(ATHENA_CALCULATION_ID)

        assert result["calculation_execution_id"] == ATHENA_CALCULATION_ID
        assert result["state"] == "COMPLETED"
        assert result["session_id"] == MOCK_DATA["session_id"]
        assert result["working_directory"] == "s3://test-bucket/spark/"
        assert result["stdout_s3_uri"] == "s3://test-bucket/spark/stdout"
        assert result["stderr_s3_uri"] == "s3://test-bucket/spark/stderr"
        assert result["result_s3_uri"] == "s3://test-bucket/spark/results"
        assert result["result_type"] == "application/vnd.aws.athena.v1+json"

    @mock.patch.object(AthenaHook, "get_spark_calculation_info")
    @mock.patch.object(AthenaHook, "get_spark_calculation_state_change_reason", return_value="Job failed")
    @mock.patch.object(AthenaHook, "poll_spark_calculation_status", return_value="FAILED")
    @mock.patch.object(AthenaHook, "start_spark_calculation", return_value=ATHENA_CALCULATION_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_execute_failure(
        self,
        mock_conn,
        mock_start_spark_calculation,
        mock_poll_spark_calculation_status,
        mock_get_spark_calculation_state_change_reason,
        mock_get_spark_calculation_info,
    ):
        mock_get_spark_calculation_info.return_value = _calculation_info("FAILED")

        with pytest.raises(RuntimeError):
            self.athena.execute({})

        mock_start_spark_calculation.assert_called_once_with(
            session_id=MOCK_DATA["session_id"],
            code_block=MOCK_DATA["code_block"],
            description=None,
            client_request_token=MOCK_DATA["client_request_token"],
        )

        mock_poll_spark_calculation_status.assert_called_once_with(
            ATHENA_CALCULATION_ID,
            waiter_delay=self.athena.waiter_delay,
            waiter_max_attempts=self.athena.waiter_max_attempts,
        )

        assert mock_get_spark_calculation_state_change_reason.call_count == 1

    @mock.patch.object(AthenaHook, "get_spark_calculation_info")
    @mock.patch.object(AthenaHook, "get_spark_calculation_state_change_reason", return_value="Canceled")
    @mock.patch.object(AthenaHook, "poll_spark_calculation_status", return_value="CANCELED")
    @mock.patch.object(AthenaHook, "start_spark_calculation", return_value=ATHENA_CALCULATION_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_execute_cancelled(
        self,
        mock_conn,
        mock_start_spark_calculation,
        mock_poll_spark_calculation_status,
        mock_get_spark_calculation_state_change_reason,
        mock_get_spark_calculation_info,
    ):
        mock_get_spark_calculation_info.return_value = _calculation_info("CANCELED")

        with pytest.raises(RuntimeError):
            self.athena.execute({})

        mock_start_spark_calculation.assert_called_once_with(
            session_id=MOCK_DATA["session_id"],
            code_block=MOCK_DATA["code_block"],
            description=None,
            client_request_token=MOCK_DATA["client_request_token"],
        )

        mock_poll_spark_calculation_status.assert_called_once_with(
            ATHENA_CALCULATION_ID,
            waiter_delay=self.athena.waiter_delay,
            waiter_max_attempts=self.athena.waiter_max_attempts,
        )

        assert mock_get_spark_calculation_state_change_reason.call_count == 1

    @mock.patch.object(AthenaHook, "stop_spark_calculation")
    @mock.patch.object(
        AthenaHook,
        "poll_spark_calculation_status",
        side_effect=WaiterMaxAttemptsError("Waiter error: max attempts reached"),
    )
    @mock.patch.object(AthenaHook, "start_spark_calculation", return_value=ATHENA_CALCULATION_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_execute_timeout(
        self,
        mock_conn,
        mock_start_spark_calculation,
        mock_poll_spark_calculation_status,
        mock_stop_spark_calculation,
    ):
        with pytest.raises(WaiterMaxAttemptsError, match="max attempts reached"):
            self.athena.execute({})

        mock_start_spark_calculation.assert_called_once_with(
            session_id=MOCK_DATA["session_id"],
            code_block=MOCK_DATA["code_block"],
            description=None,
            client_request_token=MOCK_DATA["client_request_token"],
        )

        mock_poll_spark_calculation_status.assert_called_once_with(
            ATHENA_CALCULATION_ID,
            waiter_delay=self.athena.waiter_delay,
            waiter_max_attempts=self.athena.waiter_max_attempts,
        )

        mock_stop_spark_calculation.assert_called_once_with(ATHENA_CALCULATION_ID)

    @mock.patch.object(AthenaHook, "poll_spark_calculation_status", return_value=None)
    @mock.patch.object(AthenaHook, "start_spark_calculation", return_value=ATHENA_CALCULATION_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_execute_malformed_status(
        self,
        mock_conn,
        mock_start_spark_calculation,
        mock_poll_spark_calculation_status,
    ):
        with pytest.raises(RuntimeError, match="Malformed or missing status"):
            self.athena.execute({})

        mock_start_spark_calculation.assert_called_once_with(
            session_id=MOCK_DATA["session_id"],
            code_block=MOCK_DATA["code_block"],
            description=None,
            client_request_token=MOCK_DATA["client_request_token"],
        )

    @mock.patch.object(AthenaHook, "stop_spark_calculation")
    @mock.patch.object(AthenaHook, "get_spark_calculation_info")
    @mock.patch.object(AthenaHook, "poll_spark_calculation_status")
    @mock.patch.object(AthenaHook, "start_spark_calculation", return_value=ATHENA_CALCULATION_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_execute_deferrable(
        self,
        mock_conn,
        mock_start_spark_calculation,
        mock_poll_spark_calculation_status,
        mock_get_spark_calculation_info,
        mock_stop_spark_calculation,
    ):
        self.athena.deferrable = True
        self.athena.aws_conn_id = "aws-test-custom-conn"
        self.athena.region_name = "eu-west-1"
        self.athena.verify = False
        self.athena.botocore_config = {"read_timeout": 42}

        with pytest.raises(TaskDeferred) as exc:
            self.athena.execute({})

        mock_start_spark_calculation.assert_called_once_with(
            session_id=MOCK_DATA["session_id"],
            code_block=MOCK_DATA["code_block"],
            description=None,
            client_request_token=MOCK_DATA["client_request_token"],
        )
        mock_poll_spark_calculation_status.assert_not_called()
        mock_get_spark_calculation_info.assert_not_called()
        mock_stop_spark_calculation.assert_not_called()

        deferred = exc.value
        assert deferred.method_name == "execute_complete"
        assert deferred.kwargs == {
            "calculation_execution_id": ATHENA_CALCULATION_ID,
        }
        assert isinstance(deferred.trigger, AthenaSparkTrigger)
        assert deferred.trigger.serialize() == (
            "airflow.providers.amazon.aws.triggers.athena.AthenaSparkTrigger",
            {
                "calculation_execution_id": ATHENA_CALCULATION_ID,
                "waiter_delay": 0,
                "waiter_max_attempts": 3,
                "aws_conn_id": "aws-test-custom-conn",
                "region_name": "eu-west-1",
                "verify": False,
                "botocore_config": {"read_timeout": 42},
            },
        )

    @mock.patch.object(AthenaHook, "stop_spark_calculation")
    @mock.patch.object(AthenaHook, "poll_spark_calculation_status")
    @mock.patch.object(AthenaHook, "start_spark_calculation")
    @mock.patch.object(AthenaHook, "get_spark_calculation_state_change_reason", return_value=None)
    @mock.patch.object(AthenaHook, "get_spark_calculation_info")
    @mock.patch.object(AthenaHook, "get_conn")
    def test_execute_complete_success(
        self,
        mock_conn,
        mock_get_spark_calculation_info,
        mock_get_spark_calculation_state_change_reason,
        mock_start_spark_calculation,
        mock_poll_spark_calculation_status,
        mock_stop_spark_calculation,
    ):
        submission_time = timezone.datetime(2026, 1, 1)
        completion_time = timezone.datetime(2026, 1, 1, 0, 1)
        mock_get_spark_calculation_info.return_value = _calculation_info(
            "COMPLETED",
            submission_time=submission_time,
            completion_time=completion_time,
        )

        # A resumed operator starts without the ID assigned during execute().
        assert self.athena._calculation_execution_id is None

        result = self.athena.execute_complete(
            context={},
            calculation_execution_id=ATHENA_CALCULATION_ID,
            event={"status": "success", "value": ATHENA_CALCULATION_ID},
        )

        assert result == {
            "calculation_execution_id": ATHENA_CALCULATION_ID,
            "state": "COMPLETED",
            "state_change_reason": None,
            "submission_time": str(submission_time),
            "completion_time": str(completion_time),
            "session_id": MOCK_DATA["session_id"],
            "working_directory": "s3://test-bucket/spark/",
            "stdout_s3_uri": "s3://test-bucket/spark/stdout",
            "stderr_s3_uri": "s3://test-bucket/spark/stderr",
            "result_s3_uri": "s3://test-bucket/spark/results",
            "result_type": "application/vnd.aws.athena.v1+json",
        }
        assert self.athena._calculation_execution_id == ATHENA_CALCULATION_ID
        mock_get_spark_calculation_state_change_reason.assert_called_once_with(ATHENA_CALCULATION_ID)
        mock_start_spark_calculation.assert_not_called()
        mock_poll_spark_calculation_status.assert_not_called()
        mock_stop_spark_calculation.assert_not_called()

    @pytest.mark.parametrize("state", ["FAILED", "CANCELED"])
    @mock.patch.object(AthenaHook, "stop_spark_calculation")
    @mock.patch.object(
        AthenaHook,
        "get_spark_calculation_state_change_reason",
        return_value="Test reason",
    )
    @mock.patch.object(AthenaHook, "get_spark_calculation_info")
    @mock.patch.object(AthenaHook, "get_conn")
    def test_execute_complete_terminal_failure(
        self,
        mock_conn,
        mock_get_spark_calculation_info,
        mock_get_spark_calculation_state_change_reason,
        mock_stop_spark_calculation,
        state,
    ):
        mock_get_spark_calculation_info.return_value = _calculation_info(state)

        with pytest.raises(
            RuntimeError,
            match=(
                rf"Athena Spark calculation ended in {state}\. "
                rf"CalculationExecutionId: {ATHENA_CALCULATION_ID}\. "
                r"Reason: Test reason"
            ),
        ):
            self.athena.execute_complete(
                context={},
                calculation_execution_id=ATHENA_CALCULATION_ID,
                event={"status": "success", "value": ATHENA_CALCULATION_ID},
            )

        mock_get_spark_calculation_state_change_reason.assert_called_once_with(ATHENA_CALCULATION_ID)
        mock_stop_spark_calculation.assert_not_called()

    @pytest.mark.parametrize(
        ("event", "expected_message"),
        [
            pytest.param(
                {"status": "error", "message": "Waiter exhausted"},
                "Waiter exhausted",
                id="waiter-error",
            ),
            pytest.param(None, "event is None", id="missing-event"),
            pytest.param({}, "Unknown trigger error", id="missing-status"),
        ],
    )
    @mock.patch.object(AthenaHook, "get_spark_calculation_info")
    @mock.patch.object(AthenaHook, "stop_spark_calculation")
    @mock.patch.object(AthenaHook, "get_conn")
    def test_execute_complete_trigger_error(
        self,
        mock_conn,
        mock_stop_spark_calculation,
        mock_get_spark_calculation_info,
        event,
        expected_message,
    ):
        with pytest.raises(RuntimeError, match=expected_message):
            self.athena.execute_complete(
                context={},
                calculation_execution_id=ATHENA_CALCULATION_ID,
                event=event,
            )

        mock_stop_spark_calculation.assert_called_once_with(ATHENA_CALCULATION_ID)
        mock_get_spark_calculation_info.assert_not_called()

    @pytest.mark.parametrize("state", ["RUNNING", "UNKNOWN", None])
    @mock.patch.object(AthenaHook, "get_spark_calculation_state_change_reason")
    @mock.patch.object(AthenaHook, "stop_spark_calculation")
    @mock.patch.object(AthenaHook, "get_spark_calculation_info")
    @mock.patch.object(AthenaHook, "get_conn")
    def test_execute_complete_invalid_state(
        self,
        mock_conn,
        mock_get_spark_calculation_info,
        mock_stop_spark_calculation,
        mock_get_spark_calculation_state_change_reason,
        state,
    ):
        mock_get_spark_calculation_info.return_value = _calculation_info(state)

        with pytest.raises(RuntimeError, match="Expected a terminal state"):
            self.athena.execute_complete(
                context={},
                calculation_execution_id=ATHENA_CALCULATION_ID,
                event={"status": "success", "value": ATHENA_CALCULATION_ID},
            )

        mock_stop_spark_calculation.assert_called_once_with(ATHENA_CALCULATION_ID)
        mock_get_spark_calculation_state_change_reason.assert_not_called()

    @mock.patch.object(
        AthenaHook,
        "stop_spark_calculation",
        side_effect=RuntimeError("Stop failed"),
    )
    @mock.patch.object(AthenaHook, "get_conn")
    def test_execute_complete_stop_failure_preserves_trigger_error(
        self,
        mock_conn,
        mock_stop_spark_calculation,
    ):
        with pytest.raises(RuntimeError, match="Waiter exhausted"):
            self.athena.execute_complete(
                context={},
                calculation_execution_id=ATHENA_CALCULATION_ID,
                event={"status": "error", "message": "Waiter exhausted"},
            )

        mock_stop_spark_calculation.assert_called_once_with(ATHENA_CALCULATION_ID)

    @mock.patch.object(AthenaHook, "stop_spark_calculation")
    @mock.patch.object(AthenaHook, "get_conn")
    def test_on_kill_calls_stop_spark_calculation(self, mock_conn, mock_stop_spark_calculation):
        self.athena._calculation_execution_id = ATHENA_CALCULATION_ID

        self.athena.on_kill()

        mock_stop_spark_calculation.assert_called_once_with(ATHENA_CALCULATION_ID)

    @mock.patch.object(AthenaHook, "stop_spark_calculation")
    @mock.patch.object(AthenaHook, "get_conn")
    def test_on_kill_no_op_when_no_calculation_execution_id(self, mock_conn, mock_stop_spark_calculation):
        self.athena.on_kill()

        mock_stop_spark_calculation.assert_not_called()

    def test_template_fields(self):
        validate_template_fields(self.athena)
