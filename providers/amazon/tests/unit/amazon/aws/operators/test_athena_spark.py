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

from airflow.models import DAG
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.operators.athena_spark import AthenaSparkOperator

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
            poll_interval=0,
            max_attempts=3,
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
        assert self.athena.poll_interval == 0
        assert self.athena.max_attempts == 3
        assert self.athena._calculation_execution_id is None

    @mock.patch.object(AthenaHook, "get_spark_calculation_info")
    @mock.patch.object(AthenaHook, "get_spark_calculation_state_change_reason", return_value=None)
    @mock.patch.object(AthenaHook, "check_spark_calculation_status", side_effect=("COMPLETED",))
    @mock.patch.object(AthenaHook, "start_spark_calculation", return_value=ATHENA_CALCULATION_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_execute_success(
        self,
        mock_conn,
        mock_start_spark_calculation,
        mock_check_spark_calculation_status,
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

        assert mock_check_spark_calculation_status.call_count == 1
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
    @mock.patch.object(AthenaHook, "get_spark_calculation_state_change_reason", return_value=None)
    @mock.patch.object(
        AthenaHook,
        "check_spark_calculation_status",
        side_effect=("RUNNING", "COMPLETED"),
    )
    @mock.patch.object(AthenaHook, "start_spark_calculation", return_value=ATHENA_CALCULATION_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_execute_poll_then_success(
        self,
        mock_conn,
        mock_start_spark_calculation,
        mock_check_spark_calculation_status,
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
        assert mock_check_spark_calculation_status.call_count == 2
        assert result["state"] == "COMPLETED"

    @mock.patch.object(AthenaHook, "get_spark_calculation_info")
    @mock.patch.object(AthenaHook, "get_spark_calculation_state_change_reason", return_value="Job failed")
    @mock.patch.object(AthenaHook, "check_spark_calculation_status", return_value="FAILED")
    @mock.patch.object(AthenaHook, "start_spark_calculation", return_value=ATHENA_CALCULATION_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_execute_failure(
        self,
        mock_conn,
        mock_start_spark_calculation,
        mock_check_spark_calculation_status,
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
        assert mock_get_spark_calculation_state_change_reason.call_count == 1

    @mock.patch.object(AthenaHook, "get_spark_calculation_info")
    @mock.patch.object(AthenaHook, "get_spark_calculation_state_change_reason", return_value="Canceled")
    @mock.patch.object(AthenaHook, "check_spark_calculation_status", return_value="CANCELED")
    @mock.patch.object(AthenaHook, "start_spark_calculation", return_value=ATHENA_CALCULATION_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_execute_cancelled(
        self,
        mock_conn,
        mock_start_spark_calculation,
        mock_check_spark_calculation_status,
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

    @mock.patch.object(AthenaHook, "check_spark_calculation_status", return_value="RUNNING")
    @mock.patch.object(AthenaHook, "start_spark_calculation", return_value=ATHENA_CALCULATION_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_execute_timeout(
        self,
        mock_conn,
        mock_start_spark_calculation,
        mock_check_spark_calculation_status,
    ):
        with pytest.raises(RuntimeError):
            self.athena.execute({})

        mock_start_spark_calculation.assert_called_once_with(
            session_id=MOCK_DATA["session_id"],
            code_block=MOCK_DATA["code_block"],
            description=None,
            client_request_token=MOCK_DATA["client_request_token"],
        )
        assert mock_check_spark_calculation_status.call_count == self.athena.max_attempts

    @mock.patch.object(AthenaHook, "check_spark_calculation_status", return_value=None)
    @mock.patch.object(AthenaHook, "start_spark_calculation", return_value=ATHENA_CALCULATION_ID)
    @mock.patch.object(AthenaHook, "get_conn")
    def test_execute_malformed_status(
        self,
        mock_conn,
        mock_start_spark_calculation,
        mock_check_spark_calculation_status,
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
