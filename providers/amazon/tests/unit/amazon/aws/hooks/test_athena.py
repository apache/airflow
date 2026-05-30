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
"""
Unit tests for AthenaHook with boto3 client mocked.

Test strategy:
- Mock the boto3 client via AthenaHook.get_conn() so no real AWS calls are made.
- Cover success, failure (exceptions), and bad/edge-case input for each hook method.
- Use botocore.exceptions.ClientError for API failure scenarios.
"""

from __future__ import annotations

from unittest import mock

import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.athena import (
    MULTI_LINE_QUERY_LOG_PREFIX,
    AthenaHook,
    query_params_to_string,
)

MULTILINE_QUERY = """
SELECT * FROM TEST_TABLE
WHERE Success='True';"""

MOCK_DATA = {
    "query": "SELECT * FROM TEST_TABLE",
    "database": "TEST_DATABASE",
    "output_location": "s3://test_s3_bucket/",
    "client_request_token": "eac427d0-1c6d-4dfb-96aa-2835d3ac6595",
    "workgroup": "primary",
    "query_execution_id": "eac427d0-1c6d-4dfb-96aa-2835d3ac6595",
    "next_token_id": "eac427d0-1c6d-4dfb-96aa-2835d3ac6595",
    "max_items": 1000,
    "code_block": "print('hello spark')",
    "calculation_execution_id": "calc-123456",
    "session_id": "session-123456",
    "description": "spark-calc",
}

mock_query_context = {"Database": MOCK_DATA["database"]}
mock_result_configuration = {"OutputLocation": MOCK_DATA["output_location"]}

MOCK_RUNNING_QUERY_EXECUTION = {"QueryExecution": {"Status": {"State": "RUNNING"}}}
MOCK_SUCCEEDED_QUERY_EXECUTION = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

MOCK_QUERY_EXECUTION = {"QueryExecutionId": MOCK_DATA["query_execution_id"]}

MOCK_QUERY_EXECUTION_OUTPUT = {
    "QueryExecution": {
        "QueryExecutionId": MOCK_DATA["query_execution_id"],
        "ResultConfiguration": {"OutputLocation": "s3://test_bucket/test.csv"},
        "Status": {"StateChangeReason": "Terminated by user."},
    }
}
MOCK_CALCULATION_EXECUTION = {"CalculationExecutionId": MOCK_DATA["calculation_execution_id"]}

MOCK_RUNNING_CALC_EXECUTION = {"Status": {"State": "RUNNING"}}
MOCK_SUCCEEDED_CALC_EXECUTION = {"Status": {"State": "COMPLETED"}}


@mock_aws
class TestAthenaHook:
    def setup_method(self, _):
        self.athena = AthenaHook()

    def test_init(self):
        assert self.athena.aws_conn_id == "aws_default"

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_query_without_token(self, mock_conn):
        mock_conn.return_value.start_query_execution.return_value = MOCK_QUERY_EXECUTION
        result = self.athena.run_query(
            query=MOCK_DATA["query"],
            query_context=mock_query_context,
            result_configuration=mock_result_configuration,
        )
        expected_call_params = {
            "QueryString": MOCK_DATA["query"],
            "QueryExecutionContext": mock_query_context,
            "ResultConfiguration": mock_result_configuration,
            "WorkGroup": MOCK_DATA["workgroup"],
        }
        mock_conn.return_value.start_query_execution.assert_called_with(**expected_call_params)
        assert result == MOCK_DATA["query_execution_id"]

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_query_with_token(self, mock_conn):
        mock_conn.return_value.start_query_execution.return_value = MOCK_QUERY_EXECUTION
        result = self.athena.run_query(
            query=MOCK_DATA["query"],
            query_context=mock_query_context,
            result_configuration=mock_result_configuration,
            client_request_token=MOCK_DATA["client_request_token"],
        )
        expected_call_params = {
            "QueryString": MOCK_DATA["query"],
            "QueryExecutionContext": mock_query_context,
            "ResultConfiguration": mock_result_configuration,
            "ClientRequestToken": MOCK_DATA["client_request_token"],
            "WorkGroup": MOCK_DATA["workgroup"],
        }
        mock_conn.return_value.start_query_execution.assert_called_with(**expected_call_params)
        assert result == MOCK_DATA["query_execution_id"]

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_query_boto3_failure(self, mock_conn):
        """Failure case: boto3 start_query_execution raises ClientError."""
        mock_conn.return_value.start_query_execution.side_effect = ClientError(
            error_response={"Error": {"Code": "InvalidRequestException", "Message": "Invalid query"}},
            operation_name="start_query_execution",
        )
        with pytest.raises(ClientError) as exc_info:
            self.athena.run_query(
                query=MOCK_DATA["query"],
                query_context=mock_query_context,
                result_configuration=mock_result_configuration,
            )
        assert exc_info.value.response["Error"]["Code"] == "InvalidRequestException"

    @mock.patch.object(AthenaHook, "log")
    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_query_log_query(self, mock_conn, log):
        self.athena.run_query(
            query=MOCK_DATA["query"],
            query_context=mock_query_context,
            result_configuration=mock_result_configuration,
        )
        assert self.athena.log.info.call_count == 2

    @mock.patch.object(AthenaHook, "log")
    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_run_query_no_log_query(self, mock_conn, log):
        athena_hook_no_log_query = AthenaHook(log_query=False)
        athena_hook_no_log_query.run_query(
            query=MOCK_DATA["query"],
            query_context=mock_query_context,
            result_configuration=mock_result_configuration,
        )
        assert athena_hook_no_log_query.log.info.call_count == 1

    @mock.patch("airflow.providers.amazon.aws.hooks.athena.send_sql_hook_lineage")
    @mock.patch.object(AthenaHook, "get_conn")
    def test_run_query_hook_lineage(self, mock_conn, mock_send_lineage):
        mock_conn.return_value.start_query_execution.return_value = MOCK_QUERY_EXECUTION
        self.athena.run_query(
            query=MOCK_DATA["query"],
            query_context=mock_query_context,
            result_configuration=mock_result_configuration,
        )
        mock_send_lineage.assert_called_once()
        call_kw = mock_send_lineage.call_args.kwargs
        assert call_kw["context"] is self.athena
        assert call_kw["sql"] == MOCK_DATA["query"]
        assert call_kw["job_id"] == MOCK_DATA["query_execution_id"]

    # new test cases
    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_start_calculation_default_params(self, mock_conn):
        mock_conn.return_value.start_calculation_execution.return_value = MOCK_CALCULATION_EXECUTION

        result = self.athena.start_calculation(
            session_id=MOCK_DATA["session_id"],
            code_block=MOCK_DATA["code_block"],
        )

        expected_call_params = {
            "SessionId": MOCK_DATA["session_id"],
            "CodeBlock": MOCK_DATA["code_block"],
        }
        mock_conn.return_value.start_calculation_execution.assert_called_with(**expected_call_params)
        assert result == MOCK_DATA["calculation_execution_id"]

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_start_calculation_with_optional_params(self, mock_conn):
        mock_conn.return_value.start_calculation_execution.return_value = MOCK_CALCULATION_EXECUTION

        calculation_configuration = {"CodeBlock": MOCK_DATA["code_block"]}

        result = self.athena.start_calculation(
            session_id=MOCK_DATA["session_id"],
            code_block=MOCK_DATA["code_block"],
            description=MOCK_DATA["description"],
            calculation_configuration=calculation_configuration,
            client_request_token=MOCK_DATA["client_request_token"],
        )

        expected_call_params = {
            "SessionId": MOCK_DATA["session_id"],
            "CodeBlock": MOCK_DATA["code_block"],
            "Description": MOCK_DATA["description"],
            "CalculationConfiguration": calculation_configuration,
            "ClientRequestToken": MOCK_DATA["client_request_token"],
        }
        mock_conn.return_value.start_calculation_execution.assert_called_with(**expected_call_params)
        assert result == MOCK_DATA["calculation_execution_id"]

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_calculation_info(self, mock_conn):
        mock_conn.return_value.get_calculation_execution.return_value = MOCK_SUCCEEDED_CALC_EXECUTION

        result = self.athena.get_calculation_info(
            calculation_execution_id=MOCK_DATA["calculation_execution_id"]
        )

        mock_conn.return_value.get_calculation_execution.assert_called_once_with(
            CalculationExecutionId=MOCK_DATA["calculation_execution_id"]
        )
        assert result == MOCK_SUCCEEDED_CALC_EXECUTION

    @mock.patch.object(AthenaHook, "get_conn")
    def test_check_calculation_status_normal(self, mock_conn):
        mock_conn.return_value.get_calculation_execution.return_value = MOCK_RUNNING_CALC_EXECUTION

        state = self.athena.check_calculation_status(
            calculation_execution_id=MOCK_DATA["calculation_execution_id"]
        )

        assert state == "RUNNING"

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_stop_calculation(self, mock_conn):
        self.athena.stop_calculation(calculation_execution_id=MOCK_DATA["calculation_execution_id"])

        mock_conn.return_value.stop_calculation_execution.assert_called_once_with(
            CalculationExecutionId=MOCK_DATA["calculation_execution_id"]
        )

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_query_results_with_non_succeeded_query(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_RUNNING_QUERY_EXECUTION
        result = self.athena.get_query_results(query_execution_id=MOCK_DATA["query_execution_id"])
        assert result is None

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_query_results_with_default_params(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_SUCCEEDED_QUERY_EXECUTION
        self.athena.get_query_results(query_execution_id=MOCK_DATA["query_execution_id"])
        expected_call_params = {"QueryExecutionId": MOCK_DATA["query_execution_id"], "MaxResults": 1000}
        mock_conn.return_value.get_query_results.assert_called_with(**expected_call_params)

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_query_results_with_next_token(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_SUCCEEDED_QUERY_EXECUTION
        self.athena.get_query_results(
            query_execution_id=MOCK_DATA["query_execution_id"], next_token_id=MOCK_DATA["next_token_id"]
        )
        expected_call_params = {
            "QueryExecutionId": MOCK_DATA["query_execution_id"],
            "NextToken": MOCK_DATA["next_token_id"],
            "MaxResults": 1000,
        }
        mock_conn.return_value.get_query_results.assert_called_with(**expected_call_params)

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_query_results_invalid_state_returns_none(self, mock_conn):
        """Edge case: get_query_execution returns malformed response; check_query_status returns None."""
        mock_conn.return_value.get_query_execution.return_value = {"QueryExecution": {}}
        result = self.athena.get_query_results(query_execution_id=MOCK_DATA["query_execution_id"])
        assert result is None
        mock_conn.return_value.get_query_results.assert_not_called()

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_query_info_boto3_failure(self, mock_conn):
        """Failure case: boto3 get_query_execution raises ClientError."""
        mock_conn.return_value.get_query_execution.side_effect = ClientError(
            error_response={"Error": {"Code": "InvalidRequestException"}},
            operation_name="get_query_execution",
        )
        with pytest.raises(ClientError):
            self.athena.get_query_info(query_execution_id=MOCK_DATA["query_execution_id"])

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_paginator_with_non_succeeded_query(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_RUNNING_QUERY_EXECUTION
        result = self.athena.get_query_results_paginator(query_execution_id=MOCK_DATA["query_execution_id"])
        assert result is None

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_paginator_with_default_params(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_SUCCEEDED_QUERY_EXECUTION
        self.athena.get_query_results_paginator(query_execution_id=MOCK_DATA["query_execution_id"])
        expected_call_params = {
            "QueryExecutionId": MOCK_DATA["query_execution_id"],
            "PaginationConfig": {"MaxItems": None, "PageSize": None, "StartingToken": None},
        }
        mock_conn.return_value.get_paginator.return_value.paginate.assert_called_with(**expected_call_params)

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_paginator_invalid_state_returns_none(self, mock_conn):
        """Edge case: malformed response leads to None state; paginator not created."""
        mock_conn.return_value.get_query_execution.return_value = {"QueryExecution": {}}
        result = self.athena.get_query_results_paginator(query_execution_id=MOCK_DATA["query_execution_id"])
        assert result is None
        mock_conn.return_value.get_paginator.assert_not_called()

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_paginator_with_pagination_config(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_SUCCEEDED_QUERY_EXECUTION
        self.athena.get_query_results_paginator(
            query_execution_id=MOCK_DATA["query_execution_id"],
            max_items=MOCK_DATA["max_items"],
            page_size=MOCK_DATA["max_items"],
            starting_token=MOCK_DATA["next_token_id"],
        )
        expected_call_params = {
            "QueryExecutionId": MOCK_DATA["query_execution_id"],
            "PaginationConfig": {
                "MaxItems": MOCK_DATA["max_items"],
                "PageSize": MOCK_DATA["max_items"],
                "StartingToken": MOCK_DATA["next_token_id"],
            },
        }
        mock_conn.return_value.get_paginator.return_value.paginate.assert_called_with(**expected_call_params)

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_poll_query_when_final(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_SUCCEEDED_QUERY_EXECUTION
        result = self.athena.poll_query_status(
            query_execution_id=MOCK_DATA["query_execution_id"], sleep_time=0
        )
        mock_conn.return_value.get_query_execution.assert_called_once()
        assert result == "SUCCEEDED"

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_poll_query_with_timeout(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_RUNNING_QUERY_EXECUTION
        result = self.athena.poll_query_status(
            query_execution_id=MOCK_DATA["query_execution_id"], max_polling_attempts=1, sleep_time=0
        )
        mock_conn.return_value.get_query_execution.assert_called_once()
        assert result == "RUNNING"

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_poll_query_with_exception(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_QUERY_EXECUTION_OUTPUT
        result = self.athena.poll_query_status(
            query_execution_id=MOCK_DATA["query_execution_id"], max_polling_attempts=1, sleep_time=0
        )
        mock_conn.return_value.get_query_execution.assert_called_once()
        assert not result

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_output_location(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_QUERY_EXECUTION_OUTPUT
        result = self.athena.get_output_location(query_execution_id=MOCK_DATA["query_execution_id"])
        assert result == "s3://test_bucket/test.csv"

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_stop_query_success(self, mock_conn):
        """Success case: stop_query_execution returns normally."""
        mock_conn.return_value.stop_query_execution.return_value = {}
        result = self.athena.stop_query(query_execution_id=MOCK_DATA["query_execution_id"])
        mock_conn.return_value.stop_query_execution.assert_called_once_with(
            QueryExecutionId=MOCK_DATA["query_execution_id"]
        )
        assert result == {}

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_stop_query_boto3_failure(self, mock_conn):
        """Failure case: boto3 stop_query_execution raises ClientError."""
        mock_conn.return_value.stop_query_execution.side_effect = ClientError(
            error_response={
                "Error": {"Code": "InvalidRequestException", "Message": "Query already finished"}
            },
            operation_name="stop_query_execution",
        )
        with pytest.raises(ClientError) as exc_info:
            self.athena.stop_query(query_execution_id=MOCK_DATA["query_execution_id"])
        assert exc_info.value.response["Error"]["Code"] == "InvalidRequestException"

    @pytest.mark.parametrize(
        "query_execution_id", [pytest.param("", id="empty-string"), pytest.param(None, id="none")]
    )
    def test_hook_get_output_location_empty_execution_id(self, query_execution_id):
        with pytest.raises(ValueError, match="Invalid Query execution id"):
            self.athena.get_output_location(query_execution_id=query_execution_id)

    @pytest.mark.parametrize("response", [pytest.param({}, id="empty-dict"), pytest.param(None, id="none")])
    def test_hook_get_output_location_no_response(self, response):
        with mock.patch.object(AthenaHook, "get_query_info", return_value=response) as m:
            with pytest.raises(ValueError, match="Unable to get query information"):
                self.athena.get_output_location(query_execution_id="PLACEHOLDER")
            m.assert_called_once_with(query_execution_id="PLACEHOLDER", use_cache=True)

    def test_hook_get_output_location_invalid_response(self, caplog):
        with mock.patch.object(AthenaHook, "get_query_info") as m:
            m.return_value = {"foo": "bar"}
            caplog.clear()
            caplog.set_level("ERROR")
            with pytest.raises(KeyError):
                self.athena.get_output_location(query_execution_id="PLACEHOLDER")
            assert "Error retrieving OutputLocation" in caplog.text

    @mock.patch.object(AthenaHook, "get_query_info")
    def test_check_query_status_normal(self, mock_get_query_info):
        mock_get_query_info.return_value = MOCK_SUCCEEDED_QUERY_EXECUTION
        state = self.athena.check_query_status(query_execution_id=MOCK_DATA["query_execution_id"])
        assert state == "SUCCEEDED"

    @mock.patch.object(AthenaHook, "get_query_info")
    def test_check_query_status_exception(self, mock_get_query_info):
        mock_get_query_info.return_value = MOCK_QUERY_EXECUTION_OUTPUT
        state = self.athena.check_query_status(query_execution_id=MOCK_DATA["query_execution_id"])
        assert not state

    @mock.patch.object(AthenaHook, "get_query_info")
    def test_get_state_change_reason_missing_key_returns_none(self, mock_get_query_info):
        """Edge case: response has no StateChangeReason; hook returns None and logs."""
        mock_get_query_info.return_value = {"QueryExecution": {"Status": {}}}
        result = self.athena.get_state_change_reason(query_execution_id=MOCK_DATA["query_execution_id"])
        assert result is None

    @mock.patch.object(AthenaHook, "get_conn")
    def test_hook_get_query_info_caching(self, mock_conn):
        mock_conn.return_value.get_query_execution.return_value = MOCK_QUERY_EXECUTION_OUTPUT
        self.athena.get_state_change_reason(query_execution_id=MOCK_DATA["query_execution_id"])
        assert not self.athena._AthenaHook__query_results
        # get_output_location uses cache
        self.athena.get_output_location(query_execution_id=MOCK_DATA["query_execution_id"])
        assert MOCK_DATA["query_execution_id"] in self.athena._AthenaHook__query_results
        mock_conn.return_value.get_query_execution.assert_called_with(
            QueryExecutionId=MOCK_DATA["query_execution_id"]
        )
        self.athena.get_state_change_reason(
            query_execution_id=MOCK_DATA["query_execution_id"], use_cache=False
        )
        mock_conn.return_value.get_query_execution.assert_called_with(
            QueryExecutionId=MOCK_DATA["query_execution_id"]
        )

    def test_single_line_query_log_formatting(self):
        params = {
            "QueryString": MOCK_DATA["query"],
            "QueryExecutionContext": {"Database": MOCK_DATA["database"]},
            "ResultConfiguration": {"OutputLocation": MOCK_DATA["output_location"]},
            "WorkGroup": MOCK_DATA["workgroup"],
        }

        result = query_params_to_string(params)

        assert isinstance(result, str)
        assert result.count("\n") == len(params.keys())

    def test_multi_line_query_log_formatting(self):
        params = {
            "QueryString": MULTILINE_QUERY,
            "QueryExecutionContext": {"Database": MOCK_DATA["database"]},
            "ResultConfiguration": {"OutputLocation": MOCK_DATA["output_location"]},
            "WorkGroup": MOCK_DATA["workgroup"],
        }
        num_query_lines = MULTILINE_QUERY.count("\n")

        result = query_params_to_string(params)

        assert isinstance(result, str)
        assert result.count("\n") == len(params.keys()) + num_query_lines
        # All lines except the first line of the multiline query log message get the double prefix/indent.
        assert result.count(MULTI_LINE_QUERY_LOG_PREFIX) == (num_query_lines * 2) - 1
