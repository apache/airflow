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


class TestAthenaHook:
    def setup_method(self):
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
